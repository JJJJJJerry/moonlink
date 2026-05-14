use crate::storage::index::{FileIndex as MooncakeFileIndex, MooncakeIndex};
use crate::storage::io_utils;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::mooncake_table::DiskFileEntry;
use crate::storage::mooncake_table::Snapshot as MooncakeSnapshot;
use crate::storage::storage_utils::{create_data_file, FileId, TableId, TableUniqueFileId};
use crate::storage::table::iceberg::deletion_vector::DeletionVector;
use crate::storage::table::iceberg::iceberg_table_manager::*;
use crate::storage::table::iceberg::index::FileIndexBlob;
use crate::storage::table::iceberg::marker::{Marker, MoonlinkMarkerDir};
use crate::storage::table::iceberg::private_manifest::{
    validate_deployment, DeploymentStatus, PrivateManifestStore, MOONCAKE_NESTED_PRIVATE_ROOT_ACK,
};
use crate::storage::table::iceberg::puffin_utils;
use crate::storage::table::iceberg::puffin_utils::PuffinBlobRef;
#[cfg(any(test, debug_assertions))]
use crate::storage::table::iceberg::schema_utils;
use crate::storage::table::iceberg::snapshot_utils;
use crate::storage::table::iceberg::table_property;
use crate::storage::table::iceberg::utils;
use crate::storage::table::iceberg::validation as IcebergValidation;
use crate::Result;

use std::collections::{HashMap, HashSet};
use std::vec;

use iceberg::io::FileIO;
use iceberg::spec::{DataFileFormat, ManifestEntry};
use iceberg::Error as IcebergError;
use iceberg::Result as IcebergResult;
use uuid::Uuid;

impl IcebergTableManager {
    /// Validate schema consistency at load operation.
    fn validate_schema_consistency_at_load(&self) {
        // Validate is expensive, only enable at tests.
        #[cfg(any(test, debug_assertions))]
        {
            // Assert table schema matches iceberg table metadata.
            schema_utils::assert_table_schema_consistent(
                self.iceberg_table.as_ref().unwrap(),
                &self.mooncake_table_metadata,
            );
        }
    }

    /// Load index file into table manager from the current manifest entry.
    async fn load_file_index_from_manifest_entry(
        &mut self,
        entry: &ManifestEntry,
        file_io: &FileIO,
        next_file_id: &mut u64,
    ) -> IcebergResult<Option<MooncakeFileIndex>> {
        if !utils::is_file_index(entry) {
            return Ok(None);
        }

        // Load mooncake file indices from iceberg file index blobs.
        let file_index_blob =
            FileIndexBlob::load_from_index_blob(file_io.clone(), entry.data_file()).await?;
        let mut cur_iceberg_file_index = file_index_blob.file_index;
        let table_id = TableId(self.mooncake_table_metadata.table_id);
        let mooncake_file_index = cur_iceberg_file_index
            .as_mooncake_file_index(
                &self.remote_data_file_to_file_id,
                self.object_storage_cache.clone(),
                self.filesystem_accessor.as_ref(),
                table_id,
                next_file_id,
            )
            .await?;

        self.persisted_file_indices.insert(
            mooncake_file_index.clone(),
            entry.data_file().file_path().to_string(),
        );

        Ok(Some(mooncake_file_index))
    }

    /// Verify the per-table private-root deployment shape on load.
    ///
    /// Run once per recovery / table-open. Reads the `mooncake.private_index_root`
    /// + `mooncake.nested_private_root_ack` Iceberg properties (when the table
    /// was created with private-root binding), classifies the deployment, logs
    /// the outcome, and rejects nested deployments that lack an acknowledgement.
    pub(super) fn validate_private_root_deployment(&self) -> Result<()> {
        let iceberg_table = self.iceberg_table.as_ref().unwrap();
        let metadata = iceberg_table.metadata();
        let Some(private_root) = table_property::get_bound_private_index_root(
            metadata.properties(),
            self.config.private_index_root.as_deref(),
        ) else {
            return Ok(());
        };
        let iceberg_root = iceberg_table.identifier().to_string();
        let ack = metadata
            .properties()
            .get(MOONCAKE_NESTED_PRIVATE_ROOT_ACK)
            .map(String::as_str);
        // `metadata.location()` is the only root that downstream Iceberg cleanup
        // (RemoveOrphanFiles, lifecycle policies, third-party governance) actually
        // walks, so it is the only one that defines the threat surface. The convention
        // path `warehouse/<ns>/<table>` may differ for custom-location / externally
        // registered tables; we deliberately ignore it here to avoid false positives.
        let physical_root = metadata.location();
        match validate_deployment(physical_root, private_root, ack) {
            Ok(DeploymentStatus::Compliant) => {
                tracing::info!(
                    iceberg_root = %physical_root,
                    private_root = %private_root,
                    iceberg_table = %iceberg_root,
                    "mooncake private root validated (external-root deployment)"
                );
                Ok(())
            }
            Ok(DeploymentStatus::NestedWithAck { ack_id }) => {
                tracing::warn!(
                    iceberg_root = %physical_root,
                    private_root = %private_root,
                    iceberg_table = %iceberg_root,
                    ack_id = %ack_id,
                    "mooncake private root nested inside Iceberg table root; relying on operator-configured storage-level guards"
                );
                Ok(())
            }
            Err(e) => Err(crate::Error::IcebergError(
                moonlink_error::ErrorStruct::new(
                    format!("mooncake private root deployment rejected: {e}"),
                    moonlink_error::ErrorStatus::Permanent,
                ),
            )),
        }
    }

    /// Reconcile the per-table marker directory + private manifest set against
    /// the live Iceberg snapshot set. No-op when no private root is configured.
    ///
    /// Two passes, executed in order so the "this commit clearly failed"
    /// branch runs before the more delicate "this commit succeeded but its
    /// private write didn't land" check:
    ///
    /// * [`Self::reap_orphan_marker_dirs`] — marker-side reconciliation. Reaps
    ///   marker dirs whose snapshot is no longer live and the staging
    ///   artifacts they witness.
    /// * [`Self::report_missing_current_manifest`] — snapshot-side
    ///   reconciliation. Observability only at this layer: reports the
    ///   current snapshot if its private manifest is missing. Durable
    ///   quarantine + rebuild from parent is a future enhancement.
    async fn reconcile_marker_dir_on_load(&self) -> Result<()> {
        let metadata = self.iceberg_table.as_ref().unwrap().metadata();
        let Some(private_root) = table_property::get_bound_private_index_root(
            metadata.properties(),
            self.config.private_index_root.as_deref(),
        ) else {
            return Ok(());
        };
        let table_uuid = metadata.uuid();
        let live: HashSet<i64> = metadata.snapshots().map(|s| s.snapshot_id()).collect();

        let marker_dir = MoonlinkMarkerDir::new(
            self.filesystem_accessor.clone(),
            private_root.to_string(),
            table_uuid,
        );
        let store = PrivateManifestStore::new(
            self.filesystem_accessor.clone(),
            private_root.to_string(),
            table_uuid,
        );

        let private_prefix = DirectoryPrefix::new(format!(
            "{}/{}",
            private_root.trim_end_matches('/'),
            table_uuid
        ));
        let table_root_prefix = DirectoryPrefix::new(metadata.location());
        let sandbox = MarkerSandbox {
            private_prefix,
            table_data_prefix: table_root_prefix.join_subdir(TABLE_DATA_SUBDIR),
        };

        self.reap_orphan_marker_dirs(&marker_dir, &store, &live, &sandbox, table_uuid)
            .await?;
        if let Some(current) = metadata.current_snapshot() {
            self.report_missing_current_manifest(&store, current.snapshot_id(), table_uuid)
                .await;
        }
        Ok(())
    }

    /// Marker-side reconciliation. For each `.markers/<snap_id>/` whose
    /// `snap_id ∉ live`, classify and dispatch the witnessed artifacts, then
    /// roll back the dir only when every artifact ended in a handled state.
    /// Any delete failure or untrusted payload retains the dir so the next
    /// boot keeps the recovery evidence.
    async fn reap_orphan_marker_dirs(
        &self,
        marker_dir: &MoonlinkMarkerDir,
        store: &PrivateManifestStore,
        live: &HashSet<i64>,
        sandbox: &MarkerSandbox,
        table_uuid: Uuid,
    ) -> Result<()> {
        let hanging = marker_dir.scan_hanging(live).await?;
        if hanging.is_empty() {
            return Ok(());
        }

        // The syncer inherits puffin entries from the parent's private manifest
        // (see `iceberg_table_syncer::sync_snapshot_impl`), so an expired
        // hanging snapshot's marker can name a puffin that some other live
        // snapshot's private manifest still depends on. Walk every live
        // snapshot's manifest to build the union of in-use puffin paths.
        // `reliable = false` means at least one read failed — in that case
        // table-root deletes fail-closed (conservatively retained for retry)
        // because the in-use set is no longer a sound witness.
        let in_use = read_in_use_puffin_paths(store, live).await;

        for h in &hanging {
            let markers = match marker_dir.read_markers(h.snapshot_id).await {
                Ok(ms) => ms,
                Err(e) => {
                    tracing::warn!(
                        snapshot_id = h.snapshot_id,
                        table_uuid = %table_uuid,
                        error = %e,
                        "boot reconciliation: failed to read markers; leaving marker dir for retention sweeper",
                    );
                    continue;
                }
            };

            let outcome = self
                .apply_orphan_marker_policy(h.snapshot_id, &markers, &in_use, sandbox, table_uuid)
                .await;

            if outcome.should_retain_marker_dir() {
                tracing::warn!(
                    snapshot_id = h.snapshot_id,
                    table_uuid = %table_uuid,
                    deleted = outcome.deleted,
                    skipped_referenced = outcome.skipped_referenced,
                    skipped_unreliable = outcome.skipped_unreliable,
                    quarantined = outcome.quarantined,
                    failed = outcome.failed,
                    "boot reconciliation: retaining hanging marker dir for retry",
                );
                continue;
            }
            match marker_dir.rollback(h.snapshot_id).await {
                Ok(()) => tracing::info!(
                    snapshot_id = h.snapshot_id,
                    table_uuid = %table_uuid,
                    deleted = outcome.deleted,
                    skipped_referenced = outcome.skipped_referenced,
                    "boot reconciliation: purged hanging marker dir",
                ),
                Err(e) => tracing::warn!(
                    snapshot_id = h.snapshot_id,
                    table_uuid = %table_uuid,
                    error = %e,
                    "boot reconciliation: failed to remove hanging marker dir; will retry on next boot",
                ),
            }
        }
        Ok(())
    }

    /// Per-snapshot artifact disposition. Each marker is classified by
    /// [`classify_marker_target`] (with the marker's own snapshot id, so
    /// a marker can only authorize deletion of its **own** private manifest,
    /// not a live snapshot's) and routed:
    ///
    /// * `Trusted(PrivateManifest)` — `manifest/snap-<marker.snapshot_id>.json`
    ///   under the table's private root. Per-snapshot, never inherited; safe
    ///   to delete once the snapshot is orphaned.
    /// * `Trusted(HashPuffin)` — a puffin in the table's data root. Cross-
    ///   checked against the in-use set; **fail-closed** when the in-use set
    ///   is unreliable (a missing/unreadable live manifest means the set may
    ///   under-count references — deleting could break parent inheritance).
    /// * `Quarantined(_)` — anything else: path outside owned roots, `..`
    ///   segment, table-root path that is not a hash-puffin file, or a
    ///   private-root path naming some **other** snapshot's manifest. The
    ///   marker is never dereferenced as a delete target.
    async fn apply_orphan_marker_policy(
        &self,
        snapshot_id: i64,
        markers: &[Marker],
        in_use: &InUsePuffinSet,
        sandbox: &MarkerSandbox,
        table_uuid: Uuid,
    ) -> ReapOutcome {
        let mut outcome = ReapOutcome::default();
        for m in markers {
            match classify_marker_target(&m.file_path, m.snapshot_id, sandbox) {
                MarkerTargetClassification::Quarantined(reason) => {
                    tracing::warn!(
                        snapshot_id,
                        marker_snapshot_id = m.snapshot_id,
                        table_uuid = %table_uuid,
                        file_path = %m.file_path,
                        reason = reason.as_str(),
                        "boot reconciliation: untrusted marker payload; quarantining",
                    );
                    outcome.quarantined += 1;
                }
                MarkerTargetClassification::Trusted(MarkerArtifactKind::HashPuffin)
                    if !in_use.reliable =>
                {
                    // Fail-closed: the in-use witness is incomplete; we cannot
                    // tell whether this puffin is still referenced by parent
                    // inheritance. Retain the marker so the next boot retries
                    // when the live manifests may be readable again.
                    outcome.skipped_unreliable += 1;
                }
                MarkerTargetClassification::Trusted(MarkerArtifactKind::HashPuffin)
                    if in_use.paths.contains(&m.file_path) =>
                {
                    outcome.skipped_referenced += 1;
                }
                MarkerTargetClassification::Trusted(_) => {
                    // opendal-backed accessors are idempotent on missing
                    // objects (NotFound → Ok), so a genuine straggler from a
                    // failed commit lands here as Ok with no filesystem
                    // effect. Any Err is a transient/permission error worth
                    // retrying — retain the marker dir.
                    match self.filesystem_accessor.delete_object(&m.file_path).await {
                        Ok(()) => outcome.deleted += 1,
                        Err(e) => {
                            tracing::warn!(
                                snapshot_id,
                                table_uuid = %table_uuid,
                                file_path = %m.file_path,
                                error = %e,
                                "boot reconciliation: failed to delete hanging artifact",
                            );
                            outcome.failed += 1;
                        }
                    }
                }
            }
        }
        outcome
    }

    /// Snapshot-side reconciliation. Detection only at this layer; a missing
    /// private manifest leaves hash indices empty but does not block boot.
    /// A future enhancement may rebuild the manifest from the parent snapshot.
    async fn report_missing_current_manifest(
        &self,
        store: &PrivateManifestStore,
        current_snapshot_id: i64,
        table_uuid: Uuid,
    ) {
        let persisted: HashSet<i64> = match store.list_snapshot_ids().await {
            Ok(ids) => ids.into_iter().collect(),
            Err(e) => {
                tracing::warn!(
                    table_uuid = %table_uuid,
                    error = %e,
                    "boot reconciliation: failed to list persisted private manifests; skipping current-snapshot check",
                );
                return;
            }
        };
        if !persisted.contains(&current_snapshot_id) {
            tracing::warn!(
                snapshot_id = current_snapshot_id,
                table_uuid = %table_uuid,
                "boot reconciliation: current Iceberg snapshot has no private manifest; hash indices will load empty until the manifest is rebuilt",
            );
        }
    }

    /// Rebuild hash file indices from the per-table private manifest.
    ///
    /// Returns an empty vec when the table has no private root configured, or
    /// when no manifest exists for the current Iceberg snapshot (freshly
    /// restored private root, expired snapshot, or a snapshot that produced no
    /// hash blobs). The caller may treat the index as degraded and trigger an
    /// async rebuild from data files (cost-gated fallback in the meantime).
    async fn load_file_indices_from_private_manifest(
        &mut self,
        file_io: &FileIO,
        next_file_id: &mut u64,
    ) -> IcebergResult<Vec<MooncakeFileIndex>> {
        let metadata = self.iceberg_table.as_ref().unwrap().metadata();
        let Some(private_root) = table_property::get_bound_private_index_root(
            metadata.properties(),
            self.config.private_index_root.as_deref(),
        ) else {
            return Ok(Vec::new());
        };
        let Some(snapshot) = metadata.current_snapshot() else {
            return Ok(Vec::new());
        };
        let table_uuid = metadata.uuid();
        let store = PrivateManifestStore::new(
            self.filesystem_accessor.clone(),
            private_root.to_string(),
            table_uuid,
        );
        // Surface the IO error as an iceberg error so the caller's error path stays uniform.
        // Permanent moonlink errors (schema/UUID mismatch surfaced by read_snap_manifest)
        // become DataInvalid — that's the iceberg signal for "retrying will not help";
        // everything else stays Unexpected so transient IO can still be retried upstream.
        let manifest = store
            .read_snap_manifest(snapshot.snapshot_id())
            .await
            .map_err(|e| {
                let kind = match e.get_status() {
                    moonlink_error::ErrorStatus::Permanent => iceberg::ErrorKind::DataInvalid,
                    _ => iceberg::ErrorKind::Unexpected,
                };
                IcebergError::new(
                    kind,
                    format!(
                        "read private manifest for snapshot {}",
                        snapshot.snapshot_id()
                    ),
                )
                .with_source(e)
            })?;
        let Some(manifest) = manifest else {
            return Ok(Vec::new());
        };

        let table_id = TableId(self.mooncake_table_metadata.table_id);
        let mut out = Vec::with_capacity(manifest.hash_index_entries.len());
        for entry in manifest.hash_index_entries.iter() {
            // PrivateManifestStore stores one blob per puffin file (the same invariant
            // load_blob_from_puffin_file enforces); offset/size are kept for future
            // multi-blob layouts and as a cross-check, not used for byte-range loads
            // until the puffin reader supports it.
            let blob =
                puffin_utils::load_blob_from_puffin_file(file_io.clone(), &entry.puffin_file_path)
                    .await?;
            let file_index_blob = FileIndexBlob::from_blob(blob)?;
            let mut iceberg_file_index = file_index_blob.file_index;
            let mooncake_file_index = iceberg_file_index
                .as_mooncake_file_index(
                    &self.remote_data_file_to_file_id,
                    self.object_storage_cache.clone(),
                    self.filesystem_accessor.as_ref(),
                    table_id,
                    next_file_id,
                )
                .await?;
            self.persisted_file_indices
                .insert(mooncake_file_index.clone(), entry.puffin_file_path.clone());
            out.push(mooncake_file_index);
        }
        Ok(out)
    }

    /// Load data file into table manager from the current manifest entry.
    async fn load_data_file_from_manifest_entry(
        &mut self,
        entry: &ManifestEntry,
        next_file_id: &mut u64,
    ) -> IcebergResult<()> {
        if !utils::is_data_file_entry(entry) {
            return Ok(());
        }

        let data_file = entry.data_file();
        let num_rows = data_file.record_count();
        assert_eq!(data_file.file_format(), DataFileFormat::Parquet);
        let new_data_file_entry = DataFileEntry {
            data_file: data_file.clone(),
            deletion_vector: BatchDeletionVector::new(num_rows as usize),
        };

        self.persisted_data_files
            .insert(FileId(*next_file_id), new_data_file_entry);
        self.remote_data_file_to_file_id
            .insert(data_file.file_path().to_string(), FileId(*next_file_id));
        *next_file_id += 1;

        Ok(())
    }

    /// Load deletion vector into table manager from the current manifest entry.
    /// Return maps from data file's file id to persisted deletion vector.
    async fn load_deletion_vector_from_manifest_entry(
        &mut self,
        entry: &ManifestEntry,
        file_io: &FileIO,
        next_file_id: &mut u64,
    ) -> IcebergResult<Option<(FileId, PuffinBlobRef)>> {
        // Skip data files and file indices.
        if !utils::is_deletion_vector_entry(entry) {
            return Ok(None);
        }

        let data_file = entry.data_file();
        let referenced_data_file = data_file.referenced_data_file().unwrap();
        let data_file_id = self
            .remote_data_file_to_file_id
            .get(&referenced_data_file)
            .unwrap();
        let data_file_entry = self.persisted_data_files.get_mut(data_file_id).unwrap();

        IcebergValidation::validate_puffin_manifest_entry(entry)?;
        let deletion_vector = DeletionVector::load_from_dv_blob(file_io.clone(), data_file).await?;
        let num_rows = data_file.record_count();

        let batch_deletion_vector = deletion_vector.take_as_batch_delete_vector();
        data_file_entry.deletion_vector = batch_deletion_vector.clone();

        // Load remote puffin file to local cache and pin.
        let cur_file_id = *next_file_id;
        *next_file_id += 1;
        let unique_file_id = TableUniqueFileId {
            table_id: TableId(self.mooncake_table_metadata.table_id),
            file_id: FileId(cur_file_id),
        };
        let (cache_handle, evicted_files_to_delete) = self
            .object_storage_cache
            .get_cache_entry(
                unique_file_id,
                data_file.file_path(),
                self.filesystem_accessor.as_ref(),
            )
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to get cache entry for {}", data_file.file_path(),),
                )
                .with_retryable(true)
                .with_source(e)
            })?;
        io_utils::delete_local_files(&evicted_files_to_delete)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to delete files for {evicted_files_to_delete:?}"),
                )
                .with_retryable(true)
                .with_source(e)
            })?;

        let persisted_deletion_vector = PuffinBlobRef {
            // Deletion vector should be pinned on cache.
            puffin_file_cache_handle: cache_handle.unwrap(),
            start_offset: data_file.content_offset().unwrap() as u32,
            blob_size: data_file.content_size_in_bytes().unwrap() as u32,
            num_rows: num_rows as usize,
        };

        Ok(Some((*data_file_id, persisted_deletion_vector)))
    }

    /// -------- Transformation util functions ---------
    ///
    /// Util function to transform iceberg table status to mooncake table snapshot, assign file id uniquely to all data files.
    fn transform_to_mooncake_snapshot(
        &self,
        mut loaded_deletion_vector: HashMap<FileId, PuffinBlobRef>,
        loaded_file_indices: Vec<MooncakeFileIndex>,
        flush_lsn: Option<u64>,
    ) -> MooncakeSnapshot {
        let mut mooncake_snapshot = MooncakeSnapshot::new(self.mooncake_table_metadata.clone());

        // Assign snapshot version.
        let iceberg_table_metadata = self.iceberg_table.as_ref().unwrap().metadata();
        mooncake_snapshot.snapshot_version =
            if let Some(ver) = iceberg_table_metadata.current_snapshot_id() {
                ver as u64
            } else {
                0
            };

        // Fill in disk files.
        mooncake_snapshot.disk_files = HashMap::with_capacity(self.persisted_data_files.len());
        for (file_id, data_file_entry) in self.persisted_data_files.iter() {
            let data_file =
                create_data_file(file_id.0, data_file_entry.data_file.file_path().to_string());

            let puffin_deletion_blob = loaded_deletion_vector.remove(file_id);
            mooncake_snapshot.disk_files.insert(
                data_file,
                DiskFileEntry {
                    num_rows: data_file_entry.data_file.record_count() as usize,
                    file_size: data_file_entry.data_file.file_size_in_bytes() as usize,
                    cache_handle: None,
                    puffin_deletion_blob,
                    committed_deletion_vector: data_file_entry.deletion_vector.clone(),
                },
            );
        }

        // Fill in indices.
        mooncake_snapshot.indices = MooncakeIndex {
            in_memory_index: HashSet::new(),
            file_indices: loaded_file_indices,
        };

        // Fill in flush LSN.
        mooncake_snapshot.flush_lsn = flush_lsn;

        mooncake_snapshot
    }

    pub(crate) async fn load_snapshot_from_table_impl(
        &mut self,
    ) -> Result<(u32, MooncakeSnapshot)> {
        assert!(!self.snapshot_loaded);
        self.snapshot_loaded = true;

        // Unique file id to assign to every data file.
        let mut next_file_id = 0;

        // Handle cases which iceberg table doesn't exist.
        self.initialize_iceberg_table_if_exists().await?;
        if self.iceberg_table.is_none() {
            let empty_mooncake_snapshot =
                MooncakeSnapshot::new(self.mooncake_table_metadata.clone());
            return Ok((next_file_id as u32, empty_mooncake_snapshot));
        }

        // Perform validation before load operation.
        self.validate_schema_consistency_at_load();
        self.validate_private_root_deployment()?;
        // Reconcile the marker directory + private manifest set against the live
        // Iceberg snapshot set. Best-effort: failure to reconcile is downgraded
        // to a warning so a transient IO error during boot does not block the
        // table from coming online — the next boot retries the same probes.
        if let Err(e) = self.reconcile_marker_dir_on_load().await {
            tracing::warn!(
                error = %e,
                "boot marker-dir reconciliation failed; continuing load — markers will be re-evaluated on next boot"
            );
        }

        // Load moonlink related metadata.
        let table_metadata = self.iceberg_table.as_ref().unwrap().metadata();

        // There's nothing stored in iceberg table.
        if table_metadata.current_snapshot().is_none() {
            let empty_mooncake_snapshot =
                MooncakeSnapshot::new(self.mooncake_table_metadata.clone());
            return Ok((next_file_id as u32, empty_mooncake_snapshot));
        }

        // Load table state into iceberg table manager.
        let snapshot_meta = table_metadata.current_snapshot().unwrap();
        let snapshot_property = snapshot_utils::get_snapshot_properties(table_metadata)?;
        let manifest_list = snapshot_meta
            .load_manifest_list(
                self.iceberg_table.as_ref().unwrap().file_io(),
                table_metadata,
            )
            .await?;

        let file_io = self.iceberg_table.as_ref().unwrap().file_io().clone();
        let mut loaded_file_indices = vec![];

        // On load, we do two passes on all entries.
        // Data files are loaded first, because we need to get <data file, file id> mapping, which is used for later deletion vector and file indices recovery.
        // Deletion vector puffin and file indices have no dependency, and could be loaded in parallel.
        //
        // Cache manifest file by manifest filepath to avoid repeated IO.
        let mut manifest_file_cache = HashMap::new();

        // Attempt to load data files first.
        for manifest_file in manifest_list.entries().iter() {
            let manifest = manifest_file.load_manifest(&file_io).await?;
            assert!(manifest_file_cache
                .insert(manifest_file.manifest_path.clone(), manifest.clone())
                .is_none());
            let (manifest_entries, _) = manifest.into_parts();
            assert!(!manifest_entries.is_empty());

            // One manifest file only store one type of entities (i.e. data file, deletion vector, file indices).
            if !utils::is_data_file_entry(&manifest_entries[0]) {
                continue;
            }
            for entry in manifest_entries.iter() {
                self.load_data_file_from_manifest_entry(entry.as_ref(), &mut next_file_id)
                    .await?;
            }
        }

        // Attempt to load file indices and deletion vector.
        let mut loaded_deletion_vector = HashMap::new();
        for manifest_file in manifest_list.entries().iter() {
            let manifest = manifest_file_cache
                .remove(&manifest_file.manifest_path)
                .unwrap();
            let (manifest_entries, _) = manifest.into_parts();
            assert!(!manifest_entries.is_empty());
            if utils::is_data_file_entry(&manifest_entries[0]) {
                continue;
            }

            for entry in manifest_entries.iter() {
                // Load file indices.
                let recovered_file_index = self
                    .load_file_index_from_manifest_entry(
                        entry.as_ref(),
                        &file_io,
                        &mut next_file_id,
                    )
                    .await?;
                if let Some(recovered_file_index) = recovered_file_index {
                    loaded_file_indices.push(recovered_file_index);
                }

                // Load deletion vector puffin.
                if let Some((file_id, puffin_blob_ref)) = self
                    .load_deletion_vector_from_manifest_entry(
                        entry.as_ref(),
                        &file_io,
                        &mut next_file_id,
                    )
                    .await?
                {
                    assert!(loaded_deletion_vector
                        .insert(file_id, puffin_blob_ref)
                        .is_none());
                }
            }
        }

        // Rehydrate hash file indices from the per-table private manifest.
        // Legacy tables that still carry hash-index entries in the Iceberg
        // manifest list pick them up via the pass above; both sources merge
        // into `loaded_file_indices`.
        let private_indices = self
            .load_file_indices_from_private_manifest(&file_io, &mut next_file_id)
            .await?;
        loaded_file_indices.extend(private_indices);

        let mooncake_snapshot = self.transform_to_mooncake_snapshot(
            loaded_deletion_vector,
            loaded_file_indices,
            snapshot_property.flush_lsn,
        );
        Ok((next_file_id as u32, mooncake_snapshot))
    }
}

/// Filename suffix mooncake assigns to every hash-index puffin
/// (see [`crate::storage::table::iceberg::utils::get_unique_hash_index_v1_filepath`]).
const HASH_INDEX_PUFFIN_SUFFIX: &str = "-hash-index-v1-puffin.bin";

/// Subdirectory under the Iceberg table root where mooncake's location
/// generator places hash-index puffins. Restricting trust to this subdir
/// keeps a stray marker payload from naming a parquet, manifest, or
/// deletion-vector file elsewhere under the table root.
const TABLE_DATA_SUBDIR: &str = "data/";

/// A directory prefix string normalized to always end with `/`. Constructing
/// the newtype is the only place where the trailing slash invariant is
/// applied, so callers can compare paths with `starts_with` without worrying
/// about double-slash or missing-slash bugs.
struct DirectoryPrefix(String);

impl DirectoryPrefix {
    fn new(s: impl AsRef<str>) -> Self {
        let trimmed = s.as_ref().trim_end_matches('/');
        Self(format!("{trimmed}/"))
    }

    fn join_subdir(&self, subdir: &str) -> Self {
        let trimmed = subdir.trim_end_matches('/');
        Self(format!("{}{}/", self.0, trimmed))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

/// Sandbox derived from the table's deployment shape. The classifier matches
/// a marker payload's `file_path` against these prefixes and applies a
/// per-prefix shape check — only mooncake-issued artifact patterns are
/// trusted, never the prefix on its own.
struct MarkerSandbox {
    /// `<private_root>/<table_uuid>/`. The classifier trusts only the
    /// marker's own `manifest/snap-<marker.snapshot_id>.json` under this
    /// prefix; other private-root paths are quarantined.
    private_prefix: DirectoryPrefix,
    /// `<metadata.location()>/data/`. The classifier trusts only paths
    /// directly under this prefix whose basename matches the mooncake
    /// hash-index puffin pattern; data parquet, deletion-vector puffins,
    /// and paths under `metadata/` or other subdirs are quarantined.
    table_data_prefix: DirectoryPrefix,
}

/// What kind of artifact a trusted marker payload points at. The distinction
/// shapes the deletion policy: a private manifest is per-snapshot and never
/// inherited (safe to delete once its snapshot is orphaned); a hash puffin
/// may be referenced via parent inheritance and needs cross-checking against
/// the live-manifest in-use set.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum MarkerArtifactKind {
    PrivateManifest,
    HashPuffin,
}

/// Why the classifier refused to trust a marker payload. Each variant maps to
/// a stable operator-visible string via `as_str` so per-reason metrics and
/// log filtering have a stable dictionary instead of free-form text.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum QuarantineReason {
    PathContainsParentSegment,
    PrivateRootPathNotOwnManifest,
    TableRootPathNotHashPuffin,
    OutsideOwnedRoots,
}

impl QuarantineReason {
    fn as_str(self) -> &'static str {
        match self {
            QuarantineReason::PathContainsParentSegment => "path contains .. segment",
            QuarantineReason::PrivateRootPathNotOwnManifest => {
                "private-root path does not match this marker's snap-<id>.json"
            }
            QuarantineReason::TableRootPathNotHashPuffin => {
                "table-root path is not a mooncake hash-index puffin"
            }
            QuarantineReason::OutsideOwnedRoots => "path outside owned roots",
        }
    }
}

/// Classification verdict for a marker payload's `file_path`.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum MarkerTargetClassification {
    Trusted(MarkerArtifactKind),
    Quarantined(QuarantineReason),
}

/// Decide whether a marker payload can be acted on.
///
/// Trust is granted only when the path matches a shape mooncake actually
/// writes for **this marker's snapshot**:
///
/// * `<private_root>/<table_uuid>/manifest/snap-<marker.snapshot_id>.json` —
///   the marker's own private manifest. A marker can authorize the deletion
///   of its own manifest, never some other live snapshot's.
/// * `<metadata.location()>/data/<uuid>-hash-index-v1-puffin.bin` — a
///   mooncake hash puffin written by the syncer's pre-write hook. The
///   basename must be a UUID followed by the version suffix; suffix-only
///   matching would let `data/foo-hash-index-v1-puffin.bin` slip through.
///
/// Anything else — paths under `metadata/`, other subdirs of the table root,
/// non-UUID basenames, `..` segments, paths outside both roots — is
/// quarantined so a stale or corrupt marker can never escalate into
/// unbounded delete authority.
fn classify_marker_target(
    path: &str,
    marker_snapshot_id: i64,
    sandbox: &MarkerSandbox,
) -> MarkerTargetClassification {
    if path.split('/').any(|segment| segment == "..") {
        return MarkerTargetClassification::Quarantined(
            QuarantineReason::PathContainsParentSegment,
        );
    }
    if path.starts_with(sandbox.private_prefix.as_str()) {
        let expected = format!(
            "{}manifest/snap-{}.json",
            sandbox.private_prefix.as_str(),
            marker_snapshot_id
        );
        return if path == expected {
            MarkerTargetClassification::Trusted(MarkerArtifactKind::PrivateManifest)
        } else {
            MarkerTargetClassification::Quarantined(QuarantineReason::PrivateRootPathNotOwnManifest)
        };
    }
    if let Some(relative) = path.strip_prefix(sandbox.table_data_prefix.as_str()) {
        // The trusted shape is a *direct* child of `<table>/data/`, never a
        // nested subdir. A path like `<table>/data/scratch/<uuid>-hash-index-v1-puffin.bin`
        // would otherwise pass the basename check even though mooncake's
        // location generator never writes there.
        let basename_only = !relative.contains('/');
        return if basename_only && is_mooncake_hash_puffin_basename(relative) {
            MarkerTargetClassification::Trusted(MarkerArtifactKind::HashPuffin)
        } else {
            MarkerTargetClassification::Quarantined(QuarantineReason::TableRootPathNotHashPuffin)
        };
    }
    MarkerTargetClassification::Quarantined(QuarantineReason::OutsideOwnedRoots)
}

/// True when `basename` matches the `<uuid>-hash-index-v1-puffin.bin` shape
/// emitted by mooncake's location generator. Requires the UUID prefix to
/// reject hand-crafted names that happen to end in the version suffix.
fn is_mooncake_hash_puffin_basename(basename: &str) -> bool {
    let Some(uuid_part) = basename.strip_suffix(HASH_INDEX_PUFFIN_SUFFIX) else {
        return false;
    };
    Uuid::parse_str(uuid_part).is_ok()
}

/// Per-snapshot reap result. The marker dir is rolled back only when every
/// artifact ended in a handled state (deleted, NotFound-via-idempotent-delete,
/// or skipped because still referenced). Any path we could not safely act on
/// (failed delete, quarantined payload, unreliable in-use witness) retains the
/// dir so the next boot keeps the recovery evidence.
#[derive(Default)]
struct ReapOutcome {
    deleted: usize,
    skipped_referenced: usize,
    skipped_unreliable: usize,
    quarantined: usize,
    failed: usize,
}

impl ReapOutcome {
    fn should_retain_marker_dir(&self) -> bool {
        self.failed > 0 || self.quarantined > 0 || self.skipped_unreliable > 0
    }
}

/// Union of puffin paths referenced by any live snapshot's private manifest.
///
/// `reliable` is `false` whenever at least one live snapshot fails to produce
/// a usable manifest — either the read errored (corrupt / IO) or the manifest
/// is absent (`Ok(None)`). On a private-root-bound table both are anomalies
/// equivalent to the commit↔first-record gap reported by the snapshot-side
/// reconciler, and either case leaves the in-use set as a lower bound rather
/// than a complete witness. Callers must fail-closed on table-root puffin
/// deletions in that case — under-counting a reference could otherwise delete
/// a puffin that a still-live snapshot inherits from its parent.
struct InUsePuffinSet {
    paths: HashSet<String>,
    reliable: bool,
}

async fn read_in_use_puffin_paths(
    store: &PrivateManifestStore,
    live: &HashSet<i64>,
) -> InUsePuffinSet {
    let mut paths = HashSet::new();
    let mut reliable = true;
    for id in live {
        match store.read_snap_manifest(*id).await {
            Ok(Some(manifest)) => {
                for entry in &manifest.hash_index_entries {
                    paths.insert(entry.puffin_file_path.clone());
                }
            }
            Ok(None) => {
                reliable = false;
                tracing::warn!(
                    snapshot_id = id,
                    "boot reconciliation: live snapshot has no private manifest; in-use witness downgraded — downstream puffin deletes will fail-closed",
                );
            }
            Err(e) => {
                reliable = false;
                tracing::warn!(
                    snapshot_id = id,
                    error = %e,
                    "boot reconciliation: failed to read private manifest while assembling in-use set; downstream puffin deletes will fail-closed",
                );
            }
        }
    }
    InUsePuffinSet { paths, reliable }
}

#[cfg(test)]
mod classify_tests {
    //! Direct unit tests for the marker-target classifier. The integration
    //! tests in `iceberg/tests.rs` exercise it indirectly via reconciliation;
    //! these cases pin the precise verdicts the classifier returns so future
    //! refactors cannot loosen the sandbox without a failing test.
    use super::*;

    fn sandbox() -> MarkerSandbox {
        // Use a synthetic table UUID; the classifier never parses it, only
        // matches the prefix it appears in.
        MarkerSandbox {
            private_prefix: DirectoryPrefix::new("s3://wh/_mooncake_private/table-uuid"),
            table_data_prefix: DirectoryPrefix::new("s3://wh/db/tbl").join_subdir("data"),
        }
    }

    fn puffin_basename() -> String {
        format!("{}{}", Uuid::now_v7(), HASH_INDEX_PUFFIN_SUFFIX)
    }

    #[test]
    fn trusts_own_private_manifest() {
        let verdict = classify_marker_target(
            "s3://wh/_mooncake_private/table-uuid/manifest/snap-42.json",
            42,
            &sandbox(),
        );
        assert_eq!(
            verdict,
            MarkerTargetClassification::Trusted(MarkerArtifactKind::PrivateManifest),
        );
    }

    #[test]
    fn quarantines_other_snapshot_private_manifest() {
        let verdict = classify_marker_target(
            "s3://wh/_mooncake_private/table-uuid/manifest/snap-7.json",
            42,
            &sandbox(),
        );
        assert_eq!(
            verdict,
            MarkerTargetClassification::Quarantined(
                QuarantineReason::PrivateRootPathNotOwnManifest
            ),
        );
    }

    #[test]
    fn quarantines_arbitrary_private_root_path() {
        let verdict = classify_marker_target(
            "s3://wh/_mooncake_private/table-uuid/scratch/whatever.bin",
            42,
            &sandbox(),
        );
        assert_eq!(
            verdict,
            MarkerTargetClassification::Quarantined(
                QuarantineReason::PrivateRootPathNotOwnManifest
            ),
        );
    }

    #[test]
    fn trusts_hash_puffin_under_table_data_dir() {
        let path = format!("s3://wh/db/tbl/data/{}", puffin_basename());
        let verdict = classify_marker_target(&path, 42, &sandbox());
        assert_eq!(
            verdict,
            MarkerTargetClassification::Trusted(MarkerArtifactKind::HashPuffin),
        );
    }

    #[test]
    fn quarantines_hash_puffin_pattern_under_metadata_dir() {
        // Same filename pattern as a real hash puffin, but under
        // `<table>/metadata/` rather than `<table>/data/`. The classifier
        // must not trust it.
        let path = format!("s3://wh/db/tbl/metadata/{}", puffin_basename());
        let verdict = classify_marker_target(&path, 42, &sandbox());
        assert_eq!(
            verdict,
            MarkerTargetClassification::Quarantined(QuarantineReason::OutsideOwnedRoots),
        );
    }

    #[test]
    fn quarantines_non_uuid_basename_under_data_dir() {
        let verdict = classify_marker_target(
            "s3://wh/db/tbl/data/foo-hash-index-v1-puffin.bin",
            42,
            &sandbox(),
        );
        assert_eq!(
            verdict,
            MarkerTargetClassification::Quarantined(QuarantineReason::TableRootPathNotHashPuffin),
        );
    }

    #[test]
    fn quarantines_data_parquet_under_data_dir() {
        let verdict =
            classify_marker_target("s3://wh/db/tbl/data/00000-0-xxxx.parquet", 42, &sandbox());
        assert_eq!(
            verdict,
            MarkerTargetClassification::Quarantined(QuarantineReason::TableRootPathNotHashPuffin),
        );
    }

    #[test]
    fn quarantines_hash_puffin_pattern_in_data_subdir() {
        // mooncake's location generator writes hash puffins as direct
        // children of `<table>/data/`. A path one level deeper (e.g.
        // `<table>/data/scratch/<uuid>-hash-index-v1-puffin.bin`) must be
        // quarantined even though its basename matches the pattern.
        let path = format!("s3://wh/db/tbl/data/scratch/{}", puffin_basename());
        let verdict = classify_marker_target(&path, 42, &sandbox());
        assert_eq!(
            verdict,
            MarkerTargetClassification::Quarantined(QuarantineReason::TableRootPathNotHashPuffin),
        );
    }

    #[test]
    fn quarantines_other_puffin_under_data_dir() {
        // A deletion-vector puffin (or any other non-hash-index puffin) does
        // not match the hash-index basename pattern.
        let verdict = classify_marker_target(
            "s3://wh/db/tbl/data/00000-deletion-vector-v1-puffin.bin",
            42,
            &sandbox(),
        );
        assert_eq!(
            verdict,
            MarkerTargetClassification::Quarantined(QuarantineReason::TableRootPathNotHashPuffin),
        );
    }

    #[test]
    fn quarantines_path_with_parent_segment() {
        let path = format!(
            "s3://wh/_mooncake_private/table-uuid/manifest/../escape/{}",
            puffin_basename()
        );
        let verdict = classify_marker_target(&path, 42, &sandbox());
        assert_eq!(
            verdict,
            MarkerTargetClassification::Quarantined(QuarantineReason::PathContainsParentSegment),
        );
    }

    #[test]
    fn quarantines_path_outside_owned_roots() {
        let verdict =
            classify_marker_target("s3://other-bucket/foreign/object.bin", 42, &sandbox());
        assert_eq!(
            verdict,
            MarkerTargetClassification::Quarantined(QuarantineReason::OutsideOwnedRoots),
        );
    }
}
