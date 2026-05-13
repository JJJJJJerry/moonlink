use crate::storage::index::{FileIndex as MooncakeFileIndex, MooncakeIndex};
use crate::storage::io_utils;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::mooncake_table::DiskFileEntry;
use crate::storage::mooncake_table::Snapshot as MooncakeSnapshot;
use crate::storage::storage_utils::{create_data_file, FileId, TableId, TableUniqueFileId};
use crate::storage::table::iceberg::deletion_vector::DeletionVector;
use crate::storage::table::iceberg::iceberg_table_manager::*;
use crate::storage::table::iceberg::index::FileIndexBlob;
use crate::storage::table::iceberg::private_manifest::{
    validate_deployment, DeploymentStatus, PrivateManifestStore, MOONCAKE_CONTROLLED_BETA_SIGNOFF,
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
    /// + `mooncake.controlled_beta_signoff` Iceberg properties (when the table was
    /// created with private-root binding), classifies the deployment, logs the
    /// outcome, and fails recovery when the table sits inside the Iceberg root
    /// without a signoff (Path c).
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
        let signoff = metadata
            .properties()
            .get(MOONCAKE_CONTROLLED_BETA_SIGNOFF)
            .map(String::as_str);
        // `metadata.location()` is the only root that downstream Iceberg cleanup
        // (RemoveOrphanFiles, lifecycle policies, third-party governance) actually
        // walks, so it is the only one that defines the threat surface. The convention
        // path `warehouse/<ns>/<table>` may differ for custom-location / externally
        // registered tables; we deliberately ignore it here to avoid false positives.
        let physical_root = metadata.location();
        match validate_deployment(physical_root, private_root, signoff) {
            Ok(DeploymentStatus::Compliant) => {
                tracing::info!(
                    iceberg_root = %physical_root,
                    private_root = %private_root,
                    iceberg_table = %iceberg_root,
                    "mooncake private root validated (Compliant)"
                );
                Ok(())
            }
            Ok(DeploymentStatus::SignedOff { signoff_id }) => {
                tracing::warn!(
                    iceberg_root = %physical_root,
                    private_root = %private_root,
                    iceberg_table = %iceberg_root,
                    signoff_id = %signoff_id,
                    "mooncake private root sits inside Iceberg table root; relying on storage-level guards (controlled-beta signoff present)"
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

    /// Rebuild hash file indices from the mooncake-private manifest (Mode 2a).
    ///
    /// Returns an empty vec when the table has no private root configured (legacy) or
    /// when no manifest exists for the current Iceberg snapshot (freshly-restored
    /// private root, expired snapshot, or a snapshot that produced no hash blobs).
    /// In the latter case the caller may treat the index as `Degraded` and trigger
    /// an async rebuild from data files (cost-gating fallback in the meantime).
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

        // Rehydrate hash file indices from the mooncake-private manifest. Legacy
        // tables (pre-Mode 2a) still get their hash entries via the manifest_list
        // pass above; both are merged into `loaded_file_indices`.
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
