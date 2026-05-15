//! Private hash-index storage with `snapshot.summary` pointer publication.
//!
//! Mooncake's hash index is mooncake-only metadata: external Iceberg readers
//! reject the out-of-spec `Data + Puffin` manifest entry the legacy path
//! emits. This module relocates hash-index puffin files outside the Iceberg
//! table root and publishes the **complete live pointer set** through the
//! Iceberg `snapshot.summary` map. Pointer and data files share one catalog
//! CAS — no marker protocol, no half-commit window.
//!
//! Core invariant — each committed snapshot's `moonlink.hash-index` summary
//! value represents the **entire** live hash-index reference set for that
//! snapshot, not a delta from the previous snapshot. Loaders rebuild state
//! solely from one snapshot's summary; absence of the key means "no hash
//! indexes for this snapshot".

use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::index::FileIndex as MooncakeFileIndex;
use crate::storage::table::iceberg::index::{FileIndexBlob, MOONCAKE_HASH_INDEX_V1};
use crate::storage::table::iceberg::puffin_utils;
use crate::storage::table::iceberg::puffin_writer_proxy::get_puffin_metadata_and_close;

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use iceberg::io::FileIO;
use iceberg::puffin::CompressionCodec;
use iceberg::spec::TableMetadata;
use iceberg::{Error as IcebergError, Result as IcebergResult};
use serde::{Deserialize, Serialize};

// ============================================================================
// Public contract
// ============================================================================

/// Snapshot summary key carrying the JSON-encoded `HashIndexSnapshotState`.
/// Iceberg spec allows implementation-specific summary keys; the
/// `moonlink.*` prefix matches `MOONCAKE_TABLE_FLUSH_LSN`'s convention.
pub(crate) const SNAPSHOT_SUMMARY_HASH_INDEX_KEY: &str = "moonlink.hash-index";

/// Wire-format version. Bump only on breaking changes; readers must accept
/// equal-or-older versions when required fields are present.
pub(crate) const SCHEMA_VERSION_V1: &str = "v1";

/// Default sweeper retention: orphans younger than this are skipped to avoid
/// racing in-flight commits.
#[allow(dead_code)]
pub(crate) const DEFAULT_ORPHAN_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);

/// Configuration for private hash-index storage. Setting `Some(...)` on
/// `IcebergTableConfig::hash_index_private_storage` activates this path;
/// `None` falls back to the legacy `manifest_list` path.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrivateHashIndexConfig {
    /// Private root URI (e.g. `s3://mooncake-private/`). Per-table state
    /// lives under `<private_root>/<iceberg_table_uuid>/puffin/`.
    pub private_root: String,
}

impl PrivateHashIndexConfig {
    pub(crate) fn puffin_dir(&self, table_uuid: uuid::Uuid) -> String {
        let trimmed = self.private_root.trim_end_matches('/');
        format!("{trimmed}/{table_uuid}/puffin")
    }

    pub(crate) fn generate_puffin_uri(&self, table_uuid: uuid::Uuid) -> String {
        format!(
            "{}/{}-hash-index-v1.puffin",
            self.puffin_dir(table_uuid),
            uuid::Uuid::now_v7()
        )
    }

    /// Reject overlap with the Iceberg table root: overlap exposes private
    /// puffins to external readers, defeating this module's purpose.
    ///
    /// Inputs are normalized by stripping the optional `file://` scheme so
    /// `file:///wh/tbl` and `/wh/tbl` compare equal — iceberg-rust's FS
    /// catalog emits the former while the storage accessor uses the latter.
    pub(crate) fn validate_against_iceberg_table_root(
        &self,
        iceberg_table_root: &str,
    ) -> IcebergResult<()> {
        let private = normalize_for_overlap(&self.private_root);
        let iceberg = normalize_for_overlap(iceberg_table_root);
        let overlaps = private == iceberg
            || private.starts_with(&format!("{iceberg}/"))
            || iceberg.starts_with(&format!("{private}/"));
        if overlaps {
            return Err(IcebergError::new(
                iceberg::ErrorKind::DataInvalid,
                format!(
                    "private hash-index root overlaps Iceberg table root \
                     (private={private:?}, iceberg={iceberg:?})"
                ),
            ));
        }
        Ok(())
    }
}

fn normalize_for_overlap(path: &str) -> &str {
    path.strip_prefix("file://")
        .unwrap_or(path)
        .trim_end_matches('/')
}

// ============================================================================
// Snapshot.summary payload
// ============================================================================

/// One puffin reference. The contract is intentionally minimal: only fields
/// the read or retention path actually consume.
///
/// - `puffin_uri`: the file to open via `FileIO`.
/// - `index_block_uris`: the `.bin` files the puffin's JSON blob references.
///   Carried in the summary so the sweeper's live set protects them without
///   parsing every puffin.
/// - `cardinality`: planner cost-gating without opening the puffin.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct HashIndexPointer {
    pub puffin_uri: String,
    pub index_block_uris: Vec<String>,
    pub cardinality: u64,
}

/// Complete hash-index state for one Iceberg snapshot — the **whole** live
/// set, not a delta.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct HashIndexSnapshotState {
    pub schema_version: String,
    pub entries: Vec<HashIndexPointer>,
}

impl HashIndexSnapshotState {
    pub(crate) fn new(entries: Vec<HashIndexPointer>) -> Self {
        Self {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            entries,
        }
    }

    pub(crate) fn to_summary_value(&self) -> IcebergResult<String> {
        serde_json::to_string(self).map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::DataInvalid,
                format!("serialize HashIndexSnapshotState: {e}"),
            )
        })
    }

    pub(crate) fn from_summary_value(s: &str) -> IcebergResult<Self> {
        let parsed: Self = serde_json::from_str(s).map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::DataInvalid,
                format!("deserialize HashIndexSnapshotState: {e}"),
            )
        })?;
        if parsed.schema_version != SCHEMA_VERSION_V1 {
            return Err(IcebergError::new(
                iceberg::ErrorKind::FeatureUnsupported,
                format!(
                    "unsupported HashIndexSnapshotState schema_version {:?}",
                    parsed.schema_version
                ),
            ));
        }
        Ok(parsed)
    }
}

// ============================================================================
// Write path
// ============================================================================

pub(crate) struct HashIndexWriteOutcome {
    pub(crate) pointer: HashIndexPointer,
    pub(crate) local_index_file_to_private: HashMap<String, String>,
}

/// Write one `MooncakeFileIndex` to private storage. Caller is responsible for
/// aggregating the returned pointer into the next snapshot's
/// `HashIndexSnapshotState` and only mutating manager bookkeeping after a
/// successful Iceberg commit.
pub(crate) async fn write_file_index_to_private_storage(
    file_index: &MooncakeFileIndex,
    local_data_file_to_remote: &HashMap<String, String>,
    config: &PrivateHashIndexConfig,
    table_uuid: uuid::Uuid,
    file_io: &FileIO,
    fs: &dyn BaseFileSystemAccess,
) -> crate::Result<HashIndexWriteOutcome> {
    let puffin_uri = config.generate_puffin_uri(table_uuid);
    let puffin_dir = config.puffin_dir(table_uuid);

    let mut local_index_file_to_private = HashMap::new();
    let mut index_block_uris = Vec::with_capacity(file_index.index_blocks.len());
    for block in file_index.index_blocks.iter() {
        let local_path = block.index_file.file_path();
        let basename = Path::new(local_path)
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| {
                IcebergError::new(
                    iceberg::ErrorKind::DataInvalid,
                    format!("index block path has no basename: {local_path}"),
                )
            })?;
        let remote = format!("{puffin_dir}/{basename}");
        fs.copy_from_local_to_remote(local_path, &remote).await?;
        local_index_file_to_private.insert(local_path.clone(), remote.clone());
        index_block_uris.push(remote);
    }

    let blob = FileIndexBlob::new(
        file_index,
        &local_index_file_to_private,
        local_data_file_to_remote,
    )
    .as_blob()?;

    let mut writer = puffin_utils::create_puffin_writer(file_io, &puffin_uri).await?;
    writer.add(blob, CompressionCodec::None).await?;
    let metadata = get_puffin_metadata_and_close(file_io, &puffin_uri, writer).await?;
    let blob_meta = metadata.into_iter().next().ok_or_else(|| {
        IcebergError::new(
            iceberg::ErrorKind::DataInvalid,
            "puffin file produced no blob metadata",
        )
    })?;
    debug_assert_eq!(blob_meta.blob_type(), MOONCAKE_HASH_INDEX_V1);

    let cardinality = blob_meta
        .properties()
        .get(crate::storage::table::iceberg::index::MOONCAKE_HASH_INDEX_V1_CARDINALITY)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    Ok(HashIndexWriteOutcome {
        pointer: HashIndexPointer {
            puffin_uri,
            index_block_uris,
            cardinality,
        },
        local_index_file_to_private,
    })
}

// ============================================================================
// Read path
// ============================================================================

pub(crate) fn read_state_from_snapshot(
    table_metadata: &TableMetadata,
) -> IcebergResult<Option<HashIndexSnapshotState>> {
    let Some(snapshot) = table_metadata.current_snapshot() else {
        return Ok(None);
    };
    let Some(raw) = snapshot
        .summary()
        .additional_properties
        .get(SNAPSHOT_SUMMARY_HASH_INDEX_KEY)
    else {
        return Ok(None);
    };
    HashIndexSnapshotState::from_summary_value(raw).map(Some)
}

/// Load every `FileIndexBlob` referenced by the given state. Pairing each
/// blob with its source pointer (rather than relying on positional `zip`)
/// keeps downstream bookkeeping resilient to future entry reordering.
pub(crate) async fn load_blobs_from_state(
    state: &HashIndexSnapshotState,
    file_io: &FileIO,
) -> IcebergResult<Vec<(HashIndexPointer, FileIndexBlob)>> {
    let mut out = Vec::with_capacity(state.entries.len());
    for pointer in state.entries.iter() {
        let blob =
            puffin_utils::load_blob_from_puffin_file(file_io.clone(), &pointer.puffin_uri).await?;
        out.push((pointer.clone(), FileIndexBlob::from_blob(blob)?));
    }
    Ok(out)
}

// ============================================================================
// Sweeper — public surface, callers TBD
// ============================================================================

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct SweeperConfig {
    pub retention: Duration,
    pub dry_run: bool,
}

impl Default for SweeperConfig {
    fn default() -> Self {
        Self {
            retention: DEFAULT_ORPHAN_RETENTION,
            dry_run: false,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct SweepReport {
    pub live_uris: usize,
    pub candidates: usize,
    pub deleted: usize,
    pub skipped_young: usize,
}

/// Sweep orphan files in the per-table private root. The live set is the
/// union of every retained snapshot's puffin URIs **and** their referenced
/// `index_block_uris` — missing either set risks deleting a file still
/// referenced by a live puffin.
#[allow(dead_code)]
pub async fn sweep_orphans(
    table_metadata: &TableMetadata,
    config: &PrivateHashIndexConfig,
    table_uuid: uuid::Uuid,
    fs: Arc<dyn BaseFileSystemAccess>,
    sweeper_cfg: &SweeperConfig,
) -> crate::Result<SweepReport> {
    let live = collect_live_uris(table_metadata);
    let dir = config.puffin_dir(table_uuid);
    let mut report = SweepReport {
        live_uris: live.len(),
        ..Default::default()
    };

    let entries = match fs.list_direct_files(&dir).await {
        Ok(entries) => entries,
        Err(_) => return Ok(report),
    };

    let now = SystemTime::now();
    for entry in entries {
        report.candidates += 1;
        if live.contains(&entry) {
            continue;
        }
        match fs.stats_object(&entry).await {
            Ok(stat) => match stat.last_modified() {
                Some(mtime)
                    if now
                        .duration_since(mtime.into())
                        .map(|age| age < sweeper_cfg.retention)
                        .unwrap_or(true) =>
                {
                    report.skipped_young += 1;
                    continue;
                }
                _ => {}
            },
            Err(_) => continue,
        }
        if sweeper_cfg.dry_run {
            tracing::info!(target: "moonlink::hash_index_summary::sweep", uri = %entry, "would delete orphan");
            report.deleted += 1;
        } else {
            match fs.delete_object(&entry).await {
                Ok(()) => report.deleted += 1,
                Err(e) => tracing::warn!(
                    target: "moonlink::hash_index_summary::sweep",
                    uri = %entry,
                    error = %e,
                    "delete failed; will retry next sweep"
                ),
            }
        }
    }
    Ok(report)
}

pub(crate) fn collect_live_uris(table_metadata: &TableMetadata) -> HashSet<String> {
    let mut live = HashSet::new();
    for snapshot in table_metadata.snapshots() {
        let Some(raw) = snapshot
            .summary()
            .additional_properties
            .get(SNAPSHOT_SUMMARY_HASH_INDEX_KEY)
        else {
            continue;
        };
        let state = match HashIndexSnapshotState::from_summary_value(raw) {
            Ok(s) => s,
            Err(e) => {
                // Malformed single-snapshot summary must not abort the sweep;
                // a corrupted retained snapshot would otherwise pin all
                // orphans forever.
                tracing::warn!(
                    target: "moonlink::hash_index_summary::sweep",
                    snapshot_id = snapshot.snapshot_id(),
                    error = %e,
                    "skipping malformed hash-index state"
                );
                continue;
            }
        };
        for pointer in state.entries {
            live.insert(pointer.puffin_uri);
            live.extend(pointer.index_block_uris);
        }
    }
    live
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn pointer(puffin: &str, blocks: &[&str], card: u64) -> HashIndexPointer {
        HashIndexPointer {
            puffin_uri: puffin.into(),
            index_block_uris: blocks.iter().map(|s| s.to_string()).collect(),
            cardinality: card,
        }
    }

    #[test]
    fn state_roundtrip_preserves_all_fields() {
        let original = HashIndexSnapshotState::new(vec![
            pointer(
                "s3://priv/uuid/puffin/a.puffin",
                &["s3://priv/uuid/puffin/a-block-0.bin"],
                100,
            ),
            pointer(
                "s3://priv/uuid/puffin/b.puffin",
                &[
                    "s3://priv/uuid/puffin/b-block-0.bin",
                    "s3://priv/uuid/puffin/b-block-1.bin",
                ],
                200,
            ),
        ]);
        let serialized = original.to_summary_value().unwrap();
        let parsed = HashIndexSnapshotState::from_summary_value(&serialized).unwrap();
        assert_eq!(original, parsed);
    }

    #[test]
    fn state_rejects_future_schema_version() {
        let v2 = r#"{"schema_version":"v2","entries":[]}"#;
        let err = HashIndexSnapshotState::from_summary_value(v2).unwrap_err();
        assert!(err.message().contains("v2"), "got: {}", err.message());
    }

    #[test]
    fn state_rejects_malformed_json() {
        let err = HashIndexSnapshotState::from_summary_value("not json").unwrap_err();
        assert_eq!(err.kind(), iceberg::ErrorKind::DataInvalid);
    }

    #[test]
    fn config_puffin_dir_strips_trailing_slash() {
        let uuid = uuid::Uuid::from_u128(0x42);
        let a = PrivateHashIndexConfig {
            private_root: "s3://bucket/private/".into(),
        };
        let b = PrivateHashIndexConfig {
            private_root: "s3://bucket/private".into(),
        };
        assert_eq!(a.puffin_dir(uuid), b.puffin_dir(uuid));
        assert_eq!(a.puffin_dir(uuid), format!("s3://bucket/private/{uuid}/puffin"));
    }

    #[test]
    fn config_generates_unique_uris() {
        let uuid = uuid::Uuid::new_v4();
        let cfg = PrivateHashIndexConfig {
            private_root: "s3://bucket/private".into(),
        };
        let u1 = cfg.generate_puffin_uri(uuid);
        let u2 = cfg.generate_puffin_uri(uuid);
        assert_ne!(u1, u2);
        assert!(u1.ends_with("-hash-index-v1.puffin"));
        assert!(u1.starts_with(&cfg.puffin_dir(uuid)));
    }

    #[test]
    fn empty_state_serializes_compactly() {
        let s = HashIndexSnapshotState::new(vec![]).to_summary_value().unwrap();
        assert_eq!(s, r#"{"schema_version":"v1","entries":[]}"#);
    }

    #[test]
    fn validate_rejects_identical_roots() {
        let cfg = PrivateHashIndexConfig {
            private_root: "s3://warehouse/db/tbl".into(),
        };
        assert!(cfg
            .validate_against_iceberg_table_root("s3://warehouse/db/tbl")
            .is_err());
        assert!(cfg
            .validate_against_iceberg_table_root("s3://warehouse/db/tbl/")
            .is_err());
    }

    #[test]
    fn validate_rejects_private_under_iceberg() {
        let cfg = PrivateHashIndexConfig {
            private_root: "s3://warehouse/db/tbl/private".into(),
        };
        assert!(cfg
            .validate_against_iceberg_table_root("s3://warehouse/db/tbl")
            .is_err());
    }

    #[test]
    fn validate_rejects_iceberg_under_private() {
        let cfg = PrivateHashIndexConfig {
            private_root: "s3://warehouse".into(),
        };
        assert!(cfg
            .validate_against_iceberg_table_root("s3://warehouse/db/tbl")
            .is_err());
    }

    #[test]
    fn validate_accepts_disjoint_roots() {
        let cfg = PrivateHashIndexConfig {
            private_root: "s3://mooncake-private".into(),
        };
        assert!(cfg
            .validate_against_iceberg_table_root("s3://warehouse/db/tbl")
            .is_ok());
    }

    #[test]
    fn validate_strips_file_scheme_before_comparing() {
        // iceberg-rust's FS catalog emits `file:///...` for `location()`;
        // the storage accessor uses plain `/...`. Validator must compare
        // them as the same logical path.
        let cfg = PrivateHashIndexConfig {
            private_root: "/wh/private".into(),
        };
        assert!(cfg
            .validate_against_iceberg_table_root("file:///wh/db/tbl")
            .is_ok());

        let overlapping = PrivateHashIndexConfig {
            private_root: "/wh/db/tbl/private".into(),
        };
        assert!(overlapping
            .validate_against_iceberg_table_root("file:///wh/db/tbl")
            .is_err());
    }

    #[test]
    fn validate_accepts_sibling_prefix_not_subpath() {
        // "s3://warehouse/dbA" must not be treated as a prefix of
        // "s3://warehouse/dbAB" — string prefix check needs the trailing '/'.
        let cfg = PrivateHashIndexConfig {
            private_root: "s3://warehouse/dbAB".into(),
        };
        assert!(cfg
            .validate_against_iceberg_table_root("s3://warehouse/dbA")
            .is_ok());
    }

    #[test]
    fn pointer_includes_block_uris_in_serialization() {
        // Regression: previous schema dropped block uris; sweeper then
        // mis-classified them as orphans. This test pins the format.
        let p = pointer(
            "s3://priv/x.puffin",
            &["s3://priv/x-0.bin", "s3://priv/x-1.bin"],
            42,
        );
        let s = HashIndexSnapshotState::new(vec![p.clone()])
            .to_summary_value()
            .unwrap();
        assert!(s.contains("\"index_block_uris\":[\"s3://priv/x-0.bin\",\"s3://priv/x-1.bin\"]"));
        let parsed = HashIndexSnapshotState::from_summary_value(&s).unwrap();
        assert_eq!(parsed.entries[0], p);
    }
}
