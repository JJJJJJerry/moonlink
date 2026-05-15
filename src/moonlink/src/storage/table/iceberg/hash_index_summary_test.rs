//! End-to-end tests for the private hash-index / snapshot-summary path.
//!
//! Schema-layer serialization is covered by unit tests in
//! [`super::hash_index_summary::tests`]; this file exercises the runtime
//! invariants that only a full `sync_snapshot` ↔ `load_snapshot_from_table`
//! roundtrip can validate:
//!
//! 1. After a Route A sync, the Iceberg `manifest_list` contains **zero**
//!    hash-index puffin entries (the cross-engine compatibility goal).
//! 2. A subsequent flush that touches only data / deletion vectors still
//!    re-publishes the **complete** live hash-index pointer set in the new
//!    snapshot's summary (regression guard for the Blocking #2 bug where an
//!    earlier draft emitted only the per-flush delta and silently lost
//!    indexes on reload).
//! 3. Removing every hash index produces a snapshot whose summary omits the
//!    `moonlink.hash-index` key entirely.
//! 4. Deployment-time validation rejects a private root that overlaps the
//!    Iceberg table root.

use super::hash_index_summary::{
    PrivateHashIndexConfig, SCHEMA_VERSION_V1, SNAPSHOT_SUMMARY_HASH_INDEX_KEY,
};
use super::iceberg_table_config::IcebergTableConfig;
use super::iceberg_table_manager::IcebergTableManager;
use super::utils as iceberg_utils;

use crate::storage::index::persisted_bucket_hash_map::GlobalIndex;
use crate::storage::mooncake_table::table_creation_test_utils::{
    create_iceberg_table_config, create_test_arrow_schema, create_test_filesystem_accessor,
    create_test_object_storage_cache, create_test_table_metadata,
};
use crate::storage::mooncake_table::{
    PersistenceSnapshotDataCompactionPayload, PersistenceSnapshotImportPayload,
    PersistenceSnapshotIndexMergePayload, PersistenceSnapshotPayload,
};
use crate::storage::storage_utils::{create_data_file, MooncakeDataFileRef};
use crate::storage::table::common::table_manager::{PersistenceFileParams, TableManager};

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch, StringArray};
use parquet::arrow::AsyncArrowWriter;
use tempfile::TempDir;

// ----------------------------------------------------------------------------
// Fixtures
// ----------------------------------------------------------------------------

/// Build an [`IcebergTableConfig`] with the private hash-index path active.
///
/// Layout:
/// - `<warehouse>/` — `data_accessor_config` root; shared FS accessor
/// - `<warehouse>/<ns>/<tbl>/` — Iceberg table root (chosen by iceberg)
/// - `<warehouse>/_mooncake_private/` — private hash-index root (sibling of
///   the table root, so the overlap invariant holds yet the same FS accessor
///   still reaches it for IO).
fn config_with_private_storage(warehouse: &TempDir) -> IcebergTableConfig {
    let warehouse_str = warehouse.path().to_str().unwrap().to_string();
    let mut cfg = create_iceberg_table_config(warehouse_str.clone());
    cfg.hash_index_private_storage = Some(PrivateHashIndexConfig {
        private_root: format!("{warehouse_str}/_mooncake_private"),
    });
    cfg
}

/// Minimal-schema file index referencing one data file. `index_blocks` is
/// intentionally empty — the puffin still serializes a valid `FileIndexBlob`
/// envelope, which is all the roundtrip needs to exercise.
fn empty_block_file_index(data_files: Vec<MooncakeDataFileRef>) -> GlobalIndex {
    GlobalIndex {
        files: data_files,
        num_rows: 0,
        hash_bits: 0,
        hash_upper_bits: 0,
        hash_lower_bits: 0,
        seg_id_bits: 0,
        row_id_bits: 0,
        bucket_bits: 0,
        index_blocks: vec![],
    }
}

/// Write one Parquet file to a local path so [`IcebergTableManager::sync_snapshot`]
/// has something concrete to upload.
async fn write_local_parquet(path: &std::path::Path, ids: &[i32], names: &[&str]) {
    let schema = create_test_arrow_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
            Arc::new(Int32Array::from(ids.iter().map(|i| i * 10).collect::<Vec<_>>())),
        ],
    )
    .unwrap();
    let file = tokio::fs::File::create(path).await.unwrap();
    let mut w = AsyncArrowWriter::try_new(file, schema, None).unwrap();
    w.write(&batch).await.unwrap();
    w.close().await.unwrap();
}

async fn fresh_manager(
    cfg: &IcebergTableConfig,
    table_dir: &TempDir,
    cache_dir: &TempDir,
) -> IcebergTableManager {
    let table_metadata = create_test_table_metadata(table_dir.path().to_str().unwrap().to_string());
    IcebergTableManager::new(
        table_metadata,
        create_test_object_storage_cache(cache_dir),
        create_test_filesystem_accessor(cfg),
        cfg.clone(),
    )
    .await
    .unwrap()
}

/// Walk every manifest in the current Iceberg snapshot and return whether
/// any entry is a hash-index puffin (`is_file_index`). The headline
/// invariant of the private-storage path is that this returns `false`.
async fn manifest_list_has_hash_index_entry(mgr: &IcebergTableManager) -> bool {
    let table = mgr.iceberg_table.as_ref().unwrap();
    let table_metadata = table.metadata();
    let Some(snapshot) = table_metadata.current_snapshot() else {
        return false;
    };
    let file_io = table.file_io();
    let manifest_list = snapshot
        .load_manifest_list(file_io, table_metadata)
        .await
        .unwrap();
    for mf in manifest_list.entries() {
        let manifest = mf.load_manifest(file_io).await.unwrap();
        let (entries, _) = manifest.into_parts();
        for entry in entries.iter() {
            if iceberg_utils::is_file_index(entry.as_ref()) {
                return true;
            }
        }
    }
    false
}

fn snapshot_summary_hash_index_value(mgr: &IcebergTableManager) -> Option<String> {
    mgr.iceberg_table
        .as_ref()
        .unwrap()
        .metadata()
        .current_snapshot()
        .unwrap()
        .summary()
        .additional_properties
        .get(SNAPSHOT_SUMMARY_HASH_INDEX_KEY)
        .cloned()
}

fn payload_for_flush(
    flush_lsn: u64,
    new_data_files: Vec<MooncakeDataFileRef>,
    new_file_indices: Vec<GlobalIndex>,
    old_file_indices_to_remove: Vec<GlobalIndex>,
) -> PersistenceSnapshotPayload {
    PersistenceSnapshotPayload {
        uuid: uuid::Uuid::new_v4(),
        flush_lsn,
        new_table_schema: None,
        committed_deletion_logs: HashSet::new(),
        import_payload: PersistenceSnapshotImportPayload {
            data_files: new_data_files,
            new_deletion_vector: HashMap::new(),
            file_indices: new_file_indices,
        },
        index_merge_payload: PersistenceSnapshotIndexMergePayload {
            new_file_indices_to_import: vec![],
            old_file_indices_to_remove,
        },
        data_compaction_payload: PersistenceSnapshotDataCompactionPayload {
            new_data_files_to_import: vec![],
            old_data_files_to_remove: vec![],
            new_file_indices_to_import: vec![],
            old_file_indices_to_remove: vec![],
            data_file_records_remap: HashMap::new(),
        },
    }
}

// ----------------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------------

/// Sanity roundtrip: one snapshot with a hash index goes through commit and
/// rehydrates correctly, while the Iceberg `manifest_list` carries no
/// hash-index puffin entry.
#[tokio::test]
async fn write_roundtrip_keeps_hash_index_out_of_manifest_list() {
    let warehouse = tempfile::tempdir().unwrap();
    let table_dir = tempfile::tempdir().unwrap();
    let cache_dir = tempfile::tempdir().unwrap();
    let cfg = config_with_private_storage(&warehouse);

    let mut mgr = fresh_manager(&cfg, &table_dir, &cache_dir).await;
    let parquet_path = table_dir.path().join("data-1.parquet");
    write_local_parquet(&parquet_path, &[1, 2, 3], &["a", "b", "c"]).await;
    let data_file = create_data_file(0, parquet_path.to_str().unwrap().to_string());
    let file_index = empty_block_file_index(vec![data_file.clone()]);

    mgr.sync_snapshot(
        payload_for_flush(1, vec![data_file], vec![file_index], vec![]),
        PersistenceFileParams { table_auto_incr_ids: 1..2 },
    )
    .await
    .unwrap();

    // P0.3 — invariant #1: no hash-index puffin lives in `manifest_list`.
    assert!(
        !manifest_list_has_hash_index_entry(&mgr).await,
        "private storage active but manifest_list still references hash-index puffin"
    );

    // Summary key is present and well-formed.
    let raw = snapshot_summary_hash_index_value(&mgr)
        .expect("private storage active should publish moonlink.hash-index key");
    assert!(raw.contains(&format!("\"schema_version\":\"{SCHEMA_VERSION_V1}\"")));

    // Fresh manager reload sees the same single index.
    let mut reloader = fresh_manager(&cfg, &table_dir, &cache_dir).await;
    let (_, snapshot) = reloader.load_snapshot_from_table().await.unwrap();
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(reloader.persisted_hash_index_state.len(), 1);
}

/// P0.1 main regression guard for Blocking #2.
///
/// Snapshot N publishes a hash index. Snapshot N+1 flushes ONLY a data
/// file — no hash-index mutation. The previous draft's delta-only summary
/// would leave snapshot N+1 with no `moonlink.hash-index` key, and the
/// next loader would forget the index. With the live-state contract the
/// pointer must round-trip unchanged.
#[tokio::test]
async fn data_only_flush_preserves_hash_index_in_summary() {
    let warehouse = tempfile::tempdir().unwrap();
    let table_dir = tempfile::tempdir().unwrap();
    let cache_dir = tempfile::tempdir().unwrap();
    let cfg = config_with_private_storage(&warehouse);
    let mut mgr = fresh_manager(&cfg, &table_dir, &cache_dir).await;

    // Snapshot 1 — data + hash index.
    let p1 = table_dir.path().join("data-1.parquet");
    write_local_parquet(&p1, &[1, 2], &["a", "b"]).await;
    let df1 = create_data_file(0, p1.to_str().unwrap().to_string());
    let fi1 = empty_block_file_index(vec![df1.clone()]);
    mgr.sync_snapshot(
        payload_for_flush(1, vec![df1], vec![fi1.clone()], vec![]),
        PersistenceFileParams { table_auto_incr_ids: 1..2 },
    )
    .await
    .unwrap();
    let summary_after_first = snapshot_summary_hash_index_value(&mgr)
        .expect("snapshot 1 must publish the hash-index key");

    // Snapshot 2 — data only, NO new/removed hash indexes.
    let p2 = table_dir.path().join("data-2.parquet");
    write_local_parquet(&p2, &[3, 4], &["c", "d"]).await;
    let df2 = create_data_file(2, p2.to_str().unwrap().to_string());
    mgr.sync_snapshot(
        payload_for_flush(2, vec![df2], vec![], vec![]),
        PersistenceFileParams { table_auto_incr_ids: 3..4 },
    )
    .await
    .unwrap();

    let summary_after_second = snapshot_summary_hash_index_value(&mgr).expect(
        "data-only snapshot must still publish the live hash-index set; \
         a missing key here means we regressed Blocking #2",
    );
    assert_eq!(
        summary_after_first, summary_after_second,
        "live hash-index set must remain identical when no index op occurred",
    );

    // Reload from disk: index must survive.
    let mut reloader = fresh_manager(&cfg, &table_dir, &cache_dir).await;
    let (_, snapshot) = reloader.load_snapshot_from_table().await.unwrap();
    assert_eq!(
        snapshot.indices.file_indices.len(),
        1,
        "reload must recover the hash index that snapshot 1 published",
    );
}

/// P0.1 — removing every hash index leaves an empty live set, and the
/// summary key is omitted (loader treats absence as "no indexes").
#[tokio::test]
async fn removal_clears_summary_key() {
    let warehouse = tempfile::tempdir().unwrap();
    let table_dir = tempfile::tempdir().unwrap();
    let cache_dir = tempfile::tempdir().unwrap();
    let cfg = config_with_private_storage(&warehouse);
    let mut mgr = fresh_manager(&cfg, &table_dir, &cache_dir).await;

    let p1 = table_dir.path().join("data-1.parquet");
    write_local_parquet(&p1, &[1], &["a"]).await;
    let df1 = create_data_file(0, p1.to_str().unwrap().to_string());
    let fi1 = empty_block_file_index(vec![df1.clone()]);
    mgr.sync_snapshot(
        payload_for_flush(1, vec![df1], vec![fi1.clone()], vec![]),
        PersistenceFileParams { table_auto_incr_ids: 1..2 },
    )
    .await
    .unwrap();
    assert!(snapshot_summary_hash_index_value(&mgr).is_some());

    // Remove the index. `file_indices_to_remove` uses the same FileIndex
    // object that was loaded into `persisted_hash_index_state` — keys match
    // because both the loader and `publish_hash_index_to_summary` insert
    // the remote-path-rewritten `MooncakeFileIndex`.
    let mut to_remove: Vec<GlobalIndex> =
        mgr.persisted_hash_index_state.keys().cloned().collect();
    assert_eq!(to_remove.len(), 1);
    let removed = to_remove.pop().unwrap();
    mgr.sync_snapshot(
        payload_for_flush(2, vec![], vec![], vec![removed]),
        PersistenceFileParams { table_auto_incr_ids: 3..4 },
    )
    .await
    .unwrap();

    assert!(
        snapshot_summary_hash_index_value(&mgr).is_none(),
        "empty live set must omit the summary key entirely",
    );
    assert!(mgr.persisted_hash_index_state.is_empty());
}

/// P0.1 — `initialize_iceberg_table_for_once` rejects a `private_root`
/// that overlaps the Iceberg table root. Pure unit-level for the lift, but
/// run through the manager init path so we also catch wiring regressions.
#[tokio::test]
async fn overlapping_private_root_is_rejected_at_init() {
    let warehouse = tempfile::tempdir().unwrap();
    let table_dir = tempfile::tempdir().unwrap();
    let cache_dir = tempfile::tempdir().unwrap();
    let mut cfg = create_iceberg_table_config(warehouse.path().to_str().unwrap().to_string());
    // Point private_root at the warehouse itself: iceberg places its table
    // under `<warehouse>/<namespace>/<table>`, so the table root will be a
    // descendant of private_root and the validator must reject — without
    // depending on which exact constants iceberg picked for ns/tbl names.
    cfg.hash_index_private_storage = Some(PrivateHashIndexConfig {
        private_root: warehouse.path().to_str().unwrap().to_string(),
    });
    let mut mgr = fresh_manager(&cfg, &table_dir, &cache_dir).await;

    // sync_snapshot lazily initializes the iceberg table — the validation
    // runs in that path. An empty payload is enough to drive it.
    let result = mgr
        .sync_snapshot(
            payload_for_flush(0, vec![], vec![], vec![]),
            PersistenceFileParams { table_auto_incr_ids: 0..1 },
        )
        .await;
    let location = mgr
        .iceberg_table
        .as_ref()
        .map(|t| t.metadata().location().to_string())
        .unwrap_or_default();
    let err = result.expect_err(&format!(
        "private root overlapping the table root must be rejected; \
         iceberg location={location:?}, private_root={:?}",
        mgr.config
            .hash_index_private_storage
            .as_ref()
            .map(|c| c.private_root.clone())
    ));
    let msg = format!("{err}");
    assert!(
        msg.contains("private hash-index root overlaps"),
        "unexpected error message: {msg}",
    );
}
