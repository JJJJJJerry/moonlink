use std::collections::{HashMap, HashSet};

use deltalake::kernel::LogicalFileView;
use deltalake::{DeltaResult, DeltaTable};
use futures::{Stream, TryStreamExt};

use crate::storage::index::MooncakeIndex;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::mooncake_table::{DiskFileEntry, Snapshot as MooncakeSnapshot};
use crate::storage::storage_utils::MooncakeDataFileRef;
use crate::storage::table::common::MOONCAKE_TABLE_FLUSH_LSN;
use crate::storage::table::deltalake::deltalake_table_manager::DeltalakeTableManager;
use crate::{create_data_file, Result};

impl DeltalakeTableManager {
    // DM(Jerry): deltalake 0.31 removed `DeltaTableState::file_actions(&dyn LogStore) -> Vec<Add>`.
    // The replacement is `EagerSnapshot::file_views(log_store, predicate)`, which yields a stream of
    // typed `LogicalFileView`s instead of raw `Add` records. We consume that stream directly rather
    // than rebuilding `Add` structs, since `LogicalFileView::path()` / `size()` already cover every
    // field we care about for `DiskFileEntry`.
    #[allow(unused)]
    async fn load_data_files(
        mut stream: impl Stream<Item = DeltaResult<LogicalFileView>> + Unpin,
        next_file_id: &mut i32,
    ) -> Result<HashMap<MooncakeDataFileRef, DiskFileEntry>> {
        let mut disk_files = HashMap::new();
        while let Some(file_view) = stream.try_next().await? {
            let cur_file_id = *next_file_id;
            *next_file_id += 1;
            let path: String = file_view.path().into_owned();
            let data_file = create_data_file(cur_file_id as u64, path);
            let disk_file_entry = DiskFileEntry {
                cache_handle: None,
                num_rows: 0, // TODO(hjiang): Record and recover.
                file_size: file_view.size() as usize,
                committed_deletion_vector: BatchDeletionVector::new(/*max_rows=*/ 0),
                puffin_deletion_blob: None,
            };
            assert!(disk_files.insert(data_file, disk_file_entry).is_none());
        }
        Ok(disk_files)
    }

    #[allow(unused)]
    async fn get_flush_lsn(table: &DeltaTable) -> Result<u64> {
        let commit_infos = table.history(/*limit=*/ Some(1)).await?;
        let latest_commit = commit_infos.last().unwrap();
        let flush_lsn_json = latest_commit.info.get(MOONCAKE_TABLE_FLUSH_LSN).unwrap();
        let flush_lsn = flush_lsn_json.as_u64().unwrap();
        Ok(flush_lsn)
    }

    #[allow(unused)]
    pub(crate) async fn load_snapshot_from_table_impl(
        &mut self,
    ) -> Result<(u32, MooncakeSnapshot)> {
        assert!(!self.snapshot_loaded);
        self.snapshot_loaded = true;

        // Unique file id to assign to every data file.
        let mut next_file_id = 0;

        // Handle cases where delta table doesn't exist.
        self.initialize_table_if_exists().await?;
        if self.table.is_none() {
            let empty_mooncake_snapshot =
                MooncakeSnapshot::new(self.mooncake_table_metadata.clone());
            return Ok((next_file_id as u32, empty_mooncake_snapshot));
        }
        // TODO(hjiang): Validate schema before operation.

        // Handle cases where no snapshot.
        let table = self.table.as_ref().unwrap();
        let state = table.snapshot();
        if state.is_err() {
            let empty_mooncake_snapshot =
                MooncakeSnapshot::new(self.mooncake_table_metadata.clone());
            return Ok((next_file_id as u32, empty_mooncake_snapshot));
        }

        let state = state.unwrap();
        let flush_lsn = Self::get_flush_lsn(table).await?;

        // DM(Jerry): `file_views` borrows the LogStoreRef returned by `log_store()`; bind it to a
        // local so the trait-object reference outlives the stream.
        let log_store = table.log_store();
        let file_stream = state.snapshot().file_views(log_store.as_ref(), None);
        let disk_files = Self::load_data_files(file_stream, &mut next_file_id).await?;

        let mooncake_snapshot = MooncakeSnapshot {
            metadata: self.mooncake_table_metadata.clone(),
            disk_files,
            snapshot_version: flush_lsn,
            flush_lsn: Some(flush_lsn),
            largest_flush_lsn: Some(flush_lsn),
            indices: MooncakeIndex {
                in_memory_index: HashSet::new(),
                file_indices: Vec::new(),
            },
        };
        Ok((next_file_id as u32, mooncake_snapshot))
    }
}
