use async_trait::async_trait;
use iceberg::spec::{Schema as IcebergSchema, TableMetadata};
use iceberg::table::Table;
use iceberg::{Catalog, Result as IcebergResult, TableIdent};

use std::collections::{HashMap, HashSet};

use crate::storage::table::iceberg::puffin_writer_proxy::PuffinBlobMetadata;

pub enum PuffinBlobType {
    DeletionVector,
    FileIndex,
}

/// TODO(hjiang): iceberg-rust currently doesn't support puffin write, to workaround and reduce code change,
/// we record puffin metadata ourselves and rewrite manifest file before transaction commits.
#[async_trait]
pub trait PuffinWrite {
    /// Record persisted puffin metadata.
    fn record_puffin_metadata(
        &mut self,
        puffin_filepath: String,
        puffin_metadata: Vec<PuffinBlobMetadata>,
        puffin_blob_type: PuffinBlobType,
    );

    /// Set data files to remove, their corresponding deletion vectors will be removed alongside.
    fn set_data_files_to_remove(&mut self, data_files: HashSet<String>);

    /// Set puffin file to remove.
    fn set_index_puffin_files_to_remove(&mut self, puffin_filepaths: HashSet<String>);

    /// After transaction commits, puffin metadata should be cleared for next puffin write.
    fn clear_puffin_metadata(&mut self);

    /// Move out file-index puffin blob metadata recorded for the current transaction.
    ///
    /// Hash-index blobs do not enter the Iceberg `manifest_list`; the syncer
    /// drains them here right after `txn.commit` and persists them to the
    /// mooncake-private manifest. Calling this leaves the deletion-vector /
    /// removal sets intact for `clear_puffin_metadata`.
    ///
    /// Callers must only drain **after** the private manifest write has succeeded,
    /// otherwise the blob metadata is lost and a retry will silently emit a manifest
    /// missing the new entries. Use `peek_file_index_blobs_to_add` while the persist
    /// path is still fallible.
    fn take_file_index_blobs_to_add(&mut self) -> HashMap<String, Vec<PuffinBlobMetadata>>;

    /// Borrow the file-index puffin blob metadata without draining it.
    ///
    /// Lets the syncer attempt the private manifest write while leaving the catalog
    /// state intact, so any transient failure (marker IO, write_snap_manifest) can
    /// be retried by the next commit instead of producing a manifest missing the
    /// blobs that this commit introduced.
    fn peek_file_index_blobs_to_add(&self) -> &HashMap<String, Vec<PuffinBlobMetadata>>;
}

/// TODO(hjiang): iceberg-rust currently doesn't support schema evolution, to workaround and reduce code change,
/// we do schema evolution by directly setting table commits.
#[async_trait]
pub trait SchemaUpdate {
    /// Update table schema, and return the updated iceberg table.
    async fn update_table_schema(
        &mut self,
        new_schema: IcebergSchema,
        table_ident: TableIdent,
    ) -> IcebergResult<Table>;
}

#[async_trait]
pub trait CatalogAccess {
    /// Get warehouse location.
    #[allow(unused)]
    fn get_warehouse_location(&self) -> &str;

    /// Load metadata and its location foe the given table.
    async fn load_metadata(
        &self,
        table_ident: &TableIdent,
    ) -> IcebergResult<(String /*metadata_filepath*/, TableMetadata)>;
}

pub trait MoonlinkCatalog: PuffinWrite + SchemaUpdate + CatalogAccess + Catalog {}
impl<T: PuffinWrite + SchemaUpdate + CatalogAccess + Catalog> MoonlinkCatalog for T {}
