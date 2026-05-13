// iceberg-rust currently doesn't support writing puffin metadata into manifests, so we keep
// the manifest rewrite helpers here.
// 1. the start offset and blob size for each deletion vector
// 2. append blob metadata into manifest file
//
// deletion vector spec:
// issue collection: https://github.com/apache/iceberg/issues/11122
// deletion vector table spec: https://github.com/apache/iceberg/pull/11240
//
// puffin blob spec: https://iceberg.apache.org/puffin-spec/?h=deletion#deletion-vector-v1-blob-type
//
// TODO(hjiang): Add documentation on how we store puffin blobs inside of puffinf file, what's the relationship between puffin file and manifest file, etc.

use crate::storage::table::iceberg::manifest_utils::{self, ManifestEntryType};

use std::collections::{HashMap, HashSet};

use crate::storage::table::iceberg::data_file_manifest_manager::DataFileManifestManager;
use crate::storage::table::iceberg::deletion_vector_manifest_manager::DeletionVectorManifestManager;
use crate::storage::table::iceberg::file_index_manifest_manager::FileIndexManifestManager;
use iceberg::io::FileIO;
use iceberg::puffin::{BlobMetadata, PuffinReader, PuffinWriter};
use iceberg::spec::{FormatVersion, ManifestListWriter, Snapshot, TableMetadata};
use iceberg::Result as IcebergResult;

pub(crate) type PuffinBlobMetadata = BlobMetadata;

pub(crate) async fn get_puffin_metadata_and_close(
    file_io: &FileIO,
    puffin_filepath: &str,
    puffin_writer: PuffinWriter,
) -> IcebergResult<Vec<PuffinBlobMetadata>> {
    puffin_writer.close().await?;
    let input_file = file_io.new_input(puffin_filepath)?;
    let puffin_reader = PuffinReader::new(input_file);
    let puffin_metadata = puffin_reader.file_metadata().await?;
    Ok(puffin_metadata.blobs().to_vec())

    // Previous transmute-based body (PR #71 era, removed in PR #2150):
    //
    // let proxy = unsafe {
    //     std::mem::transmute::<PuffinWriter, PuffinWriterProxy>(puffin_writer)
    // };
    // let puffin_metadata = proxy.written_blobs_metadata.clone();
    // let puffin_writer = unsafe {
    //     std::mem::transmute::<PuffinWriterProxy, PuffinWriter>(proxy)
    // };
    // puffin_writer.close().await?;
    // Ok(puffin_metadata)
}

/// Util function to create manifest list writer and delete current one.
async fn create_new_manifest_list_writer(
    table_metadata: &TableMetadata,
    cur_snapshot: &Snapshot,
    file_io: &FileIO,
) -> IcebergResult<ManifestListWriter> {
    // Overwrite the old manifest list file.
    let manifest_list_outfile = file_io.new_output(cur_snapshot.manifest_list())?;

    let latest_seq_no = table_metadata.last_sequence_number();
    let snapshot_id = cur_snapshot.snapshot_id();

    // DM(Jerry): match (not if/else) so a future FormatVersion::V4 fails to compile here
    // instead of silently downgrading the manifest-list header.
    let manifest_list_writer = match table_metadata.format_version() {
        FormatVersion::V1 => ManifestListWriter::v1(
            manifest_list_outfile,
            snapshot_id,
            /*parent_snapshot_id=*/ None,
        ),
        FormatVersion::V2 => ManifestListWriter::v2(
            manifest_list_outfile,
            snapshot_id,
            /*parent_snapshot_id=*/ None,
            latest_seq_no,
        ),
        // TODO(Jerry): once row-lineage is enabled, plumb the real starting row id from
        // cur_snapshot.first_row_id(). Passing None for now is enough to emit a valid V3
        // manifest-list header (format-version=3, first-row-id=null).
        FormatVersion::V3 => ManifestListWriter::v3(
            manifest_list_outfile,
            snapshot_id,
            /*parent_snapshot_id=*/ None,
            latest_seq_no,
            /*first_row_id=*/ None,
        ),
    };
    Ok(manifest_list_writer)
}

/// Get all manifest files and entries,
/// - Data file entries: retain all entries except those marked for removal due to compaction.
/// - Deletion vector entries: remove entries referencing data files to be removed, and merge retained deletion vectors with the provided puffin deletion vector blob.
/// - File indices entries: retain all entries except those marked for removal due to index merging or data file compaction.
///
/// For more details, please refer to https://docs.google.com/document/d/1fIvrRfEHWBephsX0Br2G-Ils_30JIkmGkcdbFbovQjI/edit?usp=sharing
///
/// Note: this function should be called before catalog transaction commit.
///
/// # Arguments:
///
/// * data_files_to_remove: remote data file path, if non empty, both data file and deletion vector manifest entries should be updated.
/// * index_puffin_blobs_to_remove: remote file index puffin file path, if non empty, file index manifest entries should be updated.
///
/// TODO(hjiang):
/// 1. There're too many sequential IO operations to rewrite deletion vectors, need to optimize.
/// 2. Could optimize to avoid file indices manifest file to rewrite.
pub(crate) async fn append_puffin_metadata_and_rewrite(
    table_metadata: &TableMetadata,
    file_io: &FileIO,
    deletion_vector_blobs_to_add: &HashMap<String, Vec<PuffinBlobMetadata>>,
    file_index_blobs_to_add: &HashMap<String, Vec<PuffinBlobMetadata>>,
    data_files_to_remove: &HashSet<String>,
    index_puffin_blobs_to_remove: &HashSet<String>,
) -> IcebergResult<()> {
    if data_files_to_remove.is_empty()
        && deletion_vector_blobs_to_add.is_empty()
        && file_index_blobs_to_add.is_empty()
        && index_puffin_blobs_to_remove.is_empty()
    {
        return Ok(());
    }

    let cur_snapshot = table_metadata.current_snapshot().unwrap();
    let manifest_list = cur_snapshot
        .load_manifest_list(file_io, table_metadata)
        .await?;

    // Delete existing manifest list file and rewrite.
    let mut manifest_list_writer =
        create_new_manifest_list_writer(table_metadata, cur_snapshot, file_io).await?;

    // Manifest manager for data files, deletion vectors and file indices.
    let mut data_file_manifest_manager =
        DataFileManifestManager::new(table_metadata, file_io, data_files_to_remove);
    let mut deletion_vector_manifest_manager =
        DeletionVectorManifestManager::new(table_metadata, file_io, data_files_to_remove);
    let mut file_index_manifest_manager =
        FileIndexManifestManager::new(table_metadata, file_io, index_puffin_blobs_to_remove);

    // How to tell different manifest entry types:
    // - Data file: manifest content type `Data`, manifest entry file format `Parquet`
    // - Deletion vector: manifest content type `Deletes`, manifest entry file format `Puffin`
    // - File indices: manifest content type `Data`, manifest entry file format `Puffin`
    //
    // Precondition for manifest entries updates:
    // - Data file: [`data_files_to_remove`] is non empty.
    // - Deletion vector: [`deletion_vector_blobs_to_add`] is non empty, or [`data_files_to_remove`] is non empty.
    // - File index: [`file_index_blobs_to_add`] is non empty, or [`index_puffin_blobs_to_remove`] is non empty.
    for cur_manifest_file in manifest_list.entries() {
        let manifest = cur_manifest_file.load_manifest(file_io).await?;
        let (manifest_entries, manifest_metadata) = manifest.into_parts();

        // Assumption: we store all data file manifest entries in one manifest file.
        assert!(!manifest_entries.is_empty());

        // Check for data file entries, see if there're updates.
        let manifest_entry_type =
            manifest_utils::get_manifest_entry_type(&manifest_entries, &manifest_metadata);
        if manifest_entry_type == ManifestEntryType::DataFile && data_files_to_remove.is_empty() {
            manifest_list_writer.add_manifests([cur_manifest_file.clone()].into_iter())?;
            continue;
        }

        // Check for deletion vector entries, see if there're updates.
        if manifest_entry_type == ManifestEntryType::DeletionVector
            && deletion_vector_blobs_to_add.is_empty()
            && data_files_to_remove.is_empty()
        {
            manifest_list_writer.add_manifests([cur_manifest_file.clone()].into_iter())?;
            continue;
        }

        // TODO(Jerry) B-1 legacy: this `ManifestEntryType::FileIndex` early-return is
        // dead code in greenfield deployments — post-B-1 hash blobs never enter the
        // Iceberg manifest_list (they go to PrivateManifestStore). The branch is kept
        // **only** to handle pre-B-1 tables that still carry Data+Puffin entries in
        // their manifest_list, so a subsequent commit can copy those entries forward
        // (no rewrite) or expire them. Delete together with the matching arm in the
        // `match manifest_entry_type` below and the `FileIndex` finalize() call once
        // we are certain no pre-B-1 table exists (telemetry-driven; see Phase E in
        // hash_index_refactor/ROADMAP.md).
        if manifest_entry_type == ManifestEntryType::FileIndex
            && file_index_blobs_to_add.is_empty()
            && index_puffin_blobs_to_remove.is_empty()
        {
            manifest_list_writer.add_manifests([cur_manifest_file.clone()].into_iter())?;
            continue;
        }

        match manifest_entry_type {
            ManifestEntryType::DataFile => {
                data_file_manifest_manager
                    .add_manifest_entries(manifest_entries, manifest_metadata)
                    .await?;
            }
            ManifestEntryType::DeletionVector => {
                deletion_vector_manifest_manager
                    .add_manifest_entries(manifest_entries, manifest_metadata)?;
            }
            // TODO(Jerry) B-1 legacy: post-B-1 commits never produce new FileIndex
            // manifest entries; this arm only fires when ingesting a manifest_list
            // inherited from a pre-B-1 table. Remove once pre-B-1 tables are extinct.
            ManifestEntryType::FileIndex => {
                file_index_manifest_manager
                    .add_manifest_entries(manifest_entries, manifest_metadata)?;
            }
        }
    }

    // Append puffin blobs into existing manifest entries.
    deletion_vector_manifest_manager.add_new_puffin_blobs(deletion_vector_blobs_to_add)?;
    // DM(Jerry) B-1: hash-index puffin blobs are no longer registered into the Iceberg
    // standard manifest_list — that entry shape (Data + Puffin) is out-of-spec and breaks
    // Spark / pyiceberg readers. New hash blobs flow to Mode 2a PrivateManifestStore in B-2.
    //
    // TODO(Jerry) B-1 legacy retention rationale (kept, not deleted, on purpose):
    // - `file_index_blobs_to_add` and `index_puffin_blobs_to_remove` parameters are still
    //   on the signature for ABI stability with the upstream `pg_mooncake` callers and
    //   to keep the diff against upstream `moonlink` small while Phase B stabilizes.
    // - The `ManifestEntryType::FileIndex` branches above (early-return + manager fan-out
    //   + finalize) are dead code in greenfield deployments but required to migrate any
    //   pre-B-1 table whose manifest_list still carries Data+Puffin entries.
    // - Greenfield-only environments (no pre-B-1 tables, no upstream-shape callers) may
    //   delete these branches together with `FileIndexManifestManager`, the `FileIndex`
    //   enum variant, and these two parameters. Gate the deletion on: (a) zero hits in
    //   the FileIndex branch counter (telemetry to be added in Phase E), and (b) explicit
    //   confirmation that the public moonlink API does not need to keep accepting these
    //   parameters for downstream forks.
    let _ = file_index_blobs_to_add; // routed to PrivateManifestStore by iceberg_table_syncer post-commit

    // Attempt to finalize all existing manifest entries.
    if let Some(manifest_file) = data_file_manifest_manager.finalize().await? {
        manifest_list_writer.add_manifests(std::iter::once(manifest_file))?;
    }
    if let Some(manifest_file) = deletion_vector_manifest_manager.finalize().await? {
        manifest_list_writer.add_manifests(std::iter::once(manifest_file))?;
    }
    // TODO(Jerry) B-1 legacy: finalize() is a no-op for greenfield tables (the manager
    // never received any entries because new hash blobs bypass this path). Retained to
    // cover the pre-B-1 migration case described above. Remove together with the two
    // FileIndex branches in the loop once Phase E telemetry confirms zero hits.
    if let Some(manifest_file) = file_index_manifest_manager.finalize().await? {
        manifest_list_writer.add_manifests(std::iter::once(manifest_file))?;
    }

    // Flush the manifest list, there's no need to rewrite metadata.
    manifest_list_writer.close().await?;

    Ok(())
}
