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

    // Use `match` rather than `if/else` so a future `FormatVersion::V4`
    // becomes a compile-time error here, rather than silently downgrading
    // the manifest-list header.
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
        // TODO: once row-lineage is enabled, plumb the real starting row id
        // from `cur_snapshot.first_row_id()`. Passing `None` for now emits a
        // valid V3 manifest-list header (format-version=3, first-row-id=null).
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
/// Note: `file_index_blobs_to_add` is accepted for ABI parity with the
/// catalog commit path but does not on its own trigger a manifest-list
/// rewrite. New hash-index puffin blobs flow to `PrivateManifestStore`
/// instead, so the only reasons to touch the Iceberg manifest list are
/// data-file removal, deletion-vector additions, or legacy hash-index
/// removal.
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
        && index_puffin_blobs_to_remove.is_empty()
    {
        // `file_index_blobs_to_add` non-empty alone never produces work
        // here — it is consumed via `PrivateManifestStore` after the
        // catalog commit. Skipping the manifest-list rewrite avoids
        // pointless metadata IO and shrinks the failure surface.
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

        // Legacy manifest-list hash-index compatibility: new hash blobs are
        // written to `PrivateManifestStore` instead, so the only reason to
        // touch a `FileIndex` manifest is to copy forward existing entries
        // inherited from a table that still carries Data+Puffin shapes in its
        // manifest list. When the current commit has nothing to add or remove
        // for this category, pass the manifest through unchanged.
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
            // Legacy manifest-list hash-index compatibility: only fires when
            // ingesting a manifest_list that already contains Data+Puffin
            // hash-index entries. New commits never emit them.
            ManifestEntryType::FileIndex => {
                file_index_manifest_manager
                    .add_manifest_entries(manifest_entries, manifest_metadata)?;
            }
        }
    }

    // Append puffin blobs into existing manifest entries.
    deletion_vector_manifest_manager.add_new_puffin_blobs(deletion_vector_blobs_to_add)?;
    // Hash-index puffin blobs are no longer registered into the Iceberg
    // manifest list — that entry shape (Data + Puffin) is rejected by
    // cross-engine readers (Spark / pyiceberg). New hash blobs flow to
    // `PrivateManifestStore` instead.
    //
    // The legacy compatibility surface is intentionally retained:
    // - `file_index_blobs_to_add` and `index_puffin_blobs_to_remove` remain on
    //   the function signature; they are routed to `PrivateManifestStore` by
    //   `iceberg_table_syncer` after `txn.commit`.
    // - The `ManifestEntryType::FileIndex` branches (early-return + manager
    //   fan-out + finalize) are dead code on fresh tables but required to
    //   ingest legacy manifest lists that still carry Data+Puffin entries.
    //
    // Removal conditions, once both are satisfied:
    //   1. Telemetry confirms zero `FileIndex`-branch hits across loaded
    //      manifest lists for the deployment lifetime of interest.
    //   2. No public API caller still relies on `file_index_blobs_to_add` /
    //      `index_puffin_blobs_to_remove` parameters.
    let _ = file_index_blobs_to_add; // routed to PrivateManifestStore by iceberg_table_syncer post-commit

    // Attempt to finalize all existing manifest entries.
    if let Some(manifest_file) = data_file_manifest_manager.finalize().await? {
        manifest_list_writer.add_manifests(std::iter::once(manifest_file))?;
    }
    if let Some(manifest_file) = deletion_vector_manifest_manager.finalize().await? {
        manifest_list_writer.add_manifests(std::iter::once(manifest_file))?;
    }
    // Legacy manifest-list hash-index compatibility finalize: no-op for tables
    // that never had Data+Puffin hash-index entries. See the removal
    // conditions above.
    if let Some(manifest_file) = file_index_manifest_manager.finalize().await? {
        manifest_list_writer.add_manifests(std::iter::once(manifest_file))?;
    }

    // Flush the manifest list, there's no need to rewrite metadata.
    manifest_list_writer.close().await?;

    Ok(())
}
