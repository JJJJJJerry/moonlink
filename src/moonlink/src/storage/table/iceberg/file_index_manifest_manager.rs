use crate::storage::table::iceberg::index::{
    MOONCAKE_HASH_INDEX_V1, MOONCAKE_HASH_INDEX_V1_CARDINALITY,
};
use crate::storage::table::iceberg::manifest_utils;
use crate::storage::table::iceberg::manifest_utils::ManifestEntryType;
use crate::storage::table::iceberg::puffin_writer_proxy::PuffinBlobMetadata;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFile, ManifestEntry, ManifestFile, ManifestMetadata, ManifestWriter,
    TableMetadata,
};
use iceberg::Result as IcebergResult;

pub(crate) struct FileIndexManifestManager<'a> {
    table_metadata: &'a TableMetadata,
    file_io: &'a FileIO,
    index_puffin_blobs_to_remove: &'a HashSet<String>,
    writer: Option<ManifestWriter>,
}

impl<'a> FileIndexManifestManager<'a> {
    pub(crate) fn new(
        table_metadata: &'a TableMetadata,
        file_io: &'a FileIO,
        index_puffin_blobs_to_remove: &'a HashSet<String>,
    ) -> FileIndexManifestManager<'a> {
        Self {
            table_metadata,
            file_io,
            index_puffin_blobs_to_remove,
            writer: None,
        }
    }

    fn init_writer_for_once(&mut self) -> IcebergResult<()> {
        if self.writer.is_some() {
            return Ok(());
        }
        let new_writer_builder =
            manifest_utils::create_manifest_writer_builder(self.table_metadata, self.file_io)?;
        // DM(Jerry): file-index manifest is content=Data + file_format=Puffin (mooncake's
        // own convention); it follows the same V1/V2/V3 dispatch as data-file manifests.
        let new_writer = manifest_utils::build_data_manifest_writer(
            new_writer_builder,
            self.table_metadata.format_version(),
        );
        self.writer = Some(new_writer);
        Ok(())
    }

    pub(crate) fn add_manifest_entries(
        &mut self,
        manifest_entries: Vec<Arc<ManifestEntry>>,
        manifest_metadata: ManifestMetadata,
    ) -> IcebergResult<()> {
        assert_eq!(
            manifest_utils::get_manifest_entry_type(&manifest_entries, &manifest_metadata),
            ManifestEntryType::FileIndex
        );
        for cur_manifest_entry in manifest_entries.into_iter() {
            // Skip file indices which are requested to remove (due to index merge and data file compaction).
            if self
                .index_puffin_blobs_to_remove
                .contains(cur_manifest_entry.data_file().file_path())
            {
                continue;
            }

            // Keep file indices which are not requested to remove.
            self.init_writer_for_once()?;
            self.writer.as_mut().unwrap().add_file(
                cur_manifest_entry.data_file().clone(),
                cur_manifest_entry.sequence_number().unwrap(),
            )?;
        }
        Ok(())
    }

    pub(crate) fn add_new_puffin_blobs(
        &mut self,
        file_index_blobs_to_add: &HashMap<String, Vec<PuffinBlobMetadata>>,
    ) -> IcebergResult<()> {
        for (puffin_filepath, blob_metadata) in file_index_blobs_to_add.iter() {
            for cur_blob_metadata in blob_metadata.iter() {
                let data_file = get_data_file_for_file_index(puffin_filepath, cur_blob_metadata)?;
                self.init_writer_for_once()?;
                self.writer
                    .as_mut()
                    .unwrap()
                    .add_file(data_file, cur_blob_metadata.sequence_number())?;
            }
        }
        Ok(())
    }

    /// Finalize the current manifest file and return.
    pub(crate) async fn finalize(self) -> IcebergResult<Option<ManifestFile>> {
        if let Some(writer) = self.writer {
            let manifest_file = writer.write_manifest_file().await?;
            return Ok(Some(manifest_file));
        }
        Ok(None)
    }
}

/// Util function to get `DataFile` for new file index puffin blob.
fn get_data_file_for_file_index(
    puffin_filepath: &str,
    blob_metadata: &PuffinBlobMetadata,
) -> IcebergResult<DataFile> {
    assert_eq!(blob_metadata.blob_type(), MOONCAKE_HASH_INDEX_V1);
    manifest_utils::build_puffin_data_file(
        puffin_filepath,
        blob_metadata,
        DataContentType::Data,
        MOONCAKE_HASH_INDEX_V1_CARDINALITY,
        None,
    )
}
