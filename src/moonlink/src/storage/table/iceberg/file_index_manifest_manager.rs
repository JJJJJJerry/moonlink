use crate::storage::table::iceberg::manifest_utils;
use crate::storage::table::iceberg::manifest_utils::ManifestEntryType;

use std::collections::HashSet;
use std::sync::Arc;

use iceberg::io::FileIO;
use iceberg::spec::{ManifestEntry, ManifestFile, ManifestMetadata, ManifestWriter, TableMetadata};
use iceberg::Result as IcebergResult;

// Legacy manifest-list hash-index compatibility path. Tables that still carry
// hash-index entries inside the Iceberg manifest list go through this writer
// during recovery; new hash blobs flow to `PrivateManifestStore` instead. The
// writer can be removed once telemetry confirms there are no remaining
// tables emitting such entries.
#[allow(dead_code)]
pub(crate) struct FileIndexManifestManager<'a> {
    table_metadata: &'a TableMetadata,
    file_io: &'a FileIO,
    index_puffin_blobs_to_remove: &'a HashSet<String>,
    writer: Option<ManifestWriter>,
}

#[allow(dead_code)]
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
        // The file-index manifest used content=Data + file_format=Puffin
        // (mooncake's own convention) and follows the same V1/V2/V3 dispatch
        // as data-file manifests.
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

    /// Finalize the current manifest file and return.
    ///
    /// Only emits a manifest file when `add_manifest_entries` was invoked
    /// (i.e. legacy file-index entries survived the prune filter). Fresh hash
    /// blobs flow to `PrivateManifestStore` instead.
    pub(crate) async fn finalize(self) -> IcebergResult<Option<ManifestFile>> {
        if let Some(writer) = self.writer {
            let manifest_file = writer.write_manifest_file().await?;
            return Ok(Some(manifest_file));
        }
        Ok(None)
    }
}
