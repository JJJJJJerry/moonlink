use iceberg::io::FileIO;
use iceberg::puffin::BlobMetadata;
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, ManifestContentType,
    ManifestEntry, ManifestMetadata, ManifestWriter, ManifestWriterBuilder, Struct, TableMetadata,
};
use iceberg::{Error as IcebergError, ErrorKind, Result as IcebergResult};
use std::sync::Arc;
use uuid::Uuid;

/// Describes the deletion-vector reference fields a puffin manifest entry may carry.
/// Set to `Some` for deletion-vector blobs (which carry an offset/length pointing back into the
/// puffin file) and `None` for hash-index blobs.
pub(crate) struct PuffinBlobRef<'a> {
    pub referenced_data_file: &'a str,
    pub content_offset: i64,
    pub content_size_in_bytes: i64,
}

/// Build a [`DataFile`] manifest entry for one puffin blob (deletion vector or hash index).
///
/// DM(Jerry): centralizes the shared puffin-to-manifest mapping that was previously copy-pasted
/// across `deletion_vector_manifest_manager` and `file_index_manifest_manager`. `record_count` is
/// read from the blob property keyed by `cardinality_property`. `file_size_in_bytes` is kept as the
/// legacy `0` placeholder (not actually consumed for puffin entries; matches upstream behavior).
pub(crate) fn build_puffin_data_file(
    puffin_filepath: &str,
    blob_metadata: &BlobMetadata,
    content_type: DataContentType,
    cardinality_property: &str,
    dv_ref: Option<PuffinBlobRef<'_>>,
) -> IcebergResult<DataFile> {
    let cardinality = blob_metadata
        .properties()
        .get(cardinality_property)
        .ok_or_else(|| {
            IcebergError::new(
                ErrorKind::DataInvalid,
                format!("Puffin blob missing required property: {cardinality_property}"),
            )
        })?
        .parse::<u64>()
        .map_err(|e| {
            IcebergError::new(
                ErrorKind::DataInvalid,
                format!("Puffin blob {cardinality_property} property is not a valid u64: {e}"),
            )
        })?;

    let mut builder = DataFileBuilder::default();
    builder
        .content(content_type)
        .file_path(puffin_filepath.to_string())
        .file_format(DataFileFormat::Puffin)
        .partition(Struct::empty())
        .record_count(cardinality)
        // Not meaningful for puffin blob entries, but kept as the legacy value.
        .file_size_in_bytes(0);
    if let Some(dv) = dv_ref {
        builder
            .referenced_data_file(Some(dv.referenced_data_file.to_string()))
            .content_offset(Some(dv.content_offset))
            .content_size_in_bytes(Some(dv.content_size_in_bytes));
    }
    builder.build().map_err(|e| {
        IcebergError::new(
            ErrorKind::DataInvalid,
            format!("Failed to build puffin data file: {e}"),
        )
    })
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ManifestEntryType {
    DataFile,
    DeletionVector,
    FileIndex,
}

/// Util function to get type of the current manifest file.
/// Precondition: one manifest file only stores one type of manifest entries.
pub(crate) fn get_manifest_entry_type(
    manifest_entries: &[Arc<ManifestEntry>],
    manifest_metadata: &ManifestMetadata,
) -> ManifestEntryType {
    let file_format = manifest_entries.first().as_ref().unwrap().file_format();
    if *manifest_metadata.content() == ManifestContentType::Data
        && file_format == DataFileFormat::Parquet
    {
        return ManifestEntryType::DataFile;
    }
    if *manifest_metadata.content() == ManifestContentType::Deletes
        && file_format == DataFileFormat::Puffin
    {
        return ManifestEntryType::DeletionVector;
    }
    assert_eq!(*manifest_metadata.content(), ManifestContentType::Data);
    assert_eq!(file_format, DataFileFormat::Puffin);
    ManifestEntryType::FileIndex
}

/// Util function to create manifest write.
pub(crate) fn create_manifest_writer_builder(
    table_metadata: &TableMetadata,
    file_io: &FileIO,
) -> IcebergResult<ManifestWriterBuilder> {
    let manifest_writer_builder = ManifestWriterBuilder::new(
        file_io.new_output(format!(
            "{}/metadata/{}-m0.avro",
            table_metadata.location(),
            Uuid::now_v7()
        ))?,
        table_metadata.current_snapshot_id(),
        /*key_metadata=*/ None,
        table_metadata.current_schema().clone(),
        table_metadata.default_partition_spec().as_ref().clone(),
    );
    Ok(manifest_writer_builder)
}

/// Finalize a builder into a data-content manifest writer matching the table's format version.
///
/// DM(Jerry): centralized here so adding a future FormatVersion fails to compile in one place
/// instead of leaving silent V2-downgrade bugs scattered across managers.
pub(crate) fn build_data_manifest_writer(
    builder: ManifestWriterBuilder,
    format_version: FormatVersion,
) -> ManifestWriter {
    match format_version {
        FormatVersion::V1 => builder.build_v1(),
        FormatVersion::V2 => builder.build_v2_data(),
        FormatVersion::V3 => builder.build_v3_data(),
    }
}

/// Finalize a builder into a deletes-content manifest writer matching the table's format version.
/// V1 has no concept of deletion-vector / file-delete manifests, so it is rejected at runtime.
pub(crate) fn build_deletes_manifest_writer(
    builder: ManifestWriterBuilder,
    format_version: FormatVersion,
) -> IcebergResult<ManifestWriter> {
    match format_version {
        FormatVersion::V1 => Err(IcebergError::new(
            ErrorKind::FeatureUnsupported,
            "deletion-vector / delete manifests are not supported by Iceberg V1",
        )),
        FormatVersion::V2 => Ok(builder.build_v2_deletes()),
        FormatVersion::V3 => Ok(builder.build_v3_deletes()),
    }
}
