use iceberg::{arrow::ArrowFileReader, io::FileMetadata, io::InputFile, Result as IcebergResult};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};

/// Get parquet metadata from the given file.
pub(crate) async fn get_parquet_metadata(
    file_metadata: FileMetadata,
    input_file: InputFile,
) -> IcebergResult<ParquetMetaData> {
    let file_size_in_bytes = file_metadata.size;
    let reader = input_file.reader().await?;
    let mut arrow_file_reader = ArrowFileReader::new(file_metadata, reader);

    // TODO(hjiang): Check IO operation number and decide reader options.
    // As of now it's only accessing local files and will cached by filesystem.
    // DM(Jerry): `Optional` reproduces the parquet-55 runtime behavior of the prior
    // `with_*_indexes(true)` setters: load the page index if it exists, otherwise return Ok
    // with no index attached (see parquet-55 reader.rs:340-342, where `range_for_page_index()`
    // returns `None` and the function returns `Ok(())`). parquet-57's `impl From<bool> for
    // PageIndexPolicy` maps `true -> Required` as its preferred semantics, but that is stricter
    // than parquet-55 — `Required` errors with "missing offset index" on files that lack a
    // page index (parquet-57 parser.rs:309/315). mooncake's read path does not consume page
    // indexes (it only reads `row_groups()` + statistics), so `Optional` is the safe match.
    let parquet_meta_data_reader = ParquetMetaDataReader::new()
        .with_prefetch_hint(None)
        .with_page_index_policy(PageIndexPolicy::Optional);
    let parquet_metadata = parquet_meta_data_reader
        .load_and_finish(&mut arrow_file_reader, file_size_in_bytes)
        .await?;

    Ok(parquet_metadata)
}
