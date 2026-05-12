use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor_config::AccessorConfig;
use crate::storage::filesystem::storage_config::StorageConfig;
use crate::storage::table::iceberg::parquet_utils;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use iceberg::io::{FileIO, FileIOBuilder, StorageFactory};
use iceberg::spec::DataFile;
use iceberg::spec::TableMetadata as IcebergTableMetadata;
use iceberg::table::Table as IcebergTable;
use iceberg::writer::file_writer::location_generator::{
    DefaultLocationGenerator, LocationGenerator,
};
use iceberg::{Error as IcebergError, Result as IcebergResult};
use iceberg_storage_opendal::OpenDalStorageFactory;

/// Get a unique filepath for iceberg table data filepath.
fn generate_unique_data_filepath(
    table: &IcebergTable,
    local_filepath: &String,
) -> IcebergResult<String> {
    let filename_without_suffix = std::path::Path::new(local_filepath)
        .file_stem()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
    let remote_filepath = location_generator.generate_location(
        /*partition_key=*/ None,
        &format!(
            "{}-{}.parquet",
            filename_without_suffix,
            uuid::Uuid::now_v7()
        ),
    );
    Ok(remote_filepath)
}

/// Write the given record batch in the given local file to the iceberg table (parquet file keeps unchanged).
pub(crate) async fn write_record_batch_to_iceberg(
    table: &IcebergTable,
    local_filepath: &String,
    table_metadata: &IcebergTableMetadata,
    filesystem_accessor: &dyn BaseFileSystemAccess,
) -> IcebergResult<DataFile> {
    let remote_filepath = generate_unique_data_filepath(table, local_filepath)?;
    // Import local parquet file to remote.
    filesystem_accessor
        .copy_from_local_to_remote(local_filepath, &remote_filepath)
        .await
        .map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to copy from {local_filepath} to {remote_filepath}"),
            )
            .with_retryable(true)
            .with_source(e)
        })?;

    // Get data file from local parquet file.
    let data_file = parquet_utils::get_data_file_from_local_parquet_file(
        local_filepath,
        remote_filepath,
        table_metadata,
    )
    .await?;
    Ok(data_file)
}

/// Copy the given local index file to iceberg table, and return filepath within iceberg table.
pub(crate) async fn upload_index_file(
    table: &IcebergTable,
    local_index_filepath: &str,
    filesystem_accessor: &dyn BaseFileSystemAccess,
) -> IcebergResult<String> {
    let filename = Path::new(local_index_filepath)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let remote_filepath =
        location_generator.generate_location(/*partition_key=*/ None, &filename);
    filesystem_accessor
        .copy_from_local_to_remote(local_index_filepath, &remote_filepath)
        .await
        .map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to copy from {local_index_filepath} to {remote_filepath}"),
            )
            .with_retryable(true)
            .with_source(e)
        })?;
    Ok(remote_filepath)
}

/// Create iceberg [`FileIO`].
pub(crate) fn create_file_io(accessor_config: &AccessorConfig) -> IcebergResult<FileIO> {
    let (factory, props) = create_storage_factory_and_props(accessor_config)?;
    Ok(FileIOBuilder::new(factory).with_props(props).build())
}

/// Build the iceberg storage factory and the matching `FileIO` props in one pass.
///
/// DM(Jerry): factory + props were previously two `fn`s that independently `match`ed the same
/// `storage_config`. Merging removes the foot-gun where adding a backend on one side without the
/// other would yield a runtime mismatch (factory says S3 / props say Azure → cryptic auth
/// failures at first IO). Adding a new backend now touches a single arm.
pub(crate) fn create_storage_factory_and_props(
    accessor_config: &AccessorConfig,
) -> IcebergResult<(Arc<dyn StorageFactory>, HashMap<String, String>)> {
    // DM(Jerry): `mut` is only used when storage-gcs / storage-s3 features are enabled;
    // under bare `storage-fs` the empty match leaves it untouched.
    #[allow(unused_mut)]
    let mut props: HashMap<String, String> = HashMap::new();
    let factory: Arc<dyn StorageFactory> = match &accessor_config.storage_config {
        #[cfg(feature = "storage-fs")]
        StorageConfig::FileSystem { .. } => Arc::new(OpenDalStorageFactory::Fs),
        #[cfg(feature = "storage-gcs")]
        StorageConfig::Gcs {
            project,
            region,
            endpoint,
            disable_auth,
            access_key_id,
            secret_access_key,
            ..
        } => {
            if *disable_auth {
                let endpoint = endpoint.as_ref().ok_or_else(|| {
                    IcebergError::new(
                        iceberg::ErrorKind::DataInvalid,
                        "GCS no-auth FileIO requires an endpoint",
                    )
                })?;
                props.insert(iceberg::io::GCS_PROJECT_ID.to_string(), project.clone());
                props.insert(iceberg::io::GCS_SERVICE_PATH.to_string(), endpoint.clone());
                props.insert(iceberg::io::GCS_NO_AUTH.to_string(), "true".to_string());
                props.insert(
                    iceberg::io::GCS_ALLOW_ANONYMOUS.to_string(),
                    "true".to_string(),
                );
                props.insert(
                    iceberg::io::GCS_DISABLE_CONFIG_LOAD.to_string(),
                    "true".to_string(),
                );
                props.insert(
                    iceberg::io::GCS_DISABLE_VM_METADATA.to_string(),
                    "true".to_string(),
                );
                Arc::new(OpenDalStorageFactory::Gcs)
            } else {
                // DM(Jerry): GCS-with-HMAC path goes through the S3 protocol pointed at
                // storage.googleapis.com — iceberg-rust's native GCS IO does not support
                // HMAC keys (see Cargo.toml explaining the dual storage-gcs + services-s3
                // feature). Factory side is S3 with scheme "gs", matching these S3-shaped props.
                props.insert(
                    iceberg::io::S3_ENDPOINT.to_string(),
                    "https://storage.googleapis.com".to_string(),
                );
                props.insert(iceberg::io::S3_REGION.to_string(), region.clone());
                props.insert(
                    iceberg::io::S3_ACCESS_KEY_ID.to_string(),
                    access_key_id.clone(),
                );
                props.insert(
                    iceberg::io::S3_SECRET_ACCESS_KEY.to_string(),
                    secret_access_key.clone(),
                );
                props.insert(
                    iceberg::io::S3_DISABLE_CONFIG_LOAD.to_string(),
                    "true".to_string(),
                );
                props.insert(
                    iceberg::io::S3_DISABLE_EC2_METADATA.to_string(),
                    "true".to_string(),
                );
                Arc::new(OpenDalStorageFactory::S3 {
                    configured_scheme: "gs".to_string(),
                    customized_credential_load: None,
                })
            }
        }
        #[cfg(feature = "storage-s3")]
        StorageConfig::S3 {
            access_key_id,
            secret_access_key,
            region,
            endpoint,
            ..
        } => {
            props.insert(iceberg::io::S3_REGION.to_string(), region.clone());
            props.insert(
                iceberg::io::S3_ACCESS_KEY_ID.to_string(),
                access_key_id.clone(),
            );
            props.insert(
                iceberg::io::S3_SECRET_ACCESS_KEY.to_string(),
                secret_access_key.clone(),
            );
            props.insert(
                iceberg::io::S3_DISABLE_CONFIG_LOAD.to_string(),
                "true".to_string(),
            );
            props.insert(
                iceberg::io::S3_DISABLE_EC2_METADATA.to_string(),
                "true".to_string(),
            );
            if let Some(endpoint) = endpoint {
                props.insert(iceberg::io::S3_ENDPOINT.to_string(), endpoint.clone());
            }
            Arc::new(OpenDalStorageFactory::S3 {
                configured_scheme: "s3".to_string(),
                customized_credential_load: None,
            })
        }
    };
    Ok((factory, props))
}

/// Create a local filesystem [`FileIO`].
#[cfg(feature = "storage-fs")]
pub(crate) fn create_fs_file_io() -> FileIO {
    FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Fs)).build()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::storage::filesystem::accessor_config::AccessorConfig;
    use crate::storage::filesystem::storage_config::StorageConfig;
    use crate::storage::mooncake_table::table_creation_test_utils::create_test_arrow_schema;
    use crate::storage::mooncake_table::test_utils_commons::ICEBERG_TEST_NAMESPACE;
    use crate::storage::mooncake_table::test_utils_commons::ICEBERG_TEST_TABLE;
    use crate::storage::table::iceberg::file_catalog::FileCatalog;
    use crate::FsRetryConfig;
    use crate::FsTimeoutConfig;

    use iceberg::arrow as IcebergArrow;
    use iceberg::Catalog;
    use iceberg::NamespaceIdent;
    use iceberg::TableCreation;

    #[tokio::test]
    async fn test_filepath_generation() {
        let temp_dir = tempfile::tempdir().unwrap();
        let arrow_schema: std::sync::Arc<arrow_schema::Schema> = create_test_arrow_schema();
        let iceberg_schema = IcebergArrow::arrow_schema_to_schema(arrow_schema.as_ref()).unwrap();
        let accessor_config = AccessorConfig {
            storage_config: StorageConfig::FileSystem {
                root_directory: temp_dir.path().to_str().unwrap().to_string(),
                atomic_write_dir: None,
            },
            retry_config: FsRetryConfig::default(),
            timeout_config: FsTimeoutConfig::default(),
            throttle_config: None,
            chaos_config: None,
        };
        let file_catalog = FileCatalog::new(accessor_config, iceberg_schema.clone()).unwrap();

        let tbl_creation = TableCreation::builder()
            .name(ICEBERG_TEST_TABLE.to_string())
            .location(format!(
                "{}/{}/{}",
                temp_dir.path().to_str().unwrap(),
                ICEBERG_TEST_NAMESPACE,
                ICEBERG_TEST_TABLE,
            ))
            .schema(iceberg_schema)
            .build();
        let table = file_catalog
            .create_table(
                &NamespaceIdent::from_strs([ICEBERG_TEST_NAMESPACE]).unwrap(),
                tbl_creation,
            )
            .await
            .unwrap();

        // Generate filepath and check.
        let local_filepath = "/some_dir/some_file.parquet".to_string();
        let iceberg_filepath = generate_unique_data_filepath(&table, &local_filepath).unwrap();
        assert!(iceberg_filepath.ends_with(".parquet"));
        assert_eq!(iceberg_filepath.match_indices("parquet").count(), 1);
    }
}
