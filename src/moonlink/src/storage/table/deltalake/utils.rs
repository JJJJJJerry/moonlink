use deltalake::kernel::engine::arrow_conversion::TryFromArrow;
use deltalake::{open_table, operations::create::CreateBuilder, DeltaTable};
use std::sync::Arc;
use url::Url;

use crate::Error;

/// DM(Jerry): deltalake 0.31's `open_table` takes `Url` instead of `String`. We keep
/// callers passing string locations (consistent with iceberg side) and parse here.
fn parse_table_url(location: &str) -> Result<Url> {
    Url::parse(location)
        .or_else(|_| Url::from_file_path(location).map_err(|_| ()))
        .map_err(|_| {
            Error::delta_generic_error(format!("invalid delta table location: {location}"))
        })
}

use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::mooncake_table::TableMetadata as MooncakeTableMetadata;
use crate::storage::table::deltalake::deltalake_table_config::DeltalakeTableConfig;
use crate::CacheTrait;
use crate::Result;

/// Get or create a Delta table at the given location.
///
/// - If the table doesn't exist → create a new one using the Arrow schema.
/// - If it already exists → load and return.
/// - This mirrors the Iceberg `get_or_create_iceberg_table` pattern.
#[allow(unused)]
pub(crate) async fn get_or_create_deltalake_table(
    mooncake_table_metadata: Arc<MooncakeTableMetadata>,
    _object_storage_cache: Arc<dyn CacheTrait>,
    _filesystem_accessor: Arc<dyn BaseFileSystemAccess>,
    config: DeltalakeTableConfig,
) -> Result<DeltaTable> {
    match open_table(parse_table_url(&config.location)?).await {
        Ok(existing_table) => Ok(existing_table),
        Err(_) => {
            let arrow_schema = mooncake_table_metadata.schema.as_ref();
            // deltalake 0.31 pins arrow ^57, matching the mooncake workspace, so the upstream
            // arrow→delta converter accepts our schemas directly. The hand-rolled bridge that
            // lived here under deltalake 0.28 (which was stuck on arrow 55) is gone.
            let delta_schema_struct = deltalake::kernel::Schema::try_from_arrow(arrow_schema)?;
            // DM(Jerry): delta_kernel 0.19 exposes fields as a method returning
            // `impl Iterator<Item = &StructField>` (delta_kernel/.../schema/mod.rs:754),
            // hiding the underlying IndexMap. Prior deltalake 0.28 forced `.fields.iter()`
            // and tuple-destructuring of `(&String, &StructField)`; the new shape is one step.
            let delta_schema_fields: Vec<_> = delta_schema_struct.fields().cloned().collect();

            let table = CreateBuilder::new()
                .with_location(config.location.clone())
                .with_columns(delta_schema_fields)
                .with_save_mode(deltalake::protocol::SaveMode::ErrorIfExists)
                .await?;
            Ok(table)
        }
    }
}

#[allow(unused)]
pub(crate) async fn get_deltalake_table_if_exists(
    config: &DeltalakeTableConfig,
) -> Result<Option<DeltaTable>> {
    match open_table(parse_table_url(&config.location)?).await {
        Ok(table) => Ok(Some(table)),
        Err(_) => Ok(None),
    }
}
