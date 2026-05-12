/// This module defines a few iceberg table property related constants and utils.
/// Reference: https://iceberg.apache.org/docs/latest/configuration/#table-properties
use std::collections::HashMap;

/// Compression codec for parquet files.
pub(crate) const PARQUET_COMPRESSION: &str = "write.parquet.compression-codec";
pub(crate) const PARQUET_COMPRESSION_DEFAULT: &str = "snappy";

/// Compression codec for metadata.
pub(crate) const METADATA_COMPRESSION: &str = "write.metadata.compression-codec";
pub(crate) const METADATA_COMPRESSION_DEFAULT: &str = "none";

/// Retry properties.
pub(crate) const TABLE_COMMIT_RETRY_NUM: &str = "commit.retry.num-retries";
pub(crate) const TABLE_COMMIT_RETRY_NUM_DEFAULT: u64 = 5;

pub(crate) const TABLE_COMMIT_RETRY_MIN_MS: &str = "commit.retry.min-wait-ms";
pub(crate) const TABLE_COMMIT_RETRY_MIN_MS_DEFAULT: u64 = 200;

pub(crate) const TABLE_COMMIT_RETRY_MAX_MS: &str = "commit.retry.max-wait-ms";
pub(crate) const TABLE_COMMIT_RETRY_MAX_MS_DEFAULT: u64 = 30000; // 30 second

pub(crate) const TABLE_COMMIT_RETRY_TIMEOUT_MS: &str = "commit.retry.total-timeout-ms";
pub(crate) const TABLE_COMMIT_RETRY_TIMEOUT_MS_DEFAULT: u64 = 120000; // 2 min

// ---------------------------------------------------------------------------
// mooncake.* properties (Phase B Mode 2a binding)
//
// These keys live in the standard Iceberg `metadata.properties` map. Cross-engine
// readers (Spark, pyiceberg, Trino) ignore unknown property keys — see Iceberg
// spec §"Table Properties" — so emitting them here is safe.
// `mooncake.private_index_root` is the *base* root only; per-table state lives at
// `<root>/<iceberg_table_uuid>/`, with the UUID read from `metadata.table-uuid`.
// ---------------------------------------------------------------------------

pub(crate) const MOONCAKE_PRIVATE_INDEX_ROOT: &str = "mooncake.private_index_root";
pub(crate) const MOONCAKE_PRIVATE_INDEX_FORMAT: &str = "mooncake.private_index_format";
pub(crate) const MOONCAKE_PRIVATE_INDEX_FORMAT_V1: &str = "v1";
pub(crate) const MOONCAKE_PRIVATE_INDEX_LAYER1_KIND: &str = "mooncake.private_index_layer1_kind";
pub(crate) const MOONCAKE_PRIVATE_INDEX_LAYER1_KIND_HASH_V1: &str = "hash-collection-v1";

// Create iceberg table properties.
//
// `private_index_root` — when `Some`, emits the `mooncake.*` binding so future
// bgworker boots can locate the private manifest directory (B-3 / B-commit-integration).
// `None` keeps legacy tables free of mooncake metadata.
pub(crate) fn create_iceberg_table_properties(
    private_index_root: Option<&str>,
) -> HashMap<String, String> {
    let mut props = HashMap::with_capacity(6);
    // Compression properties.
    props.insert(
        PARQUET_COMPRESSION.to_string(),
        PARQUET_COMPRESSION_DEFAULT.to_string(),
    );
    props.insert(
        METADATA_COMPRESSION.to_string(),
        METADATA_COMPRESSION_DEFAULT.to_string(),
    );
    // Commit retry properties.
    props.insert(
        TABLE_COMMIT_RETRY_NUM.to_string(),
        TABLE_COMMIT_RETRY_NUM_DEFAULT.to_string(),
    );
    props.insert(
        TABLE_COMMIT_RETRY_MIN_MS.to_string(),
        TABLE_COMMIT_RETRY_MIN_MS_DEFAULT.to_string(),
    );
    props.insert(
        TABLE_COMMIT_RETRY_MAX_MS.to_string(),
        TABLE_COMMIT_RETRY_MAX_MS_DEFAULT.to_string(),
    );
    props.insert(
        TABLE_COMMIT_RETRY_TIMEOUT_MS.to_string(),
        TABLE_COMMIT_RETRY_TIMEOUT_MS_DEFAULT.to_string(),
    );
    if let Some(root) = private_index_root {
        // Trim trailing slashes so callers can compose paths predictably.
        let normalized = root.trim_end_matches('/').to_string();
        props.insert(MOONCAKE_PRIVATE_INDEX_ROOT.to_string(), normalized);
        props.insert(
            MOONCAKE_PRIVATE_INDEX_FORMAT.to_string(),
            MOONCAKE_PRIVATE_INDEX_FORMAT_V1.to_string(),
        );
        props.insert(
            MOONCAKE_PRIVATE_INDEX_LAYER1_KIND.to_string(),
            MOONCAKE_PRIVATE_INDEX_LAYER1_KIND_HASH_V1.to_string(),
        );
    }
    props
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_mooncake_props_when_root_absent() {
        let props = create_iceberg_table_properties(None);
        assert!(!props.contains_key(MOONCAKE_PRIVATE_INDEX_ROOT));
        assert!(!props.contains_key(MOONCAKE_PRIVATE_INDEX_FORMAT));
        assert!(!props.contains_key(MOONCAKE_PRIVATE_INDEX_LAYER1_KIND));
    }

    #[test]
    fn mooncake_props_emitted_when_root_present() {
        let props = create_iceberg_table_properties(Some("s3://wh/_mooncake_private/"));
        assert_eq!(
            props.get(MOONCAKE_PRIVATE_INDEX_ROOT).map(String::as_str),
            Some("s3://wh/_mooncake_private"),
        );
        assert_eq!(
            props.get(MOONCAKE_PRIVATE_INDEX_FORMAT).map(String::as_str),
            Some("v1"),
        );
        assert_eq!(
            props
                .get(MOONCAKE_PRIVATE_INDEX_LAYER1_KIND)
                .map(String::as_str),
            Some("hash-collection-v1"),
        );
    }
}
