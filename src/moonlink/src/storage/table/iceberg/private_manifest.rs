//! Per-table private manifest store for mooncake's hash-index metadata.
//!
//! Hash-index puffin blobs do not travel through the Iceberg `manifest_list`
//! chain (cross-engine readers such as Spark / pyiceberg / Trino reject the
//! mooncake-specific Data+Puffin entry shape). Instead, one JSON manifest per
//! Iceberg snapshot is written to a per-table private root:
//!
//! ```text
//! <private_root>/<table_uuid>/manifest/snap-<iceberg_snapshot_id>.json
//! ```
//!
//! The private root is keyed by Iceberg table UUID and is read at table load
//! to rebuild `MooncakeIndex`. Retention is tied to the Iceberg snapshot
//! history; a future sweeper will drop manifests for expired snapshots. The
//! hash index is rebuildable from data files, so a missing manifest degrades
//! to a cost-gated rebuild rather than data loss.

use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::{Error, Result};

use std::sync::Arc;

use moonlink_error::{ErrorStatus, ErrorStruct};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// On-disk schema version. Bump for breaking changes; readers must accept
/// older versions as long as required fields are present.
pub(crate) const PRIVATE_MANIFEST_FORMAT_V1: &str = "v1";

/// Subdirectory under `<private_root>/<table_uuid>/` holding per-snapshot manifests.
const MANIFEST_SUBDIR: &str = "manifest";

/// One hash-index puffin blob entry, identified by its remote puffin file + byte range.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct HashIndexEntry {
    /// Remote path to the puffin file holding the hash blob.
    pub puffin_file_path: String,
    /// Blob byte offset within the puffin file.
    pub blob_offset: u64,
    /// Blob byte size.
    pub blob_size: u64,
    /// Puffin blob_type (e.g. `mooncake-hash-index-v1`).
    pub blob_type: String,
    /// Row cardinality covered by this blob (advisory; for cost gating).
    pub cardinality: u64,
}

/// Per-snapshot private manifest payload.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PrivateManifest {
    pub schema_version: String,
    pub iceberg_snapshot_id: i64,
    pub table_uuid: Uuid,
    pub hash_index_entries: Vec<HashIndexEntry>,
}

impl PrivateManifest {
    pub(crate) fn new(
        iceberg_snapshot_id: i64,
        table_uuid: Uuid,
        hash_index_entries: Vec<HashIndexEntry>,
    ) -> Self {
        Self {
            schema_version: PRIVATE_MANIFEST_FORMAT_V1.to_string(),
            iceberg_snapshot_id,
            table_uuid,
            hash_index_entries,
        }
    }
}

/// Reads/writes mooncake-private snapshot manifests.
///
/// One instance per mirrored table. `root` is the private root; deployment
/// validation enforces its relationship to the Iceberg table root separately
/// (see [`validate_deployment`] below).
#[derive(Debug)]
pub(crate) struct PrivateManifestStore {
    fs: Arc<dyn BaseFileSystemAccess>,
    /// Absolute root, e.g. `s3://customer-warehouse/_mooncake_private/`.
    /// Per-table state lives under `<root>/<table_uuid>/`.
    root: String,
    table_uuid: Uuid,
}

impl PrivateManifestStore {
    pub(crate) fn new(fs: Arc<dyn BaseFileSystemAccess>, root: String, table_uuid: Uuid) -> Self {
        Self {
            fs,
            root: root.trim_end_matches('/').to_string(),
            table_uuid,
        }
    }

    fn manifest_dir(&self) -> String {
        format!("{}/{}/{}", self.root, self.table_uuid, MANIFEST_SUBDIR)
    }

    fn manifest_path(&self, iceberg_snapshot_id: i64) -> String {
        format!("{}/snap-{}.json", self.manifest_dir(), iceberg_snapshot_id)
    }

    /// Public accessor used by the marker pre-write hook to record the planned
    /// manifest write before it happens. Mirrors `manifest_path` so markers
    /// and the actual writer never disagree on the target URI.
    pub(crate) fn manifest_path_for(&self, iceberg_snapshot_id: i64) -> String {
        self.manifest_path(iceberg_snapshot_id)
    }

    /// Persist a manifest for the given Iceberg snapshot. Overwrites any
    /// prior file at the same path — callers are expected to invoke this
    /// once per successful Iceberg snapshot commit, so collisions imply a
    /// retry of the same commit.
    pub(crate) async fn write_snap_manifest(&self, manifest: &PrivateManifest) -> Result<()> {
        if manifest.table_uuid != self.table_uuid {
            return Err(Error::IcebergError(ErrorStruct::new(
                format!(
                    "refusing to write private manifest with table_uuid {}: store is bound to {}",
                    manifest.table_uuid, self.table_uuid
                ),
                ErrorStatus::Permanent,
            )));
        }
        let path = self.manifest_path(manifest.iceberg_snapshot_id);
        let body = serde_json::to_vec_pretty(manifest)?;
        self.fs.write_object(&path, body).await?;
        Ok(())
    }

    /// Read the manifest for `iceberg_snapshot_id`, returning `None` if absent
    /// (legacy tables / freshly-restored private root awaiting rebuild).
    pub(crate) async fn read_snap_manifest(
        &self,
        iceberg_snapshot_id: i64,
    ) -> Result<Option<PrivateManifest>> {
        let path = self.manifest_path(iceberg_snapshot_id);
        if !self.fs.object_exists(&path).await? {
            return Ok(None);
        }
        let bytes = self.fs.read_object(&path).await?;
        let manifest: PrivateManifest = serde_json::from_slice(&bytes)?;
        if manifest.schema_version != PRIVATE_MANIFEST_FORMAT_V1 {
            return Err(Error::IcebergError(ErrorStruct::new(
                format!(
                    "Unsupported mooncake private manifest schema version {} at {path}",
                    manifest.schema_version
                ),
                ErrorStatus::Permanent,
            )));
        }
        if manifest.table_uuid != self.table_uuid {
            return Err(Error::IcebergError(ErrorStruct::new(
                format!(
                    "Mooncake private manifest table UUID mismatch at {path}: expected {}, got {}",
                    self.table_uuid, manifest.table_uuid
                ),
                ErrorStatus::Permanent,
            )));
        }
        // Self-check the embedded snapshot id against the one the filename
        // anchors on. A renamed / copied / hand-edited file could otherwise
        // be silently loaded as the wrong snapshot's index.
        if manifest.iceberg_snapshot_id != iceberg_snapshot_id {
            return Err(Error::IcebergError(ErrorStruct::new(
                format!(
                    "Mooncake private manifest snapshot id mismatch at {path}: expected {iceberg_snapshot_id}, payload reports {}",
                    manifest.iceberg_snapshot_id
                ),
                ErrorStatus::Permanent,
            )));
        }
        Ok(Some(manifest))
    }

    /// List Iceberg snapshot ids that have a persisted manifest, in
    /// unspecified order. The caller cross-checks against the live Iceberg
    /// snapshot set (e.g. load-time reconciliation, retention sweeper).
    pub(crate) async fn list_snapshot_ids(&self) -> Result<Vec<i64>> {
        // Manifest entries are `snap-<id>.json` *files*. `list_direct_files` treats a
        // missing prefix as "no manifests yet" so the empty-store case lands here as
        // Ok([]) without callers needing a separate existence check.
        let entries = self.fs.list_direct_files(&self.manifest_dir()).await?;
        let mut out = Vec::new();
        for entry in entries {
            if let Some(id) = parse_snap_filename(&entry) {
                out.push(id);
            }
        }
        Ok(out)
    }

    /// Delete the manifest for `iceberg_snapshot_id`. No-op if missing.
    /// Used by retention / sweeper paths after the corresponding Iceberg snapshot expires.
    pub(crate) async fn delete_snap_manifest(&self, iceberg_snapshot_id: i64) -> Result<()> {
        let path = self.manifest_path(iceberg_snapshot_id);
        if self.fs.object_exists(&path).await? {
            self.fs.delete_object(&path).await?;
        }
        Ok(())
    }
}

/// Parse a `snap-<id>.json` basename, returning the embedded snapshot id.
fn parse_snap_filename(name: &str) -> Option<i64> {
    let base = name.rsplit('/').next().unwrap_or(name);
    let stem = base.strip_suffix(".json")?;
    let digits = stem.strip_prefix("snap-")?;
    digits.parse::<i64>().ok()
}

// ---------------------------------------------------------------------------
// Per-table private root deployment validation
//
// A mooncake mirror table can deploy the private root in one of three shapes:
//   - external-root:        `private_index_root` lives outside the Iceberg
//                           table root. Recommended; Iceberg cleanup tools
//                           (e.g. `RemoveOrphanFiles`) cannot reach mooncake
//                           artifacts.
//   - nested-with-ack:      private root sits inside the Iceberg table root,
//                           but the operator has configured storage-level
//                           guards (IAM Deny rules, lifecycle exclusions) and
//                           attests via an acknowledgement property on the
//                           Iceberg table.
//   - nested-without-ack:   private root inside the Iceberg table root with
//                           no acknowledgement. Rejected — Iceberg cleanup
//                           can delete mooncake artifacts.
//
// This module only enforces what mooncake can see at runtime: the prefix
// relationship between the two URIs and the presence of the acknowledgement
// property. Auditing the IAM / lifecycle configuration itself is an operator
// responsibility.
// ---------------------------------------------------------------------------

/// Iceberg property key carrying the operator's acknowledgement that the
/// nested private root deployment is intentional and protected by
/// storage-level guards. Presence flips a nested deployment from rejected
/// to allowed-with-warning.
pub(crate) const MOONCAKE_NESTED_PRIVATE_ROOT_ACK: &str = "mooncake.nested_private_root_ack";

/// Outcome of `validate_deployment`. The bgworker that loads the table maps
/// `Compliant` to OK, `NestedWithAck` to WARN, and `Err` to ERROR.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DeploymentStatus {
    /// External-root deployment: private root outside the Iceberg table root.
    Compliant,
    /// Nested deployment: private root inside the Iceberg table root, with
    /// an explicit operator acknowledgement attached as a table property.
    NestedWithAck { ack_id: String },
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum DeploymentValidationError {
    #[error(
        "mooncake.private_index_root ({private_root:?}) is under the Iceberg table root \
         ({iceberg_root:?}); configure mooncake.nested_private_root_ack to attest that \
         storage-level rules protect mooncake artifacts, or move private_index_root \
         outside the table root."
    )]
    NestedWithoutAck {
        private_root: String,
        iceberg_root: String,
    },
}

/// Validate the relationship between the Iceberg table root and the mooncake
/// private root. Returns `Compliant` for an external-root deployment,
/// `NestedWithAck` for a nested deployment with an acknowledgement, and an
/// error for a nested deployment without one.
///
/// Strips trailing slashes before comparison; URI scheme is treated as part
/// of the path string (no canonicalization across schemes — different schemes
/// are by definition disjoint roots).
pub(crate) fn validate_deployment(
    iceberg_table_root: &str,
    private_index_root: &str,
    nested_private_root_ack: Option<&str>,
) -> std::result::Result<DeploymentStatus, DeploymentValidationError> {
    let iceberg = normalize_deployment_root(iceberg_table_root);
    let private = normalize_deployment_root(private_index_root);

    // "Under" check: private root must not equal the Iceberg root or sit beneath it.
    let is_under = private == iceberg
        || private
            .strip_prefix(&iceberg)
            .map(|rest| rest.starts_with('/'))
            .unwrap_or(false);

    if !is_under {
        return Ok(DeploymentStatus::Compliant);
    }
    if let Some(ack) = nested_private_root_ack {
        if !ack.trim().is_empty() {
            return Ok(DeploymentStatus::NestedWithAck {
                ack_id: ack.to_string(),
            });
        }
    }
    Err(DeploymentValidationError::NestedWithoutAck {
        private_root: private,
        iceberg_root: iceberg,
    })
}

fn normalize_deployment_root(root: &str) -> String {
    root.strip_prefix("file://")
        .unwrap_or(root)
        .trim_end_matches('/')
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_snap_filename_round_trip() {
        assert_eq!(parse_snap_filename("snap-42.json"), Some(42));
        assert_eq!(parse_snap_filename("a/b/snap-7.json"), Some(7));
        assert_eq!(parse_snap_filename("snap--1.json"), Some(-1));
        assert_eq!(parse_snap_filename("snap-abc.json"), None);
        assert_eq!(parse_snap_filename("notsnap-1.json"), None);
        assert_eq!(parse_snap_filename("snap-1.txt"), None);
    }

    #[test]
    fn validate_path_a_compliant_when_outside_iceberg_root() {
        let status =
            validate_deployment("s3://wh/db/tbl", "s3://wh/_mooncake_private/", None).unwrap();
        assert_eq!(status, DeploymentStatus::Compliant);
    }

    #[test]
    fn validate_path_a_compliant_across_buckets() {
        let status =
            validate_deployment("s3://wh/db/tbl", "s3://mooncake-internal/uuid-1/", None).unwrap();
        assert_eq!(status, DeploymentStatus::Compliant);
    }

    #[test]
    fn validate_nested_without_ack_rejected() {
        let err =
            validate_deployment("s3://wh/db/tbl", "s3://wh/db/tbl/_mooncake/", None).unwrap_err();
        assert!(matches!(
            err,
            DeploymentValidationError::NestedWithoutAck { .. }
        ));
    }

    #[test]
    fn validate_nested_with_ack_allowed() {
        let status = validate_deployment(
            "s3://wh/db/tbl",
            "s3://wh/db/tbl/_mooncake/",
            Some("docusign-1234"),
        )
        .unwrap();
        assert_eq!(
            status,
            DeploymentStatus::NestedWithAck {
                ack_id: "docusign-1234".to_string()
            }
        );
    }

    #[test]
    fn validate_empty_ack_treated_as_absent() {
        let err = validate_deployment("s3://wh/db/tbl/", "s3://wh/db/tbl/_mooncake/", Some("   "))
            .unwrap_err();
        assert!(matches!(
            err,
            DeploymentValidationError::NestedWithoutAck { .. }
        ));
    }

    #[test]
    fn validate_sibling_prefix_is_not_under() {
        // `s3://wh/db/tbl_index/` shares the `s3://wh/db/tbl` byte prefix but is a
        // sibling, not a child — must be accepted as an external-root deployment.
        let status = validate_deployment("s3://wh/db/tbl", "s3://wh/db/tbl_index/", None).unwrap();
        assert_eq!(status, DeploymentStatus::Compliant);
    }

    #[test]
    fn validate_file_scheme_and_plain_path_are_comparable() {
        let err = validate_deployment("file:///tmp/wh/db/tbl", "/tmp/wh/db/tbl/_mooncake", None)
            .unwrap_err();
        assert!(matches!(
            err,
            DeploymentValidationError::NestedWithoutAck { .. }
        ));
    }

    #[tokio::test]
    async fn list_snapshot_ids_round_trips_written_files() {
        use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
        use tempfile::tempdir;

        let temp = tempdir().unwrap();
        let fs = FileSystemAccessor::default_for_test(&temp);
        let uuid = Uuid::new_v4();
        let store =
            PrivateManifestStore::new(fs.clone(), temp.path().to_str().unwrap().to_string(), uuid);

        // Empty store → empty list. Guards against the older bug where listing was
        // routed through `list_direct_subdirectories` and silently returned [].
        assert!(store.list_snapshot_ids().await.unwrap().is_empty());

        for snap_id in [1_i64, 42, 1234] {
            store
                .write_snap_manifest(&PrivateManifest::new(snap_id, uuid, Vec::new()))
                .await
                .unwrap();
        }

        let mut got = store.list_snapshot_ids().await.unwrap();
        got.sort();
        assert_eq!(got, vec![1, 42, 1234]);

        // delete_snap_manifest should reflect in subsequent listings.
        store.delete_snap_manifest(42).await.unwrap();
        let mut got = store.list_snapshot_ids().await.unwrap();
        got.sort();
        assert_eq!(got, vec![1, 1234]);
    }

    #[test]
    fn private_manifest_serde_round_trip() {
        let uuid = Uuid::nil();
        let m = PrivateManifest::new(
            123,
            uuid,
            vec![HashIndexEntry {
                puffin_file_path: "s3://bucket/p.puffin".to_string(),
                blob_offset: 0,
                blob_size: 4096,
                blob_type: "mooncake-hash-index-v1".to_string(),
                cardinality: 1000,
            }],
        );
        let bytes = serde_json::to_vec(&m).unwrap();
        let back: PrivateManifest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(m, back);
        assert_eq!(back.schema_version, PRIVATE_MANIFEST_FORMAT_V1);
    }

    /// Defense in depth: a `snap-100.json` file whose payload claims a
    /// different snapshot id must not be silently loaded as snapshot 100.
    /// A corrupted private root, a hand-edited file, or a misnamed copy
    /// would otherwise cross-load one snapshot's index into another.
    #[tokio::test]
    async fn read_snap_manifest_rejects_snapshot_id_mismatch() {
        use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
        let temp = tempfile::tempdir().unwrap();
        let fs = FileSystemAccessor::default_for_test(&temp);
        let uuid = Uuid::new_v4();
        let store = PrivateManifestStore::new(fs, temp.path().to_str().unwrap().to_string(), uuid);

        // Write a manifest claiming snapshot id 99 but plant it under the
        // path the store expects for snapshot id 100.
        let bogus = PrivateManifest::new(99, uuid, Vec::new());
        let bogus_path = store.manifest_path_for(100);
        store
            .fs
            .write_object(&bogus_path, serde_json::to_vec_pretty(&bogus).unwrap())
            .await
            .unwrap();

        let err = store.read_snap_manifest(100).await.unwrap_err();
        assert!(
            matches!(err, Error::IcebergError(ref es) if es.status == ErrorStatus::Permanent),
            "expected permanent error on snapshot_id mismatch, got {err:?}",
        );
    }

    /// Symmetric write-side check: a store bound to one table UUID must
    /// refuse to write a manifest claiming a different table UUID.
    #[tokio::test]
    async fn write_snap_manifest_rejects_table_uuid_mismatch() {
        use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
        let temp = tempfile::tempdir().unwrap();
        let fs = FileSystemAccessor::default_for_test(&temp);
        let bound_uuid = Uuid::new_v4();
        let other_uuid = Uuid::new_v4();
        let store =
            PrivateManifestStore::new(fs, temp.path().to_str().unwrap().to_string(), bound_uuid);

        let foreign = PrivateManifest::new(1, other_uuid, Vec::new());
        let err = store.write_snap_manifest(&foreign).await.unwrap_err();
        assert!(
            matches!(err, Error::IcebergError(ref es) if es.status == ErrorStatus::Permanent),
            "expected permanent error on table_uuid mismatch, got {err:?}",
        );
    }
}
