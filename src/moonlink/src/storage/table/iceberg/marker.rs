// Pre-write `record` / `begin_commit` and the post-commit `commit_success` now
// have real callers in `iceberg_table_syncer::persist_private_manifest`. The
// remaining APIs (`rollback`, `scan_hanging`, `read_markers`, `HangingCommit`,
// `with_commit_lsn`, `parse_suffix`, `is_marker_file`, `parse_snapshot_dirname`)
// land their first non-test callers in the boot reconciliation and audit
// paths still to ship. Keep the module-level allow until those land — flipping
// the lint per-item now would just require flipping it back later.
#![allow(dead_code)]

//! Hudi-pattern marker file primitives for the mooncake private root.
//!
//! Purpose: provide recoverability for the non-atomic window between
//! `Iceberg txn.commit` Ok and `PrivateManifestStore::write_snap_manifest` Ok
//! (see `AI_DOCs/iceberg_index_proposal/hash_index_refactor/09_b1_b2_private_manifest_walkthrough.md`
//! §5 R1). Before mooncake writes the per-snapshot private manifest, the syncer
//! lays down one marker file per artifact under
//!
//! ```text
//! <private_root>/<table_uuid>/.markers/<snapshot_id>/<artifact_uuid>.marker.<op>
//! ```
//!
//! After the Iceberg snapshot commits AND the private manifest is durably written,
//! `commit_success(snapshot_id)` deletes the whole `.markers/<snapshot_id>/` directory
//! (B-5d). On bgworker boot, `scan_hanging` lists marker directories whose
//! `<snapshot_id>` is not present in the live Iceberg snapshot set — those are
//! inflight writes interrupted by a crash and the staging files they reference are
//! rolled back (B-5e).
//!
//! Implemented sub-tasks:
//! - B-5a: `MarkerOp`, `Marker`, `CommitMarkers` data shapes.
//! - B-5b: `MoonlinkMarkerDir` with `begin_commit / commit_success / rollback /
//!   scan_hanging / read_markers`.
//! - B-5c: pre-write hook from `iceberg_table_syncer::persist_private_manifest`.
//!
//! Out of scope (later sub-tasks): commit-success wiring (B-5d), boot rollback scan
//! (B-5e), heartbeat / fencing (B-5f), retention / dry-run / audit (B-5g).
//!
//! Reference: Apache Hudi `WriteMarkers` / `MarkerFiles.deleteMarkerDir()` /
//! `IOType` (we keep `Create` and `Replace`, dropping `MoR`-specific arms because
//! mooncake is copy-on-write).

use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::{Error, Result};

use std::collections::HashSet;
use std::sync::Arc;

use moonlink_error::{ErrorStatus, ErrorStruct};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Marker directory name (sibling to `manifest/` under `<private_root>/<table_uuid>/`).
const MARKERS_SUBDIR: &str = ".markers";

/// Marker schema version. Bump for breaking on-disk changes.
pub(crate) const MARKER_FORMAT_V1: &str = "v1";

/// Operation a marker witnesses. We track only `Create` and `Replace` because
/// mooncake is copy-on-write — there is no MoR (append / log file) path.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MarkerOp {
    /// New file laid down by this commit (puffin / private manifest).
    Create,
    /// Logical replace of an existing file path (rare in mooncake today, kept for
    /// forward-compat with index merge / compaction flows that may overwrite a
    /// stable file name).
    Replace,
}

impl MarkerOp {
    /// File-name suffix used on disk: `<uuid>.marker.create` / `<uuid>.marker.replace`.
    /// Kept lowercase so case-insensitive object stores never confuse them.
    fn file_suffix(self) -> &'static str {
        match self {
            MarkerOp::Create => "create",
            MarkerOp::Replace => "replace",
        }
    }

    fn parse_suffix(s: &str) -> Option<Self> {
        match s {
            "create" => Some(MarkerOp::Create),
            "replace" => Some(MarkerOp::Replace),
            _ => None,
        }
    }
}

/// One marker witnessing the intent to write `file_path` as part of the Iceberg
/// `snapshot_id` commit. Serialized JSON payload of the marker file itself.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Marker {
    pub schema_version: String,
    /// Iceberg snapshot id this marker belongs to. We use snapshot_id instead of
    /// PG `commit_lsn` because moonlink-side reconciliation against
    /// `metadata.current_snapshot()` is what actually decides "is this commit live";
    /// PG LSN is recorded as an opaque audit field for cross-system debugging.
    pub snapshot_id: i64,
    /// Remote path of the artifact this marker witnesses (puffin or private manifest).
    pub file_path: String,
    pub op: MarkerOp,
    /// PG `XactLastCommitEnd` at marker creation, for audit only. Optional because
    /// non-PG callers (tests, future ingest paths) may not have a meaningful LSN.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_lsn: Option<u64>,
}

impl Marker {
    pub(crate) fn new_create(snapshot_id: i64, file_path: impl Into<String>) -> Self {
        Self {
            schema_version: MARKER_FORMAT_V1.to_string(),
            snapshot_id,
            file_path: file_path.into(),
            op: MarkerOp::Create,
            commit_lsn: None,
        }
    }

    pub(crate) fn with_commit_lsn(mut self, lsn: u64) -> Self {
        self.commit_lsn = Some(lsn);
        self
    }
}

/// Handle for a single in-flight commit's marker batch. The struct is intentionally
/// thin — it owns no state beyond the snapshot id and a reference to the dir, so
/// dropping it without calling `commit_success` / `rollback` does **not** trigger
/// cleanup (mooncake recovers via `scan_hanging` on boot instead, matching Hudi's
/// model where commit markers are durable until explicit reconciliation).
#[derive(Debug)]
pub(crate) struct CommitMarkers<'a> {
    dir: &'a MoonlinkMarkerDir,
    snapshot_id: i64,
}

impl CommitMarkers<'_> {
    pub(crate) fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    /// Lay down a single marker for `file_path`. Called by every artifact writer
    /// (private manifest / puffin) *before* the actual data write.
    pub(crate) async fn record(&self, marker: Marker) -> Result<()> {
        if marker.snapshot_id != self.snapshot_id {
            return Err(Error::IcebergError(ErrorStruct::new(
                format!(
                    "marker snapshot_id mismatch: handle={}, marker={}",
                    self.snapshot_id, marker.snapshot_id
                ),
                ErrorStatus::Permanent,
            )));
        }
        let path = self.dir.marker_path(self.snapshot_id, &marker);
        let body = serde_json::to_vec(&marker)?;
        self.dir.fs.write_object(&path, body).await?;
        Ok(())
    }
}

/// One Iceberg snapshot worth of markers that survived a crash. The caller
/// (boot rollback path, B-5e) is responsible for re-reading each marker file
/// referenced by `file_paths_to_delete` (full list available via
/// `MoonlinkMarkerDir::read_markers`) and deleting the staging artifacts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HangingCommit {
    pub snapshot_id: i64,
}

/// Per-table marker directory: `<private_root>/<table_uuid>/.markers/`.
///
/// Filesystem layout:
/// ```text
/// .markers/
///   <snapshot_id>/
///     <artifact_uuid>.marker.create
///     <artifact_uuid>.marker.replace
/// ```
///
/// Same `BaseFileSystemAccess` as the rest of the private root — cross-bucket
/// markers are deferred together with cross-bucket private root (see §5 R7).
#[derive(Debug)]
pub(crate) struct MoonlinkMarkerDir {
    fs: Arc<dyn BaseFileSystemAccess>,
    /// Absolute private root (no trailing slash), e.g. `s3://wh/_mooncake_private`.
    root: String,
    table_uuid: Uuid,
}

impl MoonlinkMarkerDir {
    pub(crate) fn new(fs: Arc<dyn BaseFileSystemAccess>, root: String, table_uuid: Uuid) -> Self {
        Self {
            fs,
            root: root.trim_end_matches('/').to_string(),
            table_uuid,
        }
    }

    fn markers_dir(&self) -> String {
        format!("{}/{}/{}", self.root, self.table_uuid, MARKERS_SUBDIR)
    }

    fn snapshot_dir(&self, snapshot_id: i64) -> String {
        format!("{}/{}", self.markers_dir(), snapshot_id)
    }

    fn marker_path(&self, snapshot_id: i64, marker: &Marker) -> String {
        // Per-marker UUID keeps two writers racing on the same `file_path` from
        // colliding; collisions would otherwise silently overwrite each other's
        // intent (the second writer wouldn't know there was a first).
        let id = Uuid::new_v4();
        format!(
            "{}/{}.marker.{}",
            self.snapshot_dir(snapshot_id),
            id,
            marker.op.file_suffix()
        )
    }

    /// Open a fresh marker batch for `snapshot_id`. Currently a no-op on the
    /// filesystem — the directory is created lazily by the first `record` call,
    /// matching Hudi's behaviour and avoiding an extra round-trip for empty
    /// commits.
    pub(crate) fn begin_commit(&self, snapshot_id: i64) -> CommitMarkers<'_> {
        CommitMarkers {
            dir: self,
            snapshot_id,
        }
    }

    /// Delete all markers for `snapshot_id`. Called after the Iceberg commit AND
    /// the private manifest write both succeed. Best-effort: any straggler marker
    /// is reaped by `scan_hanging` on the next boot.
    pub(crate) async fn commit_success(&self, snapshot_id: i64) -> Result<()> {
        self.purge_snapshot_dir(snapshot_id).await
    }

    /// Same on-disk effect as `commit_success`, but expresses intent ("this commit
    /// was aborted") for telemetry / audit hooks added in B-5g.
    pub(crate) async fn rollback(&self, snapshot_id: i64) -> Result<()> {
        self.purge_snapshot_dir(snapshot_id).await
    }

    /// Read every marker payload for `snapshot_id`. Used by the rollback path
    /// (B-5e) to discover which staging files need to be removed before the
    /// marker dir itself is purged.
    ///
    /// P2 (degradation policy): per-file errors /
    /// schema mismatches surface as `Err` — B-5e will quarantine.
    pub(crate) async fn read_markers(&self, snapshot_id: i64) -> Result<Vec<Marker>> {
        let dir = self.snapshot_dir(snapshot_id);
        // `list_direct_files` returns basenames relative to `dir`; re-anchor before
        // calling `read_object`, matching the convention in `private_manifest.rs`.
        let names = self.fs.list_direct_files(&dir).await?;
        let mut out = Vec::with_capacity(names.len());
        for name in names {
            if !is_marker_file(&name) {
                continue;
            }
            let full = format!("{dir}/{name}");
            let bytes = self.fs.read_object(&full).await?;
            let marker: Marker = serde_json::from_slice(&bytes)?;
            if marker.schema_version != MARKER_FORMAT_V1 {
                return Err(Error::IcebergError(ErrorStruct::new(
                    format!(
                        "Unsupported marker schema version {} at {name}",
                        marker.schema_version
                    ),
                    ErrorStatus::Permanent,
                )));
            }
            if marker.snapshot_id != snapshot_id {
                return Err(Error::IcebergError(ErrorStruct::new(
                    format!(
                        "Marker snapshot_id mismatch at {name}: dir={snapshot_id}, payload={}",
                        marker.snapshot_id
                    ),
                    ErrorStatus::Permanent,
                )));
            }
            out.push(marker);
        }
        Ok(out)
    }

    /// List marker directories whose `snapshot_id` is not present in
    /// `live_snapshot_ids`. The caller passes the live set from
    /// `metadata.snapshots()`; everything else is considered orphaned by a
    /// crashed commit attempt and forms a `HangingCommit`.
    pub(crate) async fn scan_hanging(
        &self,
        live_snapshot_ids: &HashSet<i64>,
    ) -> Result<Vec<HangingCommit>> {
        let entries = self
            .fs
            .list_direct_subdirectories(&self.markers_dir())
            .await?;
        let mut hanging = Vec::new();
        for entry in entries {
            let Some(id) = parse_snapshot_dirname(&entry) else {
                continue;
            };
            if !live_snapshot_ids.contains(&id) {
                hanging.push(HangingCommit { snapshot_id: id });
            }
        }
        Ok(hanging)
    }

    async fn purge_snapshot_dir(&self, snapshot_id: i64) -> Result<()> {
        let dir = self.snapshot_dir(snapshot_id);
        // Must remove the directory entry itself, not just the marker files. On a
        // directory-shaped backend (local FS), leaving an empty `<snapshot_id>/`
        // around would make `scan_hanging` re-report the snapshot after Iceberg
        // expires it — a false positive that would trigger another rollback on the
        // very snapshot whose commit had already succeeded. Object stores like S3
        // don't have empty prefixes, but we write to the strictest backend.
        //
        // `remove_directory` is recursive, which is what we want: any stray
        // non-marker file under `<snapshot_id>/` (e.g. an artifact a future B-5c
        // hook stages there before laying down its marker) should also be reaped.
        self.fs.remove_directory(&dir).await?;
        Ok(())
    }
}

fn is_marker_file(path: &str) -> bool {
    let base = path.rsplit('/').next().unwrap_or(path);
    // Accept any `<stem>.marker.<op>` where <op> is a known MarkerOp suffix.
    let Some(rest) = base.split_once(".marker.") else {
        return false;
    };
    MarkerOp::parse_suffix(rest.1).is_some()
}

fn parse_snapshot_dirname(name: &str) -> Option<i64> {
    let base = name
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or(name);
    base.parse::<i64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
    use tempfile::tempdir;

    fn make_dir() -> (tempfile::TempDir, MoonlinkMarkerDir, Uuid) {
        let temp = tempdir().unwrap();
        let fs = FileSystemAccessor::default_for_test(&temp);
        let uuid = Uuid::new_v4();
        let dir = MoonlinkMarkerDir::new(fs, temp.path().to_str().unwrap().to_string(), uuid);
        (temp, dir, uuid)
    }

    #[test]
    fn marker_op_suffix_round_trips() {
        assert_eq!(MarkerOp::parse_suffix("create"), Some(MarkerOp::Create));
        assert_eq!(MarkerOp::parse_suffix("replace"), Some(MarkerOp::Replace));
        assert_eq!(MarkerOp::parse_suffix("unknown"), None);
        assert_eq!(MarkerOp::Create.file_suffix(), "create");
        assert_eq!(MarkerOp::Replace.file_suffix(), "replace");
    }

    #[test]
    fn marker_serde_round_trip() {
        let m = Marker::new_create(42, "s3://b/p.puffin").with_commit_lsn(0x1234);
        let bytes = serde_json::to_vec(&m).unwrap();
        let back: Marker = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, m);
        assert_eq!(back.commit_lsn, Some(0x1234));
    }

    #[test]
    fn marker_serde_skips_optional_lsn_when_absent() {
        let m = Marker::new_create(7, "s3://b/p");
        let s = serde_json::to_string(&m).unwrap();
        assert!(!s.contains("commit_lsn"), "got: {s}");
    }

    #[test]
    fn is_marker_file_detects_known_suffixes() {
        assert!(is_marker_file("/x/y/abc.marker.create"));
        assert!(is_marker_file("/x/y/abc.marker.replace"));
        assert!(!is_marker_file("/x/y/abc.marker.foo"));
        assert!(!is_marker_file("/x/y/snap-1.json"));
        assert!(!is_marker_file("/x/y/abc.create"));
    }

    #[tokio::test]
    async fn record_and_read_markers_round_trip() {
        let (_t, dir, _) = make_dir();
        let batch = dir.begin_commit(100);

        batch
            .record(Marker::new_create(100, "s3://b/a.puffin"))
            .await
            .unwrap();
        batch
            .record(Marker::new_create(100, "s3://b/b.puffin").with_commit_lsn(9))
            .await
            .unwrap();

        let mut got = dir.read_markers(100).await.unwrap();
        got.sort_by(|a, b| a.file_path.cmp(&b.file_path));
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].file_path, "s3://b/a.puffin");
        assert_eq!(got[1].file_path, "s3://b/b.puffin");
        assert_eq!(got[1].commit_lsn, Some(9));
    }

    #[tokio::test]
    async fn record_rejects_snapshot_id_mismatch() {
        let (_t, dir, _) = make_dir();
        let batch = dir.begin_commit(100);
        let err = batch
            .record(Marker::new_create(101, "s3://b/x"))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::IcebergError(_)));
    }

    #[tokio::test]
    async fn commit_success_purges_markers() {
        let (_t, dir, _) = make_dir();
        let batch = dir.begin_commit(7);
        batch
            .record(Marker::new_create(7, "s3://b/a"))
            .await
            .unwrap();
        batch
            .record(Marker::new_create(7, "s3://b/b"))
            .await
            .unwrap();

        assert_eq!(dir.read_markers(7).await.unwrap().len(), 2);
        dir.commit_success(7).await.unwrap();
        assert!(dir.read_markers(7).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn rollback_purges_like_commit_success() {
        let (_t, dir, _) = make_dir();
        let batch = dir.begin_commit(11);
        batch
            .record(Marker::new_create(11, "s3://b/a"))
            .await
            .unwrap();
        dir.rollback(11).await.unwrap();
        assert!(dir.read_markers(11).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn commit_success_is_noop_for_empty_snapshot() {
        let (_t, dir, _) = make_dir();
        // Never recorded anything for snapshot 99 — must not error.
        dir.commit_success(99).await.unwrap();
    }

    #[tokio::test]
    async fn scan_hanging_lists_only_non_live_snapshots() {
        let (_t, dir, _) = make_dir();

        for snap in [1_i64, 2, 3] {
            dir.begin_commit(snap)
                .record(Marker::new_create(snap, format!("s3://b/{snap}")))
                .await
                .unwrap();
        }

        let live: HashSet<i64> = [2_i64].into_iter().collect();
        let mut hanging = dir.scan_hanging(&live).await.unwrap();
        hanging.sort_by_key(|h| h.snapshot_id);
        assert_eq!(
            hanging,
            vec![
                HangingCommit { snapshot_id: 1 },
                HangingCommit { snapshot_id: 3 },
            ]
        );
    }

    #[tokio::test]
    async fn scan_hanging_does_not_reappear_after_commit_success_when_iceberg_expires() {
        // Regression for the P1 bug: commit_success used to leave an empty
        // `<snapshot_id>/` directory behind. Once Iceberg expired that snapshot
        // out of the live set, scan_hanging would re-flag it as hanging on every
        // subsequent boot — and rollback would "roll back" a snapshot whose
        // commit had succeeded long ago.
        let (_t, dir, _) = make_dir();

        // Commit succeeds while snapshot 50 is live.
        dir.begin_commit(50)
            .record(Marker::new_create(50, "s3://b/a"))
            .await
            .unwrap();
        dir.commit_success(50).await.unwrap();

        // Simulate Iceberg snapshot expiration: 50 is no longer in the live set.
        let live: HashSet<i64> = HashSet::new();
        let hanging = dir.scan_hanging(&live).await.unwrap();
        assert!(
            hanging.is_empty(),
            "commit_success must wipe the directory entry, got {hanging:?}"
        );
    }

    #[tokio::test]
    async fn scan_hanging_does_not_reappear_after_rollback() {
        // Sibling regression: rollback shares the same purge path. After
        // rollback the snapshot must never resurface in scan_hanging, even
        // when the live set is empty (no Iceberg snapshot was ever committed
        // for this aborted attempt).
        let (_t, dir, _) = make_dir();
        dir.begin_commit(60)
            .record(Marker::new_create(60, "s3://b/x"))
            .await
            .unwrap();
        dir.rollback(60).await.unwrap();

        let hanging = dir.scan_hanging(&HashSet::new()).await.unwrap();
        assert!(
            hanging.is_empty(),
            "rollback must wipe the dir, got {hanging:?}"
        );
    }

    #[tokio::test]
    async fn scan_hanging_empty_when_all_live() {
        let (_t, dir, _) = make_dir();
        dir.begin_commit(5)
            .record(Marker::new_create(5, "s3://b/x"))
            .await
            .unwrap();
        let live: HashSet<i64> = [5_i64].into_iter().collect();
        assert!(dir.scan_hanging(&live).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn scan_hanging_empty_when_no_markers_dir() {
        let (_t, dir, _) = make_dir();
        let live: HashSet<i64> = HashSet::new();
        // No markers ever written → list_direct_subdirectories on a missing prefix
        // must come back as empty, not error.
        assert!(dir.scan_hanging(&live).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn read_markers_rejects_payload_snapshot_mismatch() {
        let (temp, dir, uuid) = make_dir();
        // Craft a marker whose file lives in /5/ but whose payload claims snapshot 6.
        let bad_dir = format!(
            "{}/{}/{}/{}",
            temp.path().display(),
            uuid,
            MARKERS_SUBDIR,
            5
        );
        std::fs::create_dir_all(&bad_dir).unwrap();
        let path = format!("{bad_dir}/bad.marker.create");
        let bytes = serde_json::to_vec(&Marker::new_create(6, "s3://b/x")).unwrap();
        std::fs::write(&path, bytes).unwrap();

        let err = dir.read_markers(5).await.unwrap_err();
        assert!(matches!(err, Error::IcebergError(_)));
    }
}
