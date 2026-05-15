# Hash-Index Pointer via Iceberg `snapshot.summary` — Design Notes

> **Status**: spike on branch `route-a-snapshot-summary`, based at
> pg_mooncake commit `654bd24` (moonlink SHA `7411484`, iceberg-rust `0.9.1`).
> Designed as a study alternative to the private-manifest + Hudi-style marker
> path shipped on `jerry/main`. Pre-production: integration tests against
> external readers and failure-injection harnesses are out of scope here.

## What this design does

External Iceberg readers reject mooncake's hash-index puffin entries
(`Data + Puffin` content type is non-standard). This module relocates the
hash-index puffins outside the Iceberg table root and publishes the live
pointer set inside the Iceberg `snapshot.summary` map — a place readers
already iterate and silently ignore unknown keys in. Pointer and data
files share a single catalog CAS, so there is no half-commit window and no
marker protocol.

## Hard invariants

1. **No mooncake hash-index artifact is reachable through any Iceberg
   `manifest_list` entry** — neither at write time nor after compaction.
2. **Every committed snapshot's `moonlink.hash-index` summary value is the
   complete live pointer set** for that snapshot. Loaders rebuild state
   from one snapshot's summary alone.
3. **`PrivateHashIndexConfig::private_root` does not overlap the Iceberg
   table root** — enforced at table initialization, not just documented.

Invariant #2 in particular forbids "delta-encoding" the summary across
snapshots: a snapshot whose flush touches only data files or deletion
vectors still re-publishes the complete live hash-index set.

## How to read this branch

| Concern | File |
| --- | --- |
| Schema (key, JSON layout, version) | `hash_index_summary.rs` §"Public contract" + §"Snapshot.summary payload" |
| Private root invariant + validator | `hash_index_summary.rs::PrivateHashIndexConfig::validate_against_iceberg_table_root` |
| Write path | `hash_index_summary.rs::write_file_index_to_private_storage` + `iceberg_table_syncer.rs::publish_hash_index_to_summary` |
| Read path | `hash_index_summary.rs::{read_state_from_snapshot, load_blobs_from_state}` + `iceberg_table_loader.rs::load_hash_index_from_summary` |
| Staged-then-commit (failure semantics) | `iceberg_table_syncer.rs::sync_snapshot_impl` — `staged_hash_index_state` is moved into `self.persisted_hash_index_state` only after `txn.commit().await?` succeeds |
| Sweeper (orphan cleanup) | `hash_index_summary.rs::{sweep_orphans, collect_live_uris}` — live set includes both puffin URIs and referenced index-block URIs |
| Schema-layer tests (12) | `hash_index_summary.rs::tests` |
| Diff vs baseline | `(cd moonlink && git diff HEAD)` from the worktree |

## Snapshot.summary contract

Key: `moonlink.hash-index`. Value: compact JSON of `HashIndexSnapshotState`:

```text
{
  "schema_version": "v1",
  "entries": [
    {
      "puffin_uri": "<private_root>/<iceberg_table_uuid>/puffin/<id>.puffin",
      "index_block_uris": [
        "<private_root>/<iceberg_table_uuid>/puffin/<id>-block-0.bin",
        ...
      ],
      "cardinality": 1000
    },
    ...
  ]
}
```

Field rationale:

- `puffin_uri` — opened directly via `FileIO`; minimum needed for read.
- `index_block_uris` — published in the summary so the sweeper's live set
  protects them without re-opening every puffin. Skipping this field is
  what caused the "sweeper deletes live block files" bug in the first
  spike draft.
- `cardinality` — planner cost-gating without opening the puffin.
- Fields like `blob_offset` / `blob_size` / `blob_type` are **not** carried:
  they are re-derived from the puffin file on read, so duplicating them in
  the summary would only invite divergence.

## Failure semantics

| Failure point | Effect on Iceberg state | Effect on `persisted_hash_index_state` | Recovery |
| --- | --- | --- | --- |
| Index-block upload fails | nothing committed | unchanged | next flush retries |
| Puffin write fails | nothing committed | unchanged | orphan parts swept after retention |
| `txn.commit()` fails | nothing committed | unchanged | next flush re-stages from the unchanged state |
| `txn.commit()` succeeds | data + summary atomic | replaced by staged value | loader rebuilds from summary on next boot |

Because every failure mode prior to a successful commit leaves both
Iceberg state and in-memory state untouched, the half-commit window the
private-manifest design needs marker files to recover from simply does
not exist here.

## Deployment validation

`PrivateHashIndexConfig::validate_against_iceberg_table_root` rejects:

- `private_root == iceberg_root` (with or without trailing slash)
- `private_root` is a path-prefix of `iceberg_root`
- `iceberg_root` is a path-prefix of `private_root`

A sibling like `s3://warehouse/dbA` vs `s3://warehouse/dbAB` is **not**
treated as overlap — the prefix check requires the boundary character.
This is wired into `IcebergTableManager::initialize_iceberg_table_for_once`
so any subsequent IO can assume the invariant holds.

## Test coverage in this branch (12 unit tests)

Schema layer:
- `state_roundtrip_preserves_all_fields`
- `state_rejects_future_schema_version`
- `state_rejects_malformed_json`
- `empty_state_serializes_compactly`
- `pointer_includes_block_uris_in_serialization` *(Blocking #4 regression guard)*

URI synthesis:
- `config_puffin_dir_strips_trailing_slash`
- `config_generates_unique_uris`

Deployment validation:
- `validate_rejects_identical_roots`
- `validate_rejects_private_under_iceberg`
- `validate_rejects_iceberg_under_private`
- `validate_accepts_disjoint_roots`
- `validate_accepts_sibling_prefix_not_subpath`

## Known gaps (deliberate)

The following classes of validation are out of scope for the schema-layer
unit tests and belong to a follow-up integration suite:

1. **End-to-end write → commit → load roundtrip** with mooncake's
   `TestContext` — would prove invariant #2 mechanically.
2. **External-reader compatibility** (pyiceberg / Spark Iceberg runtime /
   Trino) — would prove invariant #1.
3. **Sweeper integration test** that creates orphans + live files and
   verifies only orphans are deleted, with block URIs in the live set.
4. **Failure-injection tests** for index-block-upload, puffin-write, and
   `txn.commit()` failures — would prove the failure-semantics table above.
5. **Multi-puffin-per-file-index relaxation** — current implementation
   inherits the baseline's "one puffin per `MooncakeFileIndex`" assumption.
6. **Path-normalization mismatch on removal** — `publish_hash_index_to_summary`
   logs a warning when a removal target is not found in the staged state.
   The mismatch could come from local-path vs remote-path keys; tracking
   that down is a pre-existing concern in `persisted_file_indices` and not
   widened by this branch.

## How this contrasts with the `jerry/main` private-manifest design

| Aspect | This branch (summary-based) | `jerry/main` (private manifest + markers) |
| --- | --- | --- |
| Module surface | one file (`hash_index_summary.rs`) | four artifacts (`private_manifest.rs`, `marker.rs`, boot-reconcile helpers) |
| Catalog atomicity | pointer + data in one CAS | pointer separate; marker protocol covers the gap |
| State across snapshots | summary publishes the full live set every commit | private manifest file per snapshot; loader follows the chain |
| Sweeper input | live set = `∪ snapshot.summary[*].entries[*].{puffin_uri, index_block_uris}` | marker dir scan + chain-walk |
| Failure modes to recover | orphan-or-not (1 axis) | 4-axis (puffin × commit × manifest × marker) |
| Async-built index support (vector / FT) | no — `summary` is immutable per snapshot | yes — manifest can be published after commit |

## Manual verification

```bash
# From worktree root:
cd /Users/jerry/Code/Postgres/PG_18_ANNOTATE/contrib/pg_mooncake/.worktrees/route-a-snapshot-summary

# Workspace must compile (codex's blocking #1 regression guard):
(cd moonlink && cargo check --workspace)

# Schema-layer tests:
(cd moonlink && cargo test -p moonlink --lib hash_index_summary::)

# Diff vs baseline:
(cd moonlink && git diff HEAD)
```
