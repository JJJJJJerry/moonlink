# Hash-Index Summary — Remaining Work

> Companion to [`hash_index_summary_design.md`](./hash_index_summary_design.md).
> P0 (correctness + machine proofs) is done. The list below is what stands
> between this branch and a production replacement of the legacy
> `manifest_list` hash-index path.

## P1 — Required for production rollout

- [ ] **Wire `hash_index_private_storage` through pg_mooncake SQL surface.**
      `moonlink_backend::TableConfig` currently hard-codes `None`. Expose via
      `mooncake.create_table(..., hash_index_private_root => '<uri>')` and
      cover with one pg_regress test. Est. 2–3 d.

- [ ] **Mount `sweep_orphans` to a runtime trigger.**
      Pick one: (a) moonlink bgworker periodic tick; (b) SQL
      `mooncake.sweep_hash_index_orphans(table)`. Wire `SweepReport` into
      observability stats. Est. 2 d.

- [ ] **External-reader compatibility proof.**
      Smoke a pyiceberg `Table.scan()` and a Spark Iceberg read against a
      mooncake-mirrored table with private storage enabled. Confirm
      unknown `moonlink.*` summary keys are silently ignored. Est. 3–5 d
      (gated by env setup; reuse the Lakekeeper / TPC-H runbook).

- [ ] **Push moonlink submodule changes upstream.**
      Commit on `JJJJJJerry/moonlink`, bump `moonlink/` pointer in
      pg_mooncake, add a pg_mooncake commit for the bump. CLAUDE.md
      Gotchas #1. Est. 1 d.

## P2 — Operational hardening

- [ ] **Failure-injection coverage.**
      Inject IO faults at (a) index-block upload, (b) puffin write, (c)
      `txn.commit()` — assert in-memory state is untouched and only
      orphans accumulate. Uses existing chaos-test feature. Est. 2–3 d.

- [ ] **Migration design for legacy hash-in-manifest tables.**
      Switching `None → Some(...)` on an existing table currently leaves
      old manifest entries until the next flush rewrites the manifest.
      Document the sequence; add a smoke test that enables the feature
      mid-life and verifies no data is lost. Est. 2 d.

- [ ] **Document storage-backend support matrix.**
      Currently tested only on local FS. Confirm S3 / GCS / REST catalog
      / Glue catalog paths work end-to-end; flag any backend that fails
      the `metadata.json` size assumption (Glue ~400 KB cap). Est. 1–2 d.

## P3 — Polish

- [ ] **Path-normalization audit for `MooncakeFileIndex` keys.**
      Pre-existing legacy concern carried into Route A: removal lookups
      may miss when local-path vs remote-path keys diverge. Currently we
      log+continue; tighten once the legacy story is settled. Est. 1 d.

- [ ] **Replace `assert_eq!` on blob_type in `FileIndexBlob::from_blob`.**
      Baseline code (predates this branch); convert to structured error.
      Best as a separate baseline-hardening PR. Est. 0.5 d.

- [ ] **Observability hooks.**
      `publish_hash_index_to_summary` latency / call count; live-set size
      gauge; `mooncake.hash_index_status()` admin SQL view. Est. 1 d.

- [ ] **Multi-puffin-per-FileIndex schema extension.**
      Inherited from baseline's 1:1 invariant. Defer until vector / FT
      forces the relaxation. Est. 3–5 d when needed.

## Trigger table (when each item moves from "skip" to "do now")

| Signal | Items it unlocks |
| --- | --- |
| First external user wants Route A | P1 all |
| Multi-writer / SaaS deployment | P2 chaos + migration |
| Vector / FT work begins on D-lite | P3 multi-puffin |
| Glue catalog adoption | P2 storage-matrix |

## Definition of "production-complete"

All P1 items shipped, P2 items either shipped or formally deferred with
written rationale, and the design doc's `Known gaps` section reduced to
"no known gaps". P3 items may remain open indefinitely.
