//! Phase A index abstraction skeleton.
//!
//! Forward-compatible types and traits for the two-layer index design.
//! Phase A introduces only declarations; behavior is unchanged. The existing
//! `crate::storage::index::MooncakeIndex` / `Vec<FileIndex>` machinery remains
//! the in-use code path and is additionally exposed through
//! [`Layer1IndexCollection`] for callers built against the future Phase E shape.
//!
//! Cross-references:
//! - Roadmap §Phase A and IMPLEMENTATION_PLAN §2 Phase A tasks A-1 .. A-11.
//! - Schema alignment target: Iceberg PR #15101 IndexCatalog (`index-id`, `type`,
//!   `properties`, version builder, commit protocol, history chain).
//! - Phase E pre-commit invariants live in this module's doc-comments so future
//!   PRs reviewing new variants cannot skip the §three-of-one self-check.

use crate::storage::index::{FileIndex, MooncakeIndex};
use crate::storage::storage_utils::FileId;

// ---------------------------------------------------------------------------
// A-1: IndexType
// ---------------------------------------------------------------------------

/// Index category, kept 1:1 with the variants proposed in Iceberg PR #15101's
/// IndexCatalog metadata schema.
///
/// Phase wiring:
/// - `Hash`     -- implemented in Phase B (private manifest, Mode 2a).
/// - `Bloom`    -- implemented in Phase D (Iceberg `metadata.statistics`,
///                 Mode 1a, PR #15311 advisory path).
/// - `BTree` / `Inverted` / `Term` / `Ivf` / `Hnsw` -- declared in Phase A,
///   implemented in Phase F / G / H+. Keeping them as enum variants now is the
///   anti-fragility guarantee that Phase A's trait surface does not collapse
///   when full-text or vector indexes arrive.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum IndexType {
    /// Primary-key hash collection (Layer 1). Phase B.
    Hash,
    /// B-tree / range index. Phase H+ (PR #15101 BTREE alignment).
    BTree,
    /// Per-file Bloom filter, advisory pruning. Phase D.
    Bloom,
    /// Tokenized term index, full-text search backbone. Phase G.
    Term,
    /// Inverted index over secondary columns. Phase E / G.
    Inverted,
    /// IVF-based vector index. Phase F.
    Ivf,
    /// HNSW graph vector index. Phase F.
    Hnsw,
}

// ---------------------------------------------------------------------------
// A-2: BlobLocation
//
// A-11: each variant's doc comment states which condition of the
// §three-of-one Invariant (Iceberg live-set visible / embedded in data file /
// rebuildable + query fallback) the variant satisfies. PR review must reject
// any variant that satisfies none.
// ---------------------------------------------------------------------------

/// Physical placement of an index payload (Puffin blob, embedded Parquet
/// segment, or external per-file artifact).
///
/// Each variant must satisfy at least one branch of the §three-of-one
/// Invariant. The doc on each variant records which branch it relies on; any
/// new variant added in a later phase must do the same or fail PR review.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum BlobLocation {
    /// Mode 2a: payload stored under the mooncake-private root (outside the
    /// Iceberg table root). Used by the Phase B PK hash collection.
    ///
    /// §three-of-one branch: **(3) rebuildable + query fallback**. The blob is
    /// not visible to Iceberg readers; on loss, Layer 1 enters Degraded and
    /// rebuilds from Layer 2 / data files, while cost-gated probes fall back to
    /// scan. Industrial precedent: Apache Paimon `FileIndex` + private manifest
    /// (PIP-38).
    PrivatePuffin { path: String },

    /// Mode 1a: payload stored as a standard Iceberg Puffin blob registered in
    /// `metadata.statistics`. Used by the Phase D Bloom filter.
    ///
    /// §three-of-one branch: **(1) Iceberg live-set visible**. The blob is
    /// referenced from `metadata.statistics` so any compliant Iceberg cleanup
    /// keeps it alive. Reference: Iceberg PR #15311.
    StandardPuffin {
        puffin_path: String,
        blob_idx: usize,
    },

    /// Mode 2c: payload embedded in the Parquet body of a data file with
    /// pointers in the Parquet footer key-value metadata. Used by the Phase E
    /// Layer-2 per-file index (PK hash + secondary inverted).
    ///
    /// §three-of-one branch: **(2) embedded in data file**. The blob shares the
    /// data file's lifecycle exactly. Reference: DataFusion 2025-07
    /// user-defined Parquet indexes.
    EmbeddedParquet {
        file: String,
        offset: u64,
        size: u64,
    },

    /// Mode 2d: payload stored as a per-file external Puffin sibling, with the
    /// data file's manifest entry listing the sibling under
    /// `referenced-data-files`. Used by Phase F / G vector and full-text blobs
    /// that are too large to embed.
    ///
    /// §three-of-one branch: **(1) Iceberg live-set visible (weak) + (3)
    /// rebuildable + query fallback**. The sibling is reachable from Iceberg
    /// manifests, and at runtime brute-force / `unindexed_path` is the
    /// correctness fallback. Reference: DataFusion 2025-08 external Parquet
    /// indexes; LanceDB `unindexed_path`.
    ///
    /// **Must be declared in Phase A** even though no Phase A/B/D code
    /// emits it; omitting it would force Phase F/G to retrofit the trait
    /// surface, defeating the anti-fragility goal of this module.
    ExternalPuffinPerFile {
        puffin_path: String,
        blob_idx: usize,
    },
}

// ---------------------------------------------------------------------------
// A-3: Layer1Strategy
// ---------------------------------------------------------------------------

/// Strategy used by an `IndexType` to organise its Layer-1 (cross-file)
/// dispatcher. Layer 1 is heterogeneous on purpose: PK hash, Bloom, full-text
/// stats, and vector centroids each have fundamentally different aggregation
/// shapes, and Phase A must keep all of them as siblings so that adding a
/// new modality in Phase F-G never forces a redesign of this enum.
///
/// Phase wiring (the same staging as [`IndexType`]):
/// - `HashCollection`           -- Phase B implementation.
/// - `PerFileBloom`             -- Phase D implementation.
/// - `RuntimeStatsAggregator`   -- Phase G implementation.
/// - `IvfCentroidShared`        -- Phase F implementation.
/// - `HnswMerger`               -- Phase F implementation.
/// - `None`                     -- advisory-only indexes with no cross-file
///                                 dispatch (kept as an explicit variant so
///                                 callers must handle it).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Layer1Strategy {
    /// SingleStore SIGMOD'22 cross-segment hash collection. Phase B.
    HashCollection,
    /// Per-file Bloom filter, no cross-file merge. Phase D.
    PerFileBloom,
    /// Runtime BM25 statistics aggregator (no materialised cross-file index).
    /// Phase G. Reference: SingleStore FTS v2 `BM25_GLOBAL`, Tantivy
    /// `Searcher::collection_statistics`.
    RuntimeStatsAggregator,
    /// Shared IVF centroid table across per-file vector indexes. Phase F.
    /// Reference: LanceDB `centroids_tensor`.
    IvfCentroidShared,
    /// Incremental HNSW graph merger across per-file segments. Phase F.
    /// Reference: Lucene 9 `IncrementalHnswGraphMerger`.
    HnswMerger,
    /// No cross-file dispatcher; the index is advisory-only.
    None,
}

// ---------------------------------------------------------------------------
// A-4: Layer1IndexCollection trait
// A-8: adapter implementation over the existing Vec<FileIndex>
// ---------------------------------------------------------------------------

/// Cross-file dispatcher contract.
///
/// This trait describes the **future Phase E shape** of Layer 1:
/// `probe(key)` returns only the set of files that may contain the key, and
/// the per-row offset is resolved by Layer 2 (embedded Parquet + footer KV).
///
/// Phase A only declares the contract and provides a behaviour-preserving
/// adapter for the existing `Vec<FileIndex>` collection (see the impl on
/// [`MooncakeIndex`]). Existing call sites continue to use
/// `MooncakeIndex::find_record` / `insert_file_index` directly; this trait is
/// dead code until Phase B starts wiring it in.
#[allow(dead_code)]
pub trait Layer1IndexCollection {
    /// Return the set of data files in which `key` may live.
    ///
    /// Phase E contract: this is the only information Layer 1 surfaces.
    /// Row offsets are resolved by Layer 2 inside each candidate file.
    fn probe(&self, key: u64) -> Vec<FileId>;

    /// Register a new file's Layer-1 contribution.
    fn add_file(&mut self, file_id: FileId, file_index: FileIndex);

    /// Run the cross-file maintenance pass (Phase B: `FileIndexMerger`).
    fn merge(&mut self);
}

impl Layer1IndexCollection for MooncakeIndex {
    fn probe(&self, key: u64) -> Vec<FileId> {
        use crate::storage::index::persisted_bucket_hash_map::splitmix64;
        let hashed = splitmix64(key);
        let mut hits = Vec::new();
        for file_index in &self.file_indices {
            for index_block in &file_index.index_blocks {
                let bucket =
                    (hashed >> file_index.hash_lower_bits) & ((1u64 << file_index.bucket_bits) - 1);
                let bucket = bucket as u32;
                if bucket >= index_block.bucket_start_idx && bucket < index_block.bucket_end_idx {
                    // Conservative membership: a matching bucket range means
                    // the key may live in any data file backing this index.
                    for file in &file_index.files {
                        hits.push(file.file_id());
                    }
                    break;
                }
            }
        }
        hits
    }

    fn add_file(&mut self, _file_id: FileId, file_index: FileIndex) {
        // The FileId of each contributing data file is already carried inside
        // `file_index.files`; the adapter argument is kept for forward
        // compatibility with the Phase E shape where Layer 1 is keyed by file.
        self.insert_file_index(file_index);
    }

    fn merge(&mut self) {
        // Merging lives outside MooncakeIndex today (see FileIndexMerger /
        // compaction in storage::compaction_manager). Phase A keeps this a
        // no-op so the trait can be exposed without behaviour change; Phase B
        // will route the existing merger through this entry point.
    }
}

// ---------------------------------------------------------------------------
// A-5 / A-6 / A-7: forward-compatible stubs for full-text & vector phases.
//
// These traits and the `IndexCoverage` type exist solely so that hash-first
// work in Phase B does not freeze the surface against the Phase F/G shapes.
// They are intentionally empty: Phase F (vector) and Phase G (full-text) will
// fill them in. Adding methods now without a real consumer would either rot
// or pin a wrong contract; the TODO(Jerry) markers below are the explicit
// hand-off points.
// ---------------------------------------------------------------------------

/// Runtime aggregator for cross-file statistics (full-text BM25 `df` / `avgdl`,
/// scalar histograms, etc.). Phase G implementation; placeholder for hash-first
/// work. References: SingleStore FTS v2 `BM25_GLOBAL`, Tantivy
/// `Searcher::collection_statistics`.
//
// TODO(Jerry): Phase G — define the aggregation associated type, the
// per-file `contribute(&mut self, ...)` method, and the `finalize() -> Stats`
// shape once tantivy integration is scoped.
#[allow(dead_code)]
pub trait CrossFileAggregator {}

/// Cross-file top-K merger (vector ANN scoring, BM25 ranking). Phase F-G
/// implementation; placeholder for hash-first work. Reference: LanceDB
/// `unindexed_path` + Elasticsearch 8.10 shared-threshold pruning.
//
// TODO(Jerry): Phase F — define the `Score` associated type, the `push`
// method, and the early-termination hook once ANN library (usearch) is
// selected per F-1.
#[allow(dead_code)]
pub trait CrossFileTopK {}

/// Per-file index coverage bitmap. Tracks which data files in the current
/// Iceberg snapshot already have an index of a given `IndexType` built; used
/// by Phase F vector / Phase G FT to drive the brute-force fallback path.
/// Reference: LanceDB `fragment_bitmap`.
//
// TODO(Jerry): Phase F — replace with a real roaring / fixed-bit bitmap once
// 10M+ file scale numbers from Phase C-2 spike land. Hash path does not need
// this (every Phase B-D file is in Layer 1 by construction).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[allow(dead_code)]
pub struct IndexCoverage;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enum_variants_compile() {
        // Smoke test: every variant must be constructible so future PRs cannot
        // silently drop one. The §three-of-one self-check above is the
        // qualitative gate; this test is the syntactic one.
        let _ = IndexType::Hash;
        let _ = IndexType::BTree;
        let _ = IndexType::Bloom;
        let _ = IndexType::Term;
        let _ = IndexType::Inverted;
        let _ = IndexType::Ivf;
        let _ = IndexType::Hnsw;

        let _ = BlobLocation::PrivatePuffin {
            path: String::new(),
        };
        let _ = BlobLocation::StandardPuffin {
            puffin_path: String::new(),
            blob_idx: 0,
        };
        let _ = BlobLocation::EmbeddedParquet {
            file: String::new(),
            offset: 0,
            size: 0,
        };
        let _ = BlobLocation::ExternalPuffinPerFile {
            puffin_path: String::new(),
            blob_idx: 0,
        };

        let _ = Layer1Strategy::HashCollection;
        let _ = Layer1Strategy::PerFileBloom;
        let _ = Layer1Strategy::RuntimeStatsAggregator;
        let _ = Layer1Strategy::IvfCentroidShared;
        let _ = Layer1Strategy::HnswMerger;
        let _ = Layer1Strategy::None;
    }

    #[test]
    fn probe_on_empty_index_returns_no_hits() {
        let index = MooncakeIndex::new();
        assert!(Layer1IndexCollection::probe(&index, 42).is_empty());
    }
}
