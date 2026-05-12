use iceberg::{TableCommit, TableIdent, TableRequirement, TableUpdate};

/// Staging wrapper for building an iceberg [`TableCommit`].
///
/// iceberg-rust 0.9.1 intentionally keeps `TableCommit::builder().build()` crate-private — the
/// upstream comment at iceberg/src/catalog/mod.rs:330 warns that direct construction is "dangerous
/// and error-prone" and users should go through `crate::transaction::Transaction`. Moonlink cannot
/// yet use that path because it needs custom catalog updates (puffin manifest rewrite, data-file
/// removal) the public `Transaction` API does not model. Until the upstream API gains custom
/// actions, we transmute through this mirror struct.
///
/// SAFETY contract — every condition below must hold for `take_as_table_commit` to be sound:
///
/// 1. Field count, names, types, and *declaration order* must match upstream `TableCommit` in
///    catalog/mod.rs:335 exactly. Any upstream field reorder, addition, or type tweak (including
///    swapping `Vec<T>` for another container of the same size/align) breaks soundness.
/// 2. Both this struct and upstream `TableCommit` use the default `#[repr(Rust)]`. Adding
///    `#[repr(C)]` here would *increase* risk — `repr(Rust)` lets the compiler reorder fields,
///    and our copy must follow the same reordering algorithm as upstream's copy. Same declaration
///    order + same `repr(Rust)` + same rustc invocation gives the best chance of identical layout.
///    This is still not a *guarantee* (the algorithm is unspecified) but is the best we can do
///    without `#[repr(C)]` on the upstream definition.
/// 3. The compile-time asserts below catch the easy-to-spot regressions (size / align). They do
///    NOT catch field reordering when field-size sums coincide, so condition (1) must be re-verified
///    by hand on every iceberg-rust upgrade. See task tracker for the planned migration to the
///    upstream `Transaction` API once it grows custom-action support.
#[allow(dead_code)] // fields are read after transmute into iceberg::TableCommit
pub(crate) struct TableCommitProxy {
    pub(crate) ident: TableIdent,
    pub(crate) requirements: Vec<TableRequirement>,
    pub(crate) updates: Vec<TableUpdate>,
}

// Compile-time guard rails. Upgraded from runtime `assert_eq!` so any layout drift fails the
// build instead of crashing at the first commit attempt.
const _: () = assert!(
    std::mem::size_of::<TableCommitProxy>() == std::mem::size_of::<TableCommit>(),
    "TableCommitProxy size diverged from iceberg::TableCommit — upstream struct changed; \
     update the proxy fields to match catalog/mod.rs:335 in the new iceberg-rust release."
);
const _: () = assert!(
    std::mem::align_of::<TableCommitProxy>() == std::mem::align_of::<TableCommit>(),
    "TableCommitProxy align diverged from iceberg::TableCommit — see above."
);

impl TableCommitProxy {
    /// Take as [`TableCommit`].
    ///
    /// SAFETY: relies on the layout contract documented at the struct level. The `const _`
    /// asserts above catch size/align regressions at compile time, but cannot catch field
    /// reordering — re-audit the field list on every iceberg-rust upgrade.
    pub(crate) fn take_as_table_commit(self) -> TableCommit {
        unsafe { std::mem::transmute::<TableCommitProxy, TableCommit>(self) }
    }
}
