//! fabric: content-addressed trees and references to runnable things.

use content_addressing as cas;

mod blob;
mod function;
mod graph;
mod storage;

pub use blob::{
    Node,
    Tree,
};
pub use function::{
    Command,
    Function,
};
pub use graph::Builder;
pub use storage::Cas;

/// A content-addressable (hyper)graph.
///
/// `Graph` is not a database, it's git for your application's data:
/// - not just files but any fact
/// - not just commits a human makes but any derivations a Function makes
#[derive(Clone)]
pub struct Graph {
    /// `Graph` holds its blob-storage parts directly (not behind a
    /// `Storage` type) and builds a `Cas<'_>` view over them on demand
    /// (`Graph::cas`, in `graph.rs`), so a relation's own source material
    /// lives in the same content-addressed space as the relation itself,
    /// not in a separate system.
    /// - One ingestion pipeline, two consequences for free: store the source as a blob, run
    ///   extraction (a Function), write the resulting relations against that digest. Ingestion
    ///   itself is just a relation between the graph and an external resource, the same mechanism
    ///   any other derivation uses. That gets: no external store to sync with, since a relation's
    ///   source lives inside the graph itself; and lineage all the way back to the true source for
    ///   free, no separate provenance mechanism needed.
    /// - Re-extraction never re-fetches anything: the source is pinned by digest forever, so
    ///   changing extraction logic and rerunning it just adds new relations against the same
    ///   source, old ones left intact.
    db:       slatedb::Db,
    packing:  storage::Packing,
    chunking: cas::Chunking,
    codec:    cas::Codec,
}
