//! fabric: content-addressed trees and references to runnable things.

use std::sync::Arc;

use object_store::ObjectStore;

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
/// `Graph` is not a database, it's git for your application's data: not
/// just files but any fact, and not just commits a human makes but any
/// derivation a Function makes.
#[derive(Clone)]
pub struct Graph {
    /// `Graph` is built directly on content addressing, so a relation's own
    /// source material lives in the same content-addressed space as the
    /// relation itself, not in a separate system.
    ///
    /// One ingestion pipeline delivers two consequences for free: store the source as a blob, run
    /// extraction (a Function), then write the resulting relations against that digest. Ingestion
    /// itself is just a relation between the graph and an external resource, the same mechanism
    /// any other derivation uses. That means there is no external store to sync with, since a
    /// relation's source lives inside the graph itself, and lineage runs all the way back to the
    /// true source for free, with no separate provenance mechanism needed.
    ///
    /// Re-extraction never re-fetches anything, because the source is pinned by digest forever.
    /// Changing extraction logic and rerunning it just adds new relations against the same
    /// source, leaving old ones intact.
    db:         slatedb::Db,
    store:      Arc<dyn ObjectStore>,
    cas_prefix: String,
    flushing:   storage::Flushing,
    chunking:   content_addressing::Chunking,
    codec:      content_addressing::Codec,
}
