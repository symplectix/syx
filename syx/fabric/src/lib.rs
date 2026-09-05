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
/// `Graph` is built directly on content addressing: sources,
/// derivations, and the relations between them all live in one address
/// space, not across separate systems. That is what makes `Graph` a
/// plausible git for application data: not just files, but any fact;
/// not just commits a human makes, but any derivation a Function makes.
///
/// A relation's source is just more content in the graph, so ingesting
/// external data needs no special pipeline and nothing external to keep
/// in sync. Lineage and re-extraction follow for free: every relation
/// traces back to its true source through the graph itself, and because
/// a source is pinned by its digest forever, rerunning extraction after
/// a logic change only adds new relations against the same source,
/// leaving old ones untouched.
#[derive(Clone)]
pub struct Graph {
    // Durably holds not-yet-packed content until it's forgotten (packed
    // elsewhere). The only thing `Graph` can't default: everything below
    // can fall back to living under the same directory.
    forgetter: Arc<forgetter::Forgetter>,
    // Maps a blob's digest to where `forgetter` is holding it; see
    // `storage::KeyDir`'s own doc for why this lives here and not
    // in `forgetter` itself.
    staged:    Arc<storage::KeyDir>,

    // `db`: the pointer/relation store.
    db: slatedb::Db,

    // Packed blob object storage, and when to consolidate `forgetter`'s
    // content into it.
    blobs:    Arc<dyn ObjectStore>,
    flushing: storage::Flushing,

    // Content addressing, applies uniformly regardless of backend.
    cas_prefix: Arc<str>,
    chunking:   content_addressing::Chunking,
    codec:      content_addressing::Codec,
}
