//! A content-addressable (hyper)graph.
use std::io;
use std::sync::Arc;

use object_store::ObjectStore;

use crate::{
    Cas,
    Graph,
    storage,
};

/// Dispatches to one of several `MergeOperator`s by key prefix.
///
/// `slatedb` accepts exactly one `MergeOperator` per `db`, but the blob
/// storage engine (`storage::Cas`) and `Graph`'s own relation storage
/// each need their own merge semantics on the same `db`.
struct PrefixMergeOperator {
    routes: Vec<(Vec<u8>, Box<dyn slatedb::MergeOperator + Send + Sync>)>,
}

impl PrefixMergeOperator {
    fn route(
        &self,
        key: &cas::Bytes,
    ) -> Result<&(dyn slatedb::MergeOperator + Send + Sync), slatedb::MergeOperatorError> {
        self.routes
            .iter()
            .find(|(prefix, _)| key.starts_with(prefix.as_slice()))
            .map(|(_, operator)| operator.as_ref())
            .ok_or_else(|| slatedb::MergeOperatorError::Callback {
                message: format!("no merge operator registered for key {key:?}"),
            })
    }
}

impl slatedb::MergeOperator for PrefixMergeOperator {
    fn merge(
        &self,
        key: &cas::Bytes,
        existing_value: Option<cas::Bytes>,
        value: cas::Bytes,
    ) -> Result<cas::Bytes, slatedb::MergeOperatorError> {
        self.route(key)?.merge(key, existing_value, value)
    }

    fn merge_batch(
        &self,
        key: &cas::Bytes,
        existing_value: Option<cas::Bytes>,
        operands: &[cas::Bytes],
    ) -> Result<cas::Bytes, slatedb::MergeOperatorError> {
        self.route(key)?.merge_batch(key, existing_value, operands)
    }
}

/// Builds a `Graph`, opening the `slatedb::Db` it and its blob-storage
/// parts share.
pub struct Builder {
    db_prefix:       String,
    db_backend:      Arc<dyn ObjectStore>,
    packs_backend:   Option<Arc<dyn ObjectStore>>,
    prefix:          Option<String>,
    packs_threshold: Option<u64>,
    chunking:        Option<cas::Chunking>,
    codec:           Option<cas::Codec>,
}

impl Builder {
    fn new(db_prefix: impl Into<String>, db_backend: Arc<dyn ObjectStore>) -> Self {
        Self {
            db_prefix: db_prefix.into(),
            db_backend,
            packs_backend: None,
            prefix: None,
            packs_threshold: None,
            chunking: None,
            codec: None,
        }
    }

    /// The key prefix blobs are staged and packed under.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Writes pack objects to `packs` instead of `db`'s own backend.
    pub fn packs(mut self, packs: Arc<dyn ObjectStore>) -> Self {
        self.packs_backend = Some(packs);
        self
    }

    /// How many bytes to stage before consolidating into a pack.
    pub fn packs_threshold(mut self, packs_threshold: u64) -> Self {
        self.packs_threshold = Some(packs_threshold);
        self
    }

    /// Overrides chunking behavior.
    pub fn chunking(mut self, chunking: cas::Chunking) -> Self {
        self.chunking = Some(chunking);
        self
    }

    /// Overrides encoding/decoding behavior.
    pub fn codec(mut self, codec: cas::Codec) -> Self {
        self.codec = Some(codec);
        self
    }

    /// Opens `db`, registered with a `PrefixMergeOperator` that currently
    /// only routes to the blob storage engine's own operator, and builds
    /// the `Graph` -- `db` plus the blob-storage parts (`stage`/`packs`/
    /// `chunking`/`codec`) `Graph` holds directly.
    // TODO: `Graph`'s own relation storage will need its own merge operator
    // eventually (e.g. for growable reference sets, see hypergraph.md).
    // `PrefixMergeOperator` below is a rough scaffold for composing it with
    // the blob engine's once it exists, not a finished design -- `Graph`'s
    // own merge semantics don't exist yet.
    pub async fn build(self) -> io::Result<Graph> {
        let prefix = self.prefix.unwrap_or_else(|| storage::DEFAULT_PREFIX.to_string());
        let merge_operator: Arc<dyn slatedb::MergeOperator + Send + Sync> =
            Arc::new(PrefixMergeOperator {
                routes: vec![(prefix.clone().into_bytes(), storage::merge_operator())],
            });

        let db = slatedb::Db::builder(self.db_prefix, self.db_backend.clone())
            .with_merge_operator(merge_operator)
            .build()
            .await
            .map_err(io::Error::other)?;

        let packs_backend = self.packs_backend.unwrap_or_else(|| self.db_backend.clone());
        let packs_threshold = self.packs_threshold.unwrap_or(storage::DEFAULT_PACKS_THRESHOLD);
        let chunking = self.chunking.unwrap_or_default();
        let codec = self.codec.unwrap_or_default();

        let storage::Parts { stage, packs } =
            storage::parts(packs_backend, prefix, packs_threshold);
        Ok(Graph::new(db, stage, packs, chunking, codec))
    }
}

impl Graph {
    /// Starts building a `Graph`, which will open its own `db` at
    /// `db_prefix` in `db_backend`.
    pub fn builder(db_prefix: impl Into<String>, db_backend: Arc<dyn ObjectStore>) -> Builder {
        Builder::new(db_prefix, db_backend)
    }

    /// Assembles `Graph` from its own `db` (opened with the blob storage
    /// engine's merge operator registered, composed with whatever else
    /// `Graph`'s own future relation storage needs) and the blob-storage
    /// parts staged into that same `db`. Only `Builder::build` calls this
    /// -- construct a `Graph` via `Graph::builder` instead of opening
    /// `db`/building these parts yourself.
    const fn new(
        db: slatedb::Db,
        stage: storage::Stage,
        packs: storage::Packs,
        chunking: cas::Chunking,
        codec: cas::Codec,
    ) -> Self {
        Self { db, stage, packs, chunking, codec }
    }

    /// The blob-storage facet of this `Graph`: `get`/`put`/`read_into`/
    /// `copy_from`/`flush_pending`. A cheap, borrowed view -- construct
    /// it fresh wherever it's needed rather than holding onto one.
    pub fn cas(&self) -> Cas<'_> {
        Cas::new(&self.db, &self.stage, &self.packs, self.chunking, self.codec)
    }
}
