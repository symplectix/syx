//! A content-addressable (hyper)graph.
use std::io;
use std::sync::Arc;

use content_addressing as cas;
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
    cas_prefix:      Option<String>,
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
            cas_prefix: None,
            packs_threshold: None,
            chunking: None,
            codec: None,
        }
    }

    /// The key prefix blobs are staged and packed under. Named
    /// `cas_prefix`, not just `prefix`, to avoid confusion with
    /// `db_prefix`, the unrelated prefix `slatedb::Db` itself is opened
    /// under.
    pub fn cas_prefix(mut self, cas_prefix: impl Into<String>) -> Self {
        self.cas_prefix = Some(cas_prefix.into());
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

    // TODO: `chunking` and `codec` can be overridden independently, but
    // `cas::Codec::SNIFF_LEN` should stay below `cas::Chunking::MIN_SIZE`.
    // Nothing breaks if it happens, but `Codec::encode` compresses a chunk
    // twice instead of once.

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
    /// the `Graph`.
    // TODO: `Graph`'s own relation storage will need its own merge operator
    // eventually.
    pub async fn build(self) -> io::Result<Graph> {
        let cas_prefix = self.cas_prefix.unwrap_or_else(|| storage::DEFAULT_CAS_PREFIX.to_string());
        let merge_operator: Arc<dyn slatedb::MergeOperator + Send + Sync> =
            Arc::new(PrefixMergeOperator {
                routes: vec![(cas_prefix.clone().into_bytes(), storage::merge_operator())],
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

        let flushing = storage::Flushing::new(packs_threshold);
        Ok(Graph::new(db, packs_backend, cas_prefix, flushing, chunking, codec))
    }
}

impl Graph {
    /// Starts building a `Graph`, which will open its own `db` at
    /// `db_prefix` in `db_backend`.
    pub fn builder(db_prefix: impl Into<String>, db_backend: Arc<dyn ObjectStore>) -> Builder {
        Builder::new(db_prefix, db_backend)
    }

    /// Only `Builder::build` calls this. Construct a `Graph` via
    /// `Graph::builder` instead of opening `db`/building these parts
    /// yourself.
    const fn new(
        db: slatedb::Db,
        store: Arc<dyn ObjectStore>,
        cas_prefix: String,
        flushing: storage::Flushing,
        chunking: cas::Chunking,
        codec: cas::Codec,
    ) -> Self {
        Self { db, store, cas_prefix, flushing, chunking, codec }
    }

    /// The blob-storage facet of this `Graph`: `get`/`put`/`read_into`/
    /// `copy_from`/`flush_pending`. A cheap, borrowed view. Construct it
    /// fresh wherever it's needed rather than holding onto one.
    pub fn cas(&self) -> Cas<'_> {
        Cas::new(&self.db, &self.store, &self.cas_prefix, &self.flushing, self.chunking, self.codec)
    }
}
