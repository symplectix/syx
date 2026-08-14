//! A content-addressable (hyper)graph.
use std::io;
use std::sync::Arc;

use object_store::ObjectStore;
use tokio::io::{
    AsyncRead,
    AsyncWrite,
};

use crate::{
    Graph,
    storage,
};

/// Dispatches to one of several `MergeOperator`s by key prefix.
///
/// `slatedb` accepts exactly one `MergeOperator` per `db`, but the blob
/// storage engine (`storage::Storage`) and `Graph`'s own relation storage
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

/// Builds a `Graph`, opening the `slatedb::Db` it and its blob storage
/// engine (`storage::Storage`) share.
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
    /// the `Graph` and its `storage::Storage` over that same `db`.
    // TODO: `Graph`'s own relation storage will need its own merge operator
    // eventually (e.g. for growable reference sets, see hypergraph.md).
    // `PrefixMergeOperator` below is a rough scaffold for composing it with
    // the blob engine's once it exists, not a finished design -- `Graph`'s
    // own merge semantics don't exist yet.
    pub async fn build(self) -> io::Result<Graph> {
        let prefix =
            self.prefix.clone().unwrap_or_else(|| storage::Builder::DEFAULT_PREFIX.to_string());
        let merge_operator: Arc<dyn slatedb::MergeOperator + Send + Sync> =
            Arc::new(PrefixMergeOperator {
                routes: vec![(prefix.into_bytes(), storage::Storage::merge_operator())],
            });

        let db = slatedb::Db::builder(self.db_prefix, self.db_backend.clone())
            .with_merge_operator(merge_operator)
            .build()
            .await
            .map_err(io::Error::other)?;

        let packs_backend = self.packs_backend.unwrap_or_else(|| self.db_backend.clone());
        let mut storage_builder = storage::Storage::builder_with_db(db.clone(), packs_backend);
        if let Some(prefix) = self.prefix {
            storage_builder = storage_builder.prefix(prefix);
        }
        if let Some(packs_threshold) = self.packs_threshold {
            storage_builder = storage_builder.packs_threshold(packs_threshold);
        }
        if let Some(chunking) = self.chunking {
            storage_builder = storage_builder.chunking(chunking);
        }
        if let Some(codec) = self.codec {
            storage_builder = storage_builder.codec(codec);
        }

        Ok(Graph::new(db, storage_builder.build().await?))
    }
}

impl Graph {
    /// Starts building a `Graph`, which will open its own `db` at
    /// `db_prefix` in `db_backend`.
    pub fn builder(db_prefix: impl Into<String>, db_backend: Arc<dyn ObjectStore>) -> Builder {
        Builder::new(db_prefix, db_backend)
    }

    /// Wraps `db` (opened with the blob storage engine's merge operator
    /// registered, composed with whatever else `Graph`'s own future
    /// relation storage needs) and the `storage::Storage` built over that
    /// same `db`. Only `Builder::build` calls this -- construct a `Graph`
    /// via `Graph::builder` instead of opening `db`/building
    /// `storage::Storage` yourself.
    const fn new(db: slatedb::Db, storage: storage::Storage) -> Self {
        Self { db, storage }
    }

    /// Reads the content at `digest`, if present.
    pub async fn get<T: cas::FromBytes>(&self, digest: &cas::Digest) -> io::Result<Option<T>> {
        self.storage.get(digest).await
    }

    /// Reads the content at `digest` if present and write it to `w`.
    ///
    /// `get` is the better choice for values small enough that this doesn't matter.
    pub async fn read_into<W>(&self, digest: &cas::Digest, w: &mut W) -> io::Result<bool>
    where
        W: AsyncWrite + Unpin,
    {
        self.storage.read_into(digest, w).await
    }

    /// Store `content`, addressed by its own digest, and return that
    /// digest. A thin wrapper over `copy_from`.
    pub async fn put<T: cas::ToBytes>(&self, content: &T) -> io::Result<cas::Digest> {
        self.storage.put(content).await
    }

    /// Store the content read from `r` of `len` bytes, addressed by its
    /// own digest.
    pub async fn copy_from<R>(&self, len: u64, r: &mut R) -> io::Result<cas::Digest>
    where
        R: AsyncRead + Unpin,
    {
        self.storage.copy_from(len, r).await
    }

    /// Consolidates all currently-staged blobs into one new pack object.
    /// Mostly for tests -- `put`/`copy_from` already flush on their own
    /// once enough accumulates.
    pub async fn flush_pending(&self) -> io::Result<()> {
        self.storage.flush_pending().await
    }
}
