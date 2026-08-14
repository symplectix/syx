//! A content-addressable (hyper)graph.
use std::io;
use std::sync::Arc;

use object_store::ObjectStore;
use tokio::io::{
    AsyncRead,
    AsyncWrite,
};

use crate::Graph;

/// Builds a `Graph`, opening the `slatedb::Db` it and its `cas::Storage`
/// share.
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

    /// Opens `db`, registered with `cas::Storage::merge_operator()`, and
    /// builds the `Graph` and its `cas::Storage` over that same `db`.
    pub async fn build(self) -> io::Result<Graph> {
        let db = slatedb::Db::builder(self.db_prefix, self.db_backend.clone())
            .with_merge_operator(cas::Storage::merge_operator())
            .build()
            .await
            .map_err(io::Error::other)?;

        let packs_backend = self.packs_backend.unwrap_or_else(|| self.db_backend.clone());
        let mut cas_builder = cas::Storage::builder_with_db(db.clone(), packs_backend);
        if let Some(prefix) = self.prefix {
            cas_builder = cas_builder.prefix(prefix);
        }
        if let Some(packs_threshold) = self.packs_threshold {
            cas_builder = cas_builder.packs_threshold(packs_threshold);
        }
        if let Some(chunking) = self.chunking {
            cas_builder = cas_builder.chunking(chunking);
        }
        if let Some(codec) = self.codec {
            cas_builder = cas_builder.codec(codec);
        }

        Ok(Graph::new(db, cas_builder.build().await?))
    }
}

impl Graph {
    /// Starts building a `Graph`, which will open its own `db` at
    /// `db_prefix` in `db_backend`.
    pub fn builder(db_prefix: impl Into<String>, db_backend: Arc<dyn ObjectStore>) -> Builder {
        Builder::new(db_prefix, db_backend)
    }

    /// Wraps `db` (opened with `cas::Storage::merge_operator()` registered,
    /// composed with whatever else `Graph`'s own future relation storage
    /// needs) and the `cas::Storage` built over that same `db`. Only
    /// `Builder::build` calls this -- construct a `Graph` via
    /// `Graph::builder` instead of opening `db`/building `cas::Storage`
    /// yourself.
    const fn new(db: slatedb::Db, cas: cas::Storage) -> Self {
        Self { db, cas }
    }

    /// Reads the content at `digest`, if present.
    pub async fn get<T: cas::FromBytes>(&self, digest: &cas::Digest) -> io::Result<Option<T>> {
        self.cas.get(digest).await
    }

    /// Reads the content at `digest` if present and write it to `w`.
    ///
    /// `get` is the better choice for values small enough that this doesn't matter.
    pub async fn read_into<W>(&self, digest: &cas::Digest, w: &mut W) -> io::Result<bool>
    where
        W: AsyncWrite + Unpin,
    {
        self.cas.read_into(digest, w).await
    }

    /// Store `content`, addressed by its own digest, and return that
    /// digest. A thin wrapper over `copy_from`.
    pub async fn put<T: cas::ToBytes>(&self, content: &T) -> io::Result<cas::Digest> {
        self.cas.put(content).await
    }

    /// Store the content read from `r` of `len` bytes, addressed by its
    /// own digest.
    pub async fn copy_from<R>(&self, len: u64, r: &mut R) -> io::Result<cas::Digest>
    where
        R: AsyncRead + Unpin,
    {
        self.cas.copy_from(len, r).await
    }
}
