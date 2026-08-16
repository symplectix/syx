//! A content-addressable (hyper)graph.
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use content_addressing as cas;
use object_store::ObjectStore;

use crate::{
    Cas,
    Graph,
    storage,
};

/// Builds a `Graph`, opening the `slatedb::Db` it and its blob-storage
/// parts share.
pub struct Builder {
    db_prefix: String,
    db_backend: Arc<dyn ObjectStore>,
    bitcask_dir: PathBuf,
    packs_backend: Option<Arc<dyn ObjectStore>>,
    cas_prefix: Option<String>,
    packs_threshold: Option<u64>,
    max_staging_duration: Option<Duration>,
    chunking: Option<cas::Chunking>,
    codec: Option<cas::Codec>,
}

impl Builder {
    fn new(
        db_prefix: impl Into<String>,
        db_backend: Arc<dyn ObjectStore>,
        bitcask_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            db_prefix: db_prefix.into(),
            db_backend,
            bitcask_dir: bitcask_dir.into(),
            packs_backend: None,
            cas_prefix: None,
            packs_threshold: None,
            max_staging_duration: None,
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

    /// How long to let a blob sit staged, unpacked, before consolidating
    /// regardless of `packs_threshold`. Bounds how long staged content
    /// stays invisible to every other reader of this `Graph`.
    pub fn max_staging_duration(mut self, max_staging_duration: Duration) -> Self {
        self.max_staging_duration = Some(max_staging_duration);
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

    /// Opens `db` and `bitcask`, and builds the `Graph`.
    // TODO: `Graph`'s own relation storage will need its own merge
    // operator eventually, e.g. for growable reference sets, see
    // hypergraph.md. Nothing registers one on `db` today, since the blob
    // storage engine no longer needs merge semantics now that staging
    // lives in `bitcask`, not `db`.
    pub async fn build(self) -> io::Result<Graph> {
        let cas_prefix: Arc<str> =
            Arc::from(self.cas_prefix.unwrap_or_else(|| storage::DEFAULT_CAS_PREFIX.to_string()));

        let db = slatedb::Db::builder(self.db_prefix, self.db_backend.clone())
            .build()
            .await
            .map_err(io::Error::other)?;

        let codec = self.codec.unwrap_or_default();
        let bitcask = Arc::new(storage::Bitcask::open(self.bitcask_dir, codec).await?);

        let packs_backend = self.packs_backend.unwrap_or_else(|| self.db_backend.clone());
        let packs_threshold = self.packs_threshold.unwrap_or(storage::DEFAULT_PACKS_THRESHOLD);
        let max_staging_duration =
            self.max_staging_duration.unwrap_or(storage::DEFAULT_MAX_STAGING_DURATION);
        let chunking = self.chunking.unwrap_or_default();

        let flushing = storage::Flushing::new(packs_threshold, max_staging_duration);
        Ok(Graph::new(db, packs_backend, bitcask, cas_prefix, flushing, chunking, codec))
    }
}

impl Graph {
    /// Starts building a `Graph`, which will open its own `db` at
    /// `db_prefix` in `db_backend`, and stage not-yet-packed blobs in
    /// `bitcask_dir`, a local directory only this `Graph` should use.
    pub fn builder(
        db_prefix: impl Into<String>,
        db_backend: Arc<dyn ObjectStore>,
        bitcask_dir: impl Into<PathBuf>,
    ) -> Builder {
        Builder::new(db_prefix, db_backend, bitcask_dir)
    }

    /// Only `Builder::build` calls this. Construct a `Graph` via
    /// `Graph::builder` instead of opening `db`/building these parts
    /// yourself.
    const fn new(
        db: slatedb::Db,
        store: Arc<dyn ObjectStore>,
        bitcask: Arc<storage::Bitcask>,
        cas_prefix: Arc<str>,
        flushing: storage::Flushing,
        chunking: cas::Chunking,
        codec: cas::Codec,
    ) -> Self {
        Self { db, store, bitcask, cas_prefix, flushing, chunking, codec }
    }

    /// The blob-storage facet of this `Graph`: `get`/`put`/`read_into`/
    /// `copy_from`/`flush_pending`. A cheap, borrowed view. Construct it
    /// fresh wherever it's needed rather than holding onto one.
    pub fn cas(&self) -> Cas<'_> {
        Cas::new(
            &self.db,
            &self.store,
            &self.bitcask,
            &self.cas_prefix,
            &self.flushing,
            self.chunking,
            self.codec,
        )
    }
}
