//! A content-addressable (hyper)graph.
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use content_addressing as cas;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use tokio::fs;

use crate::{
    Cas,
    Graph,
    storage,
};

/// Builds a `Graph`, opening the `slatedb::Db` it and its blob-storage
/// parts share.
///
/// `forgetter_dir` is the only thing that must be specified: a local
/// directory `Graph` durably holds not-yet-packed blobs in. Everything
/// else, including where `db`/`blobs` physically live, defaults to also
/// living under `forgetter_dir` via a local `object_store`, so a `Graph`
/// works standalone with zero external setup; override `db_backend`/
/// `blobs` to point at S3 (or any other `object_store` backend) instead.
pub struct Builder {
    // forgetter
    forgetter_dir:          PathBuf,
    max_forgetter_duration: Option<Duration>,
    max_pending_segments:   Option<u16>,

    // db
    db_prefix:  Option<String>,
    db_backend: Option<Arc<dyn ObjectStore>>,

    // blobs
    blobs_backend:   Option<Arc<dyn ObjectStore>>,
    flush_threshold: Option<u64>,

    // content addressing
    cas_prefix: Option<String>,
    chunking:   Option<cas::Chunking>,
    codec:      Option<cas::Codec>,
}

impl Builder {
    fn new(forgetter_dir: impl Into<PathBuf>) -> Self {
        Self {
            forgetter_dir: forgetter_dir.into(),
            max_forgetter_duration: None,
            max_pending_segments: None,
            db_prefix: None,
            db_backend: None,
            blobs_backend: None,
            flush_threshold: None,
            cas_prefix: None,
            chunking: None,
            codec: None,
        }
    }

    /// How long `forgetter` lets a segment stay active before rotating
    /// it out on its own, regardless of `flush_threshold`.
    pub fn max_forgetter_duration(mut self, max_forgetter_duration: Duration) -> Self {
        self.max_forgetter_duration = Some(max_forgetter_duration);
        self
    }

    /// How many pending (rotated, not yet packed) segments `forgetter`
    /// lets accumulate before refusing further writes.
    pub fn max_pending_segments(mut self, max_pending_segments: u16) -> Self {
        self.max_pending_segments = Some(max_pending_segments);
        self
    }

    /// The key prefix `db` itself is opened under, within `db_backend`.
    /// Only needed when `db_backend` is shared with something else that
    /// also needs a prefix of its own.
    pub fn db_prefix(mut self, db_prefix: impl Into<String>) -> Self {
        self.db_prefix = Some(db_prefix.into());
        self
    }

    /// Where `db` (the pointer/relation store) lives. Defaults to a
    /// local `object_store` under `forgetter_dir` when not set.
    pub fn db_backend(mut self, db_backend: Arc<dyn ObjectStore>) -> Self {
        self.db_backend = Some(db_backend);
        self
    }

    /// Where packed blob objects live. Defaults to `db_backend` (the
    /// resolved one, whether explicit or defaulted) when not set.
    pub fn blobs(mut self, blobs: Arc<dyn ObjectStore>) -> Self {
        self.blobs_backend = Some(blobs);
        self
    }

    /// How many bytes `forgetter` stages before rotating a segment out
    /// on its own and making it worth consolidating into a pack.
    pub fn flush_threshold(mut self, flush_threshold: u64) -> Self {
        self.flush_threshold = Some(flush_threshold);
        self
    }

    /// The key prefix blobs are staged and packed under. Named
    /// `cas_prefix`, not just `prefix`, to avoid confusion with
    /// `db_prefix`, the unrelated prefix `slatedb::Db` itself is opened
    /// under.
    pub fn cas_prefix(mut self, cas_prefix: impl Into<String>) -> Self {
        self.cas_prefix = Some(cas_prefix.into());
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

    /// Opens `db` and `forgetter`, and builds the `Graph`.
    // TODO: `Graph`'s own relation storage will need its own merge
    // operator eventually, e.g. for growable reference sets, see
    // hypergraph.md. Nothing registers one on `db` today, since the blob
    // storage engine no longer needs merge semantics now that
    // not-yet-packed content lives in `forgetter`, not `db`.
    pub async fn build(self) -> io::Result<Graph> {
        let Self {
            forgetter_dir,
            max_forgetter_duration,
            max_pending_segments,
            db_prefix,
            db_backend,
            blobs_backend,
            flush_threshold,
            cas_prefix,
            chunking,
            codec,
        } = self;

        let db_backend = match db_backend {
            Some(backend) => backend,
            None => {
                let dir = forgetter_dir.join("db");
                fs::create_dir_all(&dir).await?;
                let local = LocalFileSystem::new_with_prefix(dir).map_err(io::Error::other)?;
                Arc::new(local) as Arc<dyn ObjectStore>
            }
        };
        let db_prefix = db_prefix.unwrap_or_else(|| storage::DEFAULT_DB_PREFIX.to_string());
        let db = slatedb::Db::builder(db_prefix, db_backend.clone())
            .build()
            .await
            .map_err(io::Error::other)?;

        let codec = codec.unwrap_or_default();
        let max_pending_segments =
            max_pending_segments.unwrap_or(storage::DEFAULT_MAX_PENDING_SEGMENTS);
        let flush_threshold = flush_threshold.unwrap_or(storage::DEFAULT_FLUSH_THRESHOLD);
        let max_forgetter_duration =
            max_forgetter_duration.unwrap_or(storage::DEFAULT_MAX_FORGETTER_DURATION);
        // Its own subdirectory, the same way `db_backend`'s default gets
        // `forgetter_dir.join("db")` above: `Forgetter::open` lists every
        // entry in whatever directory it's given and treats matches as
        // its own segments, so it needs one nothing else ever writes
        // into, not `forgetter_dir` itself (which `db`/`blobs` also live
        // under by default). `flush_threshold` doubles as `forgetter`'s
        // own rotate threshold: the size at which a segment is worth
        // consolidating into a pack is the same size at which it's worth
        // rotating out of the active slot in the first place.
        // `max_forgetter_duration` likewise becomes `forgetter`'s own
        // rotate-by-time cadence, so a segment that never crosses
        // `flush_threshold` still doesn't sit active forever.
        let (forgetter, replayed) = forgetter::Forgetter::open(
            forgetter_dir.join("forgetter"),
            max_pending_segments,
            flush_threshold,
            Some(max_forgetter_duration),
        )
        .await?;
        let rotated = forgetter.rotated();
        let forgetter = Arc::new(forgetter);
        let staged = Arc::new(storage::KeyDir::rebuild(replayed, codec).await);

        let blobs = blobs_backend.unwrap_or_else(|| db_backend.clone());
        let chunking = chunking.unwrap_or_default();
        let cas_prefix: Arc<str> =
            Arc::from(cas_prefix.unwrap_or_else(|| storage::DEFAULT_CAS_PREFIX.to_string()));

        let flushing = storage::Flushing::new();
        // Packs whatever `forgetter` rotates out, reacting directly to
        // its own rotation events rather than needing a write to happen
        // afterward to notice.
        storage::spawn_flush_loop(
            Arc::downgrade(&forgetter),
            rotated,
            storage::PackTarget {
                db:         db.clone(),
                blobs:      Arc::clone(&blobs),
                cas_prefix: Arc::clone(&cas_prefix),
                staged:     Arc::clone(&staged),
                flushing:   flushing.clone(),
            },
        );
        Ok(Graph::new(forgetter, staged, db, blobs, flushing, cas_prefix, chunking, codec))
    }
}

impl Graph {
    /// Starts building a `Graph`. See [`Builder`]'s own doc for what's
    /// required vs. defaulted.
    pub fn builder(forgetter_dir: impl Into<PathBuf>) -> Builder {
        Builder::new(forgetter_dir)
    }

    /// Only `Builder::build` calls this. Construct a `Graph` via
    /// `Graph::builder` instead of opening `db`/building these parts
    /// yourself.
    #[allow(clippy::too_many_arguments)]
    const fn new(
        forgetter: Arc<forgetter::Forgetter>,
        staged: Arc<storage::KeyDir>,
        db: slatedb::Db,
        blobs: Arc<dyn ObjectStore>,
        flushing: storage::Flushing,
        cas_prefix: Arc<str>,
        chunking: cas::Chunking,
        codec: cas::Codec,
    ) -> Self {
        Self { forgetter, staged, db, blobs, flushing, cas_prefix, chunking, codec }
    }

    /// The blob-storage facet of this `Graph`: `get`/`put`/`read_into`/
    /// `copy_from`/`flush_pending`. A cheap, borrowed view. Construct it
    /// fresh wherever it's needed rather than holding onto one.
    pub fn cas(&self) -> Cas<'_> {
        Cas::new(
            &self.db,
            &self.blobs,
            &self.forgetter,
            &self.staged,
            &self.cas_prefix,
            &self.flushing,
            self.chunking,
            self.codec,
        )
    }
}
