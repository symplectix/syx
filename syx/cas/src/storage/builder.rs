use std::io;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use object_store::ObjectStore;
use object_store::path::Path;

use super::{
    Builder,
    Packs,
    Stage,
    Storage,
};
use crate::{
    Chunking,
    Codec,
    other,
};

impl Builder {
    /// The default `prefix`, for the common case of `db` and `packs`
    /// existing solely for this `Storage`'s own sake.
    const DEFAULT_PREFIX: &str = "cas/";

    /// The default `packs_threshold`: 32 MiB -- enough to consolidate
    /// several dozen chunks per pack.
    const DEFAULT_PACKS_THRESHOLD: u64 = Chunking::AVG_SIZE as u64 * 64;

    /// Starts building a `Storage`.
    pub(super) fn new(db_prefix: impl Into<String>, db_backend: Arc<dyn ObjectStore>) -> Builder {
        Builder {
            db_prefix: db_prefix.into(),
            db_backend,
            packs_backend: None,
            prefix: Builder::DEFAULT_PREFIX.to_string(),
            packs_threshold: Builder::DEFAULT_PACKS_THRESHOLD,
            chunking: Chunking::new(),
            codec: Codec::new(),
        }
    }

    /// The key prefix blobs are staged and packed under.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Writes pack objects to `packs` instead of `db`'s own backend.
    /// Only needed when content should live somewhere other than
    /// wherever `db` persists itself.
    pub fn packs(mut self, packs: Arc<dyn ObjectStore>) -> Self {
        self.packs_backend = Some(packs);
        self
    }

    /// How many bytes to stage before consolidating into a pack.
    pub fn packs_threshold(mut self, packs_threshold: u64) -> Self {
        self.packs_threshold = packs_threshold;
        self
    }

    /// Overrides chunking behavior (defaults to [`Chunking::new`]).
    pub fn chunking(mut self, chunking: Chunking) -> Self {
        self.chunking = chunking;
        self
    }

    /// Overrides encoding/decoding behavior (defaults to [`Codec::new`]).
    pub fn codec(mut self, codec: Codec) -> Self {
        self.codec = codec;
        self
    }

    /// Fails if `packs` was never set and `db_prefix`/`prefix` collide.
    pub async fn build(self) -> io::Result<Storage> {
        if self.packs_backend.is_none()
            && Path::from(self.db_prefix.as_str()) == Path::from(self.prefix.as_str())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "db_prefix and prefix must differ to avoid key collisions when \
                    packs defaults to sharing db's own backend: both are {:?}",
                    self.db_prefix
                ),
            ));
        }
        let packs_store = self.packs_backend.unwrap_or_else(|| self.db_backend.clone());
        // TODO: `db` is always opened with only `Stage::merge_operator()`.
        // No way yet for a caller to supply/combine an additional merge
        // operator for another component sharing this same `db`.
        let db = slatedb::Db::builder(self.db_prefix, self.db_backend)
            .with_merge_operator(Stage::merge_operator())
            .build()
            .await
            .map_err(other)?;
        Ok(Storage {
            stage:    Stage {
                db,
                prefix: self.prefix.clone(),
                flushing: Arc::new(tokio::sync::Mutex::new(())),
                flush_failures: Arc::new(AtomicU32::new(0)),
            },
            packs:    Packs {
                store:     packs_store,
                prefix:    self.prefix,
                threshold: self.packs_threshold,
            },
            chunking: self.chunking,
            codec:    self.codec,
        })
    }
}
