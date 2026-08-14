use std::io;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use cas::{
    Chunking,
    Codec,
};
use object_store::ObjectStore;
use object_store::path::Path;

use super::{
    Builder,
    DbMode,
    Packs,
    Stage,
    Storage,
    other,
};

impl Builder {
    /// The default `prefix`, for the common case of `db` and `packs`
    /// existing solely for this `Storage`'s own sake.
    pub(crate) const DEFAULT_PREFIX: &str = "cas/";

    /// The default `packs_threshold`: 32 MiB -- enough to consolidate
    /// several dozen chunks per pack.
    const DEFAULT_PACKS_THRESHOLD: u64 = Chunking::AVG_SIZE as u64 * 64;

    /// Starts building a `Storage`, opening a new `db`. Test-only
    /// convenience -- see `Storage::builder`.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn new(db_prefix: impl Into<String>, db_backend: Arc<dyn ObjectStore>) -> Builder {
        Builder {
            db_mode:         DbMode::Open { db_prefix: db_prefix.into(), db_backend },
            packs_backend:   None,
            prefix:          Builder::DEFAULT_PREFIX.to_string(),
            packs_threshold: Builder::DEFAULT_PACKS_THRESHOLD,
            chunking:        Chunking::new(),
            codec:           Codec::new(),
        }
    }

    /// Starts building a `Storage` over an already-opened `db`.
    pub(super) fn with_db(db: slatedb::Db, packs_backend: Arc<dyn ObjectStore>) -> Builder {
        Builder {
            db_mode:         DbMode::Provided(db),
            packs_backend:   Some(packs_backend),
            prefix:          Builder::DEFAULT_PREFIX.to_string(),
            packs_threshold: Builder::DEFAULT_PACKS_THRESHOLD,
            chunking:        Chunking::new(),
            codec:           Codec::new(),
        }
    }

    /// The key prefix blobs are staged and packed under.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Writes pack objects to `packs` instead of `db`'s own backend.
    /// Only needed when content should live somewhere other than
    /// wherever `db` persists itself. Only reachable via `Builder::new`,
    /// so test-only for the same reason that is.
    #[cfg_attr(not(test), allow(dead_code))]
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

    /// Fails if, when opening a new `db`, `packs` was never set and
    /// `db_prefix`/`prefix` collide. Doesn't apply to `Builder::with_db`,
    /// which always requires `packs_backend` up front.
    pub async fn build(self) -> io::Result<Storage> {
        let (db, packs_store) = match self.db_mode {
            DbMode::Open { db_prefix, db_backend } => {
                if self.packs_backend.is_none()
                    && Path::from(db_prefix.as_str()) == Path::from(self.prefix.as_str())
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "db_prefix and prefix must differ to avoid key collisions when \
                            packs defaults to sharing db's own backend: both are {:?}",
                            db_prefix
                        ),
                    ));
                }
                let packs_store = self.packs_backend.unwrap_or_else(|| db_backend.clone());
                let db = slatedb::Db::builder(db_prefix, db_backend)
                    .with_merge_operator(Arc::from(Stage::merge_operator()))
                    .build()
                    .await
                    .map_err(other)?;
                (db, packs_store)
            }
            DbMode::Provided(db) => {
                // `Builder::with_db` always sets `packs_backend`, so this
                // is only reachable if it wasn't -- can't happen.
                let packs_store = self.packs_backend.expect("with_db always sets packs_backend");
                (db, packs_store)
            }
        };
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

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;

    fn in_memory() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    #[tokio::test]
    async fn build_rejects_a_db_prefix_that_collides_with_prefix() {
        // Not `.prefix(...)`-ed: exercises the default ("cas/"), which
        // normalizes to the same `Path` as db_prefix "cas". `packs` is also
        // left unset, so it defaults to sharing `backend` -- the check
        // only applies in that case.
        let err = Storage::builder("cas", in_memory()).build().await.err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[tokio::test]
    async fn build_allows_a_colliding_db_prefix_and_prefix_once_packs_is_set_explicitly() {
        // Same colliding db_prefix/prefix as above, but `packs` is
        // set explicitly (even to the very same backend) -- the check
        // can't tell whether that's physically shared storage, so it's
        // the caller's call, not rejected here.
        let backend = in_memory();
        Storage::builder("cas", backend.clone()).packs(backend).build().await.unwrap();
    }
}
