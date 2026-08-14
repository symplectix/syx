use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use cas::{
    Chunking,
    Codec,
};
use object_store::ObjectStore;

use super::{
    Builder,
    Packs,
    Parts,
    Stage,
};

impl Builder {
    /// The default `prefix`, for the common case of `packs`
    /// existing solely for this `Graph`'s own sake.
    pub(crate) const DEFAULT_PREFIX: &str = "cas/";

    /// The default `packs_threshold`: 32 MiB -- enough to consolidate
    /// several dozen chunks per pack.
    const DEFAULT_PACKS_THRESHOLD: u64 = Chunking::AVG_SIZE as u64 * 64;

    /// Starts building the blob-storage `Parts` that pack into
    /// `packs_backend`. Only `Graph::Builder` and this module's own
    /// tests call this -- `Graph::builder` is the public entry point.
    pub(crate) fn new(packs_backend: Arc<dyn ObjectStore>) -> Builder {
        Builder {
            packs_backend,
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

    /// Builds `Parts`. Infallible: nothing here opens or touches `db`,
    /// so there's nothing that can fail.
    pub(crate) fn build(self) -> Parts {
        Parts {
            stage:    Stage {
                prefix:         self.prefix.clone(),
                flushing:       Arc::new(tokio::sync::Mutex::new(())),
                flush_failures: Arc::new(AtomicU32::new(0)),
            },
            packs:    Packs {
                store:     self.packs_backend,
                prefix:    self.prefix,
                threshold: self.packs_threshold,
            },
            chunking: self.chunking,
            codec:    self.codec,
        }
    }
}
