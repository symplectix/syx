//! cas: content-addressed storage.

use std::io;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use bitflags::bitflags;
pub use bytes::Bytes;
use object_store::ObjectStore;

mod chunking;
mod decoding;
mod encoding;
mod hash;
mod storage;

pub use hash::{
    Digest,
    FromBytes,
    Hasher,
    ToBytes,
};

/// Chunking, encoding, and the physical storage of blobs, staged in
/// `slatedb` and packed into `packs` over time.
#[derive(Clone)]
pub struct Storage {
    stage:    Stage,
    packs:    Packs,
    chunking: Chunking,
    encoding: Encoding,
    decoding: Decoding,
}

/// Builds a `Storage`, opening `db` along the way with the merge
/// operator `Storage` needs already registered.
pub struct StorageBuilder {
    db_prefix:       String,
    db_backend:      Arc<dyn ObjectStore>,
    prefix:          String,
    packs_backend:   Option<Arc<dyn ObjectStore>>,
    packs_threshold: u64,
    chunking:        Chunking,
    encoding:        Encoding,
}

bitflags! {
    /// The trailing byte of `slatedb` value. Never seen by anything
    /// above `Entry`: it says whether that value *is* the content
    /// or a pointer to where the content currently lives instead.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct EntryFlags: u8 {
        /// This entry isn't content -- it's a pointer to where the
        /// content lives instead.
        const PACKED = 1 << 0;
    }
}

/// Where an entry currently lives.
enum Entry {
    /// Still staged: the raw bytes themselves -- opaque here, but
    /// really `[payload][ContentFlags]`, as `Encoding::encode`
    /// produced it.
    Inline(Bytes),
    /// Migrated: where to find it in an already-durable pack.
    Packed { pack_id: Digest, offset: u64, length: u64 },
}

/// `cas::Storage`'s own staging area within `db` -- entries land here
/// first, before being consolidated into `Packs`.
#[derive(Clone)]
struct Stage {
    db: slatedb::Db,
    /// This stage's own namespace within `db`.
    prefix: String,
    /// Serializes `flush_pending`.
    flushing: Arc<tokio::sync::Mutex<()>>,
    /// Consecutive `flush_pending` failures, reset to 0 on success.
    /// `put_blob`'s opportunistic call reads this to decide whether to
    /// swallow an error or propagate it.
    flush_failures: Arc<AtomicU32>,
}

/// Where staged entries get consolidated into once `threshold`
/// accumulates.
#[derive(Clone)]
struct Packs {
    store:     Arc<dyn ObjectStore>,
    prefix:    String,
    threshold: u64,
}

bitflags! {
    /// The trailing byte of a blob's own encoded content -- set once,
    /// at write time, and unchanged from then on regardless of where
    /// that content ends up physically living.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) struct ContentFlags: u8 {
        /// The payload that follows is compressed by zstd.
        const COMPRESSED = 1 << 0;
        /// The payload is chunked, contains an ordered list of Chunk,
        /// not content itself.
        const CHUNKED = 1 << 1;
    }
}

/// The chunk-size settings.
///
/// These aren't safe to change carelessly. Chunk boundaries depend on
/// these parameters, so changing them shifts where cuts fall:
/// even byte-identical content gets split into different chunks than
/// before, with different digests. Existing chunks stay perfectly readable,
/// but new writes no longer dedup against what's already stored.
#[derive(Clone, Copy)]
pub struct Chunking {
    min_size: usize,
    avg_size: usize,
    max_size: usize,
}

/// How to encode the chunk. Each constant is a pure write-time
/// heuristic, safe to change at any time: every stored chunk records
/// its own compressed-or-not decision, so changing these only affects
/// future writes, never how existing ones are read back.
#[derive(Clone, Copy)]
pub struct Encoding {
    compression_level: i32,
    sniff_len:         usize,
    sniff_max_ratio:   f64,
}

/// How to decode the chunk.
/// The read-side counterpart to `Encoding`.
#[derive(Clone, Copy)]
struct Decoding {
    // Unlike encoding, decoding needs no options for now.
}

/// A reference to one chunk: its digest and its length.
struct Chunk {
    digest: Digest,
    len:    u32,
}

fn invalid_data(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

fn other(e: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::other(e)
}
