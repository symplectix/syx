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

pub use chunking::Chunking;
pub(crate) use decoding::Decoding;
pub use encoding::Encoding;
pub use hash::{
    Digest,
    FromBytes,
    Hasher,
    ToBytes,
};
pub use storage::{
    Storage,
    StorageBuilder,
};

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

fn invalid_data(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

fn other(e: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::other(e)
}
