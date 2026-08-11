//! cas: content-addressed storage.

use std::io;

pub use bytes::Bytes;

mod chunking;
mod hash;
mod storage;

pub use chunking::Chunking;
pub use hash::{
    Digest,
    FromBytes,
    Hasher,
    ToBytes,
};
pub use storage::{
    Codec,
    Storage,
    StorageBuilder,
};

fn invalid_data(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

fn other(e: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::other(e)
}
