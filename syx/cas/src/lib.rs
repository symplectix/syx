//! cas: content-addressed storage.

pub use bytes::Bytes;
pub use hash::{
    Digest,
    FromBytes,
    Hasher,
    ToBytes,
};
pub use storage::{
    Chunking,
    Encoding,
    Storage,
};

mod hash;
mod storage;
