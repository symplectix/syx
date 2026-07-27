//! cas: content-addressed storage.

pub use bytes::Bytes;
pub use hash::{
    Digest,
    FromBytes,
    Hasher,
    ToBytes,
    digest,
};
pub use storage::{
    Reader,
    Storage,
    Writer,
};

mod hash;
mod storage;
