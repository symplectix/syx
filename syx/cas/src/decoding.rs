use std::io;

use bytes::Bytes;

use crate::{
    ContentFlags,
    invalid_data,
};

/// How to decode the chunk.
/// The read-side counterpart to `Encoding`.
#[derive(Clone, Copy)]
pub(crate) struct Decoding {
    // Unlike encoding, decoding needs no options for now.
}

impl Default for Decoding {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoding {
    pub(super) const fn new() -> Self {
        Self {}
    }

    /// The not-worth-compressing case is just a cheap sub-slice
    /// of the already-allocated buffer.
    pub(super) fn decode(&self, stored: Bytes) -> io::Result<(ContentFlags, Bytes)> {
        if stored.is_empty() {
            return Err(invalid_data("stored content is missing its trailing flag byte"));
        }
        let mut bytes = stored.slice(..stored.len() - 1);
        let flags = ContentFlags::from_bits_retain(stored[stored.len() - 1]);
        if flags.contains(ContentFlags::COMPRESSED) {
            bytes = Bytes::from(
                zstd::decode_all(bytes.as_ref())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
            );
        }
        Ok((flags, bytes))
    }
}
