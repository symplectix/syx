//! Storing and fetching one entry -- a chunk or a manifest -- as
//! opaque, possibly-compressed bytes behind a `Storage`. What the
//! bytes actually mean lives one layer up.
use std::io;

use bitflags::bitflags;
use bytes::Bytes;
use tokio::task;

use super::{
    Storage,
    consts,
    invalid_data,
};
use crate::hash::Digest;

bitflags! {
    /// The trailing byte of every entry's stored payload.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct Flags: u8 {
        /// The payload that follows is compressed by zstd.
        const COMPRESSED = 1 << 0;
        /// The payload is a manifest (an ordered list of ChunkRef),
        /// not content itself.
        const MANIFEST = 1 << 1;
    }
}

/// The compression knobs from `consts`, as a value instead of
/// globals. `entry` itself never assumes a default: `save` takes
/// one explicitly, so the actual default only lives in one place --
/// `Default` here, which callers at the top of `super` (e.g.
/// `copy_from`) resolve once and pass down. Tests build their own.
#[derive(Clone, Copy)]
pub(super) struct Encoder {
    compression_level: i32,
    sniff_len:         usize,
    sniff_max_ratio:   f64,
}

impl Default for Encoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Encoder {
    pub(super) const fn new() -> Self {
        Self {
            compression_level: consts::COMPRESSION_LEVEL,
            sniff_len:         consts::SNIFF_LEN,
            sniff_max_ratio:   consts::SNIFF_MAX_RATIO,
        }
    }

    #[allow(dead_code)]
    pub(super) const fn compression_level(mut self, level: i32) -> Self {
        self.compression_level = level;
        self
    }

    #[allow(dead_code)]
    pub(super) const fn sniff_len(mut self, len: usize) -> Self {
        self.sniff_len = len;
        self
    }

    #[allow(dead_code)]
    pub(super) const fn sniff_max_ratio(mut self, ratio: f64) -> Self {
        self.sniff_max_ratio = ratio;
        self
    }

    /// Compress `bytes` with zstd if that's worthwhile, and append a
    /// flag byte recording whether it was.
    pub(super) fn encode(&self, mut flags: Flags, mut bytes: Vec<u8>) -> Vec<u8> {
        let sample = &bytes[..bytes.len().min(self.sniff_len)];
        if self.worth_compressing(sample) {
            let mut compressed = zstd::bulk::compress(&bytes, self.compression_level)
                .expect("zstd compression of an in-memory buffer should not fail");
            flags |= Flags::COMPRESSED;
            compressed.push(flags.bits());
            return compressed;
        }
        bytes.push(flags.bits());
        bytes
    }

    /// Whether compressing `sample` shrinks it enough to be worth
    /// compressing the rest of the chunk it was taken from.
    pub(super) fn worth_compressing(&self, sample: &[u8]) -> bool {
        if sample.is_empty() {
            return false;
        }
        let compressed_len =
            zstd::bulk::compress(sample, self.compression_level).map_or(sample.len(), |c| c.len());
        (compressed_len as f64) < (sample.len() as f64) * self.sniff_max_ratio
    }

    /// Write one entry under `key`, skipping the encode step entirely
    /// if `key` is already stored.
    pub(super) async fn save<S: Storage>(
        &self,
        storage: &S,
        key: Digest,
        bytes: Vec<u8>,
        flags: Flags,
    ) -> io::Result<()> {
        if storage.contains_blob(key.as_ref()).await? {
            return Ok(());
        }
        let encoder = *self;

        // Encoding runs in its own `spawn_blocking`, independent of however
        // the backend chooses to run `put_blob` itself: it's CPU-bound work
        // that always needs to stay off the async executor, regardless of
        // which backend `S` is.
        let encoded = task::spawn_blocking(move || encoder.encode(flags, bytes))
            .await
            .expect("encode should not panic");
        storage.put_blob(key.as_ref(), Bytes::from(encoded)).await
    }
}

/// The read-side counterpart to `Encoder`.
#[derive(Clone, Copy)]
pub(super) struct Decoder {
    // Unlike encoding, decoding needs no options for now.
}

impl Default for Decoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder {
    pub(super) const fn new() -> Self {
        Self {}
    }

    /// The inverse of `Encoder::encode`.
    ///
    /// The not-worth-compressing case is just a cheap sub-slice
    /// of the already-allocated buffer.
    pub(super) fn decode(&self, stored: Bytes) -> io::Result<(Flags, Bytes)> {
        if stored.is_empty() {
            return Err(invalid_data("stored content is missing its trailing flag byte"));
        }
        let mut bytes = stored.slice(..stored.len() - 1);
        let mut flags = Flags::from_bits_retain(stored[stored.len() - 1]);
        if flags.contains(Flags::COMPRESSED) {
            // `bytes` is decompressed from here on -- the returned flags
            // should describe it.
            flags.remove(Flags::COMPRESSED);
            bytes = Bytes::from(
                zstd::decode_all(bytes.as_ref())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
            );
        }
        Ok((flags, bytes))
    }

    /// Fetch and decode one entry (a chunk or a manifest).
    ///
    /// Callers should check the digest themselves, since what it
    /// should be verified against differs for a manifest's own entry vs.
    /// one of the chunks it lists.
    pub(super) async fn load<S: Storage>(
        &self,
        storage: &S,
        digest: &Digest,
    ) -> io::Result<Option<(Flags, Bytes)>> {
        let Some(stored) = storage.get_blob(digest.as_ref()).await? else {
            return Ok(None);
        };
        let decoder = *self;
        task::spawn_blocking(move || decoder.decode(stored).map(Some))
            .await
            .expect("decode should not panic")
    }
}
