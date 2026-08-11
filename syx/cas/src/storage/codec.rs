use std::io;

use bytes::Bytes;

use super::{
    Codec,
    ContentFlags,
};
use crate::invalid_data;

impl Codec {
    /// One step above zstd's own default level (3), trading a bit more
    /// CPU for a bit better ratio. Worth it here because `SNIFF_MAX_RATIO`
    /// already filters out content that isn't worth compressing in the
    /// first place, so every chunk that reaches this point already has
    /// a real payoff to chase.
    pub const COMPRESSION_LEVEL: i32 = 4;

    /// How many bytes of a chunk to sample before deciding whether
    /// it's worth compressing.
    pub const SNIFF_LEN: usize = 16 * 1024;

    /// Skip compression if the sniffed sample doesn't shrink to less
    /// than this fraction of its own size. Already-compressed content
    /// typically doesn't shrink further, so this avoids paying to
    /// compress the rest of it for nothing. Trades CPU against storage
    /// savings: stricter (lower) only bothers compressing chunks with a
    /// clearly worthwhile payoff, saving CPU but leaving some real (if
    /// modest) compression on the table; looser values capture more of
    /// those marginal savings but spend more CPU chasing chunks that
    /// barely shrink.
    pub const SNIFF_MAX_RATIO: f64 = 0.95;

    /// Builds `Codec` with the default `COMPRESSION_LEVEL`/`SNIFF_LEN`/`SNIFF_MAX_RATIO`.
    pub const fn new() -> Self {
        Self {
            compression_level: Self::COMPRESSION_LEVEL,
            sniff_len:         Self::SNIFF_LEN,
            sniff_max_ratio:   Self::SNIFF_MAX_RATIO,
        }
    }

    /// Overrides the zstd compression level. Safe to change at any time.
    pub const fn compression_level(mut self, level: i32) -> Self {
        self.compression_level = level;
        self
    }

    /// Overrides `SNIFF_LEN`. Safe to change at any time.
    pub const fn sniff_len(mut self, len: usize) -> Self {
        self.sniff_len = len;
        self
    }

    /// Overrides `SNIFF_MAX_RATIO`. Safe to change at any time.
    pub const fn sniff_max_ratio(mut self, ratio: f64) -> Self {
        self.sniff_max_ratio = ratio;
        self
    }

    /// Compress `bytes` with zstd if that's worthwhile, and append a
    /// flag byte recording whether it was.
    pub(super) fn encode(&self, mut flags: ContentFlags, mut bytes: Vec<u8>) -> Vec<u8> {
        let sample = &bytes[..bytes.len().min(self.sniff_len)];
        if self.worth_compressing(sample) {
            let mut compressed = zstd::bulk::compress(&bytes, self.compression_level)
                .expect("zstd compression of an in-memory buffer should not fail");
            flags |= ContentFlags::COMPRESSED;
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
