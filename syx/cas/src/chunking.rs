use fastcdc::v2020;
use tokio::io::{
    AsyncRead,
    AsyncReadExt as _,
    Take,
};

use crate::Codec;

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

impl Default for Chunking {
    fn default() -> Self {
        Self::new()
    }
}

impl Chunking {
    /// Kept above `Codec::SNIFF_LEN` so a regular chunk's whole content
    /// is never "sampled": compressed once to decide, then compressed
    /// again from scratch. Enforced at compile time in `storage/tests.rs`.
    pub const MIN_SIZE: usize = Codec::SNIFF_LEN * 4;

    /// `MIN_SIZE`/`AVG_SIZE` set the dedup-vs-compression tradeoff:
    /// smaller chunks dedup more precisely (a small change in content
    /// only invalidates a small chunk) but compress worse (less context
    /// per chunk for zstd to find matches in, plus more per-chunk
    /// framing overhead); larger chunks compress better but dedup more
    /// coarsely (one changed byte invalidates the whole chunk it falls
    /// in).
    pub const AVG_SIZE: usize = Self::MIN_SIZE * 8;

    /// gRPC's default max message size is 4MB, and a chunk is expected
    /// to map to one message on the wire, so this stays comfortably
    /// under that -- not just below 4MB, leave room for message framing
    /// overhead too. Enforced at compile time in `storage/tests.rs`.
    pub const MAX_SIZE: usize = Self::AVG_SIZE * 4;

    /// Builds `Chunking` with the default `MIN_SIZE`/`AVG_SIZE`/`MAX_SIZE`.
    pub const fn new() -> Self {
        Self { min_size: Self::MIN_SIZE, avg_size: Self::AVG_SIZE, max_size: Self::MAX_SIZE }
    }

    /// Overrides the minimum chunk size. See this type's own doc before
    /// changing it -- existing chunks stay readable, but new writes stop
    /// deduping against what's already stored under the old size.
    pub const fn min_size(mut self, min_size: usize) -> Self {
        self.min_size = min_size;
        self
    }

    /// Overrides the average chunk size. See [`Chunking::min_size`].
    pub const fn avg_size(mut self, avg_size: usize) -> Self {
        self.avg_size = avg_size;
        self
    }

    /// Overrides the maximum chunk size. See [`Chunking::min_size`].
    pub const fn max_size(mut self, max_size: usize) -> Self {
        self.max_size = max_size;
        self
    }

    /// Splits `r`, bounded to exactly `len` bytes, into content-defined
    /// chunks. `r` may be a multiplexed/persistent stream where EOF
    /// doesn't mark this blob's end, so this bounds the chunker to
    /// exactly `len` bytes rather than reading until EOF.
    pub(super) fn reader<'r, R>(
        &self,
        len: u64,
        r: &'r mut R,
    ) -> v2020::AsyncStreamCDC<Take<&'r mut R>>
    where
        R: AsyncRead + Unpin,
    {
        v2020::AsyncStreamCDC::new(r.take(len), self.min_size, self.avg_size, self.max_size)
    }
}
