//! Tuning knobs for chunking and compression, and the tradeoffs
//! behind them.
//!
//! # Compression: `COMPRESSION_LEVEL`, `SNIFF_LEN`, `SNIFF_MAX_RATIO`
//!
//! Safe to change at any time. Each is a pure write-time heuristic:
//! every stored chunk records its own compressed-or-not decision,
//! so changing these only affects future writes, never how existing
//! ones are read back.
//!
//! `SNIFF_LEN`/`SNIFF_MAX_RATIO` trade CPU against storage savings:
//! stricter (a lower `SNIFF_MAX_RATIO`) only bothers compressing
//! chunks with a clearly worthwhile payoff, saving CPU but leaving
//! some real (if modest) compression on the table; looser values
//! capture more of those marginal savings but spend more CPU chasing
//! chunks that barely shrink.
//!
//! # Chunking: `CHUNK_MIN_SIZE`, `CHUNK_AVG_SIZE`, `CHUNK_MAX_SIZE`
//!
//! NOT safe to change without consequence, unlike the compression
//! knobs above. Chunk boundaries depend on these parameters, so
//! changing them shifts where cuts fall: even byte-identical
//! content gets split into different chunks than before, with
//! different digests. Existing chunks and manifests stay perfectly
//! readable (a chunk's key is just the hash of its own bytes,
//! independent of how it was cut), but new writes no longer dedup
//! against what's already stored under the old parameters -- only
//! future writes, among themselves, do.
//!
//! min/avg set the dedup-vs-compression tradeoff: smaller chunks
//! dedup more precisely (a small change in content only
//! invalidates a small chunk) but compress worse (less context per
//! chunk for zstd to find matches in, plus more per-chunk framing
//! overhead); larger chunks compress better but dedup more
//! coarsely (one changed byte invalidates the whole chunk it falls
//! in).

/// One step above zstd's own default level (3), trading a bit more
/// CPU for a bit better ratio. Worth it here because `SNIFF_MAX_RATIO`
/// already filters out content that isn't worth compressing in the
/// first place, so every chunk that reaches this point already has
/// a real payoff to chase.
pub(super) const COMPRESSION_LEVEL: i32 = 4;

/// How many bytes of a chunk to sample before deciding whether
/// it's worth compressing.
pub(super) const SNIFF_LEN: usize = 16 * 1024;

/// Skip compression if the sniffed sample doesn't shrink to less
/// than this fraction of its own size. Already-compressed content
/// typically doesn't shrink further, so this avoids paying to
/// compress the rest of it for nothing.
pub(super) const SNIFF_MAX_RATIO: f64 = 0.95;

pub(super) const CHUNK_MIN_SIZE: usize = SNIFF_LEN * 4;
pub(super) const CHUNK_AVG_SIZE: usize = CHUNK_MIN_SIZE * 8;
pub(super) const CHUNK_MAX_SIZE: usize = CHUNK_AVG_SIZE * 4;

const _: () = assert!(
    SNIFF_LEN < CHUNK_MIN_SIZE,
    "SNIFF_LEN must stay smaller than `CHUNK_MIN_SIZE`: \
    otherwise every regular chunk would have its whole content \"sampled\" \
    -- compressed once to decide, then compressed again from scratch."
);

const _: () = assert!(
    CHUNK_MAX_SIZE <= 4 * 1024 * 1024,
    "CHUNK_MAX_SIZE has a hard ceiling to respect: \
    gRPC's default max message size is 4MB, and a chunk is expected to \
    map to one message on the wire, so this should stay comfortably under that. \
    Not just below 4MB, leave room for message framing overhead too.",
);
