//! Operations over blobs, bound to one `Storage` backend -- chunking
//! and encoding on the way in, decoding and verifying on the way out.
//! `storage` is bound once here instead of being threaded through
//! every call, since `get`/`put`/`read_into`/`copy_from` all need it.
//! The free functions in `super` construct one of these per call; this
//! type itself stays private to `blob`.
use std::io;
use std::pin::pin;

use bytes::{
    BufMut,
    Bytes,
};
use fastcdc::v2020;
use futures::StreamExt as _;
use tokio::io::{
    AsyncRead,
    AsyncReadExt as _,
    AsyncWrite,
    AsyncWriteExt as _,
};

use super::{
    Storage,
    consts,
    decode_manifest,
    digest_of,
    entry,
    invalid_data,
};
use crate::hash::{
    Digest,
    FromBytes,
    Hasher,
    ToBytes,
};

pub(super) struct Blobs<S> {
    storage:        S,
    // The chunk-size knobs from `consts`, as fields instead of globals.
    // Unlike `Encoder`'s compression knobs, these aren't safe to change
    // carelessly -- see `consts` for why -- so there's no builder to
    // override them per call.
    chunk_min_size: usize,
    chunk_avg_size: usize,
    chunk_max_size: usize,
}

impl<S: Storage> Blobs<S> {
    pub(super) const fn new(storage: S) -> Self {
        Self {
            storage,
            chunk_min_size: consts::CHUNK_MIN_SIZE,
            chunk_avg_size: consts::CHUNK_AVG_SIZE,
            chunk_max_size: consts::CHUNK_MAX_SIZE,
        }
    }

    /// Reads the content at `digest`, if present.
    pub(super) async fn get<T: FromBytes>(&self, digest: &Digest) -> io::Result<Option<T>> {
        let mut bytes = Vec::new();
        if !self.read_into(digest, &mut bytes).await? {
            return Ok(None);
        }

        let content = T::from_bytes(Bytes::from(bytes))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{e:?}")))?;

        Ok(Some(content))
    }

    /// Store `content`, addressed by its own digest, and return that
    /// digest. A thin wrapper over `copy_from`, over the already
    /// in-memory bytes.
    pub(super) async fn put<T: ToBytes>(&self, content: &T) -> io::Result<Digest> {
        let bytes =
            content.to_bytes().unwrap_or_else(|_| panic!("serializing to bytes should not fail"));
        let len = bytes.len() as u64;
        self.copy_from(len, &mut io::Cursor::new(bytes)).await
    }

    /// Reads the content at `digest` if present and write it to `w`.
    ///
    /// `get` is the better choice for values small enough that this doesn't matter.
    pub(super) async fn read_into<W: AsyncWrite + Unpin>(
        &self,
        digest: &Digest,
        w: &mut W,
    ) -> io::Result<bool> {
        let decoder = entry::Decoder::new();
        let Some((flags, decoded)) = decoder.load(&self.storage, digest).await? else {
            return Ok(false);
        };

        if !flags.contains(entry::Flags::MANIFEST) {
            if digest_of(&decoded) != *digest {
                return Err(invalid_data("direct content digest mismatch"));
            }
            w.write_all(&decoded).await?;
            return Ok(true);
        }

        let manifest = decode_manifest(&decoded)?;
        let recomputed = {
            let mut h = Hasher::new();
            h.parts(manifest.iter().map(|e| e.digest.as_ref()));
            h.digest()
        };
        if recomputed != *digest {
            return Err(invalid_data("manifest digest mismatch"));
        }

        for entry in manifest {
            let Some((chunk_flags, raw)) = decoder.load(&self.storage, &entry.digest).await? else {
                return Err(invalid_data(format!(
                    "manifest references missing chunk {:x}",
                    entry.digest
                )));
            };
            if chunk_flags.contains(entry::Flags::MANIFEST) {
                return Err(invalid_data(format!(
                    "chunk {:x} entry is itself a manifest",
                    entry.digest
                )));
            }
            if raw.len() as u32 != entry.len {
                return Err(invalid_data(format!("chunk {:x} length mismatch", entry.digest)));
            }
            if digest_of(&raw) != entry.digest {
                return Err(invalid_data(format!(
                    "chunk {:x} content digest mismatch",
                    entry.digest
                )));
            }
            w.write_all(&raw).await?;
        }
        Ok(true)
    }

    /// Store the content read from `r` of `len` bytes, addressed by its own
    /// digest.
    ///
    /// Each chunk is written as soon as it's produced, not buffered in
    /// memory: only the manifest -- an ordered list of chunk digests and
    /// lengths, far smaller than the content itself -- accumulates here, so
    /// peak memory stays bounded regardless of `len`.
    pub(super) async fn copy_from<R: AsyncRead + Unpin>(
        &self,
        len: u64,
        r: &mut R,
    ) -> io::Result<Digest> {
        // `r` may be a multiplexed/persistent stream where EOF doesn't mark
        // this blob's end, so bound the chunker to exactly `len` bytes
        // rather than reading until EOF.
        let source = r.take(len);
        let mut cdc = v2020::AsyncStreamCDC::new(
            source,
            self.chunk_min_size,
            self.chunk_avg_size,
            self.chunk_max_size,
        );
        let mut chunks = pin!(cdc.as_stream());
        let encoder = entry::Encoder::new();

        // Only the most recent digest is needed to detect the zero/one/many
        // chunks cases below; the multi-chunk blob digest itself is folded
        // in incrementally, so there's no need to collect every chunk
        // digest into a `Vec` just to hash over it afterward. `manifest`
        // already grows by exactly 36 bytes per chunk, so its length alone
        // (rather than a separate counter) tells us how many chunks there
        // were.
        let mut chunk_digests = Hasher::new();
        let mut last_digest = None;
        let mut manifest = Vec::new();
        let mut total: u64 = 0;
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            total += chunk.length as u64;
            let digest = digest_of(&chunk.data);
            manifest.put_slice(digest.as_ref());
            manifest.put_u32(chunk.length as u32);
            chunk_digests.part(digest.as_ref());
            last_digest = Some(digest);
            encoder.save(&self.storage, digest, chunk.data, entry::Flags::empty()).await?;
        }

        if total != len {
            // The reader ended before supplying all of `len` bytes.
            //
            // `Take` silently short-reads on early EOF instead of erroring,
            // so this has to be checked explicitly. The chunks already written
            // above are (harmless) orphans -- no manifest ever points at them.
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("reader ended {} bytes short of the declared length {len}", len - total),
            ));
        }

        if manifest.is_empty() {
            // No chunks were emitted. The length check above already
            // guarantees `total == len`, so this can only mean `len` was 0.
            //
            // A blob digest always needs at least one chunk digest to hash
            // over, so treat empty content as exactly one (empty) chunk instead.
            // Falls through to the single-chunk shortcut below.
            let digest = digest_of(&[]);
            manifest.put_slice(digest.as_ref());
            manifest.put_u32(0);
            last_digest = Some(digest);
            encoder.save(&self.storage, digest, Vec::new(), entry::Flags::empty()).await?;
        }

        // Each chunk (real or the synthetic empty one above) appends
        // exactly one 36-byte record, so this always holds. This is what
        // makes `manifest.len() == 36` below a reliable way to detect
        // "exactly one chunk" without a separate counter.
        debug_assert!(manifest.len().is_multiple_of(36));

        if manifest.len() == 36 {
            // Exactly one chunk (one 36-byte manifest record) was emitted,
            // so its own digest is already the blob digest -- already
            // written above under that key, so there's nothing left to do.
            // This also means a small blob and the same content appearing
            // as one chunk inside a larger blob dedup against each other.
            Ok(last_digest.expect("manifest.len() == 36 implies last_digest was set"))
        } else {
            let blob_digest = chunk_digests.digest();
            encoder.save(&self.storage, blob_digest, manifest, entry::Flags::MANIFEST).await?;
            Ok(blob_digest)
        }
    }
}
