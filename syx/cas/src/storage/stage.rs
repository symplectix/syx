use std::io;

use bitflags::bitflags;
use bytes::{
    BufMut,
    Bytes,
    BytesMut,
};
use slatedb::{
    MergeOperator,
    MergeOperatorError,
    WriteBatch,
};

use super::{
    Entry,
    Stage,
};
use crate::hash::Digest;
use crate::other;

bitflags! {
    /// The trailing byte of `slatedb` value. Never seen by anything
    /// above `Entry`: it says whether that value *is* the content
    /// or a pointer to where the content currently lives instead.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct EntryFlags: u8 {
        /// This entry isn't content -- it's a pointer to where the
        /// content lives instead.
        const PACKED = 1 << 0;
    }
}

impl Entry {
    /// The tag byte trails the payload rather than leading it, so the
    /// common `Inline` case can append in place -- via `try_into_mut`,
    /// reusing `bytes`' own allocation -- instead of copying a chunk's
    /// entire content (up to a few MiB) just to prepend one byte.
    fn encode(self) -> Bytes {
        match self {
            Entry::Inline(bytes) => {
                let mut buf =
                    bytes.try_into_mut().unwrap_or_else(|bytes| BytesMut::from(&bytes[..]));
                buf.put_u8(EntryFlags::empty().bits());
                buf.freeze()
            }
            Entry::Packed { pack_id, offset, length } => {
                let mut buf = BytesMut::with_capacity(32 + 8 + 8 + 1);
                buf.extend_from_slice(pack_id.as_ref());
                buf.extend_from_slice(&offset.to_be_bytes());
                buf.extend_from_slice(&length.to_be_bytes());
                buf.put_u8(EntryFlags::PACKED.bits());
                buf.freeze()
            }
        }
    }

    fn decode(bytes: &Bytes) -> io::Result<Self> {
        let Some(&tag) = bytes.last() else {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "malformed Entry"));
        };
        let tag = EntryFlags::from_bits_retain(tag);
        if !tag.contains(EntryFlags::PACKED) {
            return Ok(Entry::Inline(bytes.slice(..bytes.len() - 1)));
        }
        if bytes.len() != 32 + 8 + 8 + 1 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "malformed Entry"));
        }
        let mut pack_id = [0u8; 32];
        pack_id.copy_from_slice(&bytes[0..32]);
        let offset = u64::from_be_bytes(bytes[32..40].try_into().unwrap());
        let length = u64::from_be_bytes(bytes[40..48].try_into().unwrap());
        Ok(Entry::Packed { pack_id: Digest::new(pack_id), offset, length })
    }
}

/// `Stage`'s merge operator.
// TODO: assumes it's the only `MergeOperator` registered on `db`. If `db`
// ever gets shared with another user that needs its own merge behavior,
// this needs to compose with that instead of being the sole dispatcher.
struct StorageMergeOperator;

impl MergeOperator for StorageMergeOperator {
    fn merge(
        &self,
        key: &Bytes,
        existing: Option<Bytes>,
        op: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        if key.ends_with(b"pending_bytes") {
            let existing = existing
                .map(|v| u64::from_be_bytes(v.as_ref().try_into().unwrap_or_default()))
                .unwrap_or(0);
            let delta = u64::from_be_bytes(op.as_ref().try_into().unwrap_or_default());
            return Ok(Bytes::copy_from_slice(&existing.saturating_add(delta).to_be_bytes()));
        }

        match existing {
            Some(existing) => {
                // Grow `existing` in place when uniquely owned.
                let mut buf = existing.try_into_mut().unwrap_or_else(|b| BytesMut::from(&b[..]));
                buf.extend_from_slice(&op);
                Ok(buf.freeze())
            }
            None => Ok(op),
        }
    }
}

impl Stage {
    /// The merge operator `db` must be opened with for `pending_bytes`
    /// and `pending_keys` to work.
    pub(super) fn merge_operator() -> Box<dyn MergeOperator + Send + Sync> {
        Box::new(StorageMergeOperator)
    }

    fn entry_key(&self, key: Digest) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.prefix.len() + 7 + 32);
        buf.extend_from_slice(self.prefix.as_bytes());
        buf.extend_from_slice(b"sha256/");
        buf.extend_from_slice(key.as_ref());
        buf
    }

    fn pending_bytes_key(&self) -> Vec<u8> {
        format!("{}pending_bytes", self.prefix).into_bytes()
    }

    fn pending_keys_key(&self) -> Vec<u8> {
        format!("{}pending_keys", self.prefix).into_bytes()
    }

    /// Test-only: `staged` finds pending entries via `pending_keys_key`,
    /// not by scanning this range.
    #[cfg(test)]
    fn entry_prefix(&self) -> Vec<u8> {
        format!("{}sha256/", self.prefix).into_bytes()
    }

    pub(super) async fn pending_bytes(&self) -> io::Result<u64> {
        match self.db.get(self.pending_bytes_key()).await.map_err(other)? {
            Some(bytes) => Ok(u64::from_be_bytes(bytes.as_ref().try_into().unwrap_or_default())),
            None => Ok(0),
        }
    }

    /// Digests currently staged (not yet packed), in the order they
    /// were merged into `pending_keys_key`.
    async fn pending_keys(&self) -> io::Result<Vec<Digest>> {
        let Some(bytes) = self.db.get(self.pending_keys_key()).await.map_err(other)? else {
            return Ok(Vec::new());
        };
        if !bytes.len().is_multiple_of(32) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "malformed pending key list"));
        }
        Ok(bytes.chunks_exact(32).map(|c| Digest::new(c.try_into().unwrap())).collect())
    }

    /// Whether `key` is already stored, without fetching its value.
    pub(super) async fn contains(&self, key: Digest) -> io::Result<bool> {
        Ok(self.db.get(self.entry_key(key)).await.map_err(other)?.is_some())
    }

    /// Fetch and decode the entry stored under `key`, if present.
    pub(super) async fn get(&self, key: Digest) -> io::Result<Option<Entry>> {
        let Some(raw) = self.db.get(self.entry_key(key)).await.map_err(other)? else {
            return Ok(None);
        };
        Entry::decode(&raw).map(Some)
    }

    /// Stage `bytes` under `key`, durable immediately -- not yet in a pack.
    pub(super) async fn put(&self, key: Digest, bytes: Bytes) -> io::Result<()> {
        let len = bytes.len() as u64;
        let entry = Entry::Inline(bytes);
        let mut batch = WriteBatch::new();
        batch.put_bytes(Bytes::from(self.entry_key(key)), entry.encode());
        batch.merge(self.pending_bytes_key(), len.to_be_bytes());
        batch.merge(self.pending_keys_key(), key.as_ref());
        self.db.write(batch).await.map_err(other)?;
        Ok(())
    }

    /// How many distinct keys have ever been stored (staged or packed)
    /// under this stage's prefix. Test-only.
    #[cfg(test)]
    pub(super) async fn entry_count(&self) -> io::Result<usize> {
        let mut iter = self.db.scan_prefix(self.entry_prefix(), ..).await.map_err(other)?;
        let mut n = 0;
        while iter.next().await.map_err(other)?.is_some() {
            n += 1;
        }
        Ok(n)
    }

    /// Currently-staged entries (per `pending_keys_key` -- not a scan
    /// over every entry ever held, packed or not), with their still-
    /// `Inline` bytes.
    pub(super) async fn staged(&self) -> io::Result<Vec<(Digest, Bytes)>> {
        let mut staged = Vec::new();
        for digest in self.pending_keys().await? {
            let Some(entry) = self.get(digest).await? else {
                // This is purely internal bookkeeping: `put` always
                // writes the entry before merging its digest into
                // pending_keys, and entries are never deleted, so
                // reaching here would mean that invariant itself broke.
                // Still safe to skip. Nothing to do for a missing entry.
                continue;
            };
            let Entry::Inline(bytes) = entry else {
                // `flushing` ensures only one `flush_pending` call runs
                // at a time, and only `commit_packed` flips Inline to Packed,
                // so reaching here would mean that serialization itself broke.
                // Still safe to skip. Nothing to do for an already packed entry.
                continue;
            };
            staged.push((digest, bytes));
        }
        Ok(staged)
    }

    /// Atomically flips `entries` to `Packed` and resets the pending
    /// counters -- called once their bytes are durable in a pack object.
    pub(super) async fn commit_packed(&self, entries: Vec<(Digest, Entry)>) -> io::Result<()> {
        let mut batch = WriteBatch::new();
        for (digest, entry) in entries {
            batch.put_bytes(Bytes::from(self.entry_key(digest)), entry.encode());
        }
        batch.put_bytes(
            Bytes::from(self.pending_bytes_key()),
            Bytes::copy_from_slice(&0u64.to_be_bytes()),
        );
        batch.put_bytes(Bytes::from(self.pending_keys_key()), Bytes::new());
        self.db.write(batch).await.map_err(other)?;
        Ok(())
    }
}
