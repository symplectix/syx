use std::io;
use std::ops::Range;

use bytes::Bytes;
use object_store::path::Path;
use object_store::{
    GetOptions,
    ObjectStoreExt as _,
    PutPayload,
};

use super::Packs;
use crate::hash::Digest;

impl Packs {
    fn path(&self, pack_id: Digest) -> Path {
        Path::from(self.prefix.as_str()).join("sha256").join(format!("{pack_id:x}"))
    }

    /// Fetch `length` bytes at `offset` from pack `pack_id`.
    pub(super) async fn get_range(
        &self,
        pack_id: Digest,
        offset: u64,
        length: u64,
    ) -> io::Result<Bytes> {
        let range: Range<u64> = offset..offset + length;
        let opts = GetOptions { range: Some(range.into()), ..Default::default() };
        let result =
            self.store.get_opts(&self.path(pack_id), opts).await.map_err(io::Error::from)?;
        Ok(result.bytes().await?)
    }

    /// Write `payload` as one new pack object identified by `pack_id`.
    pub(super) async fn write(&self, pack_id: Digest, payload: PutPayload) -> io::Result<()> {
        self.store.put(&self.path(pack_id), payload).await.map_err(io::Error::from)?;
        Ok(())
    }
}
