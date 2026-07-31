//! A `Digest`'s bytes are preserved faithfully.

#[test]
fn digest_bytes_round_trip_through_digest_new() {
    let want = cas_testing::digest_bytes(b"hello");
    let bytes: [u8; 32] = want.as_ref().try_into().unwrap();
    assert_eq!(cas::Digest::new(bytes), want);
}

#[test]
fn digest_canonical_cbor_encoding_matches_byte_buf_encoding() {
    let d = cas_testing::digest_bytes(b"hello");
    let d_bytes = serde_bytes::ByteBuf::from(d.as_ref());
    assert_eq!(cbor2::to_canonical_vec(&d_bytes).unwrap(), cbor2::to_canonical_vec(&d).unwrap());
}
