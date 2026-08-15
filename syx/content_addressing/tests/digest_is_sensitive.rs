//! Digest is sensitive to its input.

mod common;
use common::Example;

#[test]
fn different_bytes_produce_different_digests() {
    assert_ne!(cas_testing::digest_bytes(b"a"), cas_testing::digest_bytes(b"b"));
}

#[test]
fn different_struct_fields_produce_different_digests() {
    let base = Example { name: "foo".to_string(), count: 1 };
    let other_name = Example { name: "bar".to_string(), count: 1 };
    let other_count = Example { name: "foo".to_string(), count: 2 };
    assert_ne!(cas_testing::digest(&base), cas_testing::digest(&other_name));
    assert_ne!(cas_testing::digest(&base), cas_testing::digest(&other_count));
}

#[test]
fn different_part_order_produces_different_digests() {
    assert_ne!(
        cas_testing::digest_parts([b"a".as_slice(), b"b".as_slice()]),
        cas_testing::digest_parts([b"b".as_slice(), b"a".as_slice()]),
    );
}

#[test]
fn different_part_framing_produces_different_digests() {
    // Length-prefixing must stop `("a", "b")` from colliding with `("ab",)`.
    assert_ne!(
        cas_testing::digest_parts([b"a".as_slice(), b"b".as_slice()]),
        cas_testing::digest_parts([b"ab".as_slice()])
    );
}
