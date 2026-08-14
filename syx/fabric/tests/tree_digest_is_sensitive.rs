//! `Tree`'s digest is sensitive to its input.

#[test]
fn different_entry_names_produce_different_tree_digests() {
    let blob = cas_testing::digest_bytes(b"content");
    let a = fabric::Tree::new([("a".to_string(), fabric::Node::Blob(blob))], []);
    let b = fabric::Tree::new([("b".to_string(), fabric::Node::Blob(blob))], []);
    assert_ne!(cas_testing::digest(&a), cas_testing::digest(&b));
}

#[test]
fn a_blob_and_a_tree_with_the_same_inner_digest_have_different_tree_digests() {
    // A blob and a nested tree that happen to wrap the same inner
    // digest must not collide.
    let inner = cas_testing::digest_bytes(b"same");
    let a = fabric::Tree::new([("x".to_string(), fabric::Node::Blob(inner))], []);
    let b = fabric::Tree::new([("x".to_string(), fabric::Node::Tree(inner))], []);
    assert_ne!(cas_testing::digest(&a), cas_testing::digest(&b));
}

#[test]
fn different_nested_tree_content_produces_different_tree_digests() {
    let inner_a = fabric::Tree::new(
        [("f".to_string(), fabric::Node::Blob(cas_testing::digest_bytes(b"a")))],
        [],
    );
    let inner_b = fabric::Tree::new(
        [("f".to_string(), fabric::Node::Blob(cas_testing::digest_bytes(b"b")))],
        [],
    );

    let a = fabric::Tree::new(
        [("dir".to_string(), fabric::Node::Tree(cas_testing::digest(&inner_a)))],
        [],
    );
    let b = fabric::Tree::new(
        [("dir".to_string(), fabric::Node::Tree(cas_testing::digest(&inner_b)))],
        [],
    );
    assert_ne!(cas_testing::digest(&a), cas_testing::digest(&b));
}

#[test]
fn different_interns_produce_different_tree_digests() {
    let a = fabric::Tree::new([], [cas_testing::digest_bytes(b"intern-a")]);
    let b = fabric::Tree::new([], [cas_testing::digest_bytes(b"intern-b")]);
    assert_ne!(cas_testing::digest(&a), cas_testing::digest(&b));
}
