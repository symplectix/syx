//! `Tree`'s digest is deterministic.

#[test]
fn hashing_an_empty_tree_twice_gives_the_same_digest() {
    assert_eq!(
        cas_testing::digest(&fabric::Tree::new([], [])),
        cas_testing::digest(&fabric::Tree::new([], []))
    );
}

#[test]
fn tree_digest_ignores_entry_build_order() {
    let a = ("a".to_string(), fabric::Node::Blob(cas_testing::digest_bytes(b"a")));
    let b = ("b".to_string(), fabric::Node::Blob(cas_testing::digest_bytes(b"b")));

    let forward = fabric::Tree::new([a.clone(), b.clone()], []);
    let backward = fabric::Tree::new([b, a], []);

    assert_eq!(cas_testing::digest(&forward), cas_testing::digest(&backward));
}

#[test]
fn tree_digest_ignores_intern_build_order() {
    let a = cas_testing::digest_bytes(b"a");
    let b = cas_testing::digest_bytes(b"b");
    assert_eq!(
        cas_testing::digest(&fabric::Tree::new([], [a, b])),
        cas_testing::digest(&fabric::Tree::new([], [b, a]))
    );
}
