//! How `Tree` handles its `entries`.

#[test]
fn duplicate_entry_names_keep_the_last_write() {
    let first = fabric::Node::Blob(cas_testing::digest_bytes(b"first"));
    let second = fabric::Node::Blob(cas_testing::digest_bytes(b"second"));

    let tree = fabric::Tree::new([("x".to_string(), first), ("x".to_string(), second)], []);
    assert_eq!(tree, fabric::Tree::new([("x".to_string(), second)], []));
}

#[test]
fn distinct_names_with_the_same_content_are_both_kept() {
    let content = cas_testing::digest_bytes(b"same");
    let two_entries = fabric::Tree::new(
        [
            ("a".to_string(), fabric::Node::Blob(content)),
            ("b".to_string(), fabric::Node::Blob(content)),
        ],
        [],
    );
    let one_entry = fabric::Tree::new([("a".to_string(), fabric::Node::Blob(content))], []);
    assert_ne!(cas_testing::digest(&two_entries), cas_testing::digest(&one_entry));
}
