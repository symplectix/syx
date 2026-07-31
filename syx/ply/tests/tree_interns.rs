//! How `Tree` handles its `interns`.

#[test]
fn interns_are_equal_regardless_of_build_order() {
    let a = cas_testing::digest_bytes(b"a");
    let b = cas_testing::digest_bytes(b"b");
    assert_eq!(ply::Tree::new([], [a, b]), ply::Tree::new([], [b, a]));
}

#[test]
fn interning_the_same_digest_twice_does_not_change_the_tree() {
    let a = cas_testing::digest_bytes(b"a");
    assert_eq!(
        cas_testing::digest(&ply::Tree::new([], [a, a])),
        cas_testing::digest(&ply::Tree::new([], [a]))
    );
}
