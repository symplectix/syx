load("//bazel:rust.bzl", "rust")

rust.library(
    name = "ply",
    srcs = [
        "src/blob.rs",
        "src/lib.rs",
        "src/repository.rs",
    ],
    proc_macro_deps = ["//syx/cas_cbor2"],
    visibility = ["//visibility:public"],
    deps = [
        "//syx/cas",
        "//syx/cas_fjall",
        "@crates//:cbor2",
        "@crates//:serde",
        "@crates//:tokio",
    ],
)

rust.test_suite(
    name = "ply_test_suite",
    size = "small",
    srcs = [
        "tests/store_chunks_large_content.rs",
        "tests/store_fidelity.rs",
        "tests/store_is_deterministic.rs",
        "tests/store_missing.rs",
        "tests/store_persists.rs",
        "tests/store_reads_into_a_writer.rs",
        "tests/store_skips_compression_when_not_worthwhile.rs",
        "tests/tree_digest_is_deterministic.rs",
        "tests/tree_digest_is_sensitive.rs",
        "tests/tree_entries.rs",
        "tests/tree_interns.rs",
    ],
    data = ["//:.rustfmt.toml"],
    shared_srcs = ["tests/common/mod.rs"],
    deps = [
        ":ply",
        "//syx/cas",
        "//syx/cas_testing",
        "//syx/testing",
        "@crates//:tokio",
    ],
)
