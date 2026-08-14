load("//bazel:rust.bzl", "rust")

rust.library(
    name = "fabric",
    srcs = [
        "src/blob.rs",
        "src/function.rs",
        "src/graph.rs",
        "src/lib.rs",
    ],
    proc_macro_deps = ["//syx/cas_cbor2"],
    visibility = ["//visibility:public"],
    deps = [
        "//syx/cas",
        "@crates//:cbor2",
        "@crates//:object_store",
        "@crates//:serde",
        "@crates//:slatedb",
        "@crates//:tokio",
    ],
)

rust.test_suite(
    name = "fabric_test_suite",
    size = "small",
    srcs = [
        "tests/command_digest_is_deterministic.rs",
        "tests/command_digest_is_sensitive.rs",
        "tests/function_digest_is_deterministic.rs",
        "tests/function_digest_is_sensitive.rs",
        "tests/function_resolves_from_graph.rs",
        "tests/graph_chunks_large_content.rs",
        "tests/graph_fidelity.rs",
        "tests/graph_is_deterministic.rs",
        "tests/graph_missing.rs",
        "tests/graph_persists.rs",
        "tests/graph_reads_into_a_writer.rs",
        "tests/graph_skips_compression_when_not_worthwhile.rs",
        "tests/tree_digest_is_deterministic.rs",
        "tests/tree_digest_is_sensitive.rs",
        "tests/tree_entries.rs",
        "tests/tree_interns.rs",
    ],
    data = ["//:.rustfmt.toml"],
    shared_srcs = ["tests/common/mod.rs"],
    deps = [
        ":fabric",
        "//syx/cas",
        "//syx/cas_testing",
        "//syx/testing",
        "@crates//:object_store",
        "@crates//:tokio",
    ],
)
