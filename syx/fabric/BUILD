load("//bazel:rust.bzl", "rust")

rust.library(
    name = "fabric",
    srcs = [
        "src/blob.rs",
        "src/function.rs",
        "src/graph.rs",
        "src/lib.rs",
        "src/staging.rs",
        "src/staging/segment.rs",
        "src/staging/tests.rs",
        "src/storage.rs",
        "src/storage/tests.rs",
    ],
    proc_macro_deps = ["//syx/cas_cbor2"],
    visibility = ["//visibility:public"],
    deps = [
        "//syx/content_addressing",
        "@crates//:bytes",
        "@crates//:cbor2",
        "@crates//:crossbeam-skiplist",
        "@crates//:futures",
        "@crates//:memmap2",
        "@crates//:object_store",
        "@crates//:serde",
        "@crates//:slatedb",
        "@crates//:tokio",
    ],
)

rust.test(
    name = "fabric_test",
    size = "small",
    crate = ":fabric",
    deps = [
        "//syx/testing",
    ],
)

rust.test(
    name = "fabric_remote_object_store_test",
    size = "small",
    srcs = ["tests/remote_object_store.rs"],
    data = ["@minio_static//file"],
    # The bundled minio_static binary sometimes reports "Server not
    # initialized yet" on its first request right after startup.
    flaky = True,
    # The minio_static runfile is a static Linux/amd64 binary.
    target_compatible_with = [
        "@platforms//cpu:x86_64",
        "@platforms//os:linux",
    ],
    deps = [
        ":fabric",
        "//syx/content_addressing",
        "//syx/testing",
        "@crates//:aws-sdk-s3",
        "@crates//:futures",
        "@crates//:object_store",
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
        "//syx/cas_testing",
        "//syx/content_addressing",
        "//syx/testing",
        "@crates//:object_store",
        "@crates//:tokio",
    ],
)
