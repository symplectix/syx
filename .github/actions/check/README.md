# Check action

Runs pre-build checks, then builds and tests `//...`. Supports two modes:

- `presubmit`: runs the pre-build hooks, then builds and tests.
- `postsubmit`: builds and tests, and saves the Bazel caches.

## Caches

The repository cache and disk cache are restored in both modes but saved only
in postsubmit, so short-lived PR branches do not pollute caches shared across
runs. Unchanged targets are served from the restored/remote caches.

## Pre-build

In presubmit, `prek run --from-ref <base-sha>` runs the `ci` hook group over
the files changed since the base commit. `base-sha` is the PR base (or the
merge-group base). It is empty on `workflow_dispatch`, which skips this step.
