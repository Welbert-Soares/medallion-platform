# Raw is append-only; deduplication happens in bronze

Every load appends to Raw unchanged and stamps each row with its Run, so reruns produce duplicates in Raw by design. Bronze (dbt incremental merge) is the first Layer that deduplicates. We keep Raw immutable so any downstream Layer can be rebuilt from it; do not "fix" Raw duplicates with merge/overwrite.
