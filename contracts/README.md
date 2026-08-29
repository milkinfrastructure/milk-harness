# Cross-repository conformance

`snapshot-analyzer.json` binds the exact teacher request contract to the committed `milk-gateway` source revision and `records.rs` hash. The native release builder checks the frozen gateway commit, whole-file hash, system prompt, and response schema against this fixture before either repository is built. Later contract changes require coordinated commits in both repositories; the jobs runtime image binds the admitted gateway contract digest.
