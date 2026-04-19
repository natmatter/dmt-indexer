# Contributing

Thanks for wanting to help. Reports, fixes, and new features are welcome.

## Before you start

- Open an issue for non-trivial changes so we can agree on approach first.
- One logical change per pull request. Small, readable diffs get merged faster.
- New behavior needs a test or a note explaining why it can't be tested.

## Development

```sh
# build
cargo build --no-default-features

# run the full test suite (no Postgres needed)
cargo test --no-default-features

# lints the CI will run
cargo fmt --all -- --check
cargo clippy --all-targets --no-default-features -- -D warnings
```

The `verify-postgres` feature pulls in `sqlx` for cross-referencing against
a reference indexer. You don't need it for most work — the default-features
off build above skips it.

## Running against a real node

You need:

- Bitcoin Core 24+ with `txindex=1` and `rest=1`
- A readable cookie file or RPC user/password

Copy `config.example.toml` to `config.toml`, point it at your node, then:

```sh
cargo run --release -- run --config ./config.toml
```

## Style

- Prefer small functions with clear names over comments.
- Don't add backward-compatibility shims for code that isn't released yet.
- Error messages describe what was being attempted, not just what failed.
- Internal modules may use `tracing` freely; HTTP handlers should fail
  closed (return a JSON error) rather than panic.

## Scope

This repo indexes the `nat` DMT ticker today. Multi-ticker DMT support is on
the roadmap. PRs that extend the storage schema in a ticker-keyed way are
very welcome; PRs that tackle adjacent protocols (BRC-20, Runes, Bitmap,
`token-auth`/`token-trade`/`token-send`, UNAT rendering) are out of scope.

## Licensing

By contributing you agree that your contributions are licensed under the
MIT license (see `LICENSE`).
