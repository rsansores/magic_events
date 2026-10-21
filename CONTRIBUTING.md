# Contributing

Issues and pull requests are welcome.

## The tests

The suite needs a PostgreSQL (14 or later) whose user may create databases —
each test gets one of its own — and a NATS server with JetStream, where each
test makes a stream of its own:

```sh
docker run -d -p 4222:4222 nats:2.15.1 -js
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres
export NATS_URL=nats://localhost:4222
cargo test --workspace --all-features
```

Each run includes the derive's suite (`magic_events/tests/derive.rs`), which
builds every case under `magic_events/tests/derive/` as a host would: what it
accepts compiles, and each misuse fails with the message checked in beside
it. The messages are the compiler's words, so they are held to one release
of rustc, named in the test (`PINNED`); on any other the suite builds what is
accepted and says it left the messages alone. CI's `diagnostics` job runs
them on the pinned one. A change to a message is written anew with that
toolchain and `TRYBUILD=overwrite`, and read before it is committed. A
misuse the derive refuses has a case under `tests/derive/fail/`; a new
refusal comes with its case.

Some tests compare what the crate makes with a file checked in — the
messages it publishes (`magic_events/tests/golden/`), with
`MAGIC_EVENTS_BLESS=1` writing them anew; the dashboard's types and
fixtures (`dashboard/src/core/contract/`), written by the status tests with
`UPDATE_CONTRACT=1`. Read the difference before committing it.

The walk (`walk_tests.rs`) — writers committing and rolling back while
relays start, stop, crash and lose their connection — takes the seeds in
`SEEDS`; `MAGIC_EVENTS_WALK_SEEDS` gives it others, comma-separated, or
`random`, and each walk prints its seed. A nightly workflow runs it with
`random`: a walk that fails there is taken again with
`MAGIC_EVENTS_WALK_SEEDS` set to its seed, and the seed joins `SEEDS` with
the fix.

## The benchmarks

What `emit` adds to a write, and how fast the relay publishes at
`in_flight` 1, 16 and 64; the README quotes them, with the machine and the
date:

```sh
cargo test -p magic_events --release bench_ -- --ignored --nocapture --test-threads 1
```

## The README

Every Rust block of the README is code of `magic_events/tests/readme.rs`,
line for line, and a test of that file holds the two together: change one,
change the other. `scripts/check-links.sh` checks every link the documents
and the rustdoc comments make into the repository, anchors included,
against the tree, with no network; it needs [lychee](https://lychee.cli.rs).

## The public API

What a host can name is listed in `magic_events/public-api/all-features.txt`,
and CI fails when the crate's API is not that list. A change to the API is a
change to the list, made on purpose:

```sh
rustup toolchain install nightly-2026-10-10 --profile minimal
cargo install cargo-public-api --version 0.52.0 --locked
cargo +nightly-2026-10-10 public-api -p magic_events --all-features -s > magic_events/public-api/all-features.txt
```

## The example

`examples/shop` is a workspace of its own, with a lockfile of its own: run
`cargo fmt` and `cargo clippy` with `--manifest-path`, as CI does.

## What CI checks

`.github/workflows/ci.yml`, on every pull request and every push to main:

- **lint** — `cargo fmt --check` of the workspace and the example; `cargo
  clippy -D warnings` of the crates with every feature and with none, and of
  the example; the docs with `-D warnings`.
- **dependencies** — `cargo deny check` (advisories, licenses, sources and
  unmaintained crates anywhere in the graph; `deny.toml`) of the workspace
  and the example; **unused** — `cargo machete` over both. A scheduled
  workflow (`advisories.yml`) runs the advisories check daily, on days
  nothing is pushed, and Dependabot proposes the new versions of the
  actions, the crates and the dashboard's npm packages.
- **workflows** — zizmor over `.github/`: every action pinned to a commit,
  no token left behind by a checkout, no expression injected into a script.
- **test** — the suite against PostgreSQL 14 and 18, with a NATS server.
- **diagnostics** — the derive's refusals, message by message, on the
  pinned rustc.
- **public-api** — the API against `magic_events/public-api/`.
- **msrv** — the crates build on Rust 1.88.
- **example** — the shop builds against the crate as it is.
- **example-page** — the dashboard builds from source, and the shop's
  events page (formatting, lint, types) over it.
- **dashboard** — its formatting, lint, types, unused code (knip), tests,
  and the packed tarball as npm and TypeScript read it.
- **links** — every link into the repository resolves.
- **package** — one version everywhere (`scripts/check-versions.sh`), and
  `cargo package` of both crates, each verified by building the tarball.

## A change to the tables

The crate's schema is a list of steps (`MIGRATIONS` in `src/schema.rs`), one
per `SCHEMA_VERSION`. A released step is never edited: a change is a new
step, the version raised by one, and a test that upgrades a database of the
version before it. The changelog says so, and names the step for hosts with
migrations of their own.

## A change to the messages

What a consumer in any language reads — the subject, the headers, the body
— is a contract. `tests/golden/messages.txt` holds it; a change to it is a
change consumers see, and the changelog says so.
