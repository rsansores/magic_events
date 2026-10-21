# Releasing

`magic_events_derive`, `magic_events` and `magic_events_dashboard` are
released together, at one version, by one click: **Actions → Release → Run
workflow** on `main`, with the version. `.github/workflows/release.yml`
then:

1. sets the version everywhere it is written and dates the changelog's
   unreleased entry (`scripts/set-version.sh`), in the working tree only, and
   packs and verifies both crates at it;
2. runs CI on the commit released — the tree before the version is set;
   the version's own change (the manifests, the lockfiles brought along, the
   changelog's date) is verified by step 1's `cargo package`, which builds
   each crate from its tarball — and packs the dashboard at the version;
3. waits for a maintainer to approve the `release` environment, then
   publishes the derive, then the crate, then the dashboard;
4. only once every registry took the release, commits `release:
   v<version>` on the commit released, tags it `v<version>`, pushes the tag,
   and moves `main` to it.

*Dry run* does steps 1 and 2 — the version set, everything built, packed
and verified — and publishes, commits and tags nothing. Nothing is published
from a workstation.

Before the click, the changelog's entry for the release is written: an
`## <version> — unreleased` (or `## Unreleased`) heading the release dates.
It says, for a host upgrading:

- whether the stored schema changed (`SCHEMA_VERSION`): the step from the
  version before, for a host with migrations of its own;
- whether the messages changed (`tests/golden/messages.txt`), which every
  consumer reads;
- whether the dashboard's contract changed (`status::CONTRACT`);
- what in the API changed: the difference in `magic_events/public-api/`.

A release that fails half-way — the derive published, the crate refused —
is finished by fixing the cause and running the failed jobs again: a
publication whose version the registry already has says so and succeeds.
The release refuses a version that is not semver, one tagged already, and a
run on any branch but `main`. The tag is pushed on its own; `main` is moved
to it after, and refused when something was merged while the release ran —
the release is out all the same (registries, tag), and the tag is merged
into `main` by a pull request.

Nothing that compiles holds a credential. The `prepare` job packs and
verifies the tarball of each crate, the version set; the publishing job
packs the same tree again and publishes it without building it (`cargo
publish --no-verify`, in `scripts/publish-crate.sh`), so no build script or
proc macro runs next to a token. The dashboard is packed by a job with no
credentials, and the tarball is what the publishing job publishes. Every
action is pinned to a commit (`.github/dependabot.yml` proposes the new
ones, and those of the lockfiles, cargo's and npm's), CI's `workflows` job
runs [zizmor](https://docs.zizmor.sh) over the workflows, and a scheduled
workflow (`advisories.yml`) looks every day for an advisory published
against what a lockfile holds.

## Once, before the first release

- In the repository's settings, **Environments → New environment**
  `release`, with the maintainers who approve a release as its required
  reviewers.
- A package's first version has no one to trust yet — crates.io and npm
  attach a Trusted Publisher only to a package that exists — so the first
  release publishes with tokens, as the `release` environment's secrets:
  - `CARGO_REGISTRY_TOKEN`: a crates.io token (Account Settings → API
    Tokens) with the scopes `publish-new` and `publish-update`;
  - `NPM_TOKEN`: an npm granular access token, read and write on packages,
    allowed to publish new ones.

## Once, after the first release

Make the workflow the only publisher, and delete the tokens:

- **crates.io**, for each of `magic_events_derive` and `magic_events`: the
  crate's **Settings → Trusted Publishing → Add**: owner `rsansores`,
  repository `magic_events`, workflow `release.yml`, environment `release`.
- **npm**, `magic_events_dashboard`: **Settings → Trusted Publisher → GitHub
  Actions**: the same four.
- Delete the two secrets from the `release` environment, and revoke the
  tokens on crates.io and npm.

## What CI checks before a release can happen

Every pull request packs both crates and verifies each tarball builds
(`cargo package --workspace`), packs the dashboard and lints the tarball,
checks the versions and every link into the repository, and audits the
workflows. The crates carry their licenses (`LICENSE-MIT`, `LICENSE-APACHE`,
linked from the root) and the README; the dashboard's `prepack` copies the
licenses in.

docs.rs builds the crate with every feature (`[package.metadata.docs.rs]`).
