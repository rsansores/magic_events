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

## How the packages trust the workflow

0.1.0, the first version of each package, was published from a maintainer's
workstation (a package must exist before a registry lets anything else
publish it), from the release commit, the tarballs packed as the workflow
packs them. Each package now trusts this repository's `release.yml`, in the
`release` environment, as its publisher — Trusted Publishing:

- **crates.io**, `magic_events_derive` and `magic_events`: owner
  `rsansores`, repository `magic_events`, workflow `release.yml`,
  environment `release` (each crate's **Settings → Trusted Publishing**).
- **npm**, `magic_events_dashboard`: the same four (`npm trust list
  magic_events_dashboard`).

The workflow gets a token for each publication from the registry itself,
valid for minutes, and no secret is stored: the `release` environment holds
none. It still names `CARGO_REGISTRY_TOKEN` and `NPM_TOKEN` as a fallback,
for a package whose trust is lost; set one as an environment secret only for
as long as it takes to publish, and revoke it after.

The `release` environment has the maintainers who approve a release as its
required reviewers: a publication waits for one of them.

## What CI checks before a release can happen

Every pull request packs both crates and verifies each tarball builds
(`cargo package --workspace`), packs the dashboard and lints the tarball,
checks the versions and every link into the repository, and audits the
workflows. The crates carry their licenses (`LICENSE-MIT`, `LICENSE-APACHE`,
linked from the root) and the README; the dashboard's `prepack` copies the
licenses in.

docs.rs builds the crate with every feature (`[package.metadata.docs.rs]`).
