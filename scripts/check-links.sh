#!/usr/bin/env bash
# Checks every link the repository's documents make into the repository
# itself — the READMEs, the changelog, how it works, the rustdoc comments —
# against the tree, as GitHub serves it: a link to a file or an anchor that
# is not there fails. Links elsewhere are not followed, so the check does
# not depend on the network. Needs lychee.
set -euo pipefail
cd "$(dirname "$0")/.."
root=$PWD

repo=https://github.com/rsansores/magic_events
lychee --offline --include-fragments --no-progress \
  --remap "^$repo/(blob|tree|edit)/main/(.*)$ file://$root/\$2" \
  --remap "^https://raw\.githubusercontent\.com/rsansores/magic_events/main/(.*)$ file://$root/\$1" \
  --remap "^$repo/?$ file://$root/" \
  --exclude '^https?://' \
  README.md CHANGELOG.md CONTRIBUTING.md RELEASING.md HOW_IT_WORKS.md \
  dashboard/README.md examples/shop/README.md \
  'magic_events/src/**/*.rs' 'magic_events_derive/src/**/*.rs'
