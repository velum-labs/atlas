#!/usr/bin/env bash
# Usage: ./scripts/release.sh <version>
# Example: ./scripts/release.sh 0.2.0
#
# This script:
#   1. Validates the version argument
#   2. Syncs all package versions via sync-versions.py
#   3. Commits the version bump
#   4. Tags the commit (vX.Y.Z)
#   5. Pushes the tag — this triggers the publish workflow

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# --- Validate argument ---
if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <version>" >&2
    echo "Example: $0 0.2.0" >&2
    exit 1
fi

VERSION="$1"

# Basic semver check (X.Y.Z or X.Y.Z-suffix)
if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-].+)?$ ]]; then
    echo "ERROR: Version '$VERSION' does not look like a semver string (e.g. 0.2.0)" >&2
    exit 1
fi

TAG="v${VERSION}"

cd "$REPO_ROOT"

# --- Ensure working tree is clean ---
if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "ERROR: Working tree has uncommitted changes. Commit or stash them first." >&2
    exit 1
fi

# --- Sync versions ---
echo "$VERSION" > VERSION
python3 scripts/sync-versions.py

# --- Commit ---
git add VERSION packages/*/pyproject.toml
git commit -m "chore: bump version to ${VERSION}"

# --- Tag ---
git tag -a "$TAG" -m "Release ${TAG}"

echo ""
echo "Created commit + tag ${TAG}."
echo "Pushing tag to origin (this triggers the publish workflow)..."
git push origin "$TAG"

echo ""
echo "Done. Monitor the publish workflow at:"
echo "  https://github.com/$(git remote get-url origin | sed 's/.*github.com[:/]\(.*\)\.git/\1/')/actions"
