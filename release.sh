#!/usr/bin/env bash
# Bump the patch version, commit, tag, and push so GitHub Actions builds images.
set -euo pipefail

root="$(cd "$(dirname "$0")" && pwd)"
cd "$root"

if [[ ! -f Cargo.toml ]]; then
  echo "run this script from the cangling-message repository" >&2
  exit 1
fi

if [[ -n "$(git status --porcelain)" ]]; then
  echo "working tree is dirty; commit or stash first" >&2
  git status --short
  exit 1
fi

current="$(sed -n 's/^version = "\([0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\)"/\1/p' Cargo.toml | head -n1)"
if [[ -z "$current" ]]; then
  echo "could not read version from Cargo.toml" >&2
  exit 1
fi

IFS=. read -r major minor patch <<<"$current"
new="${major}.${minor}.$((patch + 1))"

if git rev-parse -q --verify "refs/tags/v${new}" >/dev/null; then
  echo "tag v${new} already exists" >&2
  exit 1
fi

sed -i "s/^version = \"${current}\"/version = \"${new}\"/" Cargo.toml

git add Cargo.toml
git commit -m "Release v${new}"
git tag -a "v${new}" -m "Release v${new}"

remote="${1:-origin}"
branch="$(git rev-parse --abbrev-ref HEAD)"
git push "$remote" "HEAD:refs/heads/${branch}"
git push "$remote" "v${new}"

echo "released v${current} -> v${new} and pushed ${branch} to ${remote}"
echo "CI will build and push:"
echo "  docker.io/mapway/cangling-message:${new}"
echo "  harbor.cangling.cn/cangling/cangling-message:${new}"
