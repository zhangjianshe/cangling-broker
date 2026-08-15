#!/usr/bin/env bash
# Bump the patch version, commit, tag, and push. GitHub Actions deploys Docker, Maven Central, and PyPI.
set -euo pipefail

root="$(cd "$(dirname "$0")" && pwd)"
cd "$root"

if [[ ! -f Cargo.toml ]]; then
  echo "run this script from the cangling-broker repository" >&2
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
python3 - "$current" "$new" <<'PY'
from pathlib import Path
import sys
current, new = sys.argv[1], sys.argv[2]
lock = Path("Cargo.lock")
old = f'name = "cangling-broker"\nversion = "{current}"'
updated = f'name = "cangling-broker"\nversion = "{new}"'
text = lock.read_text()
if old not in text:
    raise SystemExit(f"Cargo.lock is missing {old!r}")
lock.write_text(text.replace(old, updated, 1))
pom = Path("java/pom.xml")
pom_old = f"<artifactId>cangling-broker</artifactId>\n    <version>{current}</version>"
pom_new = f"<artifactId>cangling-broker</artifactId>\n    <version>{new}</version>"
pom_text = pom.read_text()
if pom_old not in pom_text:
    raise SystemExit(f"java/pom.xml is missing {pom_old!r}")
pom.write_text(pom_text.replace(pom_old, pom_new, 1))
py = Path("python/pyproject.toml")
py_old = f'version = "{current}"'
py_text = py.read_text()
if py_old not in py_text:
    raise SystemExit(f"python/pyproject.toml is missing {py_old!r}")
py.write_text(py_text.replace(py_old, f'version = "{new}"', 1))
PY

git add Cargo.toml Cargo.lock java/pom.xml python/pyproject.toml
git commit -m "Release v${new}"
git tag -a "v${new}" -m "Release v${new}"

remote="${1:-origin}"
branch="$(git rev-parse --abbrev-ref HEAD)"
git push "$remote" "HEAD:refs/heads/${branch}"
git push "$remote" "v${new}"

echo "released v${current} -> v${new} and pushed ${branch} to ${remote}"
echo "CI will build and push:"
echo "  docker.io/mapway/cangling-broker:${new}"
echo "  harbor.cangling.cn:22002/cangling/cangling-broker:${new}"
echo "  Maven Central cn.mapway:cangling-broker:${new}"
echo "  PyPI cangling-broker==${new}"
