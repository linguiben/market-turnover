#!/usr/bin/env bash
set -euo pipefail

DRY_RUN=0
FORCE=0
while getopts "nf" opt; do
  case $opt in
    n) DRY_RUN=1 ;;
    f) FORCE=1 ;;
    *) echo "Usage: $0 [-n dry-run] [-f force]"; exit 2 ;;
  esac
done

if ! command -v docker >/dev/null 2>&1; then
  echo "docker not found in PATH" >&2
  exit 1
fi

# Get dangling image IDs (REPOSITORY and TAG are <none>)
ids=$(docker images -f "dangling=true" -q)

if [ -z "$ids" ]; then
  echo "No dangling images found."
  exit 0
fi

echo "Dangling image IDs found:"
echo "$ids"

if [ "$DRY_RUN" -eq 1 ]; then
  echo "Dry run enabled — not deleting."
  exit 0
fi

if [ "$FORCE" -eq 1 ]; then
  docker rmi -f $ids
else
  docker rmi $ids
fi