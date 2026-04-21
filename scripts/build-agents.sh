#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

echo "Building base image..."
docker build -t ngs/base-agent:latest -f ./agents/base/Dockerfile .

for agent in ingest qc ai_decider trim align count de; do
  echo "Building ${agent} agent..."
  docker build -t "ngs/${agent}-agent:latest" "./agents/${agent}"
done

echo "All agents built successfully."
