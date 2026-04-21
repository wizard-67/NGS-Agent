#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

for agent in ingest qc ai_decider trim align bwa_agent gatk_agent annotation_agent count de_agent insight_agent report_builder; do
  echo "Building ${agent} agent..."
  docker build -t "ngs/${agent}-agent:latest" -f "./agents/${agent}/Dockerfile" .
done

echo "All agents built successfully."
