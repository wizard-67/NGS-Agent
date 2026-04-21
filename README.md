# NGS Agent Swarm MVP

Runnable local MVP for a containerized NGS workflow using Temporal orchestration and cache layers.

## What this includes

- Temporal server + Web UI via Docker Compose
- Redis cache + MinIO object storage
- 6 containerized agents: ingest, qc, trim, align, count, de
- Temporal workflow with conditional trim step
- Python CLI to submit and inspect runs

## Prerequisites

- Docker Desktop
- Python 3.11+

## Security

- `.env` is local-only and ignored by git.
- Copy `.env.example` to `.env` and fill values before running.

## Quick Start (Linux/macOS)

1. Start infrastructure:

```bash
docker compose up -d
```

2. Install Python dependencies:

```bash
python -m pip install -r requirements.txt
```

3. Build agent images:

```bash
./scripts/build-agents.sh
```

4. Start Temporal worker (keep terminal open):

```bash
python worker.py
```

5. Submit a run (new terminal):

```bash
python cli.py submit \
	--experiment RNA-Seq \
	--organism human \
	--ref-genome /data/ref/genome.fa \
	--gtf /data/ref/genes.gtf \
	--fastq tests/data/sample.fastq \
	--single
```

Paired-end example:

```bash
python cli.py submit \
	--experiment RNA-Seq \
	--organism human \
	--ref-genome /data/ref/genome.fa \
	--gtf /data/ref/genes.gtf \
	--fastq-r1 /data/sample_R1.fastq.gz \
	--fastq-r2 /data/sample_R2.fastq.gz \
	--paired
```

6. Check status:

```bash
python cli.py status <run-id>
```

7. Monitor in UI:

- Temporal: http://localhost:8080
- MinIO Console: http://localhost:9001

## Notes

- QC agent now has a real FastQC path. If `fastqc` is available and input files are mounted, it executes FastQC and uploads the HTML report to MinIO.
- If FastQC is unavailable, QC falls back to mock mode with explicit reasoning in payload.
- Other agents remain scaffolded and should be replaced with real tool invocations incrementally.
