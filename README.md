# NGS Agent Swarm

Temporal-orchestrated RNA-Seq pipeline with containerized agents and MinIO artifacts.

## Implemented pipeline

- Ingest: validates FASTQ input and read counts
- QC: runs FastQC and uploads report artifacts
- AI Decider: sends FastQC metrics to Claude and decides if trim is needed
- Trim: runs Trimmomatic (single or paired mode)
- Align: runs HISAT2 + samtools sort/index
- Count: runs featureCounts
- DE: runs DESeq2, PCA, MA, volcano, and heatmap generation
- Insight: runs GO enrichment and grounded AI interpretation
- Report Builder: generates a self-contained HTML report
- DNA branch: runs BWA-MEM2, GATK calling, annotation, and coverage summaries

## Prerequisites

- Docker Engine/Desktop
- Python 3.11+
- Linux/macOS shell (Windows users should run under WSL2)

## Security

- `.env` is git-ignored
- Copy `.env.example` to `.env`
- Rotate any credentials if they were ever exposed in commit history

## Setup

```bash
cp .env.example .env
python -m pip install -r requirements.txt
docker compose up -d
bash scripts/build-agents.sh
```

Start worker:

```bash
python worker.py
```

Quick-start wizard:

```bash
make wizard
```

DNA branch note:

- Provide `--experiment WGS` or `--experiment WES` together with `--reference-fasta`.
- Optionally mount a prebuilt `snpEff.jar` and set `SNPEFF_JAR=/path/to/snpEff.jar` for richer annotation.

## Real data example (paired-end)

1. Download a tiny paired FASTQ test set:

```bash
mkdir -p data/fastq
curl -L -o data/fastq/test_R1.fastq.gz "https://ftp.sra.ebi.ac.uk/vol1/fastq/SRR258/008/SRR2584868/SRR2584868_1.fastq.gz"
curl -L -o data/fastq/test_R2.fastq.gz "https://ftp.sra.ebi.ac.uk/vol1/fastq/SRR258/008/SRR2584868/SRR2584868_2.fastq.gz"
```

2. Download reference + annotation and build HISAT2 index:

```bash
mkdir -p data/ref
curl -L -o data/ref/genome.fa.gz "https://ftp.ensembl.org/pub/release-112/fasta/homo_sapiens/dna/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz"
curl -L -o data/ref/genes.gtf.gz "https://ftp.ensembl.org/pub/release-112/gtf/homo_sapiens/Homo_sapiens.GRCh38.112.gtf.gz"
gunzip -f data/ref/genome.fa.gz
gunzip -f data/ref/genes.gtf.gz
hisat2-build data/ref/genome.fa data/ref/grch38_idx
```

3. Submit run:

```bash
python cli.py submit \
  --experiment RNA-Seq \
  --organism human \
  --ref-genome data/ref/grch38_idx \
  --gtf data/ref/genes.gtf \
  --fastq-r1 data/fastq/test_R1.fastq.gz \
  --fastq-r2 data/fastq/test_R2.fastq.gz \
  --paired
```

4. Check run:

```bash
python cli.py status <run-id>
```

## Artifact locations

- QC report: `s3://ngs-artifacts/<run_id>/qc/...`
- Trimmed FASTQ: `s3://ngs-artifacts/<run_id>/trim/...`
- BAM + BAI: `s3://ngs-artifacts/<run_id>/align/...`
- Count matrix/summary: `s3://ngs-artifacts/<run_id>/count/...`
- DNA BAM/VCF/annotation outputs: `s3://ngs-artifacts/<run_id>/dna/...`

## Tests

Functional test harness:

```bash
RUN_NGS_FUNCTIONAL=1 \
TEST_FASTQ_R1=/abs/path/R1.fastq.gz \
TEST_FASTQ_R2=/abs/path/R2.fastq.gz \
TEST_HISAT2_INDEX_DIR=/abs/path/index_dir \
TEST_GTF=/abs/path/genes.gtf \
pytest -q tests/test_pipeline.py
```

## Current limitation

- DNA variant-calling branch is not yet wired into the workflow and remains the next expansion path.
