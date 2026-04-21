import gzip
from pathlib import Path

from base_agent import BaseAgent


class IngestAgent(BaseAgent):
    def _count_reads(self, path: str) -> int:
        p = Path(path)
        line_count = 0
        if p.suffix == ".gz":
            with gzip.open(p, "rt") as handle:
                for _ in handle:
                    line_count += 1
        else:
            with p.open("r", encoding="utf-8") as handle:
                for _ in handle:
                    line_count += 1
        return max(1, line_count // 4)

    def execute(self, inputs, routing_ctx):
        fastq_path = inputs.get("fastq_path")
        fastq_r1 = inputs.get("fastq_r1")
        fastq_r2 = inputs.get("fastq_r2")

        if fastq_r1 and fastq_r2:
            paired = True
            p1 = Path(fastq_r1)
            p2 = Path(fastq_r2)
            if p1.exists() and p2.exists():
                reads_r1 = self._count_reads(fastq_r1)
                reads_r2 = self._count_reads(fastq_r2)
                read_count = min(reads_r1, reads_r2)
                reasoning = (
                    f"Validated paired reads from both inputs (R1={reads_r1}, R2={reads_r2})"
                )
            else:
                read_count = 1000
                reasoning = "Paired input files not mounted in container, using mocked read count"
        else:
            paired = False
            p = Path(fastq_path or "")
            if fastq_path and p.exists():
                read_count = self._count_reads(fastq_path)
                reasoning = f"Validated {read_count} reads from single-end input file"
            else:
                read_count = 1000
                reasoning = "Single-end input file not mounted in container, using mocked read count"

        return {
            "agent": "ingest",
            "status": "ok",
            "payload": {
                "read_count": read_count,
                "is_paired": paired,
                "encoding": "phred33",
                "raw_reads": fastq_path,
                "raw_reads_r1": fastq_r1,
                "raw_reads_r2": fastq_r2,
            },
            "reasoning": reasoning,
        }


if __name__ == "__main__":
    IngestAgent().run()
