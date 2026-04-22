import csv
import os
import subprocess
import tempfile

from base_agent import BaseAgent
from storage import MinioStorage


class CountAgent(BaseAgent):
    def _count_rows_cols(self, matrix_file: str) -> tuple[int, int]:
        with open(matrix_file, "r", encoding="utf-8") as handle:
            reader = csv.reader(handle, delimiter="\t")
            rows = list(reader)
        if not rows:
            return 0, 0
        return max(0, len(rows) - 1), len(rows[0])

    def _materialize_bam(self, bam_path: str, storage: MinioStorage, workdir: str) -> str:
        if bam_path.startswith("s3://"):
            local_bam = os.path.join(workdir, "input.bam")
            return storage.download_file(bam_path, local_bam)
        return bam_path

    def execute(self, inputs, routing_ctx):
        run_id = routing_ctx.get("run_id", "unknown")
        gtf = routing_ctx.get("gtf") or inputs.get("gtf")
        if not gtf:
            raise RuntimeError("GTF path is required for featureCounts")

        bam_path = inputs.get("payload", {}).get("bam_path") or inputs.get("bam_path")
        if not bam_path:
            raise RuntimeError("BAM input not found for count agent")

        storage = MinioStorage()
        with tempfile.TemporaryDirectory(prefix="count-") as workdir:
            local_bam = self._materialize_bam(bam_path, storage, workdir)
            counts_tsv = os.path.join(workdir, "counts.tsv")
            summary_txt = os.path.join(workdir, "counts.tsv.summary")

            threads = os.environ.get("AGENT_THREADS", "2")
            cmd = [
                "featureCounts",
                "-T", threads,
                "-a",
                gtf,
                "-o",
                counts_tsv,
                local_bam,
            ]
            res = subprocess.run(cmd, capture_output=True, text=True)
            if res.returncode != 0:
                raise RuntimeError(f"featureCounts failed: {res.stderr.strip()}")

            matrix_uri = storage.upload_file(counts_tsv, f"{run_id}/count/counts.tsv")
            summary_uri = storage.upload_file(summary_txt, f"{run_id}/count/counts.summary.txt")
            n_genes, n_cols = self._count_rows_cols(counts_tsv)

        return {
            "agent": "count",
            "status": "ok",
            "payload": {
                "count_matrix": matrix_uri,
                "count_summary": summary_uri,
                "n_genes": n_genes,
                "n_columns": n_cols,
            },
            "reasoning": "featureCounts completed on aligned BAM",
        }


if __name__ == "__main__":
    CountAgent().run()
