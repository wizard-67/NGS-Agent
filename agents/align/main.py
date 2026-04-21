import os
import re
import subprocess
import tempfile
from pathlib import Path

from base_agent import BaseAgent
from storage import MinioStorage


class AlignAgent(BaseAgent):
    def _materialize_input(self, value: str, storage: MinioStorage, workdir: str) -> str:
        if value.startswith("s3://"):
            local_path = os.path.join(workdir, Path(value).name)
            return storage.download_file(value, local_path)
        return value

    def _extract_rate(self, stderr: str) -> float:
        m = re.search(r"([0-9]+(?:\.[0-9]+)?)% overall alignment rate", stderr)
        if not m:
            return 0.0
        return float(m.group(1))

    def execute(self, inputs, routing_ctx):
        run_id = routing_ctx.get("run_id", "unknown")
        ref_genome = routing_ctx.get("reference_genome") or inputs.get("ref_genome")
        if not ref_genome:
            raise RuntimeError("reference_genome is required for alignment")

        storage = MinioStorage()

        fastq_single = inputs.get("fastq_path") or inputs.get("payload", {}).get("raw_reads")
        fastq_r1 = inputs.get("fastq_r1") or inputs.get("payload", {}).get("raw_reads_r1")
        fastq_r2 = inputs.get("fastq_r2") or inputs.get("payload", {}).get("raw_reads_r2")

        # Accept trimmed outputs if trim agent was executed.
        fastq_single = fastq_single or inputs.get("payload", {}).get("fastq_path")
        fastq_r1 = fastq_r1 or inputs.get("payload", {}).get("fastq_r1")
        fastq_r2 = fastq_r2 or inputs.get("payload", {}).get("fastq_r2")

        if not fastq_single and not (fastq_r1 and fastq_r2):
            raise RuntimeError("No FASTQ input available for align agent")

        with tempfile.TemporaryDirectory(prefix="align-") as workdir:
            if fastq_single:
                fastq_single = self._materialize_input(fastq_single, storage, workdir)
            if fastq_r1 and fastq_r2:
                fastq_r1 = self._materialize_input(fastq_r1, storage, workdir)
                fastq_r2 = self._materialize_input(fastq_r2, storage, workdir)

            sam_path = os.path.join(workdir, "aligned.sam")
            unsorted_bam = os.path.join(workdir, "aligned.unsorted.bam")
            sorted_bam = os.path.join(workdir, "aligned.sorted.bam")

            if fastq_r1 and fastq_r2:
                hisat2_cmd = [
                    "hisat2",
                    "-x",
                    ref_genome,
                    "-1",
                    fastq_r1,
                    "-2",
                    fastq_r2,
                    "-S",
                    sam_path,
                ]
            else:
                hisat2_cmd = ["hisat2", "-x", ref_genome, "-U", fastq_single, "-S", sam_path]

            hisat2_res = subprocess.run(hisat2_cmd, capture_output=True, text=True)
            if hisat2_res.returncode != 0:
                raise RuntimeError(f"hisat2 failed: {hisat2_res.stderr.strip()}")

            subprocess.run(
                ["samtools", "view", "-bS", sam_path, "-o", unsorted_bam],
                check=True,
                capture_output=True,
                text=True,
            )
            subprocess.run(
                ["samtools", "sort", unsorted_bam, "-o", sorted_bam],
                check=True,
                capture_output=True,
                text=True,
            )
            subprocess.run(
                ["samtools", "index", sorted_bam],
                check=True,
                capture_output=True,
                text=True,
            )

            bam_uri = storage.upload_file(sorted_bam, f"{run_id}/align/aligned.sorted.bam")
            bai_uri = storage.upload_file(
                f"{sorted_bam}.bai", f"{run_id}/align/aligned.sorted.bam.bai"
            )
            mapping_rate = self._extract_rate(hisat2_res.stderr)

        return {
            "agent": "align",
            "status": "ok",
            "payload": {
                "bam_path": bam_uri,
                "bam_index": bai_uri,
                "mapping_rate": round(mapping_rate, 2),
                "hisat2_stderr": hisat2_res.stderr[-4000:],
            },
            "reasoning": f"Aligned with HISAT2, mapping rate {mapping_rate:.1f}%",
        }


if __name__ == "__main__":
    AlignAgent().run()
