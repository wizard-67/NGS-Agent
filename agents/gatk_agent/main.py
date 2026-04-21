import os
import subprocess
import tempfile
from pathlib import Path

import pandas as pd

from base_agent import BaseAgent
from storage import MinioStorage


class GATKAgent(BaseAgent):
    def _materialize(self, value: str, storage: MinioStorage, workdir: str) -> str:
        if value and value.startswith("s3://"):
            return storage.download_file(value, os.path.join(workdir, Path(value).name))
        return value

    def execute(self, inputs, routing_ctx):
        run_id = routing_ctx.get("run_id", "unknown")
        reference_fasta = routing_ctx.get("reference_fasta") or inputs.get("reference_fasta")
        if not reference_fasta:
            raise RuntimeError("reference_fasta is required for GATK")

        bam_path = inputs.get("payload", {}).get("bam_path") or inputs.get("bam_path")
        if not bam_path:
            raise RuntimeError("BAM input not found for GATK")

        known_sites = routing_ctx.get("known_sites") or inputs.get("known_sites") or []
        storage = MinioStorage()

        with tempfile.TemporaryDirectory(prefix="gatk-") as workdir:
            local_bam = self._materialize(bam_path, storage, workdir)
            local_ref = self._materialize(reference_fasta, storage, workdir)
            dedup_bam = os.path.join(workdir, "dedup.bam")
            metrics = os.path.join(workdir, "dedup.metrics.txt")
            recal_table = os.path.join(workdir, "recal.table")
            post_recal_table = os.path.join(workdir, "post_recal.table")
            recal_bam = os.path.join(workdir, "recalibrated.bam")
            vcf_gz = os.path.join(workdir, "variants.vcf.gz")

            subprocess.run([
                "gatk", "MarkDuplicatesSpark",
                "-I", local_bam,
                "-O", dedup_bam,
                "--metrics-file", metrics,
                "--conf", "spark.executor.cores=1"
            ], check=True, capture_output=True, text=True)

            if known_sites:
                known_args = []
                for ks in known_sites:
                    ks_local = self._materialize(ks, storage, workdir)
                    known_args.extend(["--known-sites", ks_local])
                subprocess.run([
                    "gatk", "BaseRecalibrator",
                    "-R", local_ref,
                    "-I", dedup_bam,
                    "-O", recal_table,
                    *known_args,
                ], check=True, capture_output=True, text=True)
                subprocess.run([
                    "gatk", "ApplyBQSR",
                    "-R", local_ref,
                    "-I", dedup_bam,
                    "--bqsr-recal-file", recal_table,
                    "-O", recal_bam,
                ], check=True, capture_output=True, text=True)
                final_bam = recal_bam
            else:
                final_bam = dedup_bam

            subprocess.run([
                "gatk", "HaplotypeCaller",
                "-R", local_ref,
                "-I", final_bam,
                "-O", vcf_gz,
            ], check=True, capture_output=True, text=True)

            final_bam_uri = storage.upload_file(final_bam, f"{run_id}/dna/gatk/final.bam")
            vcf_uri = storage.upload_file(vcf_gz, f"{run_id}/dna/gatk/variants.vcf.gz")
            metrics_uri = storage.upload_file(metrics, f"{run_id}/dna/gatk/mark_duplicates.metrics.txt")

            payload = {
                "final_bam": final_bam_uri,
                "variants_vcf": vcf_uri,
                "metrics": metrics_uri,
                "used_bqsr": bool(known_sites),
            }

        return {
            "agent": "gatk_agent",
            "status": "ok",
            "payload": payload,
            "reasoning": "GATK best-practice calling completed",
        }


if __name__ == "__main__":
    GATKAgent().run()
