import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from base_agent import BaseAgent
from storage import MinioStorage


class BWAMem2Agent(BaseAgent):
    def _materialize(self, value: str, storage: MinioStorage, workdir: str) -> str:
        if value and value.startswith("s3://"):
            return storage.download_file(value, os.path.join(workdir, Path(value).name))
        return value

    def _aligner_cmd(self) -> str:
        if shutil.which("bwa-mem2"):
            return "bwa-mem2"
        if shutil.which("bwa"):
            return "bwa"
        raise RuntimeError("Neither bwa-mem2 nor bwa is installed")

    def _coverage_from_bed(self, bam: str, panel_bed: str, workdir: str) -> str:
        coverage_rows = []
        with open(panel_bed, "r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip() or line.startswith("#"):
                    continue
                cols = line.rstrip().split("\t")
                if len(cols) < 3:
                    continue
                chrom, start, end = cols[:3]
                name = cols[3] if len(cols) >= 4 else f"{chrom}:{start}-{end}"
                region = f"{chrom}:{int(start)+1}-{end}"
                cmd = ["samtools", "depth", "-r", region, bam]
                res = subprocess.run(cmd, capture_output=True, text=True, check=True)
                depths = [int(row.split("\t")[2]) for row in res.stdout.splitlines() if row.strip()]
                mean_depth = sum(depths) / len(depths) if depths else 0.0
                coverage_rows.append({"gene": name, "region": region, "mean_depth": round(mean_depth, 4)})

        df = pd.DataFrame(coverage_rows)
        csv_path = Path(workdir) / "coverage_depth.csv"
        df.to_csv(csv_path, index=False)

        if not df.empty:
            plt.figure(figsize=(max(10, len(df) * 0.35), 5))
            plt.bar(df["gene"], df["mean_depth"], color="#1f77b4")
            plt.xticks(rotation=90)
            plt.ylabel("Mean depth")
            plt.tight_layout()
            plt.savefig(Path(workdir) / "coverage_depth.png", dpi=300)
            plt.close()
        return str(csv_path)

    def execute(self, inputs, routing_ctx):
        run_id = routing_ctx.get("run_id", "unknown")
        ref_genome = routing_ctx.get("reference_fasta") or inputs.get("reference_fasta") or routing_ctx.get("reference_genome")
        if not ref_genome:
            raise RuntimeError("reference_fasta is required for DNA alignment")

        storage = MinioStorage()
        fastq_single = inputs.get("fastq_path") or inputs.get("payload", {}).get("raw_reads")
        fastq_r1 = inputs.get("fastq_r1") or inputs.get("payload", {}).get("raw_reads_r1")
        fastq_r2 = inputs.get("fastq_r2") or inputs.get("payload", {}).get("raw_reads_r2")
        fastq_single = fastq_single or inputs.get("payload", {}).get("fastq_path")
        fastq_r1 = fastq_r1 or inputs.get("payload", {}).get("fastq_r1")
        fastq_r2 = fastq_r2 or inputs.get("payload", {}).get("fastq_r2")
        panel_bed = routing_ctx.get("panel_bed") or inputs.get("panel_bed")

        if not fastq_single and not (fastq_r1 and fastq_r2):
            raise RuntimeError("No FASTQ input available for DNA alignment")

        with tempfile.TemporaryDirectory(prefix="bwa-") as workdir:
            if fastq_single:
                fastq_single = self._materialize(fastq_single, storage, workdir)
            if fastq_r1 and fastq_r2:
                fastq_r1 = self._materialize(fastq_r1, storage, workdir)
                fastq_r2 = self._materialize(fastq_r2, storage, workdir)
            if panel_bed:
                panel_bed = self._materialize(panel_bed, storage, workdir)

            sam_path = os.path.join(workdir, "aligned.sam")
            unsorted_bam = os.path.join(workdir, "aligned.unsorted.bam")
            sorted_bam = os.path.join(workdir, "aligned.sorted.bam")

            aligner = self._aligner_cmd()
            threads = os.environ.get("AGENT_THREADS", "2")
            if aligner == "bwa-mem2":
                base_cmd = ["bwa-mem2", "mem", "-t", threads, ref_genome]
            else:
                base_cmd = ["bwa", "mem", "-t", threads, ref_genome]

            if fastq_r1 and fastq_r2:
                align_cmd = base_cmd + [fastq_r1, fastq_r2]
            else:
                align_cmd = base_cmd + [fastq_single]

            with open(sam_path, "w", encoding="utf-8") as sam_handle:
                align_res = subprocess.run(align_cmd, stdout=sam_handle, stderr=subprocess.PIPE, text=True)
            if align_res.returncode != 0:
                raise RuntimeError(f"{aligner} failed: {align_res.stderr.strip()}")

            subprocess.run(["samtools", "view", "-bS", sam_path, "-o", unsorted_bam], check=True, capture_output=True, text=True)
            subprocess.run(["samtools", "sort", "-@", threads, unsorted_bam, "-o", sorted_bam], check=True, capture_output=True, text=True)
            subprocess.run(["samtools", "index", "-@", threads, sorted_bam], check=True, capture_output=True, text=True)
            flagstat = subprocess.run(["samtools", "flagstat", "-@", threads, sorted_bam], capture_output=True, text=True, check=True).stdout

            coverage_csv = None
            if panel_bed and Path(panel_bed).exists():
                coverage_csv = self._coverage_from_bed(sorted_bam, panel_bed, workdir)

            bam_uri = storage.upload_file(sorted_bam, f"{run_id}/dna/bwa/aligned.sorted.bam")
            bai_uri = storage.upload_file(f"{sorted_bam}.bai", f"{run_id}/dna/bwa/aligned.sorted.bam.bai")
            flagstat_path = Path(workdir) / "flagstat.txt"
            flagstat_path.write_text(flagstat, encoding="utf-8")
            flagstat_uri = storage.upload_file(str(flagstat_path), f"{run_id}/dna/bwa/flagstat.txt")

            artifacts = {"bam_path": bam_uri, "bam_index": bai_uri, "flagstat": flagstat_uri}
            if coverage_csv:
                artifacts["coverage_depth_csv"] = storage.upload_file(coverage_csv, f"{run_id}/dna/bwa/coverage_depth.csv")
                png_path = Path(workdir) / "coverage_depth.png"
                if png_path.exists():
                    artifacts["coverage_depth_png"] = storage.upload_file(str(png_path), f"{run_id}/dna/bwa/coverage_depth.png")

        return {
            "agent": "bwa_agent",
            "status": "ok",
            "payload": {
                "artifacts": artifacts,
                "aligner": aligner,
                "command": " ".join(align_cmd),
                "stderr": align_res.stderr[-4000:],
            },
            "reasoning": f"DNA alignment completed with {aligner}",
        }


if __name__ == "__main__":
    BWAMem2Agent().run()
