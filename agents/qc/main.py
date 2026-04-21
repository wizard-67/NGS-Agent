import os
import random
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path

import boto3

from base_agent import BaseAgent


class QCAgent(BaseAgent):
    def _input_fastq(self, inputs):
        payload = inputs.get("payload", {})
        return (
            inputs.get("fastq_path")
            or payload.get("raw_reads")
            or inputs.get("fastq_r1")
            or payload.get("raw_reads_r1")
        )

    def _upload_artifact(self, local_path: Path, run_id: str) -> str:
        endpoint = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
        bucket = os.environ.get("ARTIFACT_BUCKET", "ngs-artifacts")
        key = f"{run_id}/qc/{local_path.name}"
        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "minioadmin"),
        )
        s3.upload_file(str(local_path), bucket, key)
        return f"s3://{bucket}/{key}"

    def _run_fastqc(self, fastq_path: str):
        run_id = "unknown"
        with tempfile.TemporaryDirectory(prefix="qc-") as out_dir:
            cmd = ["fastqc", "--outdir", out_dir, fastq_path]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise RuntimeError(result.stderr.strip() or "FastQC failed")

            base = Path(fastq_path).name
            if base.endswith(".gz"):
                base = base[:-3]
            if "." in base:
                base = base.rsplit(".", 1)[0]

            html_path = Path(out_dir) / f"{base}_fastqc.html"
            zip_path = Path(out_dir) / f"{base}_fastqc.zip"

            verdict = "pass"
            if zip_path.exists():
                with zipfile.ZipFile(zip_path, "r") as zf:
                    summary_name = None
                    for name in zf.namelist():
                        if name.endswith("summary.txt"):
                            summary_name = name
                            break
                    if summary_name:
                        with zf.open(summary_name) as summary_file:
                            summary = summary_file.read().decode("utf-8", errors="ignore")
                        if "FAIL\tPer base sequence quality" in summary:
                            verdict = "trim_required"

            if html_path.exists():
                run_id = os.environ.get("RUN_ID", "unknown")
                report_uri = self._upload_artifact(html_path, run_id)
            else:
                report_uri = None

            return {
                "verdict": verdict,
                "report_html": report_uri,
            }

    def execute(self, inputs, routing_ctx):
        run_id = routing_ctx.get("run_id", "unknown")
        os.environ["RUN_ID"] = run_id
        fastq_path = self._input_fastq(inputs)

        if fastq_path and Path(fastq_path).exists() and shutil.which("fastqc"):
            try:
                real = self._run_fastqc(fastq_path)
                verdict = real["verdict"]
                return {
                    "agent": "qc",
                    "status": "ok",
                    "payload": {
                        "mean_quality": None,
                        "drop_pos": None,
                        "verdict": verdict,
                        "trim_to_bp": 150 if verdict == "pass" else 100,
                        "report_html": real["report_html"],
                        "qc_mode": "fastqc",
                    },
                    "reasoning": f"FastQC completed, verdict: {verdict}",
                }
            except Exception as exc:
                fallback_reason = f"FastQC path failed ({exc}), falling back to mock QC"
        else:
            fallback_reason = "FastQC unavailable or input not mounted, falling back to mock QC"

        mean_q = random.uniform(30, 40)
        drop_pos = None if mean_q > 35 else random.randint(50, 100)
        verdict = "pass" if mean_q > 35 else "trim_required"

        return {
            "agent": "qc",
            "status": "ok",
            "payload": {
                "mean_quality": round(mean_q, 2),
                "drop_pos": drop_pos,
                "verdict": verdict,
                "trim_to_bp": (drop_pos - 5) if drop_pos else 150,
                "report_html": None,
                "qc_mode": "mock",
            },
            "reasoning": f"QC verdict: {verdict}. {fallback_reason}",
        }


if __name__ == "__main__":
    QCAgent().run()
