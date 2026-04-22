import os
import subprocess
import tempfile
from pathlib import Path

from base_agent import BaseAgent
from storage import MinioStorage


class TrimAgent(BaseAgent):
    def _resolve_input(self, value: str, storage: MinioStorage, workdir: str) -> str:
        if value.startswith("s3://"):
            return storage.download_file(value, os.path.join(workdir, Path(value).name))
        return value

    def _param_list(self, params: dict) -> list[str]:
        """Build the Trimmomatic step list.
        
        Applies standard dynamic quality trimming parameters:
        LEADING, TRAILING, SLIDINGWINDOW, and MINLEN.
        """
        p = {
            "LEADING": params.get("LEADING", 3),
            "TRAILING": params.get("TRAILING", 3),
            "SLIDINGWINDOW": params.get("SLIDINGWINDOW", "4:20"),
            "MINLEN": max(36, int(params.get("MINLEN", 36))),
        }
        steps = []
        steps.extend([
            f"LEADING:{p['LEADING']}",
            f"TRAILING:{p['TRAILING']}",
            f"SLIDINGWINDOW:{p['SLIDINGWINDOW']}",
            f"MINLEN:{p['MINLEN']}",
        ])
        return steps

    def execute(self, inputs, routing_ctx):
        run_id = routing_ctx.get("run_id", "unknown")
        payload = inputs.get("payload", {})
        trim_params = payload.get("trim_params", {})

        # Trimmomatic uses SLIDINGWINDOW, LEADING, TRAILING, and MINLEN 
        # from the AI decider's output, avoiding hard-truncation (CROP).

        fastq_single = payload.get("raw_reads") or inputs.get("fastq_path")
        fastq_r1 = payload.get("raw_reads_r1") or inputs.get("fastq_r1")
        fastq_r2 = payload.get("raw_reads_r2") or inputs.get("fastq_r2")

        if not fastq_single and not (fastq_r1 and fastq_r2):
            raise RuntimeError("trim agent requires FASTQ input")

        storage = MinioStorage()
        with tempfile.TemporaryDirectory(prefix="trim-") as workdir:
            if fastq_single:
                fastq_single = self._resolve_input(fastq_single, storage, workdir)
            if fastq_r1 and fastq_r2:
                fastq_r1 = self._resolve_input(fastq_r1, storage, workdir)
                fastq_r2 = self._resolve_input(fastq_r2, storage, workdir)

            threads = os.environ.get("AGENT_THREADS", "2")
            trimmomatic = ["trimmomatic"]
            params = self._param_list(trim_params)

            if fastq_r1 and fastq_r2:
                out_r1_paired = os.path.join(workdir, "trimmed_R1.paired.fastq.gz")
                out_r1_unpaired = os.path.join(workdir, "trimmed_R1.unpaired.fastq.gz")
                out_r2_paired = os.path.join(workdir, "trimmed_R2.paired.fastq.gz")
                out_r2_unpaired = os.path.join(workdir, "trimmed_R2.unpaired.fastq.gz")
                cmd = (
                    trimmomatic
                    + [
                        "PE",
                        "-threads", threads,
                        "-phred33",
                        fastq_r1,
                        fastq_r2,
                        out_r1_paired,
                        out_r1_unpaired,
                        out_r2_paired,
                        out_r2_unpaired,
                    ]
                    + params
                )
                subprocess.run(cmd, check=True, capture_output=True, text=True)

                r1_uri = storage.upload_file(out_r1_paired, f"{run_id}/trim/trimmed_R1.paired.fastq.gz")
                r2_uri = storage.upload_file(out_r2_paired, f"{run_id}/trim/trimmed_R2.paired.fastq.gz")
                r1u_uri = storage.upload_file(
                    out_r1_unpaired, f"{run_id}/trim/trimmed_R1.unpaired.fastq.gz"
                )
                r2u_uri = storage.upload_file(
                    out_r2_unpaired, f"{run_id}/trim/trimmed_R2.unpaired.fastq.gz"
                )

                payload_out = {
                    "fastq_r1": r1_uri,
                    "fastq_r2": r2_uri,
                    "fastq_r1_unpaired": r1u_uri,
                    "fastq_r2_unpaired": r2u_uri,
                    "trim_params_used": trim_params,
                }
            else:
                out_single = os.path.join(workdir, "trimmed.single.fastq.gz")
                cmd = trimmomatic + ["SE", "-threads", threads, "-phred33", fastq_single, out_single] + params
                subprocess.run(cmd, check=True, capture_output=True, text=True)

                single_uri = storage.upload_file(out_single, f"{run_id}/trim/trimmed.single.fastq.gz")
                payload_out = {
                    "fastq_path": single_uri,
                    "trim_params_used": trim_params,
                }

        return {
            "agent": "trim",
            "status": "ok",
            "payload": payload_out,
            "reasoning": "Trimmomatic completed successfully",
        }


if __name__ == "__main__":
    TrimAgent().run()
