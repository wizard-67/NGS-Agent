import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from base_agent import BaseAgent
from storage import MinioStorage

# ---------------------------------------------------------------------------
# Structured logger
# ---------------------------------------------------------------------------

def _log(level: str, msg: str, **extra) -> None:
    """Emit a structured JSON log line to stderr."""
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "agent": "align",
        "msg": msg,
        **extra,
    }
    print(json.dumps(entry), file=sys.stderr, flush=True)



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

    @staticmethod
    def _extract_json(text: str) -> dict | None:
        """Pull the first JSON object out of a string."""
        if not text:
            return None
        for candidate in [text.strip()] + [
            m.group(0) for m in [re.search(r"\{[\s\S]*\}", text)] if m
        ]:
            try:
                obj = json.loads(candidate)
                if isinstance(obj, dict):
                    return obj
            except Exception:
                continue
        return None

    def _ask_claude(self, stderr_log: str, mapping_rate: float) -> dict:
        """Send HISAT2 stderr to Claude to determine why alignment failed."""
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        model = os.environ.get("ANTHROPIC_MODEL", "claude-3-5-sonnet-20241022")

        if not api_key:
            _log("warn", "No ANTHROPIC_API_KEY set, cannot run alignment AI diagnosis")
            return {
                "reasoning": "Heuristic: Mapping rate is low. Consider aggressive re-trimming or checking reference genome.",
                "action": "abort",
            }

        prompt = (
            "You are an expert bioinformatician analyzing a failed RNA-seq alignment.\n"
            f"The HISAT2 alignment completed with an overall mapping rate of {mapping_rate}%, "
            "which is considered poor (<60%).\n\n"
            "Below is the HISAT2 stderr log showing the read fate (e.g., unmapped, mapped concordantly, etc.).\n"
            "Based on these metrics, determine the most likely cause (e.g., reads too short due to over-trimming, "
            "adapter contamination, wrong species/genome) and suggest a corrective action.\n\n"
            "Produce a JSON object with EXACTLY this schema:\n"
            "{\n"
            '  "reasoning": "<2-4 sentence explanation of the failure mode>",\n'
            '  "action": "re_trim" | "change_genome" | "abort",\n'
            '  "new_trim_params": { "LEADING": 5, "TRAILING": 5, "MINLEN": 36 } // ONLY if action is "re_trim", suggest more aggressive or less aggressive parameters depending on the issue.\n'
            "}\n\n"
            "Rules:\n"
            '- "re_trim" = the reads are likely failing due to adapter contamination or low quality. Suggest new Trimmomatic params.\n'
            '- "change_genome" = the reads are high quality but just aren\'t mapping (likely wrong species or major contamination).\n'
            '- "abort" = data is fundamentally unusable.\n\n'
            "=== HISAT2 Log ===\n"
            f"{stderr_log[-4000:]}"
        )

        _log("info", "Calling Claude API for alignment diagnosis", prompt_chars=len(prompt))

        try:
            from anthropic import Anthropic
            client = Anthropic(api_key=api_key)
            msg = client.messages.create(
                model=model,
                max_tokens=400,
                temperature=0,
                messages=[{"role": "user", "content": prompt}],
            )

            text = "".join(block.text for block in msg.content if getattr(block, "type", "") == "text")
            parsed = self._extract_json(text)
            
            if not parsed or "action" not in parsed:
                return {"reasoning": "Failed to parse AI JSON output.", "action": "abort"}

            _log("info", "AI alignment diagnosis produced", action=parsed["action"])
            return parsed

        except Exception as exc:
            _log("error", "Claude API call failed", error=str(exc))
            return {"reasoning": f"AI API failure: {exc}", "action": "abort"}

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

            threads = os.environ.get("AGENT_THREADS", "2")

            if fastq_r1 and fastq_r2:
                hisat2_cmd = [
                    "hisat2",
                    "-p", threads,
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
                hisat2_cmd = ["hisat2", "-p", threads, "-x", ref_genome, "-U", fastq_single, "-S", sam_path]

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
                ["samtools", "sort", "-@", threads, unsorted_bam, "-o", sorted_bam],
                check=True,
                capture_output=True,
                text=True,
            )
            subprocess.run(
                ["samtools", "index", "-@", threads, sorted_bam],
                check=True,
                capture_output=True,
                text=True,
            )

            bam_uri = storage.upload_file(sorted_bam, f"{run_id}/align/aligned.sorted.bam")
            bai_uri = storage.upload_file(
                f"{sorted_bam}.bai", f"{run_id}/align/aligned.sorted.bam.bai"
            )
            mapping_rate = self._extract_rate(hisat2_res.stderr)

        ai_evaluation = None
        alignment_status = "pass"
        if mapping_rate < 60.0:
            _log("warn", "Poor alignment rate detected, running AI diagnosis", mapping_rate=mapping_rate)
            alignment_status = "fail"
            ai_evaluation = self._ask_claude(hisat2_res.stderr, mapping_rate)

        return {
            "agent": "align",
            "status": "ok",
            "payload": {
                "bam_path": bam_uri,
                "bam_index": bai_uri,
                "mapping_rate": round(mapping_rate, 2),
                "hisat2_stderr": hisat2_res.stderr[-4000:],
                "alignment_status": alignment_status,
                "ai_evaluation": ai_evaluation,
            },
            "reasoning": f"Aligned with HISAT2, mapping rate {mapping_rate:.1f}%",
        }


if __name__ == "__main__":
    AlignAgent().run()
