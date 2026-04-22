import json
import os
import re
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3

from base_agent import BaseAgent

# ---------------------------------------------------------------------------
# Structured logger — every AI interaction is traced
# ---------------------------------------------------------------------------

def _log(level: str, msg: str, **extra) -> None:
    """Emit a structured JSON log line to stderr (stdout is reserved for
    the agent output consumed by the activity layer)."""
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "agent": "qc",
        "msg": msg,
        **extra,
    }
    print(json.dumps(entry), file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# QC Agent — real FastQC + Claude AI interpretation
# ---------------------------------------------------------------------------

class QCAgent(BaseAgent):
    """QC agent that:
    1. Runs real FastQC on a mounted FASTQ file.
    2. Uploads the HTML report to MinIO.
    3. Sends the parsed fastqc_data.txt to Claude for an AI-driven
       quality verdict with structured reasoning.
    4. Logs every step.
    """

    # ── helpers ─────────────────────────────────────────────────────────

    def _resolve_fastq_path(self, inputs: dict) -> str | None:
        """Extract the FASTQ file path from various possible input layouts."""
        payload = inputs.get("payload", {})
        return (
            inputs.get("fastq_path")
            or payload.get("raw_reads")
            or inputs.get("fastq_r1")
            or payload.get("raw_reads_r1")
        )

    def _s3_client(self):
        """Build a boto3 S3 client pointing at MinIO."""
        return boto3.client(
            "s3",
            endpoint_url=os.environ.get("S3_ENDPOINT", "http://localhost:9000"),
            aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "minioadmin"),
        )

    def _upload_artifact(self, local_path: Path, run_id: str) -> str:
        """Upload a single file to MinIO under <run_id>/qc/<filename>."""
        bucket = os.environ.get("ARTIFACT_BUCKET", "ngs-artifacts")
        key = f"{run_id}/qc/{local_path.name}"
        self._s3_client().upload_file(str(local_path), bucket, key)
        uri = f"s3://{bucket}/{key}"
        _log("info", "Artifact uploaded", key=key, uri=uri)
        return uri

    # ── core FastQC runner ──────────────────────────────────────────────

    def _run_fastqc(self, fastq_path: str, run_id: str) -> dict:
        """Run FastQC, upload HTML, return raw results dict."""
        _log("info", "Starting FastQC", fastq=fastq_path)

        threads = os.environ.get("AGENT_THREADS", "2")
        with tempfile.TemporaryDirectory(prefix="qc-") as out_dir:
            cmd = ["fastqc", "--threads", threads, "--outdir", out_dir, fastq_path]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                _log("error", "FastQC failed", stderr=result.stderr.strip())
                raise RuntimeError(result.stderr.strip() or "FastQC failed")

            _log("info", "FastQC finished successfully")

            base = Path(fastq_path).name
            if base.endswith(".gz"):
                base = base[:-3]
            if "." in base:
                base = base.rsplit(".", 1)[0]

            html_path = Path(out_dir) / f"{base}_fastqc.html"
            zip_path = Path(out_dir) / f"{base}_fastqc.zip"

            # --- parse the FastQC zip for summary + raw data -----------
            summary_text = ""
            fastqc_data = ""
            if zip_path.exists():
                with zipfile.ZipFile(zip_path, "r") as zf:
                    summary_name = None
                    fastqc_data_name = None
                    for name in zf.namelist():
                        if name.endswith("summary.txt"):
                            summary_name = name
                        if name.endswith("fastqc_data.txt"):
                            fastqc_data_name = name
                    if summary_name:
                        with zf.open(summary_name) as f:
                            summary_text = f.read().decode("utf-8", errors="ignore")
                    if fastqc_data_name:
                        with zf.open(fastqc_data_name) as f:
                            fastqc_data = f.read().decode("utf-8", errors="ignore")

            read_length = self._parse_read_length(fastqc_data)

            _log("info", "FastQC data parsed",
                 summary_lines=summary_text.count("\n"),
                 data_bytes=len(fastqc_data),
                 read_length=read_length)

            # --- upload the HTML report to MinIO ----------------------
            report_uri = None
            if html_path.exists():
                report_uri = self._upload_artifact(html_path, run_id)

            return {
                "summary_text": summary_text,
                "fastqc_data": fastqc_data,
                "report_html": report_uri,
                "read_length": read_length,
            }

    # ── read-length extraction ──────────────────────────────────────────

    @staticmethod
    def _parse_read_length(fastqc_data: str) -> int | None:
        """Extract the actual read length from the >>Basic Statistics module
        of fastqc_data.txt.

        The line looks like:
            Sequence length\t150
        or for variable-length reads:
            Sequence length\t35-151
        We return the maximum value (the upper bound).
        """
        match = re.search(
            r"^Sequence length\t(\d+(?:-(\d+))?)$",
            fastqc_data,
            flags=re.MULTILINE,
        )
        if not match:
            return None
        # "150" → 150, "35-151" → 151
        parts = match.group(1).split("-")
        try:
            return int(parts[-1])
        except (ValueError, IndexError):
            return None

    # ── AI interpretation layer ─────────────────────────────────────────

    def _ask_claude(
        self,
        fastqc_data: str,
        summary_text: str,
        read_length: int | None = None,
    ) -> dict:
        """Send FastQC metrics to Claude and get a structured QC verdict.

        Returns:
            {
                "verdict": "pass" | "trim_required" | "fail",
                "reasoning": str,
                "quality_summary": str,
                "confidence": float,
                "recommended_trim_bp": int | null,
                "source": "llm" | "heuristic"
            }
        """
        # --- heuristic baseline (used if Claude is unavailable) -------
        heuristic = self._heuristic_verdict(summary_text)

        api_key = os.environ.get("ANTHROPIC_API_KEY")
        model = os.environ.get("ANTHROPIC_MODEL", "claude-3-5-sonnet-20241022")

        if not api_key:
            _log("warn", "No ANTHROPIC_API_KEY set, falling back to heuristic")
            return heuristic

        read_len_hint = (
            f"- The actual read length is {read_length}bp. "
            f"recommended_trim_bp MUST be ≤ {read_length} and ≥ 36.\n"
            if read_length
            else ""
        )

        prompt = (
            "You are an expert bioinformatician reviewing FastQC output for a "
            "Next-Generation Sequencing quality control step.\n\n"
            "Given the FastQC data below, produce a JSON object with EXACTLY "
            "this schema (no markdown fences, no extra keys):\n"
            "{\n"
            '  "verdict": "pass" | "trim_required" | "fail",\n'
            '  "reasoning": "<2-4 sentence explanation citing specific FastQC modules>",\n'
            '  "quality_summary": "<one-line overall quality assessment>",\n'
            '  "confidence": <float 0.0-1.0>\n'
            "}\n\n"
            "Rules:\n"
            '- "pass" = quality is good, no trimming needed.\n'
            '- "trim_required" = quality degrades in certain positions, trimming recommended.\n'
            '- "fail" = data is fundamentally unusable (extreme adapter contamination, '
            "very low quality across all positions, etc.).\n"
            "- Be conservative: only recommend trim if Per-base quality drops below Q20 "
            "or adapter content is >5%.\n\n"
            "=== FastQC Summary ===\n"
            f"{summary_text}\n\n"
            "=== FastQC Data (first 12000 chars) ===\n"
            f"{fastqc_data[:12000]}"
        )

        _log("info", "Calling Claude API",
             model=model,
             prompt_chars=len(prompt))

        try:
            from anthropic import Anthropic
            client = Anthropic(api_key=api_key)
            msg = client.messages.create(
                model=model,
                max_tokens=600,
                temperature=0,
                messages=[{"role": "user", "content": prompt}],
            )

            # extract text from response blocks
            text = ""
            for block in msg.content:
                if getattr(block, "type", "") == "text":
                    text += block.text

            _log("info", "Claude responded",
                 response_chars=len(text),
                 model=model,
                 input_tokens=getattr(msg.usage, "input_tokens", None),
                 output_tokens=getattr(msg.usage, "output_tokens", None))

            parsed = self._extract_json(text)
            if not parsed:
                _log("warn", "Claude returned non-JSON, using heuristic",
                     raw_response=text[:500])
                return {
                    **heuristic,
                    "reasoning": f"Claude returned non-JSON output; "
                                 f"heuristic verdict used. Raw: {text[:200]}",
                }

            validated = self._validate_ai_verdict(parsed, actual_read_length=read_length)
            if not validated:
                _log("warn", "Claude JSON missing required fields",
                     parsed_keys=list(parsed.keys()))
                return {
                    **heuristic,
                    "reasoning": "Claude JSON missing required fields; "
                                 "heuristic verdict used.",
                }

            _log("info", "AI verdict produced",
                 verdict=validated["verdict"],
                 confidence=validated["confidence"],
                 source="llm")
            return validated

        except Exception as exc:
            _log("error", "Claude API call failed, using heuristic",
                 error=str(exc))
            return {
                **heuristic,
                "reasoning": f"Claude API call failed ({exc}); "
                             f"heuristic verdict: {heuristic['verdict']}.",
            }

    # ── AI response parsing helpers ────────────────────────────────────

    @staticmethod
    def _extract_json(text: str) -> dict | None:
        """Pull the first JSON object out of a string."""
        if not text:
            return None
        # try the whole thing first, then regex-extract
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

    # Biological limits for trim length
    MIN_USABLE_READ_BP = 36   # below this, most aligners can't map reliably
    MAX_READ_BP = 300         # longest standard Illumina read length

    @staticmethod
    def _normalize_verdict(raw: str) -> str | None:
        """Fuzzy-map Claude's verdict string to one of the three canonical
        values, or return None if it's truly unrecognisable.

        This avoids throwing away a perfectly good AI response just because
        Claude said "trim_needed" instead of "trim_required".
        """
        v = raw.strip().lower().replace("-", "_").replace(" ", "_")

        # exact match first
        if v in ("pass", "trim_required", "fail"):
            return v

        # fuzzy mapping — order matters: check "fail" before "trim"
        # because "fail" is a substring of nothing ambiguous, but
        # "trim" appears in many plausible variants.
        if v in ("fail", "failed", "unusable"):
            return "fail"
        if "trim" in v:  # trim_needed, trimming_recommended, needs_trim, …
            return "trim_required"
        if v in ("pass", "passed", "good", "ok", "okay", "accept", "accepted"):
            return "pass"

        _log("warn", "Unrecognised verdict from Claude, cannot normalise",
             raw_verdict=raw)
        return None

    @classmethod
    def _validate_ai_verdict(cls, parsed: dict) -> dict | None:
        """Validate the JSON structure and map the verdict."""
        required_keys = {"verdict", "reasoning", "quality_summary", "confidence"}
        if not required_keys.issubset(parsed.keys()):
            return None

        verdict = cls._normalize_verdict(str(parsed["verdict"]))
        if verdict is None:
            return None

        confidence = float(parsed.get("confidence", 0.7))
        confidence = max(0.0, min(1.0, confidence))

        return {
            "verdict": verdict,
            "reasoning": str(parsed.get("reasoning", "AI verdict produced."))[:1200],
            "quality_summary": str(parsed.get("quality_summary", ""))[:500],
            "confidence": round(confidence, 3),
            "source": "llm",
        }

    @staticmethod
    def _heuristic_verdict(summary_text: str) -> dict:
        """Deterministic fallback when Claude is unavailable."""
        fails = len(re.findall(r"^FAIL\t", summary_text, flags=re.MULTILINE))
        warns = len(re.findall(r"^WARN\t", summary_text, flags=re.MULTILINE))
        if fails >= 2:
            verdict = "fail"
        elif fails > 0 or warns >= 3:
            verdict = "trim_required"
        else:
            verdict = "pass"
        confidence = 0.55 + min(0.4, 0.1 * fails + 0.03 * warns)
        return {
            "verdict": verdict,
            "reasoning": f"Heuristic from FastQC summary flags: "
                         f"FAIL={fails}, WARN={warns}.",
            "quality_summary": f"{fails} failures, {warns} warnings in FastQC modules.",
            "confidence": round(min(0.95, confidence), 3),
            "source": "heuristic",
        }

    # ── entry point ─────────────────────────────────────────────────────

    def execute(self, inputs: dict, routing_ctx: dict) -> dict:
        run_id = routing_ctx.get("run_id", "unknown")
        fastq_path = self._resolve_fastq_path(inputs)

        _log("info", "QC agent starting", run_id=run_id, fastq_path=fastq_path)

        # --- guard: need a real FASTQ and a real fastqc binary --------
        if not fastq_path:
            raise RuntimeError(
                "No FASTQ path found in inputs. "
                "Expected one of: inputs.fastq_path, inputs.payload.raw_reads, "
                "inputs.fastq_r1, inputs.payload.raw_reads_r1"
            )

        if not Path(fastq_path).exists():
            raise RuntimeError(
                f"FASTQ file does not exist at resolved path: {fastq_path}"
            )

        if not _fastqc_available():
            raise RuntimeError(
                "fastqc is not installed or not on PATH. "
                "Install it (apt-get install fastqc) or use the Docker image."
            )

        # --- 1. Run real FastQC ----------------------------------------
        qc_result = self._run_fastqc(fastq_path, run_id)
        read_length = qc_result.get("read_length")

        # --- 2. Ask Claude to interpret the results --------------------
        ai_verdict = self._ask_claude(
            fastqc_data=qc_result["fastqc_data"],
            summary_text=qc_result["summary_text"],
            read_length=read_length,
        )

        verdict = ai_verdict["verdict"]

        _log("info", "QC agent finished",
             run_id=run_id,
             verdict=verdict,
             source=ai_verdict["source"],
             confidence=ai_verdict["confidence"],
             read_length=read_length,
             report_html=qc_result["report_html"])

        # --- 3. Return structured output --------------------------------
        return {
            "agent": "qc",
            "status": "ok",
            "payload": {
                "verdict": verdict,
                "report_html": qc_result["report_html"],
                "fastqc_data": qc_result.get("fastqc_data", ""),
                "qc_mode": "fastqc",
                "read_length": read_length,
                # AI-specific fields
                "ai_source": ai_verdict["source"],
                "ai_confidence": ai_verdict["confidence"],
                "ai_quality_summary": ai_verdict.get("quality_summary", ""),
            },
            "reasoning": ai_verdict["reasoning"],
        }


def _fastqc_available() -> bool:
    """Return True if the fastqc binary is on PATH and executable."""
    import shutil
    return shutil.which("fastqc") is not None


if __name__ == "__main__":
    QCAgent().run()
