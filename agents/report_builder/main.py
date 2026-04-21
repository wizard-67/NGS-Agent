import json
import os
import subprocess
import tempfile
from pathlib import Path

from base_agent import BaseAgent
from storage import MinioStorage


class ReportBuilderAgent(BaseAgent):
    def _write_json(self, path: Path, obj) -> None:
        path.write_text(json.dumps(obj, indent=2), encoding="utf-8")

    def _materialize_uri(self, value: str, storage: MinioStorage, target: Path) -> str:
        if isinstance(value, str) and value.startswith("s3://"):
            return storage.download_file(value, str(target))
        return value

    def execute(self, inputs, routing_ctx):
        run_id = routing_ctx.get("run_id", "unknown")

        storage = MinioStorage()
        with tempfile.TemporaryDirectory(prefix="report-") as workdir:
            local_dir = Path(workdir) / "artifacts"
            local_dir.mkdir(parents=True, exist_ok=True)

            payload = inputs.get("payload", {})
            self._write_json(local_dir / "qc.json", payload.get("qc", {}))
            self._write_json(local_dir / "align.json", payload.get("align", {}))
            self._write_json(local_dir / "count.json", payload.get("count", {}))
            self._write_json(local_dir / "de_summary.json", payload.get("de", {}).get("payload", {}).get("de_summary", {}))
            self._write_json(local_dir / "insight.json", payload.get("insight", {}))
            (local_dir / "ai_summary.md").write_text(
                payload.get("insight", {}).get("payload", {}).get("ai_summary", ""),
                encoding="utf-8",
            )
            (local_dir / "methods.md").write_text(
                "Temporal orchestrated pipeline; command provenance should be added from workflow metadata.",
                encoding="utf-8",
            )
            variants_csv = payload.get("variants_csv", "")
            if isinstance(variants_csv, str) and variants_csv.startswith("s3://"):
                variants_csv = self._materialize_uri(variants_csv, storage, local_dir / "variants.csv")
            else:
                (local_dir / "variants.csv").write_text(variants_csv, encoding="utf-8")

            coverage_csv = payload.get("coverage_depth_csv")
            if isinstance(coverage_csv, str) and coverage_csv.startswith("s3://"):
                self._materialize_uri(coverage_csv, storage, local_dir / "coverage_depth.csv")

            coverage_png = payload.get("coverage_depth_png")
            if isinstance(coverage_png, str) and coverage_png.startswith("s3://"):
                self._materialize_uri(coverage_png, storage, local_dir / "coverage_depth.png")

            cmd = ["python3", "/app/report_builder.py", "--artifacts-dir", str(local_dir), "--output", str(Path(workdir) / "index.html")]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise RuntimeError(f"Report build failed: {result.stderr.strip()}")

            html_path = Path(workdir) / "index.html"
            report_uri = storage.upload_file(str(html_path), f"{run_id}/report/index.html")

        return {
            "agent": "report_builder",
            "status": "ok",
            "payload": {
                "report_html": report_uri,
            },
            "reasoning": "Interactive HTML report generated",
        }


if __name__ == "__main__":
    ReportBuilderAgent().run()
