import json
import os
import subprocess
import tempfile
from pathlib import Path

from base_agent import BaseAgent
from storage import MinioStorage


class DEAgent(BaseAgent):
    def _materialize(self, value: str, storage: MinioStorage, workdir: str) -> str:
        if value and value.startswith("s3://"):
            return storage.download_file(value, os.path.join(workdir, Path(value).name))
        return value

    def execute(self, inputs, routing_ctx):
        run_id = routing_ctx.get("run_id", "unknown")
        count_uri = inputs.get("payload", {}).get("count_matrix") or inputs.get("count_matrix")
        sample_sheet_uri = inputs.get("sample_sheet") or routing_ctx.get("sample_sheet")
        if not count_uri:
            raise RuntimeError("count_matrix is required for DE analysis")
        if not sample_sheet_uri:
            raise RuntimeError("sample_sheet is required for DE analysis")

        storage = MinioStorage()
        with tempfile.TemporaryDirectory(prefix="de-") as workdir:
            local_count = self._materialize(count_uri, storage, workdir)
            local_sheet = self._materialize(sample_sheet_uri, storage, workdir)
            out_dir = os.path.join(workdir, "out")
            os.makedirs(out_dir, exist_ok=True)

            cmd = ["Rscript", "/app/de_analysis.R", local_count, local_sheet, out_dir]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise RuntimeError(f"DESeq2 analysis failed: {result.stderr.strip()}")

            uploaded = {}
            for path in Path(out_dir).glob("**/*"):
                if path.is_file():
                    key = f"{run_id}/de/{path.name}"
                    uploaded[path.name] = storage.upload_file(str(path), key)

            summary_path = Path(out_dir) / "de_summary.json"
            summary = json.loads(summary_path.read_text()) if summary_path.exists() else {}

        return {
            "agent": "de",
            "status": "ok",
            "payload": {
                "artifacts": uploaded,
                "de_summary": summary,
            },
            "reasoning": "DESeq2 completed with statistical outputs and plots",
        }


if __name__ == "__main__":
    DEAgent().run()
