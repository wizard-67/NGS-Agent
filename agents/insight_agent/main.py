import json
import os
import subprocess
import tempfile
from pathlib import Path

import pandas as pd
from anthropic import Anthropic

from base_agent import BaseAgent
from storage import MinioStorage


class InsightAgent(BaseAgent):
    def _materialize(self, value: str, storage: MinioStorage, workdir: str) -> str:
        if value and value.startswith("s3://"):
            return storage.download_file(value, os.path.join(workdir, Path(value).name))
        return value

    def _build_prompt(self, treatment: str, control: str, go_terms: list[dict], sig_genes: list[str]) -> str:
        return (
            "You are a molecular biologist. Summarize these GO results into a 3-sentence paragraph a PI would understand. "
            f"Focus on the biological story: '{treatment}' vs '{control}'.\n\n"
            "Rules:\n"
            "- Use only the GO database evidence below.\n"
            "- Do not hallucinate pathways, phenotypes, or mechanisms.\n"
            "- Mention only biological themes supported by the evidence.\n"
            "- Keep it concise and report-ready.\n\n"
            f"Top GO terms: {json.dumps(go_terms[:10], indent=2)}\n\n"
            f"Significant genes: {', '.join(sig_genes[:25])}\n\n"
            "Return markdown with the heading:\n"
            "## AI-Generated Summary: What does this mean?"
        )

    def _llm_summary(self, treatment: str, control: str, go_terms: list[dict], sig_genes: list[str]) -> str:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            return (
                "## AI-Generated Summary: What does this mean?\n"
                f"The GO analysis suggests the {treatment} condition differs from {control} across the enriched biological themes reported above. "
                "The summary was generated without a live LLM because ANTHROPIC_API_KEY is not configured."
            )

        client = Anthropic(api_key=api_key)
        prompt = self._build_prompt(treatment, control, go_terms, sig_genes)
        msg = client.messages.create(
            model=os.environ.get("ANTHROPIC_MODEL", "claude-3-5-sonnet-20241022"),
            max_tokens=500,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        text = ""
        for block in msg.content:
            if getattr(block, "type", "") == "text":
                text += block.text
        return text.strip()

    def execute(self, inputs, routing_ctx):
        run_id = routing_ctx.get("run_id", "unknown")
        de_summary = inputs.get("payload", {}).get("de_summary", {})
        de_artifacts = inputs.get("payload", {}).get("artifacts", {})
        go_input_uri = inputs.get("go_input") or routing_ctx.get("go_input")
        if not go_input_uri:
            raise RuntimeError("GO enrichment input is required for insight analysis")

        storage = MinioStorage()
        with tempfile.TemporaryDirectory(prefix="insight-") as workdir:
            local_de = self._materialize(de_artifacts.get("deseq_results.csv", ""), storage, workdir)
            local_go_input = self._materialize(go_input_uri, storage, workdir)
            out_dir = os.path.join(workdir, "out")
            os.makedirs(out_dir, exist_ok=True)

            cmd = ["Rscript", "/app/go_analysis.R", local_de, local_go_input, out_dir]
            res = subprocess.run(cmd, capture_output=True, text=True)
            if res.returncode != 0:
                raise RuntimeError(f"GO enrichment failed: {res.stderr.strip()}")

            go_csv = Path(out_dir) / "go_enrichment.csv"
            go_df = pd.read_csv(go_csv) if go_csv.exists() else pd.DataFrame()
            sig_genes = []
            if local_de and Path(local_de).exists():
                de_df = pd.read_csv(local_de)
                if {"gene", "padj", "log2FoldChange"}.issubset(de_df.columns):
                    sig = de_df[(de_df["padj"] < 0.05) & (de_df["log2FoldChange"].abs() > 1)]
                    sig_genes = sig["gene"].dropna().astype(str).tolist()

            go_terms = go_df.head(10).to_dict(orient="records") if not go_df.empty else []
            ai_summary = self._llm_summary(
                routing_ctx.get("treatment", "treated"),
                routing_ctx.get("control", "control"),
                go_terms,
                sig_genes,
            )

            summary_md = Path(out_dir) / "ai_summary.md"
            summary_md.write_text(ai_summary, encoding="utf-8")

            insight_json = Path(out_dir) / "insight.json"
            insight_json.write_text(
                json.dumps(
                    {
                        "treatment": routing_ctx.get("treatment", "treated"),
                        "control": routing_ctx.get("control", "control"),
                        "top_go_terms": go_terms,
                        "sig_genes": sig_genes,
                        "ai_summary": ai_summary,
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )

            uploaded = {}
            for path in Path(out_dir).glob("**/*"):
                if path.is_file():
                    key = f"{run_id}/insight/{path.name}"
                    uploaded[path.name] = storage.upload_file(str(path), key)

        return {
            "agent": "insight",
            "status": "ok",
            "payload": {
                "artifacts": uploaded,
                "ai_summary": ai_summary,
            },
            "reasoning": "GO enrichment and biological interpretation completed",
        }


if __name__ == "__main__":
    InsightAgent().run()
