import json
import os

from anthropic import Anthropic

from base_agent import BaseAgent


class AIDeciderAgent(BaseAgent):
    def _fallback(self):
        return {
            "trim": False,
            "params": {
                "LEADING": 3,
                "TRAILING": 3,
                "SLIDINGWINDOW": "4:20",
                "MINLEN": 36,
            },
            "model_reasoning": "Fallback decision: missing AI response.",
        }

    def _ask_model(self, fastqc_data: str, is_paired: bool):
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        model = os.environ.get("ANTHROPIC_MODEL", "claude-3-5-sonnet-20241022")
        if not api_key:
            return self._fallback()

        client = Anthropic(api_key=api_key)
        prompt = (
            "You are an RNA-Seq QC decision assistant. Given FastQC metrics, decide whether "
            "to trim reads and provide Trimmomatic parameters. Return strict JSON only with keys "
            'trim (boolean), params (object with LEADING, TRAILING, SLIDINGWINDOW, MINLEN), '
            "and model_reasoning (short string).\n"
            f"paired_end={is_paired}\n"
            "FastQC data follows:\n"
            f"{fastqc_data[:12000]}"
        )

        msg = client.messages.create(
            model=model,
            max_tokens=500,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        text = ""
        for block in msg.content:
            if getattr(block, "type", "") == "text":
                text += block.text

        try:
            parsed = json.loads(text)
            if "trim" not in parsed or "params" not in parsed:
                return self._fallback()
            return parsed
        except Exception:
            return self._fallback()

    def execute(self, inputs, routing_ctx):
        qc_payload = inputs.get("payload", {})
        fastqc_data = qc_payload.get("fastqc_data", "")
        is_paired = bool(routing_ctx.get("paired_end", False))

        decision = self._ask_model(fastqc_data, is_paired)
        return {
            "agent": "ai_decider",
            "status": "ok",
            "payload": {
                "trim": bool(decision.get("trim", False)),
                "trim_params": decision.get("params", {}),
            },
            "reasoning": decision.get("model_reasoning", "AI decision produced."),
        }


if __name__ == "__main__":
    AIDeciderAgent().run()
