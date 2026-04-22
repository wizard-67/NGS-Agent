import json
import os
import re
from typing import Any, Dict

from anthropic import Anthropic

from base_agent import BaseAgent


DEFAULT_TRIM_PARAMS = {
    "LEADING": 3,
    "TRAILING": 3,
    "SLIDINGWINDOW": "4:20",
    "MINLEN": 36,
}


class AIDeciderAgent(BaseAgent):
    def _normalize_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        merged = {**DEFAULT_TRIM_PARAMS, **(params or {})}

        try:
            merged["LEADING"] = max(0, min(40, int(merged["LEADING"])))
            merged["TRAILING"] = max(0, min(40, int(merged["TRAILING"])))
            merged["MINLEN"] = max(36, min(200, int(merged["MINLEN"])))
        except Exception:
            return dict(DEFAULT_TRIM_PARAMS)

        sw = str(merged["SLIDINGWINDOW"])
        if not re.match(r"^\d+:\d+$", sw):
            sw = DEFAULT_TRIM_PARAMS["SLIDINGWINDOW"]
        merged["SLIDINGWINDOW"] = sw
        return merged

    def _extract_json_object(self, text: str) -> Dict[str, Any] | None:
        if not text:
            return None
        candidates = [text.strip()]
        match = re.search(r"\{[\s\S]*\}", text)
        if match:
            candidates.append(match.group(0))

        for candidate in candidates:
            try:
                parsed = json.loads(candidate)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                continue
        return None

    def _heuristic_decision(self, fastqc_data: str) -> Dict[str, Any]:
        if not fastqc_data:
            return {
                "trim": False,
                "params": dict(DEFAULT_TRIM_PARAMS),
                "model_reasoning": "No FastQC metrics provided; defaulting to no trim.",
                "confidence": 0.45,
                "source": "heuristic",
            }

        fails = len(re.findall(r"^FAIL\t", fastqc_data, flags=re.MULTILINE))
        warns = len(re.findall(r"^WARN\t", fastqc_data, flags=re.MULTILINE))

        trim = fails > 0 or warns >= 3
        params = dict(DEFAULT_TRIM_PARAMS)
        if fails >= 2:
            params.update({"LEADING": 5, "TRAILING": 5, "SLIDINGWINDOW": "4:22", "MINLEN": 40})
        elif warns >= 4:
            params.update({"LEADING": 4, "TRAILING": 4, "SLIDINGWINDOW": "4:21", "MINLEN": 38})

        confidence = 0.55 + min(0.4, 0.1 * fails + 0.03 * warns)
        confidence = round(min(0.95, confidence), 2)
        return {
            "trim": trim,
            "params": params,
            "model_reasoning": f"Heuristic decision from FastQC flags: FAIL={fails}, WARN={warns}.",
            "confidence": confidence,
            "source": "heuristic",
        }

    def _validate_model_decision(self, parsed: Dict[str, Any]) -> Dict[str, Any] | None:
        if "trim" not in parsed:
            return None
        normalized = {
            "trim": bool(parsed.get("trim", False)),
            "params": self._normalize_params(parsed.get("params", {})),
            "model_reasoning": str(parsed.get("model_reasoning", "Model decision produced."))[:1200],
            "confidence": float(parsed.get("confidence", 0.7)),
            "source": "llm",
        }
        normalized["confidence"] = max(0.0, min(1.0, normalized["confidence"]))
        return normalized

    def _ask_model(self, fastqc_data: str, is_paired: bool) -> Dict[str, Any]:
        heuristic = self._heuristic_decision(fastqc_data)

        api_key = os.environ.get("ANTHROPIC_API_KEY")
        model = os.environ.get("ANTHROPIC_MODEL", "claude-3-5-sonnet-20241022")
        if not api_key:
            return {
                **heuristic,
                "model_reasoning": "Anthropic API key not configured; using deterministic QC heuristic.",
            }

        prompt = (
            "You are an RNA-Seq quality control decision assistant. "
            "Given FastQC metrics, decide whether trimming is needed before alignment. "
            "Output STRICT JSON only using this schema:\n"
            '{"trim": bool, "params": {"LEADING": int, "TRAILING": int, '
            '"SLIDINGWINDOW": "w:q", "MINLEN": int}, "model_reasoning": str, "confidence": float}\n'
            "Do not include markdown. Keep reasoning concise and practical.\n"
            f"paired_end={is_paired}\n"
            f"Heuristic baseline suggestion={json.dumps(heuristic)}\n"
            "FastQC data follows:\n"
            f"{fastqc_data[:12000]}"
        )

        try:
            client = Anthropic(api_key=api_key)
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

            parsed = self._extract_json_object(text)
            if not parsed:
                return {
                    **heuristic,
                    "model_reasoning": "Model returned non-JSON output; using deterministic heuristic.",
                }

            validated = self._validate_model_decision(parsed)
            if not validated:
                return {
                    **heuristic,
                    "model_reasoning": "Model JSON missing required fields; using deterministic heuristic.",
                }
            return validated
        except Exception as exc:
            return {
                **heuristic,
                "model_reasoning": f"Model call failed ({exc}); using deterministic heuristic.",
            }

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
                "trim_params": self._normalize_params(decision.get("params", {})),
                "decision_source": decision.get("source", "heuristic"),
                "decision_confidence": float(decision.get("confidence", 0.5)),
            },
            "reasoning": decision.get("model_reasoning", "AI decision produced."),
        }


if __name__ == "__main__":
    AIDeciderAgent().run()
