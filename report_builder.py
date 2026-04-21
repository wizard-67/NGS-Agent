import argparse
import json
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape


def build_report(artifacts_dir: Path, output_html: Path) -> None:
    template_dir = Path(__file__).parent / "tools" / "templates"
    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    template = env.get_template("report.html.j2")

    def load_text(name: str) -> str:
        p = artifacts_dir / name
        return p.read_text(encoding="utf-8") if p.exists() else ""

    context = {
        "qc": json.loads(load_text("qc.json") or "{}"),
        "align": json.loads(load_text("align.json") or "{}"),
        "count": json.loads(load_text("count.json") or "{}"),
        "de": json.loads(load_text("de_summary.json") or "{}"),
        "insight": json.loads(load_text("insight.json") or "{}"),
        "ai_summary": load_text("ai_summary.md"),
        "methods": load_text("methods.md"),
        "variants_csv": load_text("variants.csv"),
    }

    output_html.write_text(template.render(**context), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifacts-dir", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    build_report(Path(args.artifacts_dir), Path(args.output))


if __name__ == "__main__":
    main()
