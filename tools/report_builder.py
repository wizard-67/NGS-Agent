import argparse
import json
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape


def load_text(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def build_report(artifacts_dir: Path, output_html: Path) -> None:
    template_dir = Path(__file__).parent / "templates"
    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    template = env.get_template("report.html.j2")

    ai_summary = load_text(artifacts_dir / "ai_summary.md")
    de_summary = json.loads(load_text(artifacts_dir / "de_summary.json") or "{}")
    variants = load_text(artifacts_dir / "variants.csv")

    html = template.render(
        ai_summary=ai_summary,
        de_summary=de_summary,
        variants_csv=variants,
        methods_json=load_text(artifacts_dir / "methods.json"),
    )
    output_html.write_text(html, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifacts-dir", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    build_report(Path(args.artifacts_dir), Path(args.output))


if __name__ == "__main__":
    main()
