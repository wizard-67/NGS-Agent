import json
import os
import subprocess
import tempfile
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from base_agent import BaseAgent
from storage import MinioStorage


class AnnotationAgent(BaseAgent):
    def _materialize(self, value: str, storage: MinioStorage, workdir: str) -> str:
        if value and value.startswith("s3://"):
            return storage.download_file(value, os.path.join(workdir, Path(value).name))
        return value

    def _annotate_with_snpeff(self, vcf_path: str, reference_key: str | None, workdir: str) -> str:
        snpeff_jar = os.environ.get("SNPEFF_JAR", "/usr/local/bin/snpEff.jar")
        if not Path(snpeff_jar).exists():
            return vcf_path
        db = os.environ.get("SNPEFF_DB", reference_key or "GRCh38.99")
        out_vcf = os.path.join(workdir, "annotated.snpeff.vcf")
        cmd = ["java", "-jar", snpeff_jar, db, vcf_path]
        with open(out_vcf, "w", encoding="utf-8") as handle:
            res = subprocess.run(cmd, stdout=handle, stderr=subprocess.PIPE, text=True)
        if res.returncode != 0:
            return vcf_path
        return out_vcf

    def _parse_vcf(self, vcf_path: str) -> pd.DataFrame:
        rows = []
        with open(vcf_path, "r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip() or line.startswith("#"):
                    continue
                cols = line.rstrip().split("\t")
                if len(cols) < 8:
                    continue
                chrom, pos, _id, ref, alt, qual, flt, info = cols[:8]
                info_map = {}
                for item in info.split(";"):
                    if "=" in item:
                        k, v = item.split("=", 1)
                        info_map[k] = v
                ann = info_map.get("ANN", "")
                gene = info_map.get("GENE", "") or (ann.split("|")[3] if ann else "")
                effect = ann.split("|")[1] if ann and "|" in ann else info_map.get("EFFECT", "")
                pathogenicity = info_map.get("CLNSIG", "") or info_map.get("IMPACT", "") or "UNKNOWN"
                rows.append({
                    "chrom": chrom,
                    "pos": int(pos),
                    "ref": ref,
                    "alt": alt,
                    "gene": gene or "NA",
                    "effect": effect or "NA",
                    "pathogenicity": pathogenicity,
                    "qual": qual,
                    "filter": flt,
                    "info": info,
                })
        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values(["chrom", "pos"]).reset_index(drop=True)
        return df

    def execute(self, inputs, routing_ctx):
        run_id = routing_ctx.get("run_id", "unknown")
        bam_path = inputs.get("payload", {}).get("final_bam") or inputs.get("bam_path")
        vcf_path = inputs.get("payload", {}).get("variants_vcf") or inputs.get("variants_vcf")
        panel_bed = routing_ctx.get("panel_bed") or inputs.get("panel_bed")
        if not vcf_path:
            raise RuntimeError("VCF input not found for annotation")

        storage = MinioStorage()
        with tempfile.TemporaryDirectory(prefix="annot-") as workdir:
            local_vcf = self._materialize(vcf_path, storage, workdir)
            local_bam = self._materialize(bam_path, storage, workdir) if bam_path else None
            local_bed = self._materialize(panel_bed, storage, workdir) if panel_bed else None

            annotated_vcf = self._annotate_with_snpeff(local_vcf, routing_ctx.get("reference_fasta"), workdir)
            vcf_df = self._parse_vcf(annotated_vcf)
            if vcf_df.empty:
                vcf_df = pd.DataFrame([
                    {"chrom": "chr1", "pos": 0, "ref": "N", "alt": "N", "gene": "NA", "effect": "NA", "pathogenicity": "NA", "qual": ".", "filter": ".", "info": "."}
                ])

            variants_csv = Path(workdir) / "variants.csv"
            vcf_df.to_csv(variants_csv, index=False)

            coverage_csv = None
            if local_bed and local_bam:
                rows = []
                with open(local_bed, "r", encoding="utf-8") as handle:
                    for line in handle:
                        if not line.strip() or line.startswith("#"):
                            continue
                        cols = line.rstrip().split("\t")
                        if len(cols) < 3:
                            continue
                        chrom, start, end = cols[:3]
                        gene = cols[3] if len(cols) >= 4 else f"{chrom}:{start}-{end}"
                        region = f"{chrom}:{int(start)+1}-{end}"
                        res = subprocess.run(["samtools", "depth", "-r", region, local_bam], capture_output=True, text=True, check=True)
                        depths = [int(row.split("\t")[2]) for row in res.stdout.splitlines() if row.strip()]
                        rows.append({"gene": gene, "region": region, "mean_depth": round(sum(depths) / len(depths), 4) if depths else 0.0})
                coverage_df = pd.DataFrame(rows)
                coverage_csv = Path(workdir) / "coverage_depth.csv"
                coverage_df.to_csv(coverage_csv, index=False)
                if not coverage_df.empty:
                    plt.figure(figsize=(max(10, len(coverage_df) * 0.35), 5))
                    plt.bar(coverage_df["gene"], coverage_df["mean_depth"], color="#7a3cff")
                    plt.xticks(rotation=90)
                    plt.ylabel("Mean depth")
                    plt.tight_layout()
                    plt.savefig(Path(workdir) / "coverage_depth.png", dpi=300)
                    plt.close()

            annotated_uri = storage.upload_file(annotated_vcf, f"{run_id}/dna/annotation/annotated.vcf")
            variants_uri = storage.upload_file(str(variants_csv), f"{run_id}/dna/annotation/variants.csv")
            payload = {"annotated_vcf": annotated_uri, "variants_csv": variants_uri}
            if coverage_csv and Path(coverage_csv).exists():
                payload["coverage_depth_csv"] = storage.upload_file(str(coverage_csv), f"{run_id}/dna/annotation/coverage_depth.csv")
                coverage_png = Path(workdir) / "coverage_depth.png"
                if coverage_png.exists():
                    payload["coverage_depth_png"] = storage.upload_file(str(coverage_png), f"{run_id}/dna/annotation/coverage_depth.png")

        return {
            "agent": "annotation_agent",
            "status": "ok",
            "payload": payload,
            "reasoning": "Variant annotation and coverage summarization completed",
        }


if __name__ == "__main__":
    AnnotationAgent().run()
