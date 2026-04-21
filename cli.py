#!/usr/bin/env python3
import asyncio
import csv
import os
import uuid
from pathlib import Path

import click
from dotenv import load_dotenv
from temporalio.client import Client

from workflows.pipeline_workflow import NGSPipelineWorkflow, RunInput

load_dotenv()


@click.group()
def cli() -> None:
    """NGS Agent Swarm CLI."""


@cli.command()
@click.option("--fastq", required=False, help="Path to single-end FASTQ")
@click.option("--fastq-r1", required=False, help="Path to paired-end R1 FASTQ")
@click.option("--fastq-r2", required=False, help="Path to paired-end R2 FASTQ")
@click.option("--experiment", default="RNA-Seq", type=click.Choice(["RNA-Seq", "WGS", "WES"]))
@click.option(
    "--organism",
    required=True,
    type=click.Choice(["human", "mouse", "rat", "zebrafish", "yeast", "other"]),
    help="Target organism",
)
@click.option("--ref-genome", required=True, help="HISAT2 index basename path")
@click.option("--reference-fasta", required=False, help="Reference FASTA path for DNA branch tools")
@click.option("--gtf", required=False, help="Annotation GTF path (required for RNA-Seq counting)")
@click.option("--panel-bed", required=False, help="Optional panel BED for DNA coverage plots")
@click.option("--known-sites", required=False, multiple=True, help="Known sites VCFs for GATK BQSR (repeatable)")
@click.option("--paired/--single", default=False, help="Use paired-end mode")
def submit(
    fastq: str | None,
    fastq_r1: str | None,
    fastq_r2: str | None,
    experiment: str,
    organism: str,
    ref_genome: str,
    reference_fasta: str | None,
    gtf: str | None,
    panel_bed: str | None,
    known_sites: tuple[str, ...],
    paired: bool,
) -> None:
    """Submit a new pipeline run."""

    def ensure_file(path_value: str, label: str) -> None:
        if not Path(path_value).exists() or not Path(path_value).is_file():
            raise click.BadParameter(f"{label} does not exist or is not a file: {path_value}")

    if paired:
        if not fastq_r1 or not fastq_r2:
            raise click.BadParameter("--paired requires both --fastq-r1 and --fastq-r2")
        ensure_file(fastq_r1, "--fastq-r1")
        ensure_file(fastq_r2, "--fastq-r2")
    else:
        if not fastq:
            raise click.BadParameter("--single requires --fastq")
        ensure_file(fastq, "--fastq")

    if not Path(ref_genome).exists():
        raise click.BadParameter(f"--ref-genome path does not exist: {ref_genome}")
    if experiment in {"WGS", "WES"} and not reference_fasta:
        raise click.BadParameter("DNA-Seq analysis requires --reference-fasta")
    if reference_fasta:
        ensure_file(reference_fasta, "--reference-fasta")

    if experiment == "RNA-Seq" and not gtf:
        raise click.BadParameter("RNA-Seq requires --gtf")
    if gtf:
        ensure_file(gtf, "--gtf")
    if panel_bed:
        ensure_file(panel_bed, "--panel-bed")
    for known_site in known_sites:
        ensure_file(known_site, "--known-sites")

    run_id = f"run-{uuid.uuid4().hex[:8]}"
    routing_ctx = {
        "experiment_type": experiment,
        "organism": organism,
        "paired_end": paired,
        "reference_genome": ref_genome,
        "reference_fasta": reference_fasta,
        "gtf": gtf,
        "panel_bed": panel_bed,
        "known_sites": list(known_sites),
        "run_id": run_id,
    }
    inputs = {"ref_genome": ref_genome, "gtf": gtf, "reference_fasta": reference_fasta}
    if panel_bed:
        inputs["panel_bed"] = panel_bed
    if known_sites:
        inputs["known_sites"] = list(known_sites)
    if paired:
        inputs["fastq_r1"] = fastq_r1
        inputs["fastq_r2"] = fastq_r2
    else:
        inputs["fastq_path"] = fastq

    async def run_submit() -> None:
        temporal_host = os.environ.get("TEMPORAL_HOST", "localhost:7233")
        client = await Client.connect(temporal_host)
        handle = await client.start_workflow(
            NGSPipelineWorkflow.run,
            RunInput(run_id, experiment, routing_ctx, inputs),
            id=f"ngs-{run_id}",
            task_queue="ngs-pipeline",
        )
        click.echo(f"Run submitted: {run_id}")
        click.echo(
            f"Monitor at http://localhost:8080/namespaces/default/workflows/{handle.id}"
        )

    asyncio.run(run_submit())


@cli.command()
@click.argument("run_id")
def status(run_id: str) -> None:
    """Get status of a run."""

    async def run_status() -> None:
        client = await Client.connect("localhost:7233")
        handle = client.get_workflow_handle(f"ngs-{run_id}")
        desc = await handle.describe()
        click.echo(f"Status: {desc.status.name}")
        if desc.status.name == "COMPLETED":
            result = await handle.result()
            click.echo(f"Result: {result}")

    asyncio.run(run_status())


@cli.command()
@click.option("--output-env", default=".env", show_default=True)
@click.option("--output-inputs", default="input.csv", show_default=True)
def wizard(output_env: str, output_inputs: str) -> None:
    """Interactive setup wizard for non-technical users."""
    experiment_type = click.prompt("Analysis type", type=click.Choice(["RNA", "DNA"]))
    paired = click.confirm("Is the dataset paired-end?", default=True)
    organism = click.prompt("Genome preset", type=click.Choice(["hg38", "mm10", "custom"]))

    if paired:
        fastq_r1 = click.prompt("Path to R1 FASTQ", type=str)
        fastq_r2 = click.prompt("Path to R2 FASTQ", type=str)
        fastq = ""
    else:
        fastq = click.prompt("Path to FASTQ", type=str)
        fastq_r1 = fastq_r2 = ""

    ref_genome = click.prompt("Reference genome index basename", type=str)
    gtf = click.prompt("Annotation GTF path", type=str, default="", show_default=False)
    sample_sheet = click.prompt("Sample sheet path", type=str, default="sample_sheet.csv", show_default=True)

    env_lines = [
        f"EXPERIMENT_TYPE={experiment_type}",
        f"ORGANISM={organism}",
        f"PAIRED_END={str(paired).lower()}",
        f"REF_GENOME={ref_genome}",
        f"GTF={gtf}",
        f"SAMPLE_SHEET={sample_sheet}",
    ]
    Path(output_env).write_text("\n".join(env_lines) + "\n", encoding="utf-8")

    with open(output_inputs, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["fastq", "fastq_r1", "fastq_r2", "ref_genome", "gtf", "paired"])
        writer.writerow([fastq, fastq_r1, fastq_r2, ref_genome, gtf, paired])

    click.echo(f"Wrote {output_env} and {output_inputs}")
    click.echo("Next: run `python cli.py submit` with the generated inputs or wire this into your workflow runner.")


if __name__ == "__main__":
    cli()
