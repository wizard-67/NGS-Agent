#!/usr/bin/env python3
import asyncio
import uuid

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
@click.option("--experiment", default="RNA-Seq", type=click.Choice(["RNA-Seq", "WGS"]))
@click.option("--organism", required=True, help="Organism label (e.g. human, mouse)")
@click.option("--ref-genome", required=True, help="Reference genome index or fasta path")
@click.option("--gtf", required=False, help="Annotation GTF path (required for RNA-Seq counting)")
@click.option("--paired/--single", default=False, help="Use paired-end mode")
def submit(
    fastq: str | None,
    fastq_r1: str | None,
    fastq_r2: str | None,
    experiment: str,
    organism: str,
    ref_genome: str,
    gtf: str | None,
    paired: bool,
) -> None:
    """Submit a new pipeline run."""
    if paired:
        if not fastq_r1 or not fastq_r2:
            raise click.BadParameter("--paired requires both --fastq-r1 and --fastq-r2")
    else:
        if not fastq:
            raise click.BadParameter("--single requires --fastq")

    if experiment == "RNA-Seq" and not gtf:
        raise click.BadParameter("RNA-Seq requires --gtf")

    run_id = f"run-{uuid.uuid4().hex[:8]}"
    routing_ctx = {
        "experiment_type": experiment,
        "organism": organism,
        "paired_end": paired,
        "reference_genome": ref_genome,
        "gtf": gtf,
        "run_id": run_id,
    }
    inputs = {"ref_genome": ref_genome, "gtf": gtf}
    if paired:
        inputs["fastq_r1"] = fastq_r1
        inputs["fastq_r2"] = fastq_r2
    else:
        inputs["fastq_path"] = fastq

    async def run_submit() -> None:
        client = await Client.connect("localhost:7233")
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


if __name__ == "__main__":
    cli()
