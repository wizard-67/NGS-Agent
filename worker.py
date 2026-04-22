#!/usr/bin/env python3
import asyncio

from dotenv import load_dotenv
from temporalio.client import Client
from temporalio.worker import Worker

from workflows import activities
from workflows.pipeline_workflow import NGSPipelineWorkflow, NGSSampleWorkflow

load_dotenv()


async def main() -> None:
    client = await Client.connect("localhost:7233")
    worker = Worker(
        client,
        task_queue="ngs-pipeline",
        workflows=[NGSPipelineWorkflow, NGSSampleWorkflow],
        activities=[
            activities.ingest_activity,
            activities.qc_activity,
            activities.ai_decider_activity,
            activities.trim_activity,
            activities.align_activity,
            activities.bwa_activity,
            activities.gatk_activity,
            activities.annotation_activity,
            activities.count_activity,
            activities.de_activity,
            activities.insight_activity,
            activities.report_builder_activity,
        ],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
