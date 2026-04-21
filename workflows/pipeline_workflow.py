from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict

from temporalio import workflow

from workflows.activities import (
    align_activity,
    count_activity,
    de_activity,
    ingest_activity,
    qc_activity,
    trim_activity,
)


@dataclass
class RunInput:
    run_id: str
    experiment_type: str
    routing_context: Dict[str, Any]
    initial_inputs: Dict[str, Any]


@workflow.defn
class NGSPipelineWorkflow:
    @workflow.run
    async def run(self, input_data: RunInput) -> Dict[str, Any]:
        ingest = await workflow.execute_activity(
            ingest_activity,
            args=(input_data.initial_inputs, input_data.routing_context),
            start_to_close_timeout=timedelta(minutes=5),
        )

        qc = await workflow.execute_activity(
            qc_activity,
            args=(ingest, input_data.routing_context),
            start_to_close_timeout=timedelta(minutes=15),
        )

        trim_was_run = False
        if qc.get("payload", {}).get("verdict") == "trim_required":
            align_input = await workflow.execute_activity(
                trim_activity,
                args=(qc, input_data.routing_context),
                start_to_close_timeout=timedelta(minutes=30),
            )
            trim_was_run = True
        else:
            align_input = ingest

        align = await workflow.execute_activity(
            align_activity,
            args=(align_input, input_data.routing_context),
            start_to_close_timeout=timedelta(hours=2),
        )

        count = await workflow.execute_activity(
            count_activity,
            args=(align, input_data.routing_context),
            start_to_close_timeout=timedelta(minutes=30),
        )

        de = await workflow.execute_activity(
            de_activity,
            args=(count, input_data.routing_context),
            start_to_close_timeout=timedelta(minutes=10),
        )

        outputs = {
            "mapping_rate": align.get("payload", {}).get("mapping_rate"),
            "n_genes": de.get("payload", {}).get("n_up", 0)
            + de.get("payload", {}).get("n_down", 0),
        }

        return {
            "run_id": input_data.run_id,
            "status": "complete",
            "trim_was_run": trim_was_run,
            "agents": ["ingest", "qc", "trim", "align", "count", "de"],
            "outputs": outputs,
        }
