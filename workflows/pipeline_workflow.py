from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict

from temporalio import workflow

from workflows.activities import (
    ai_decider_activity,
    align_activity,
    count_activity,
    de_activity,
    ingest_activity,
    insight_activity,
    qc_activity,
    report_builder_activity,
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

        ai_decision = await workflow.execute_activity(
            ai_decider_activity,
            args=(qc, input_data.routing_context),
            start_to_close_timeout=timedelta(minutes=5),
        )

        trim_was_run = False
        if ai_decision.get("payload", {}).get("trim", False):
            trim_request = {
                "payload": {
                    **qc.get("payload", {}),
                    "trim_params": ai_decision.get("payload", {}).get("trim_params", {}),
                }
            }
            align_input = await workflow.execute_activity(
                trim_activity,
                args=(trim_request, input_data.routing_context),
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
            args=(
                {
                    **count,
                    "sample_sheet": input_data.routing_context.get("sample_sheet"),
                },
                input_data.routing_context,
            ),
            start_to_close_timeout=timedelta(minutes=25),
        )

        insight = await workflow.execute_activity(
            insight_activity,
            args=(
                {
                    **de,
                    "go_input": input_data.routing_context.get("go_input"),
                },
                input_data.routing_context,
            ),
            start_to_close_timeout=timedelta(minutes=15),
        )

        report = await workflow.execute_activity(
            report_builder_activity,
            args=(
                {
                    "payload": {
                        "qc": qc,
                        "align": align,
                        "count": count,
                        "de": de,
                        "insight": insight,
                    },
                    "artifacts_dir": input_data.routing_context.get("artifacts_dir"),
                },
                input_data.routing_context,
            ),
            start_to_close_timeout=timedelta(minutes=10),
        )

        outputs = {
            "qc_report_html": qc.get("payload", {}).get("report_html"),
            "mapping_rate": align.get("payload", {}).get("mapping_rate"),
            "bam_path": align.get("payload", {}).get("bam_path"),
            "bam_index": align.get("payload", {}).get("bam_index"),
            "count_matrix": count.get("payload", {}).get("count_matrix"),
            "count_summary": count.get("payload", {}).get("count_summary"),
            "de_artifacts": de.get("payload", {}).get("artifacts", {}),
            "insight_summary": insight.get("payload", {}).get("ai_summary"),
            "report_html": report.get("payload", {}).get("report_html"),
        }

        return {
            "run_id": input_data.run_id,
            "status": "complete",
            "trim_was_run": trim_was_run,
            "agents": [
                "ingest",
                "qc",
                "ai_decider",
                "trim",
                "align",
                "count",
                "de_agent",
                "insight_agent",
                "report_builder",
            ],
            "outputs": outputs,
            "ai_decision": ai_decision.get("payload", {}),
        }
