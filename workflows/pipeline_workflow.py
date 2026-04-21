from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict

from temporalio import workflow

from workflows.activities import (
    ai_decider_activity,
    annotation_activity,
    align_activity,
    bwa_activity,
    count_activity,
    gatk_activity,
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
        is_dna = input_data.experiment_type in {"WGS", "WES"}

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

        if is_dna:
            bwa = await workflow.execute_activity(
                bwa_activity,
                args=(align_input, input_data.routing_context),
                start_to_close_timeout=timedelta(hours=4),
            )

            gatk = await workflow.execute_activity(
                gatk_activity,
                args=(bwa, input_data.routing_context),
                start_to_close_timeout=timedelta(hours=6),
            )

            annotation = await workflow.execute_activity(
                annotation_activity,
                args=(
                    {
                        **gatk,
                        "panel_bed": input_data.routing_context.get("panel_bed"),
                    },
                    input_data.routing_context,
                ),
                start_to_close_timeout=timedelta(minutes=45),
            )

            report = await workflow.execute_activity(
                report_builder_activity,
                args=(
                    {
                        "payload": {
                            "qc": qc,
                            "align": bwa,
                            "count": {},
                            "de": {},
                            "insight": {},
                            "annotation": annotation,
                            "variants_csv": annotation.get("payload", {}).get("variants_csv"),
                            "coverage_depth_png": annotation.get("payload", {}).get("coverage_depth_png"),
                            "coverage_depth_csv": annotation.get("payload", {}).get("coverage_depth_csv"),
                        },
                        "artifacts_dir": input_data.routing_context.get("artifacts_dir"),
                    },
                    input_data.routing_context,
                ),
                start_to_close_timeout=timedelta(minutes=10),
            )

            outputs = {
                "qc_report_html": qc.get("payload", {}).get("report_html"),
                "bam_path": bwa.get("payload", {}).get("artifacts", {}).get("bam_path"),
                "bam_index": bwa.get("payload", {}).get("artifacts", {}).get("bam_index"),
                "flagstat": bwa.get("payload", {}).get("artifacts", {}).get("flagstat"),
                "coverage_depth_csv": bwa.get("payload", {}).get("artifacts", {}).get("coverage_depth_csv"),
                "coverage_depth_png": bwa.get("payload", {}).get("artifacts", {}).get("coverage_depth_png"),
                "final_bam": gatk.get("payload", {}).get("final_bam"),
                "variants_vcf": gatk.get("payload", {}).get("variants_vcf"),
                "annotated_vcf": annotation.get("payload", {}).get("annotated_vcf"),
                "variants_csv": annotation.get("payload", {}).get("variants_csv"),
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
                    "bwa_agent",
                    "gatk_agent",
                    "annotation_agent",
                    "report_builder",
                ],
                "outputs": outputs,
                "ai_decision": ai_decision.get("payload", {}),
            }

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
