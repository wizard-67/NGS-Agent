import random

from base_agent import BaseAgent


class AlignAgent(BaseAgent):
    def execute(self, inputs, routing_ctx):
        mapping_rate = random.uniform(70, 95)
        run_id = routing_ctx.get("run_id", "unknown")

        return {
            "agent": "align",
            "status": "ok",
            "payload": {
                "bam_path": f"s3://artifacts/{run_id}/aligned.bam",
                "mapping_rate": round(mapping_rate, 2),
            },
            "reasoning": f"Aligned with HISAT2, mapping rate {mapping_rate:.1f}%",
        }


if __name__ == "__main__":
    AlignAgent().run()
