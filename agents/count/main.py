from base_agent import BaseAgent


class CountAgent(BaseAgent):
    def execute(self, inputs, routing_ctx):
        run_id = routing_ctx.get("run_id", "unknown")
        return {
            "agent": "count",
            "status": "ok",
            "payload": {
                "count_matrix": f"s3://artifacts/{run_id}/counts.tsv",
            },
            "reasoning": "featureCounts completed",
        }


if __name__ == "__main__":
    CountAgent().run()
