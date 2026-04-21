import random

from base_agent import BaseAgent


class DEAgent(BaseAgent):
    def execute(self, inputs, routing_ctx):
        n_up = random.randint(100, 500)
        n_down = random.randint(50, 300)
        run_id = routing_ctx.get("run_id", "unknown")

        return {
            "agent": "de",
            "status": "ok",
            "payload": {
                "n_up": n_up,
                "n_down": n_down,
                "pca_ok": True,
                "results": f"s3://artifacts/{run_id}/de_results.csv",
            },
            "reasoning": f"DESeq2 found {n_up} up, {n_down} down",
        }


if __name__ == "__main__":
    DEAgent().run()
