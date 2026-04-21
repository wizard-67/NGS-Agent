from base_agent import BaseAgent


class TrimAgent(BaseAgent):
    def execute(self, inputs, routing_ctx):
        trim_to = inputs.get("payload", {}).get("trim_to_bp", 100)
        run_id = routing_ctx.get("run_id", "unknown")

        return {
            "agent": "trim",
            "status": "ok",
            "payload": {
                "trimmed_reads": f"s3://artifacts/{run_id}/trimmed.fastq.gz",
                "trim_length": trim_to,
            },
            "reasoning": f"Trimmed reads to {trim_to}bp",
        }


if __name__ == "__main__":
    TrimAgent().run()
