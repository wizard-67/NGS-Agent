import json
import os
import subprocess
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv
from temporalio import activity

from shared.cache import CacheManager

load_dotenv()
cache = CacheManager()


def _replace_local_file_paths(
    obj: Any, mounts: list[tuple[str, str]], mount_index: list[int]
) -> Any:
    if isinstance(obj, dict):
        return {k: _replace_local_file_paths(v, mounts, mount_index) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_replace_local_file_paths(v, mounts, mount_index) for v in obj]
    if isinstance(obj, str):
        p = Path(obj)
        if p.exists() and p.is_file():
            idx = mount_index[0]
            mount_index[0] += 1
            container_path = f"/mnt/inputs/{idx}_{p.name}"
            mounts.append((str(p.resolve()), container_path))
            return container_path
    return obj


@activity.defn
async def ingest_activity(inputs: Dict[str, Any], routing_ctx: Dict[str, Any]) -> Dict[str, Any]:
    return await run_agent_container("ingest", inputs, routing_ctx)


@activity.defn
async def qc_activity(inputs: Dict[str, Any], routing_ctx: Dict[str, Any]) -> Dict[str, Any]:
    return await run_agent_container("qc", inputs, routing_ctx)


@activity.defn
async def ai_decider_activity(inputs: Dict[str, Any], routing_ctx: Dict[str, Any]) -> Dict[str, Any]:
    return await run_agent_container("ai_decider", inputs, routing_ctx)


@activity.defn
async def trim_activity(inputs: Dict[str, Any], routing_ctx: Dict[str, Any]) -> Dict[str, Any]:
    return await run_agent_container("trim", inputs, routing_ctx)


@activity.defn
async def align_activity(inputs: Dict[str, Any], routing_ctx: Dict[str, Any]) -> Dict[str, Any]:
    return await run_agent_container("align", inputs, routing_ctx)


@activity.defn
async def bwa_activity(inputs: Dict[str, Any], routing_ctx: Dict[str, Any]) -> Dict[str, Any]:
    return await run_agent_container("bwa_agent", inputs, routing_ctx)


@activity.defn
async def gatk_activity(inputs: Dict[str, Any], routing_ctx: Dict[str, Any]) -> Dict[str, Any]:
    return await run_agent_container("gatk_agent", inputs, routing_ctx)


@activity.defn
async def annotation_activity(inputs: Dict[str, Any], routing_ctx: Dict[str, Any]) -> Dict[str, Any]:
    return await run_agent_container("annotation_agent", inputs, routing_ctx)


@activity.defn
async def count_activity(inputs: Dict[str, Any], routing_ctx: Dict[str, Any]) -> Dict[str, Any]:
    return await run_agent_container("count", inputs, routing_ctx)


@activity.defn
async def de_activity(inputs: Dict[str, Any], routing_ctx: Dict[str, Any]) -> Dict[str, Any]:
    return await run_agent_container("de_agent", inputs, routing_ctx)


@activity.defn
async def insight_activity(inputs: Dict[str, Any], routing_ctx: Dict[str, Any]) -> Dict[str, Any]:
    return await run_agent_container("insight_agent", inputs, routing_ctx)


@activity.defn
async def report_builder_activity(inputs: Dict[str, Any], routing_ctx: Dict[str, Any]) -> Dict[str, Any]:
    return await run_agent_container("report_builder", inputs, routing_ctx)


async def run_agent_container(
    agent_name: str, inputs: Dict[str, Any], routing_ctx: Dict[str, Any]
) -> Dict[str, Any]:
    cache_key = cache.compute_hash(agent_name, {"inputs": inputs, "routing_ctx": routing_ctx})
    cached = await cache.get(cache_key)
    if cached:
        return cached

    mounts: list[tuple[str, str]] = []
    mount_index = [0]
    container_inputs = _replace_local_file_paths(inputs, mounts, mount_index)

    # --- Resource Governor ---
    # Default boundaries (Low/Standard profile)
    cpus = os.environ.get("AGENT_CPUS", "2")
    memory = os.environ.get("AGENT_MEMORY", "4g")

    # High resource profile for intensive mapping/calling agents
    if agent_name in {"align", "bwa_agent", "gatk_agent"}:
        cpus = os.environ.get("HIGH_AGENT_CPUS", cpus)
        memory = os.environ.get("HIGH_AGENT_MEMORY", "6g")

    cmd = [
        "docker",
        "run",
        "--rm",
        f"--cpus={cpus}",
        f"--memory={memory}",
    ]

    for host_path, container_path in mounts:
        cmd.extend(["-v", f"{host_path}:{container_path}:ro"])

    cmd.extend([
        "-e",
        f"AGENT_THREADS={cpus}",
        "-e",
        f"AGENT_INPUTS={json.dumps(container_inputs)}",
        "-e",
        f"ROUTING_CONTEXT={json.dumps(routing_ctx)}",
        "-e",
        f"RUN_ID={routing_ctx.get('run_id', 'unknown')}",
        "-e",
        f"S3_ENDPOINT={os.environ.get('S3_ENDPOINT', 'http://localhost:9000')}",
        "-e",
        f"S3_ACCESS_KEY={os.environ.get('S3_ACCESS_KEY', 'minioadmin')}",
        "-e",
        f"S3_SECRET_KEY={os.environ.get('S3_SECRET_KEY', 'minioadmin')}",
        "-e",
        f"ARTIFACT_BUCKET={os.environ.get('ARTIFACT_BUCKET', 'ngs-artifacts')}",
        "-e",
        f"ANTHROPIC_API_KEY={os.environ.get('ANTHROPIC_API_KEY', '')}",
        "-e",
        f"ANTHROPIC_MODEL={os.environ.get('ANTHROPIC_MODEL', 'claude-3-5-sonnet-20241022')}",
        f"ngs/{agent_name}-agent:latest",
    ])

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Agent {agent_name} failed: {result.stderr.strip()}")

    stdout = result.stdout.strip()
    if not stdout:
        raise RuntimeError(f"Agent {agent_name} returned empty output")

    output = json.loads(stdout)
    await cache.set(cache_key, output)
    return output
