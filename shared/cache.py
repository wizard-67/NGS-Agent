import hashlib
import json
import os
from typing import Any, Dict, Optional

import boto3
import redis


class CacheManager:
    def __init__(self) -> None:
        self.redis = redis.from_url(
            os.environ.get("REDIS_URL", "redis://localhost:6379"), decode_responses=True
        )
        self.s3 = boto3.client(
            "s3",
            endpoint_url=os.environ.get("S3_ENDPOINT", "http://localhost:9000"),
            aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "minioadmin"),
        )
        self.bucket = os.environ.get("CACHE_BUCKET", "ngs-cache")

    def compute_hash(self, agent_name: str, inputs: Dict[str, Any]) -> str:
        content = json.dumps({"agent": agent_name, "inputs": inputs}, sort_keys=True)
        return hashlib.blake2b(content.encode("utf-8")).hexdigest()[:16]

    async def get(self, cache_key: str) -> Optional[Dict[str, Any]]:
        redis_key = f"cache:{cache_key}"
        data = self.redis.get(redis_key)
        if data:
            return json.loads(data)

        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=f"{cache_key}.json")
            return json.loads(obj["Body"].read().decode("utf-8"))
        except Exception:
            return None

    async def set(self, cache_key: str, data: Dict[str, Any], ttl_days: int = 30) -> None:
        redis_key = f"cache:{cache_key}"
        self.redis.setex(redis_key, ttl_days * 24 * 3600, json.dumps(data))
        self.s3.put_object(Bucket=self.bucket, Key=f"{cache_key}.json", Body=json.dumps(data))
