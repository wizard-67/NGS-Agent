import os
from pathlib import Path

import boto3


class MinioStorage:
    def __init__(self) -> None:
        self.endpoint = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
        self.access_key = os.environ.get("S3_ACCESS_KEY", "minioadmin")
        self.secret_key = os.environ.get("S3_SECRET_KEY", "minioadmin")
        self.artifact_bucket = os.environ.get("ARTIFACT_BUCKET", "ngs-artifacts")
        self.client = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

    def upload_file(self, local_path: str, key: str) -> str:
        self.client.upload_file(local_path, self.artifact_bucket, key)
        return f"s3://{self.artifact_bucket}/{key}"

    def download_file(self, s3_uri: str, local_path: str) -> str:
        bucket, key = self.parse_s3_uri(s3_uri)
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        self.client.download_file(bucket, key, local_path)
        return local_path

    @staticmethod
    def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
        if not s3_uri.startswith("s3://"):
            raise ValueError(f"Expected s3:// URI, got: {s3_uri}")
        no_scheme = s3_uri[5:]
        bucket, key = no_scheme.split("/", 1)
        return bucket, key
