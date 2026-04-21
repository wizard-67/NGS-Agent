FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip ca-certificates samtools bwa wget unzip \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --no-cache-dir boto3 pandas matplotlib pydantic

WORKDIR /app
COPY agents/base/base_agent.py /app/base_agent.py
COPY shared/storage.py /app/storage.py
COPY agents/bwa_agent/main.py /app/main.py
CMD ["python3", "-u", "/app/main.py"]
