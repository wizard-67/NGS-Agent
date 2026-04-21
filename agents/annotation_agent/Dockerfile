FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip ca-certificates default-jre-headless wget unzip \
    && rm -rf /var/lib/apt/lists/*

# SnpEff is optional here: mount a prebuilt snpEff.jar or set SNPEFF_JAR at runtime.

RUN python3 -m pip install --no-cache-dir boto3 pandas matplotlib pydantic

WORKDIR /app
COPY agents/base/base_agent.py /app/base_agent.py
COPY shared/storage.py /app/storage.py
COPY agents/annotation_agent/main.py /app/main.py
CMD ["python3", "-u", "/app/main.py"]
