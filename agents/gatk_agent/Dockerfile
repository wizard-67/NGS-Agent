FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV GATK_VERSION=4.6.1.0

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip ca-certificates default-jre-headless samtools wget unzip \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/gatk \
    && wget -qO /tmp/gatk.zip https://github.com/broadinstitute/gatk/releases/download/${GATK_VERSION}/gatk-${GATK_VERSION}.zip \
    && unzip -q /tmp/gatk.zip -d /opt/gatk \
    && ln -s /opt/gatk/gatk-${GATK_VERSION}/gatk /usr/local/bin/gatk \
    && rm /tmp/gatk.zip

RUN python3 -m pip install --no-cache-dir boto3 pandas pydantic

WORKDIR /app
COPY agents/base/base_agent.py /app/base_agent.py
COPY shared/storage.py /app/storage.py
COPY agents/gatk_agent/main.py /app/main.py
CMD ["python3", "-u", "/app/main.py"]
