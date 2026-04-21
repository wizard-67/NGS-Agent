FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN python3 -m pip install --no-cache-dir jinja2 boto3 pydantic

WORKDIR /app
COPY report_builder.py /app/report_builder.py
COPY agents/base/base_agent.py /app/base_agent.py
COPY shared/storage.py /app/storage.py
COPY agents/report_builder/main.py /app/main.py
COPY tools/templates /app/tools/templates
CMD ["python3", "-u", "/app/main.py"]
