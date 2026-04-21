FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        ca-certificates \
        r-base \
        r-base-dev \
        r-cran-tidyverse \
        r-cran-jsonlite \
        r-cran-readr \
        r-cran-dplyr \
        r-cran-tibble \
        r-cran-httr \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --no-cache-dir boto3 anthropic pydantic

WORKDIR /app
COPY agents/base/base_agent.py /app/base_agent.py
COPY shared/storage.py /app/storage.py
COPY agents/insight_agent/main.py /app/main.py
COPY agents/insight_agent/go_analysis.R /app/go_analysis.R
CMD ["python3", "-u", "/app/main.py"]
