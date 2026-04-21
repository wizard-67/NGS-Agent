FROM ngs/base-agent:latest
WORKDIR /agent
RUN apt-get update \
	&& apt-get install -y --no-install-recommends fastqc default-jre-headless \
	&& rm -rf /var/lib/apt/lists/*
COPY main.py ./main.py
CMD ["python", "-u", "main.py"]
