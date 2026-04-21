FROM ngs/base-agent:latest
WORKDIR /agent
RUN python3 -m pip install --no-cache-dir anthropic
COPY main.py ./main.py
CMD ["python3", "-u", "main.py"]
