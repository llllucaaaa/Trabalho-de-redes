FROM python:3.12-slim
WORKDIR /app
COPY peer.py peers.docker.txt ./
ENTRYPOINT ["python", "-u", "peer.py"]
