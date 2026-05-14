FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# proto 생성
COPY proto/ /app/proto_src/
RUN pip install grpcio-tools && \
    python -m grpc_tools.protoc \
      -I/app/proto_src \
      --python_out=/app/proto \
      /app/proto_src/telemetry.proto \
      /app/proto_src/sensor.proto && \
    touch /app/proto/__init__.py

COPY src/ /app/

# CONSUMER_TYPE 환경변수로 telemetry / sensor 전환
CMD ["sh", "-c", "python consumer_${CONSUMER_TYPE}.py"]
