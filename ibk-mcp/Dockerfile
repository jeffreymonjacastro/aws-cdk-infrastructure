FROM public.ecr.aws/docker/library/python:3.11-slim

# Instalamos el AWS Lambda Web Adapter (Magia pura: permite correr FastAPI en Lambda sin cambios)
COPY --from=public.ecr.aws/awsguru/aws-lambda-adapter:0.8.4 /lambda-adapter /opt/extensions/lambda-adapter

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

# Configurar el puerto para AWS Lambda Adapter
# Asumimos que el servidor MCP (Uvicorn/FastAPI) corre en el puerto 8000 por defecto
ENV AWS_LWA_PORT=8000
# Importante: Aumentar el tiempo de espera del wrapper para que Uvicorn tenga tiempo de arrancar
ENV AWS_LWA_READINESS_CHECK_MIN_INTERVAL_SECONDS=2


COPY src/ .

# El Adapter escucha en el puerto 8080 por defecto
CMD ["python", "server.py"]