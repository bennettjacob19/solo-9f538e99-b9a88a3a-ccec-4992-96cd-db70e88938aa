FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /srv

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 8000 8001 8002
# Default entrypoint is the web UI/API.  docker-compose overrides the command
# for the two origin services (app.origin_app:app on 8001 / 8002).
CMD ["uvicorn", "app.gateway_app:app", "--host", "0.0.0.0", "--port", "8000"]
