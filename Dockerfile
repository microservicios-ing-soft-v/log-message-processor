FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV REDIS_HOST=localhost
ENV REDIS_PORT=6379
ENV REDIS_CHANNEL=log_channel
ENV ZIPKIN_URL=http://localhost:9411/api/v2/spans

CMD ["python", "main.py"]
