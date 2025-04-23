import time
import redis
import os
import json
import requests
from py_zipkin.zipkin import zipkin_span, ZipkinAttrs, generate_random_64bit_string
import time
import random
from flask import Flask, jsonify

app = Flask(__name__)
redis_client = None

def log_message(message):
    time_delay = random.randrange(0, 2000)
    time.sleep(time_delay / 1000)
    print('message received after waiting for {}ms: {}'.format(time_delay, message))

@app.route('/health')
def health_check():
    global redis_client
    if redis_client:
        try:
            redis_client.ping()
            return jsonify({"status": "UP"}), 200
        except Exception as e:
            return jsonify({"status": "DOWN", "details": f"Redis not connected: {e}"}), 503
    else:
        return jsonify({"status": "DOWN", "details": "Redis client not initialized"}), 503

def redis_consumer():
    global redis_client
    redis_host = os.environ['REDIS_HOST']
    redis_port = int(os.environ['REDIS_PORT'])
    redis_channel = os.environ['REDIS_CHANNEL']
    zipkin_url = os.environ['ZIPKIN_URL'] if 'ZIPKIN_URL' in os.environ else ''

    redis_client = redis.Redis(host=redis_host, port=redis_port, db=0)
    pubsub = redis_client.pubsub()
    pubsub.subscribe([redis_channel])

    def http_transport(encoded_span):
        requests.post(
            zipkin_url,
            data=encoded_span,
            headers={'Content-Type': 'application/x-thrift'},
        )

    for item in pubsub.listen():
        try:
            message = json.loads(str(item['data'].decode("utf-8")))
        except Exception as e:
            log_message(e)
            continue

        if not zipkin_url or 'zipkinSpan' not in message:
            log_message(message)
            continue

        span_data = message['zipkinSpan']
        try:
            with zipkin_span(
                service_name='log-message-processor',
                zipkin_attrs=ZipkinAttrs(
                    trace_id=span_data['_traceId']['value'],
                    span_id=generate_random_64bit_string(),
                    parent_span_id=span_data['_spanId'],
                    is_sampled=span_data['_sampled']['value'],
                    flags=None
                ),
                span_name='save_log',
                transport_handler=http_transport,
                sample_rate=100
            ):
                log_message(message)
        except Exception as e:
            print('did not send data to Zipkin: {}'.format(e))
            log_message(message)

if __name__ == '__main__':
    import threading
    # Iniciar el consumidor de Redis en un hilo separado
    redis_thread = threading.Thread(target=redis_consumer)
    redis_thread.daemon = True
    redis_thread.start()

    # Iniciar el servidor web Flask en el hilo principal
    app.run(host='0.0.0.0', port=5000) # Puedes cambiar el puerto si es necesario
