#!/usr/bin/env python3
import json
import time
import requests
from kafka import KafkaProducer

# Open-Meteo API (Berlin) - FREE, NO API KEY
API_URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=52.52"
    "&longitude=13.41"
    "&current=temperature_2m,wind_speed_10m,relative_humidity_2m,apparent_temperature"
    "&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"
    "&timezone=Europe/Berlin"
)

KAFKA_BOOTSTRAP_SERVERS = ["10.0.0.94:9092", "10.0.0.96:9092"]
KAFKA_TOPIC = "weather_raw"

def create_producer():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        api_version=(3, 6, 0)
    )
    return producer

def fetch_weather():
    resp = requests.get(API_URL, timeout=10)
    resp.raise_for_status()
    return resp.json()

def build_message(payload):
    return {
        "source": "open-meteo",
        "city": "Berlin",
        "timestamp": time.time(),
        "latitude": payload.get("latitude"),
        "longitude": payload.get("longitude"),
        "current": payload.get("current"),
        "hourly": payload.get("hourly"),
    }

def main():
    producer = create_producer()
    print(f"Connected to 2-broker cluster: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC} (10min intervals)")
    print("Starting Berlin weather streaming... (Ctrl+C to stop)")

    try:
        while True:
            try:
                raw_data = fetch_weather()
                message = build_message(raw_data)

                future = producer.send(
                    topic=KAFKA_TOPIC,
                    key="berlin",
                    value=message,
                )

                record_md = future.get(timeout=10)
                temp = raw_data['current']['temperature_2m']
                print(f"Sent: {temp:.1f}C | offset={record_md.offset} | partition={record_md.partition}")

            except Exception as e:
                print(f"Error: {e}")
                time.sleep(30)

            time.sleep(600)  # 10 MINUTES

    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()