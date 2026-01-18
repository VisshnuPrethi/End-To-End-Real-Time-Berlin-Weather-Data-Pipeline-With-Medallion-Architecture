#!/usr/bin/env python3
from confluent_kafka import Consumer, KafkaError
import json
from datetime import datetime

conf = {
    'bootstrap.servers': '10.0.0.94:9092,10.0.0.96:9092',
    'group.id': 'weather-consumer-group-v2',
    'auto.offset.reset': 'latest',  # NEW MESSAGES ONLY
    'enable.auto.commit': True,
    'session.timeout.ms': 30000
}

TOPIC = 'weather_raw'

consumer = Consumer(conf)
consumer.subscribe([TOPIC])

print("Berlin Weather Monitor (10min updates)")
print(f"Topic: {TOPIC} | Group: weather-consumer-group-v2")
print("Waiting for live updates... (Ctrl+C to stop)")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            print(f"\r[{datetime.now().strftime('%H:%M:%S')}] Waiting for next update...", end="")
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"\nError: {msg.error()}")
                break

        data = json.loads(msg.value().decode('utf-8'))

        print("\n" + "="*50)
        print(f"BERLIN WEATHER UPDATE | Offset: {msg.offset()} | Partition: {msg.partition()}")
        print("="*50)

        if 'current' in data:
            current = data['current']
            temp = current.get('temperature_2m', 'N/A')
            wind = current.get('wind_speed_10m', 'N/A')
            humidity = current.get('relative_humidity_2m', 'N/A')
            feels_like = current.get('apparent_temperature', 'N/A')

            print(f"Temperature:        {temp}C")
            print(f"Wind speed:         {wind} m/s ({wind*3.6:.1f} km/h)")
            print(f"Humidity:           {humidity}%")
            print(f"Feels like:         {feels_like}C")
            print(f"Timestamp:          {datetime.fromtimestamp(data['timestamp']):%Y-%m-%d %H:%M}")

        if 'hourly' in data:
            hourly = data['hourly']
            print("\nHourly forecast (next 3 hours):")
            times = hourly.get('time', [])[:3]
            temps = hourly.get('temperature_2m', [])[:3]
            for i, (t, temp) in enumerate(zip(times, temps)):
                print(f"  {i+1}: {t} -> {temp}C")

        print("="*50)

except KeyboardInterrupt:
    print("\nConsumer stopped by user")
finally:
    consumer.close()
    print("Consumer closed cleanly")