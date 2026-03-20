from kafka import KafkaProducer
import json
import time
import random

# Connect to your local Kafka broker
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Starting sensor loop... Press Ctrl+C to stop.")

while True:
    # 1. Create fake sensor data
    sensor_data = {
        "sensor_id": "temp_sensor_01",
        "temperature": round(random.uniform(20.0, 30.0), 2),
        "timestamp": time.time()
    }
    
    # 2. Send to a Kafka topic named 'weather_data'
    producer.send('weather_data', value=sensor_data)
    print(f"Sent: {sensor_data}")
    
    # 3. Wait 2 seconds before the next loop
    time.sleep(2)
