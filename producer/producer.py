import csv
import hashlib
import json
import random
import time

from datetime import datetime
from kafka import KafkaProducer


def get_controller_id(sensor_id):
    hash_object = hashlib.sha1(sensor_id.encode('utf-8'))
    return hash_object.hexdigest()


def setup_sensors(filename):
    sensors = []

    with open(filename, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            sensors.append(dict(
                sensor_id=row[0],
                controller_id=get_controller_id(row[0]),
                latitude=row[1],
                longitude=row[2]
            ))

    return sensors


def generate_event(sensors, producer, topic, count):
    for sensor in sensors:
        count += 1
        temperature = random.randint(-20, 20)
        occur_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        sensor["occur_time"] = occur_time
        sensor["temperature"] = temperature

        producer.send(topic, sensor)

        print(f'{occur_time}: Запись события #{count} в Kafka: датчик #{sensor["sensor_id"]} ({sensor["latitude"]}, '
              f'{sensor["longitude"]}): температура: {temperature}')
    return count


hostname = '172.16.0.3'
port = '9092'
topic = 'iot'
sensors = setup_sensors('coords.csv')
count = 0


while True:
    try:
        producer = KafkaProducer(bootstrap_servers=[f'{hostname}:{port}'],
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        break
    except:
        continue
print(f'Подключено к Kafka: {hostname}:{port}, топик: {topic}')

try:
    while True:
        count = generate_event(sensors, producer, topic, count)
        time.sleep(1)
except KeyboardInterrupt:
    print(f'Обработано {count} записей')
