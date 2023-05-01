import json
import psycopg2

from kafka import KafkaConsumer

kafka_hostname = '172.16.0.3'
kafka_port = '9092'
topic = 'iot'

greenplum_hostname = '172.16.0.2'
greenplum_port = '5432'
greenplum_database_name = 'db'
greenplum_database_user = 'gpuser'
greenplum_database_password = 'pwd'

while True:
    try:
        consumer = KafkaConsumer(topic, bootstrap_servers=[f'{kafka_hostname}:{kafka_port}'], auto_offset_reset='earliest',
                                enable_auto_commit=True, value_deserializer=lambda v: json.loads(v.decode('utf-8')))
        break
    except:
        continue

print(f'Подключено к Kafka: {kafka_hostname}:{kafka_port}, топик: {topic}')

while True:
    try:
        conn = psycopg2.connect(host=greenplum_hostname, port=greenplum_port, database=greenplum_database_name,
                                user=greenplum_database_user, password=greenplum_database_password)
        break
    except:
        continue

cursor = conn.cursor()
print(f'Подключено к Greenplum: {greenplum_hostname}:{greenplum_port}, БД: {greenplum_database_name}')

count = 0

try:
    for message in consumer:
        record = message.value
        count += 1
        print(f'Считана запись #{count} из Kafka: {record}')

        cursor.execute('insert into records('
                       'occur_time,'
                       'sensor_id,'
                       'latitude,'
                       'longitude,'
                       'temperature,'
                       'controller_id)'
                       'values(%s, %s, %s, %s, %s, %s)',
                       [record['occur_time'], record['sensor_id'], record['latitude'], record['longitude'],
                        record['temperature'], record['controller_id']])
        conn.commit()
        print(f"Внесение записи #{count}")

except KeyboardInterrupt:
    print(f'Обработано {count} записей')
