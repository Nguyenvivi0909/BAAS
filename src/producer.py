from kafka import KafkaProducer
import csv
import time
import json

producer = KafkaProducer(bootstrap_servers='localhost:9092')

try:
    with open('./HPG.csv', 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            print(row)
            producer.send('Nhom8_Topic', value=json.dumps(row).encode('utf-8'))
            time.sleep(1)
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    producer.close()
