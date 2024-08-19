import ast

from kafka import KafkaConsumer
import json
from put_data_hdfs import  store_data_in_hdfs

def map_numeric_to_brand(number):
    numeric_mapping = {
        1: 'Samsung',
        2: 'Apple',
        3: 'Xiaomi',
        4: 'Oppo',
        5: 'Realme',
        6: 'Tecno',
        7: 'ZTE',
        8: 'Vivo',
        9: 'BPhone',
        10: 'Nokia',
        11: 'Google',
        12: 'Oscal', 
        13: 'TCL',
        14: 'INOI'
    }
    return numeric_mapping.get(number, 'Unknown')

def consum_hdfs():
    # Kafka broker configuration
    bootstrap_servers = 'localhost:9092'
    topic = 'smartphoneTopic'

    # Create a Kafka consumer
    consumer = KafkaConsumer(topic,
                             group_id='my_consumer_group',
                             auto_offset_reset='latest',
                             bootstrap_servers=bootstrap_servers,
                             value_deserializer=lambda x: x.decode('utf-8'))



    for message in consumer:
        try:
            data = message.value
            data = ast.literal_eval(data)
            data[1] = map_numeric_to_brand(int(data[1]))
            store_data_in_hdfs(data)



            print("-------------------")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            continue
        