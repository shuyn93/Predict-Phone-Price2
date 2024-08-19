from kafka import KafkaProducer
#import time

bootstrap_servers = 'localhost:9092'
topic = 'smartphoneTopic'

producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

def send_message(message):
    try:
        producer.send(topic, str(message).encode('utf-8'))
        #time.sleep(5) #. this is time to send message into topic
        print(f'Produced: {message} to Kafka topic: {topic}')
    except Exception as error:
        print(f'Error: {error}')
        

# Test the send_message function
if __name__ == "__main__":
    test_message = ['379', '1', '128.0', '8.0', 'Mediatek Helio G99 (6nm)', '6.5', '5000.0', '50', '2', '5', '4589000.0']
    send_message(test_message)