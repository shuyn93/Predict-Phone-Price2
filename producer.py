from kafka import KafkaProducer
#import time

bootstrap_servers = 'localhost:9092'
topic = 'topic1_logs' #smartphoneTopic'

producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

def send_message(message):
    try:
        producer.send(topic, str(message).encode('utf-8'))
        #test time.sleep(5). this is time to send message into topic
        print(f'Produced: {message} to Kafka topic: {topic}')
    except Exception as error:
        print(f'Error: {error}')
        

# Test the send_message function
if __name__ == "__main__":
    test_message = "Test smartphone data"
    send_message(test_message)