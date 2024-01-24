from random import uniform
import streamschema_pb2
import time
from player_simulator import Footballer
from confluent_kafka import Producer

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def main(bootstrap_servers):
    producer_config = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'python-producer'
    }

    producer = Producer(producer_config)

    player_1 = Footballer("Krish","CB","A")
    player_1.enterMatch(strategy="Aggressive",match=1,sensor_id=1)

    player_2 = Footballer("Van","RF","A")
    player_2.enterMatch(strategy="Aggressive",match=1,sensor_id=2)

    for j in range(10):

        iot1 = player_1.move()
        producer.produce(topic, value=iot1, callback=delivery_report)
        iot2 = player_2.move()
        producer.produce(topic, value=iot2, callback=delivery_report)

    producer.flush()



if __name__ == "__main__":
    bootstrap_servers= "192.168.178.78:9092"
    topic = "topic1"
    main(bootstrap_servers)
    