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
    player_1.enterMatch(strategy="Aggressive", match=1, sensor_id=1)

    player_2 = Footballer("Van","RF","A")
    player_2.enterMatch(strategy="Conservative", match=1, sensor_id=2)

    player_3 = Footballer("Rajkumar","CM","B")
    player_3.enterMatch(strategy="Defensive", match=1, sensor_id=3)
    
    player_4 = Footballer("Gary","GK","B")
    player_4.enterMatch(strategy="Conservative", match=1, sensor_id=4)


    while(True):

        iot1 = player_1.move()
        producer.produce(topic[0], value=iot1, partition=0, callback=delivery_report)
        iot2 = player_2.move()
        producer.produce(topic[0], value=iot2, partition=1, callback=delivery_report)
        iot3 = player_3.move()
        producer.produce(topic[1], value=iot3, partition=0, callback=delivery_report)
        iot4 = player_4.move()
        producer.produce(topic[1], value=iot4, partition=1, callback=delivery_report)
        
        producer.flush()
        
        time.sleep(1)

  


if __name__ == "__main__":

    bootstrap_servers= "192.168.178.78:9092"
    topic = ["leagueA","leagueB"]
    main(bootstrap_servers)
