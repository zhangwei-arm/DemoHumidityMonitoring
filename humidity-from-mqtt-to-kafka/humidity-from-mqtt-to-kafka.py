import json
#from bson import json_util
from time import sleep
import paho.mqtt.client as mqtt

from kafka import KafkaConsumer, KafkaProducer

def kafka_publish_message(producer_instance, topic_name, the_value):
    try:
        #value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, value=the_value)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

def kafka_publish_message2(producer_instance, topic_name, value):
    print("publishing message:\n")
    print(value)
    try:
        print("sending message:\n")
        value = json.loads(value)
        print(value)
        producer_instance.send(topic_name, value)
        #producer_instance.send(topic_name, {"type":"event", "value":{"humidity": 10}))

        print("sending message done.\n")
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['10.169.109.56:9094'])
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

global_producer = connect_kafka_producer()

def mqtt_on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    #kafka_publish_message(global_producer, "humidity", str(msg.payload))
    kafka_publish_message(global_producer, "humidity", msg.payload)

def mqtt_on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("humidity")

if __name__ == '__main__':
    #### Setup MQTT
    client = mqtt.Client()
    client.on_connect = mqtt_on_connect
    client.on_message = mqtt_on_message
    client.connect("10.169.109.56", 1883, 60)
    client.loop_forever()

