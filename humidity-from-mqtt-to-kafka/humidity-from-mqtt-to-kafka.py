import json
from time import sleep
import paho.mqtt.client as mqtt

from kafka import KafkaConsumer, KafkaProducer

def kafka_publish_message(producer_instance, topic_name, the_value):
    try:
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
    kafka_publish_message(global_producer, "humidity", msg.payload)

def mqtt_on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker with result code "+str(rc))
    client.subscribe("humidity")

if __name__ == '__main__':
    import getopt
    import sys
    mqtt_host = "localhost"
    mqtt_port = 1883
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'h:p:', ['mqtthost', 'mqttport'])
        if len(opts) != 2:
            print ('usage: python humidity-from-mqtt-to-kafka.py -h <mqtt host> -p <mqtt port>')
            sys.exit(2)
        else:
            for opt, arg in opts:
                if opt == '-h':
                    mqtt_host = arg
                else:
                    mqtt_port= int(arg)

    except getopt.GetoptError:
        print ('usage: python humidity-from-mqtt-to-kafka.py -h <mqtt host> -p <mqtt port>')
        sys.exit(-1)

    #### Setup MQTT
    client = mqtt.Client()
    client.on_connect = mqtt_on_connect
    client.on_message = mqtt_on_message
    print("Connecting to mqtt server from {}:{}...".format(mqtt_host, mqtt_port))
    client.connect(mqtt_host, mqtt_port, 60)
    client.loop_forever()

