# DemoHumidityMonitoring

This is a demo that shows an event-driven processing pattern using open source projects. The high level architecture is shown as follows. 

![](images/high-level.png)

# Folder Explained
- flink-playgrounds  - A clone from https://github.com/apache/flink-playgrounds.git with minor modification. It's to startup flink local services including jobmanager and taskmanager. It also start up a kafka and a zookeeper services;

- humidity-monitoring-flink-app - A very simple java project which contains two applications; One is the monitoring each of the humidity sensor readings and generate alerts to socket port 12345 when it's higher than a threshold; The other monitoring the average of humidity sensor readings in 30 seconds, when the average of the readings exceeds the threshold, then generate an alerts to socket port 12346;

- humidity-collection-on-pi  - There is a real sensor used in this demo. This folder contains a script to collect humidity readings from the sensor. The sensor readings are sent to a mosquitto server hosted on localhost, the topic is *humidity*;


- humidity-from-mqtt-to-kafka  - It contains a script to pass the humidity sensor readings from mqtt broker to local hosted kafka container; The kafka topic is also *humidity*; 

# Dependencies
## Hardware
- Raspberry Pi
- DHT22 Humidity&Temperature Sensor
- Laptop or Desktop with Docker installed;

## Middleware
- Mosquitto - An open source mqtt broker;
- Kafka - An open source message broker;
- Flink - An open source stream processing engine;

## Tools
- netcat
- pip
- Docker
- kafkacat (optional)
- Kafka Tool (optional)
- MQTT fx (optional)

## Programing Language
- Java 
- Python