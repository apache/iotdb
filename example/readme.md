<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
# 1. Kafka-IoTDB Demo
## Function
```
The example is to show how to send data from localhost to IoTDB through Kafka.
```
## Usage
### Version usage
IoTDB: 0.8.0-SNAPSHOT  
Kafka: 0.8.2.0
### Dependencies with Maven

```
<dependencies>
    <dependency>
    	<groupId>org.apache.kafka</groupId>
    	<artifactId>kafka_2.10</artifactId>
    	<version>0.8.2.0</version>
    </dependency>
    <dependency>
	    <groupId>org.apache.iotdb</groupId>
	    <artifactId>iotdb-jdbc</artifactId>
	    <version>0.8.0-SNAPSHOT</version>
    </dependency>
</dependencies>
```

### Launch the servers

```
  Before you run the program, make sure you have launched the servers of Kafka and IoTDB.
  For details, please refer to http://kafka.apache.org/081/documentation.html#quickstart
```

### Run KafkaProducer.java

```
  The class is to send data from localhost to Kafka clusters.
  Firstly, you have to change the parameter of TOPIC in Constant.java to what you create：(for example : "Kafka-Test")
  > public final static String TOPIC = "Kafka-Test";
  The default format of data is "device,timestamp,value ". (for example : "sensor1,2017/10/24 19:30:00,60")
  Then you need to create data in Constat.ALL_DATA
  Finally, run KafkaProducer.java
```

### Run KafkaConsumer.java

```
  The class is to show how to consume data from kafka through multi-threads.
  The data is sent by class KafkaProducer.
  You can set the parameter of CONSUMER_THREAD_NUM in Constant.java to make sure the number of consumer threads:(for example: "5")
  > private final static int CONSUMER_THREAD_NUM = 5;
```

#### Notice 
  If you want to use multiple consumers, please make sure that the number of topic's partition you create is more than 1.

# 2. Rocketmq-IoTDB Demo
##Introduction
This demo shows how to store data into IoTDB via rocketmq
##Basic Concept
The following basic concepts are involved in IoTDB:

* Device

A devices is an installation equipped with sensors in real scenarios. In IoTDB, all sensors should have their corresponding devices.

* Sensor

A sensor is a detection equipment in an actual scene, which can sense the information to be measured, and can transform the sensed information into an electrical signal or other desired form of information output and send it to IoTDB. In IoTDB, all data and paths stored are organized in units of sensors.

* Storage Group

Storage groups are used to let users define how to organize and isolate different time series data on disk. Time series belonging to the same storage group will be continuously written to the same file in the corresponding folder. The file may be closed due to user commands or system policies, and hence the data coming next from these sensors will be stored in a new file in the same folder. Time series belonging to different storage groups are stored in different folders.
##Connector
> note:In this sample program, there are some update operations for historical data, so it is necessary to ensure the sequential transmission and consumption of data via RocketMQ. If there is no update operation in use, then there is no need to guarantee the order of data. IoTDB will process these data which may be disorderly.

###Producer
Producers insert IoTDB insert statements into partitions according to devices, ensuring that the same device's data is inserted or updated in the same MessageQueue.
###Consumer 
1. At startup, the consumer client first creates a JDBC connection and check whether the storage groups and timeseries are created in IoTDB. If not, create it.  
2. Then consume client consume data from rocketmq using MessageListener Orderly to ensure orderly consumption, and insert the sql statement into IoTDB.

## Usage
### Version usage
IoTDB: 0.8.0-SNAPSHOT  
Kafka: 4.4.0
### Dependencies with Maven

```
<dependencies>
    <dependency>
      <groupId>org.apache.iotdb</groupId>
      <artifactId>iotdb-jdbc</artifactId>
      <version>0.8.0-SNAPSHOT</version>
      <exclusions>
        <exclusion>
          <groupId>io.netty</groupId>
          <artifactId>netty-common</artifactId>
        </exclusion>
        <exclusion>
          <groupId>io.netty</groupId>
          <artifactId>netty-buffer</artifactId>
        </exclusion>
      </exclusions>
    </dependency>
    <dependency>
      <groupId>org.apache.rocketmq</groupId>
      <artifactId>rocketmq-client</artifactId>
      <version>4.4.0</version>
    </dependency>
  </dependencies>
```
Note: The maven dependencies of io.netty in IoTDB are in conflicts with those dependencies in RocketMQ-Client.
###1. Install IoTDB
please refer to [https://iotdb.apache.org/#/Download](https://iotdb.apache.org/#/Download)
###2. Install RocketMQ
pleasr refer to [http://rocketmq.apache.org/docs/quick-start/](http://rocketmq.apache.org/docs/quick-start/)
###3. Startup IoTDB
please refer to [https://iotdb.apache.org/#/Documents](https://iotdb.apache.org/#/Documents)
###4. Startup RocketMQ
please refer to [http://rocketmq.apache.org/docs/quick-start/](http://rocketmq.apache.org/docs/quick-start/)
###5. Start the consumer client:RocketMQConsumer
###6. Start the producer client:RocketMQProducer
