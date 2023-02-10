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
# Kafka-IoTDB Demo
## Function
```
The example is to show how to send data from localhost to IoTDB through Kafka.
```
## Usage
### Version usage
IoTDB: 1.0.0  
Kafka: 2.8.0
### Dependencies with Maven

```
<dependencies>
         <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka_2.13</artifactId>
            <version>2.8.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.iotdb</groupId>
            <artifactId>iotdb-session</artifactId>
            <version>1.0.0</version>
        </dependency>
</dependencies>
```

### Launch the servers

```
  Before you run the program, make sure you have launched the servers of Kafka and IoTDB.
  For details, please refer to http://kafka.apache.org/081/documentation.html#quickstart
```

### Run Producer.java

```
  The class is to send data from localhost to Kafka clusters.
  Firstly, you have to change the parameter of TOPIC in Constant.java to what you create：(for example : "Kafka-Test")
  > public final static String TOPIC = "Kafka-Test";
  The default format of data is "device,timestamp,value ". (for example : "measurement1,2017/10/24 19:30:00,60")
  Then you need to create data in Constat.ALL_DATA
  Finally, run KafkaProducer.java
```

### Run Consumer.java

```
  The class is to show how to consume data from kafka through multi-threads.
  The data is sent by class KafkaProducer.
  You can set the parameter of CONSUMER_THREAD_NUM in Constant.java to make sure the number of consumer threads:(for example: "3")
  > private final static int CONSUMER_THREAD_NUM = 3;
```

#### Notice 
  If you want to use multiple consumers, please make sure that the number of topic's partition you create is more than 1.