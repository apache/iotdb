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

# Function
```
The example is to show how to send data from localhost to IoTDB through Kafka.
```
# Usage
## Dependencies with Maven

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
	    <version>0.7.0</version>
    </dependency>
</dependencies>
```

## Launch the servers

```
  Before you run the program, make sure you have launched the servers of Kafka and IoTDB.
  For details, please refer to http://kafka.apache.org/081/documentation.html#quickstart
```

## Run KafkaProducer.java

```
  The class is to send data from localhost to Kafka clusters.
  Firstly, you have to change the parameter of TOPIC to what you create：(for example : "test")
  > public final static String TOPIC = "test";
  The default format of data is "device,timestamp,value ". (for example : "sensor1,2017/10/24 19:30:00,60")
  Then you need to create a .CSV file in your localhost, which contains the data to be transmitted.Then set the parameter of Filename：(for example : examples/data/kafka_data.csv)
  > private final static String fileName = "kafka_data.csv";
  Finally, run KafkaProducer.java
```

## Run KafkaConsumer.java

```
  The class is to show how to consume data from kafka through multi-threads.
  The data is sent by class KafkaProducer.
  You can set the parameter of threadsNum to make sure the number of consumer threads:(for example: "5")
  > private final static int threadsNum = 5;
```

### Notice 
  If you want to use multiple consumers, please make sure that the number of topic's partition you create is more than 1.

## Run SendDataToIotdb.java

```
  The class is to show how to write and read date from IoTDB through JDBC
  You can set the parameter of threadsNum to make sure the number of consumer threads:(for example: "5")
  > private final static int threadsNum = 5;
  Then you change the insert EQL according to your localhost's data.
  Finally, run SendDataToIotdb.java
```
