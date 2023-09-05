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
# Rocketmq-IoTDB Demo
## Introduction
This demo shows how to store data into IoTDB via rocketmq
## Basic Concept
The following basic concepts are involved in IoTDB:

* Device

A devices is an installation equipped with measurements in real scenarios. In IoTDB, all measurements should have their corresponding devices.

* Measurement

A measurement is a detection equipment in an actual scene, which can sense the information to be measured, and can transform the sensed information into an electrical signal or other desired form of information output and send it to IoTDB. In IoTDB, all data and paths stored are organized in units of sensors.

* Database

Databases are used to let users define how to organize and isolate different time series data on disk. Time series belonging to the same database will be continuously written to the same file in the corresponding folder. The file may be closed due to user commands or system policies, and hence the data coming next from these measurements will be stored in a new file in the same folder. Time series belonging to different databases are stored in different folders.
## Connector
> note:In this sample program, there are some update operations for historical data, so it is necessary to ensure the sequential transmission and consumption of data via RocketMQ. If there is no update operation in use, then there is no need to guarantee the order of data. IoTDB will process these data which may be disorderly.

### Producer
Producers insert IoTDB insert statements into partitions according to devices, ensuring that the same device's data is inserted or updated in the same MessageQueue.
### Consumer 
1. At startup, the consumer client first creates a IOTDB-Session connection and check whether the databases and timeseries are created in IoTDB. If not, create it.  
2. Then consume client consume data from RocketMQ using MessageListener Orderly to ensure orderly consumption, and insert the sql statement into IoTDB.

## Usage
### Version usage
IoTDB: 1.0.0  
RocketMQ: 4.4.0
### Dependencies with Maven

```
<dependencies>
    <dependency>
      <groupId>org.apache.iotdb</groupId>
      <artifactId>iotdb-session</artifactId>
      <version>1.0.0</version>
    </dependency>
    <dependency>
      <groupId>org.apache.rocketmq</groupId>
      <artifactId>rocketmq-client</artifactId>
      <version>4.4.0</version>
    </dependency>
  </dependencies>
```
Note: The maven dependencies of io.netty in IoTDB are in conflicts with those dependencies in RocketMQ-Client.

### 1. Install IoTDB
please refer to [https://iotdb.apache.org/#/Download](https://iotdb.apache.org/#/Download)

### 2. Install RocketMQ
please refer to [http://rocketmq.apache.org/docs/quick-start/](http://rocketmq.apache.org/docs/quick-start/)

### 3. Startup IoTDB
please refer to [Quick Start](http://iotdb.apache.org/UserGuide/Master/Get%20Started/QuickStart.html)

### 4. Startup RocketMQ
please refer to [http://rocketmq.apache.org/docs/quick-start/](http://rocketmq.apache.org/docs/quick-start/)

### 5. Start the consumer client:RocketMQConsumer

### 6. Start the producer client:RocketMQProducer
