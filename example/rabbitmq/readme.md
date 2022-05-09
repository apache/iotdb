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
# RabbitMQ-IoTDB Demo
## Function
```
The example is to show how to send data from localhost to IoTDB through RabbitMQ.
```
## Usage
### Version usage
IoTDB: 0.12.0  
RabbitMQ server: 3.8.15
RabbitMQ client jar: 5.12.0

### Dependencies with Maven

```
<dependencies>
    <dependency>
        <groupId>org.apache.iotdb</groupId>
        <artifactId>iotdb-jdbc</artifactId>
        <version>${project.version}</version>
    </dependency>
    <dependency>
        <groupId>com.rabbitmq</groupId>
        <artifactId>amqp-client</artifactId>
        <version>5.12.0</version>
    </dependency>
</dependencies>
```

### 1. Install IoTDB
please refer to [https://iotdb.apache.org/#/Download](https://iotdb.apache.org/#/Download)

### 2. Install RabbitMQ
please refer to [https://www.rabbitmq.com/download.html](https://www.rabbitmq.com/download.html)

### 3. Startup IoTDB
please refer to [Quick Start](http://iotdb.apache.org/UserGuide/Master/Get%20Started/QuickStart.html)

### 4. Startup RocketMQ
please refer to [https://www.rabbitmq.com/download.html](https://www.rabbitmq.com/download.html)

### 5. Start the consumer client: RabbitMQConsumer

### 6. Start the producer client: RabbitMQProducer