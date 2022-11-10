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
# IoTDB-MQTT-Broker Example

## Function
```
The example is to show how to send data to IoTDB from a mqtt client.
```

## Usage

* Update configuration to enable MQTT service. (`enable_mqtt_service=true` in iotdb-datanode.properties)
* Launch the IoTDB server.
* Setup database `CREATE DATABASE root.sg` and create time timeseries `CREATE TIMESERIES root.sg.d1.s1 WITH DATATYPE=DOUBLE, ENCODING=PLAIN`.
* Run `org.apache.iotdb.mqtt.MQTTClient` to run the mqtt client and send events to server.
