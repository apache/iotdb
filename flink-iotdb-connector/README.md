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
# IoTDB-Flink-Connector

IoTDB integration for [Apache Flink](https://flink.apache.org/). This module includes the iotdb sink that allows a flink job to write events into timeseries.

## IoTDBSink
To use the `IoTDBSink`,  you need construct an instance of it by specifying `IoTDBOptions` and `IoTSerializationSchema` instances.
The `IoTDBSink` send only one event after another by default, but you can change to batch by invoking `withBatchSize(int)`. 

## Examples
The following is an example which receiving events from sensor source and then sending events to iotdb.

 ```java
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        IoTDBOptions options = new IoTDBOptions();
        options.setHost("127.0.0.1");
        options.setPort(6667);
        options.setUser("root");
        options.setPassword("root");
        options.setStorageGroup("root.sg");
        options.setTimeseries(Lists.newArrayList("root.sg.d1.s1"));

        IoTSerializationSchema serializationSchema = new DefaultIoTSerializationSchema();
        IoTDBSink ioTDBSink = new IoTDBSink(options, serializationSchema)
                // enable batching
                .withBatchSize(10)
                ;

        env.addSource(new SensorSource())
                .name("sensor-source")
                .setParallelism(1)
                .addSink(ioTDBSink)
                .name("iotdb-sink")
                .setParallelism(1)
        ;

        env.execute("iotdb-flink-example");
 ```
