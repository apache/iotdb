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
# IoTDB-Flink-Connector Example

## Function
```
The example is to show how to send data to a IoTDB server from a Flink job.
```

## Usage

* Launch the IoTDB server.
* Run `org.apache.iotdb.flink.FlinkIoTDBSink.java` to run the flink job on local mini cluster.

# TsFile-Flink-Connector Example

## Usage

* Run `org.apache.iotdb.flink.FlinkTsFileBatchSource.java` to create a tsfile and read it via a flink DataSet job on local mini cluster.
* Run `org.apache.iotdb.flink.FlinkTsFileStreamSource.java` to create a tsfile and read it via a flink DataStream job on local mini cluster.
* Run `org.apache.iotdb.flink.FlinkTsFileBatchSink.java` to write a tsfile via a flink DataSet job on local mini cluster and print the content to stdout.
* Run `org.apache.iotdb.flink.FlinkTsFileStreamSink.java` to write a tsfile via a flink DataStream job on local mini cluster and print the content to stdout.
