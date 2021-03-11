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

# TsFile的Flink连接器

## 使用

* 运行 `org.apache.iotdb.flink.FlinkTsFileBatchSource.java` 在本地集群上创建一个 Tsfile 并通过一个 Flink Dataset job 读取它。
* 运行 `org.apache.iotdb.flink.FlinkTsFileStreamSource.java` 在本地集群上创建一个 Tsfile 并通过一个 Flink DataStream job 读取它。
* 运行 `org.apache.iotdb.flink.FlinkTsFileBatchSink.java` 在本地集群上通过一个 Flink Dataset job 写入一个 tsfile 并把内容打印在标准输出中。
* 运行 `org.apache.iotdb.flink.FlinkTsFileStreamSink.java` 在本地集群上通过一个 Flink DataStream job 写入一个 tsfile 并把内容打印在标准输出中。
