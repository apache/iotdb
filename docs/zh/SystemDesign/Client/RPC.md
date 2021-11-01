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

## 增加 RPC 方法

我们使用 Thrift Rpc 框架， thrift 定义文件目录：

thrift/src/main/thrift/rpc.thrift

可按需增加 struct 结构，并在 service TSIService 中增加对应方法

## 生成代码

```
mvn clean compile -pl service-rpc -am -DskipTests
```

或者

```
mvn clean compile -pl thrift
```

生成代码位置：

thrift/target/generated-sources/thrift/org/apache/iotdb/service/rpc/thrift

## 实现新接口

java 客户端在 Session 类中

session/src/main/java/org/apache/iotdb/session/Session.java

服务器端代码在

server/src/main/java/org/apache/iotdb/db/service/TSServiceImpl.java
