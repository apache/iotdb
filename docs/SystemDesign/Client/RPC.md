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

## Rpc definition file

We use thrift rpc between client and server, the definition file is :

thrift/src/main/thrift/rpc.thrift

You can add struct and corresponding method in TSIService (service)

## Code generation

```
mvn clean compile -pl service-rpc -am -DskipTests
```

or

```
mvn clean compile -pl thrift

```

Generated codes:

thrift/target/generated-sources/thrift/org/apache/iotdb/service/rpc/thrift

## Implement the new interface

java client is in Session:

session/src/main/java/org/apache/iotdb/session/Session.java

Rpc service implementation is: 

server/src/main/java/org/apache/iotdb/db/service/TSServiceImpl.java


