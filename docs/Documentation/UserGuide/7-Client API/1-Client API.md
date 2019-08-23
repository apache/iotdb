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

# Chaper7: JDBC API

# Usage

## Dependencies

* JDK >= 1.8
* Maven >= 3.0

## How to package only client module

In root directory:
> mvn clean package -pl client -am -Dmaven.test.skip=true

## How to install in local maven repository

In root directory:
> mvn clean install -pl client -am -Dmaven.test.skip=true

## Using IoTDB Client with Maven

```
<dependencies>
    <dependency>
      <groupId>org.apache.iotdb</groupId>
      <artifactId>iotdb-client</artifactId>
      <version>0.9.0-SNAPSHOT</version>
    </dependency>
</dependencies>
```


## Examples with RPC Client

This chapter provides an example of how to open an IoTDB session, execute a batch insertion.

Requires that you include the packages containing the Client classes needed for database programming.

```Java
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.service.rpc.thrift.IoTDBDataType;
import org.apache.iotdb.session.IoTDBRowBatch;
import org.apache.iotdb.session.Session;

  public static void main(String[] args) {
    Session session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    List<IoTDBDataType> dataTypes = new ArrayList<>();
    dataTypes.add(IoTDBDataType.FLOAT);
    dataTypes.add(IoTDBDataType.FLOAT);
    dataTypes.add(IoTDBDataType.FLOAT);

    IoTDBRowBatch rowBatch = new IoTDBRowBatch("root.sg1.d1", measurements, dataTypes);
    for (long i = 1; i <= 100; i++) {
      List<Object> values = new ArrayList<>();
      values.add(1.0f);
      values.add(1.0f);
      values.add(1.0f);
      rowBatch.addRow(i, values);
    }
    session.insertBatch(rowBatch);
    session.close();
  }
```

> The code is in example/client/src/main/java/org/apache/iotdb/client/SessionExample.java