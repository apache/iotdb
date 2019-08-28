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

# Chapter7: Session API

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

## Using IoTDB Session with Maven

```
<dependencies>
    <dependency>
      <groupId>org.apache.iotdb</groupId>
      <artifactId>iotdb-client</artifactId>
      <version>0.9.0-SNAPSHOT</version>
    </dependency>
</dependencies>
```


## Examples with Session

This chapter provides an example of how to open an IoTDB session, execute a batch insertion.

Requires that you include the packages containing the Client classes needed for database programming.

```Java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.RowBatch;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

public class SessionExample {

  public static void main(String[] args) throws ClassNotFoundException, IoTDBSessionException {
    Session session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    
    session.setStorageGroup("root.sg1");
    session.createTimeseriesResp("root.sg1.d1.s1", TSDataType.INT64, TSEncoding.RLE);
    session.createTimeseriesResp("root.sg1.d1.s2", TSDataType.INT64, TSEncoding.RLE);
    session.createTimeseriesResp("root.sg1.d1.s3", TSDataType.INT64, TSEncoding.RLE);

    Schema schema = new Schema();
    schema.registerMeasurement(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    schema.registerMeasurement(new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
    schema.registerMeasurement(new MeasurementSchema("s3", TSDataType.INT64, TSEncoding.RLE));

    RowBatch rowBatch = schema.createRowBatch("root.sg1.d1", 100);

    long[] timestamps = rowBatch.timestamps;
    Object[] values = rowBatch.values;

    for (long time = 0; time < 30000; time++) {
      int row = rowBatch.batchSize++;
      timestamps[row] = time;
      for (int i = 0; i < 3; i++) {
        long[] sensor = (long[]) values[i];
        sensor[row] = time;
      }
      if (rowBatch.batchSize == rowBatch.getMaxBatchSize()) {
        session.insertBatch(rowBatch);
        rowBatch.reset();
      }
    }

    if (rowBatch.batchSize != 0) {
      session.insertBatch(rowBatch);
      rowBatch.reset();
    }

    session.close();
  }
}
```

> The code is in example/session/src/main/java/org/apache/iotdb/session/SessionExample.java