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

[English](./README.md) | [中文](./README-zh.md)
# TsFile Java Document
<pre>
___________    ___________.__.__          
\__    ___/____\_   _____/|__|  |   ____  
  |    | /  ___/|    __)  |  |  | _/ __ \ 
  |    | \___ \ |     \   |  |  |_\  ___/ 
  |____|/____  >\___  /   |__|____/\___  >  version 1.0.0
             \/     \/                 \/  
</pre>

## Use TsFile

### Add TsFile as a dependency in Maven

The current release version is `1.0.0`

```xml  
<dependencies>
    <dependency>
      <groupId>org.apache.tsfile</groupId>
      <artifactId>tsfile</artifactId>
      <version>1.0.0</version>
    </dependency>
<dependencies>
```

### TsFile Java API

#### Write Data

Data written is through TsFileWriter.

1. Construct TsFileWriter

 
    ```java
    File f = new File("test.tsfile");
    TsFileWriter tsFileWriter = new TsFileWriter(f);
    ```

2. Register timeseries
  
    ```java
    List<MeasurementSchema> schema1 = new ArrayList<>();
    schemas.add(new MeasurementSchema("voltage", TSDataType.FLOAT));
    schemas.add(new MeasurementSchema("electricity", TSDataType.FLOAT));
    tsFileWriter.registerTimeseries(new Path("solarpanel1"), schema1);
   
     List<MeasurementSchema> schema2 = new ArrayList<>();
    schemas.add(new MeasurementSchema("voltage", TSDataType.FLOAT));
    schemas.add(new MeasurementSchema("electricity", TSDataType.FLOAT));
    schemas.add(new MeasurementSchema("windspeed", TSDataType.FLOAT));
    tsFileWriter.registerTimeseries(new Path("turbine1"), schema2);
    ```

3. Write data

    ```java
    TSRecord tsRecord = new TSRecord(1, "solarpanel1");
    tsRecord.addTuple(DataPoint.getDataPoint(TSDataType.FLOAT, "voltage", 1.1f));
    tsRecord.addTuple(DataPoint.getDataPoint(TSDataType.FLOAT, "electricity", 2.2f));
    tsFileWriter.write(tsRecord);
    ```

4. Close TsFileWriter, only closed TsFile could be queried.

    ```java
    tsFileWriter.close();
    ```

Write TsFile Example

[Write Data By TSRecord](../examples/src/main/java/org/apache/tsfile/TsFileWriteAlignedWithTSRecord.java)。

[Write Data By Tablet](../examples/src/main/java/org/apache/tsfile/TsFileWriteAlignedWithTablet.java)。


#### Read TsFile

Data query is through TsFileReader.

1. Construct TsFileReader

   ```java
   TsFileSequenceReader reader = new TsFileSequenceReader(path);
   TsFileReader tsFileReader = new TsFileReader(reader)；
   ```

2. Construct query expression, including predicate and filter

      ```java
      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path("solarpanel1", "voltage"));
      paths.add(new Path("solarpanel1", "electricity"));
   
      IExpression timeFilter = BinaryExpression.and(
      new GlobalTimeExpression(TimeFilterApi.gtEq(1L)),
      new GlobalTimeExpression(TimeFilterApi.ltEq(10L)));
   
      QueryExpression queryExpression = QueryExpression.create(paths, timeFilter);
      ```

3. Query data

   ```java
   QueryDataSet queryDataSet = readTsFile.query(queryExpression);
   while (queryDataSet.hasNext()) {
        queryDataSet.next();
   }
   ```

4. Close TsFileReader

   ```java
   tsFileReader.close();
   ```



Read TsFile Example

[Query Data](../examples/src/main/java/org/apache/tsfile/TsFileRead.java)

[Scan whole TsFile](../examples/src/main/java/org/apache/tsfile/TsFileSequenceRead.java)


## Building With Java

### Prerequisites

To build TsFile wirh Java, you need to have:

1. Java >= 1.8 (1.8, 11 to 17 are verified. Please make sure the environment path has been set accordingly).
2. Maven >= 3.6.3 (If you want to compile TsFile from source code).


### Build TsFile with Maven

```
mvn clean package -P with-java -DskipTests
```

### Install to local machine

```
mvn install -P with-java -DskipTests
```
