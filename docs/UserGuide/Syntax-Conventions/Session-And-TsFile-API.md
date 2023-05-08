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

# Session And TsFile API

When using the Session and TsFile APIs, if the method you call requires parameters such as measurement, device, database, path in the form of String, **please ensure that the parameters passed in the input string is the same as when using the SQL statement**, here are some examples to help you understand. Code example could be found at: `example/session/src/main/java/org/apache/iotdb/SyntaxConventionRelatedExample.java`

1. Take creating a time series createTimeseries as an example:

```java
public void createTimeseries(
    String path,
    TSDataType dataType,
    TSEncoding encoding,
    CompressionType compressor)
    throws IoTDBConnectionException, StatementExecutionException;
```

If you wish to create the time series root.sg.a, root.sg.\`a.\`\`"b\`, root.sg.\`111\`, the SQL statement you use should look like this:

```sql
create timeseries root.sg.a with datatype=FLOAT,encoding=PLAIN,compressor=SNAPPY;

# node names contain special characters, each node in the time series is ["root","sg","a.`\"b"]
create timeseries root.sg.`a.``"b` with datatype=FLOAT,encoding=PLAIN,compressor=SNAPPY;

# node names are pure numbers
create timeseries root.sg.`111` with datatype=FLOAT,encoding=PLAIN,compressor=SNAPPY;
```

When you call the createTimeseries method, you should assign the path string as follows to ensure that the content of the path string is the same as when using SQL:

```java
// timeseries root.sg.a
String path = "root.sg.a";

// timeseries root.sg.`a.``"b`
String path = "root.sg.`a.``\"b`";

// timeseries root.sg.`111`
String path = "root.sg.`111`";
```

2. Take inserting data insertRecord as an example:

```java
public void insertRecord(
    String deviceId,
    long time,
    List<String> measurements,
    List<TSDataType> types,
    Object... values)
    throws IoTDBConnectionException, StatementExecutionException;
```

If you want to insert data into the time series root.sg.a, root.sg.\`a.\`\`"b\`, root.sg.\`111\`, the SQL statement you use should be as follows:

```sql
insert into root.sg(timestamp, a, `a.``"b`, `111`) values (1, 2, 2, 2);
```

When you call the insertRecord method, you should assign deviceId and measurements as follows:

```java
// deviceId is root.sg
String deviceId = "root.sg";

// measurements
String[] measurements = new String[]{"a", "`a.``\"b`", "`111`"};
List<String> measurementList = Arrays.asList(measurements);
```

3. Take executeRawDataQuery as an example:

```java
public SessionDataSet executeRawDataQuery(
    List<String> paths, 
    long startTime, 
    long endTime)
    throws StatementExecutionException, IoTDBConnectionException;
```

If you wish to query the data of the time series root.sg.a, root.sg.\`a.\`\`"b\`, root.sg.\`111\`, the SQL statement you use should be as follows :

```sql
select a from root.sg

# node name contains special characters
select `a.``"b` from root.sg;

# node names are pure numbers
select `111` from root.sg
```

When you call the executeRawDataQuery method, you should assign paths as follows:

```java
// paths
String[] paths = new String[]{"root.sg.a", "root.sg.`a.``\"b`", "root.sg.`111`"};
List<String> pathList = Arrays.asList(paths);
```
