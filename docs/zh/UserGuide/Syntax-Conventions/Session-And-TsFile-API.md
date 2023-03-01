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

在使用Session、TsFIle API时，如果您调用的方法需要以字符串形式传入物理量（measurement）、设备（device）、数据库（database）、路径（path）等参数，**请保证所传入字符串与使用 SQL 语句时的写法一致**，下面是一些帮助您理解的例子。具体代码示例可以参考：`example/session/src/main/java/org/apache/iotdb/SyntaxConventionRelatedExample.java`

1. 以创建时间序列 createTimeseries 为例：

```java
public void createTimeseries(
    String path,
    TSDataType dataType,
    TSEncoding encoding,
    CompressionType compressor)
    throws IoTDBConnectionException, StatementExecutionException;
```

如果您希望创建时间序列 root.sg.a，root.sg.\`a.\`\`"b\`，root.sg.\`111\`，您使用的 SQL 语句应该如下所示：

```sql
create timeseries root.sg.a with datatype=FLOAT,encoding=PLAIN,compressor=SNAPPY;

# 路径结点名中包含特殊字符，时间序列各结点为["root","sg","a.`\"b"]
create timeseries root.sg.`a.``"b` with datatype=FLOAT,encoding=PLAIN,compressor=SNAPPY;

# 路径结点名为实数
create timeseries root.sg.`111` with datatype=FLOAT,encoding=PLAIN,compressor=SNAPPY;
```

您在调用 createTimeseries 方法时，应该按照如下方法赋值 path 字符串，保证 path 字符串内容与使用 SQL 时一致：

```java
// 时间序列 root.sg.a
String path = "root.sg.a";

// 时间序列 root.sg.`a``"b`
String path = "root.sg.`a``\"b`";

// 时间序列 root.sg.`111`
String path = "root.sg.`111`";
```

2. 以插入数据 insertRecord 为例：

```java
public void insertRecord(
    String deviceId,
    long time,
    List<String> measurements,
    List<TSDataType> types,
    Object... values)
    throws IoTDBConnectionException, StatementExecutionException;
```

如果您希望向时间序列 root.sg.a，root.sg.\`a.\`\`"b\`，root.sg.\`111\`中插入数据，您使用的 SQL 语句应该如下所示：

```sql
insert into root.sg(timestamp, a, `a.``"b`, `111`) values (1, 2, 2, 2);
```

您在调用 insertRecord 方法时，应该按照如下方法赋值 deviceId 和 measurements：

```java
// deviceId 为 root.sg
String deviceId = "root.sg";

// measurements
String[] measurements = new String[]{"a", "`a.``\"b`", "`111`"};
List<String> measurementList = Arrays.asList(measurements);
```

3. 以查询数据 executeRawDataQuery 为例：

```java
public SessionDataSet executeRawDataQuery(
    List<String> paths, 
    long startTime, 
    long endTime)
    throws StatementExecutionException, IoTDBConnectionException;
```

如果您希望查询时间序列 root.sg.a，root.sg.\`a.\`\`"b\`，root.sg.\`111\`的数据，您使用的 SQL 语句应该如下所示：

```sql
select a from root.sg

# 路径结点名中包含特殊字符
select `a.``"b` from root.sg;

# 路径结点名为实数
select `111` from root.sg
```

您在调用 executeRawDataQuery 方法时，应该按照如下方法赋值 paths：

```java
// paths
String[] paths = new String[]{"root.sg.a", "root.sg.`a.``\"b`", "root.sg.`111`"};
List<String> pathList = Arrays.asList(paths);
```
