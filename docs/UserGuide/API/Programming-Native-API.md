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

## Java Native API

### Dependencies

* JDK >= 1.8
* Maven >= 3.6

### Installation

In root directory:
> mvn clean install -pl session -am -DskipTests

### Using IoTDB Java Native API with Maven

```
<dependencies>
    <dependency>
      <groupId>org.apache.iotdb</groupId>
      <artifactId>iotdb-session</artifactId>
      <version>0.12.0</version>
    </dependency>
</dependencies>
```


### Native APIs

Here we show the commonly used interfaces and their parameters in the Native API:

* Initialize a Session

```
Session(String host, int rpcPort)

Session(String host, String rpcPort, String username, String password)

Session(String host, int rpcPort, String username, String password)
```

* Open a Session

```
Session.open()
```

* Close a Session

```
​Session.close()
```
  
* Set storage group

```
void setStorageGroup(String storageGroupId)    
```

* Delete one or several storage groups

```
void deleteStorageGroup(String storageGroup)
void deleteStorageGroups(List<String> storageGroups)
```

* Create one or multiple timeseries

```
void createTimeseries(String path, TSDataType dataType,
      TSEncoding encoding, CompressionType compressor, Map<String, String> props,
      Map<String, String> tags, Map<String, String> attributes, String measurementAlias)
      
void createMultiTimeseries(List<String> paths, List<TSDataType> dataTypes,
      List<TSEncoding> encodings, List<CompressionType> compressors,
      List<Map<String, String>> propsList, List<Map<String, String>> tagsList,
      List<Map<String, String>> attributesList, List<String> measurementAliasList)
```

* Delete one or several timeseries

```
void deleteTimeseries(String path)
void deleteTimeseries(List<String> paths)
```

* Delete data before or equal to a timestamp of one or several timeseries

```
void deleteData(String path, long time)
void deleteData(List<String> paths, long time)
```

* Insert a Record，which contains multiple measurement value of a device at a timestamp. Without type info the server has to do type inference, which may cost some time

```
void insertRecord(String deviceId, long time, List<String> measurements, List<String> values)
```

* Insert a Tablet，which is multiple rows of a device, each row has the same measurements

```
void insertTablet(Tablet tablet)
```

* Insert multiple Tablets

```
void insertTablets(Map<String, Tablet> tablet)
```
  
* Insert multiple Records. Without type info the server has to do type inference, which may cost some time

```
void insertRecords(List<String> deviceIds, List<Long> times, 
                   List<List<String>> measurementsList, List<List<String>> valuesList)
```
  
* Insert a Record，which contains multiple measurement value of a device at a timestamp. With type info the server has no need to do type inference, which leads a better performance

```
void insertRecord(String deviceId, long time, List<String> measurements,
   List<TSDataType> types, List<Object> values)
```

* Insert multiple Records. With type info the server has no need to do type inference, which leads a better performance

```
void insertRecords(List<String> deviceIds, List<Long> times,
    List<List<String>> measurementsList, List<List<TSDataType>> typesList,
    List<List<Object>> valuesList)
```
* Insert multiple Records that belong to the same device. 
  With type info the server has no need to do type inference, which leads a better performance

```
void insertRecordsOfOneDevice(String deviceId, List<Long> times,
    List<List<String>> measurementsList, List<List<TSDataType>> typesList,
    List<List<Object>> valuesList)
```
  
* Raw data query. Time interval include startTime and exclude endTime

```
SessionDataSet executeRawDataQuery(List<String> paths, long startTime, long endTime)
```

* Execute query statement

```
SessionDataSet executeQueryStatement(String sql)
```
  
* Execute non query statement

```
void executeNonQueryStatement(String sql)
```

### Native APIs for profiling network cost

* Test the network and client cost of insertRecords. This method NOT insert data into database and server just return after accept the request, this method should be used to test other time cost in client

```
void testInsertRecords(List<String> deviceIds, List<Long> times,
              List<List<String>> measurementsList, List<List<String>> valuesList)
```
  or
```
void testInsertRecords(List<String> deviceIds, List<Long> times,
    List<List<String>> measurementsList, List<List<TSDataType>> typesList,
    List<List<Object>> valuesList)
```

* Test the network and client cost of insertRecordsOfOneDevice. 
This method NOT insert data into database and server just return after accept the request, 
this method should be used to test other time cost in client

```
void testInsertRecordsOfOneDevice(String deviceId, List<Long> times,
    List<List<String>> measurementsList, List<List<TSDataType>> typesList,
    List<List<Object>> valuesList)
```  

* Test the network and client cost of insertRecord. This method NOT insert data into database and server just return after accept the request, this method should be used to test other time cost in client

```
void testInsertRecord(String deviceId, long time, List<String> measurements, List<String> values)
```
  or
```
void testInsertRecord(String deviceId, long time, List<String> measurements,
    List<TSDataType> types, List<Object> values)
```

* Test the network and client cost of insertTablet. This method NOT insert data into database and server just return after accept the request, this method should be used to test other time cost in client

```
void testInsertTablet(Tablet tablet)
```
  
### Coding example

To get more information of the following interfaces, please view session/src/main/java/org/apache/iotdb/session/Session.java

The sample code of using these interfaces is in example/session/src/main/java/org/apache/iotdb/SessionExample.java，which provides an example of how to open an IoTDB session, execute a batch insertion.


### Session Pool for Native API

We provide a connection pool (`SessionPool) for Native API.
Using the interface, you need to define the pool size.

If you can not get a session connection in 60 seconds, there is a warning log but the program will hang.

If a session has finished an operation, it will be put back to the pool automatically.
If a session connection is broken, the session will be removed automatically and the pool will try 
to create a new session and redo the operation.

For query operations:

1. When using SessionPool to query data, the result set is `SessionDataSetWrapper`;
2. Given a `SessionDataSetWrapper`, if you have not scanned all the data in it and stop to use it,
you have to call `SessionPool.closeResultSet(wrapper)` manually;
3. When you call `hasNext()` and `next()` of a `SessionDataSetWrapper` and there is an exception, then
you have to call `SessionPool.closeResultSet(wrapper)` manually;
4. You can call `getColumnNames()` of `SessionDataSetWrapper` to get the column names of query result;

Examples: ```session/src/test/java/org/apache/iotdb/session/pool/SessionPoolTest.java```

Or `example/session/src/main/java/org/apache/iotdb/SessionPoolExample.java`


### New Interfaces

```
void open(boolean enableRPCCompression)
```

Open a session, with a parameter to specify whether to enable RPC compression. 
Please pay attention that this RPC compression status of client must comply with the status of IoTDB server

```
void insertRecord(String deviceId, long time, List<String> measurements,
      List<TSDataType> types, List<Object> values)
```

Insert one record, in a way that user has to provide the type information of each measurement, which is different from the original insertRecord() interface.
The values should be provided in their primitive types. This interface is more proficient than the one without type parameters.

```
void insertRecords(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, 
                   List<List<TSDataType>> typesList, List<List<Object>> valuesList)
```

Insert multiple records with type parameters. This interface is more proficient than the one without type parameters.

```
void insertTablet(Tablet tablet, boolean sorted)
```

An additional insertTablet() interface that providing a "sorted" parameter indicating if the tablet is in order. A sorted tablet may accelerate the insertion process.

```
void insertTablets(Map<String, Tablet> tablets)
```

A new insertTablets() for inserting multiple tablets. 

```
void insertTablets(Map<String, Tablet> tablets, boolean sorted)
```

insertTablets() with an additional "sorted" parameter. 

```
void testInsertRecord(String deviceId, long time, List<String> measurements, List<TSDataType> types, 
                      List<Object> values)
void testInsertRecords(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, 
                      List<List<TSDataType>> typesList, List<List<Object>> valuesList)
void testInsertTablet(Tablet tablet, boolean sorted)
void testInsertTablets(Map<String, Tablet> tablets)
void testInsertTablets(Map<String, Tablet> tablets, boolean sorted)
```

The above interfaces are newly added to test responsiveness of new insert interfaces.

```
void createTimeseries(String path, TSDataType dataType, TSEncoding encoding, CompressionType compressor, 	
                      Map<String, String> props, Map<String, String> tags, Map<String, String> attributes, 
                      String measurementAlias)
```

Create a timeseries with path, datatype, encoding and compression. Additionally, users can provide props, tags, attributes and measurementAlias。

```
void createMultiTimeseries(List<String> paths, List<TSDataType> dataTypes, List<TSEncoding> encodings, 
                           List<CompressionType> compressors, List<Map<String, String>> propsList, 
                           List<Map<String, String>> tagsList, List<Map<String, String>> attributesList, 
                           List<String> measurementAliasList)
```

Create multiple timeseries with a single method. Users can provide props, tags, attributes and measurementAlias as well for detailed timeseries information.

```
boolean checkTimeseriesExists(String path)
```

Add a method to check whether the specific timeseries exists.

```
public Session(String host, int rpcPort, String username, String password,
      boolean isEnableCacheLeader)
```

Open a session and specifies whether the Leader cache is enabled. Note that this interface improves performance for distributed IoTDB, but adds less cost to the client for stand-alone IoTDB.