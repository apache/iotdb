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

```xml
<dependencies>
    <dependency>
      <groupId>org.apache.iotdb</groupId>
      <artifactId>iotdb-session</artifactId>
      <version>0.13.0-SNAPSHOT</version>
    </dependency>
</dependencies>
```



### Native APIs Description

Here we show the commonly used interfaces and their parameters in the Native API:

* Initialize a Session

```java
    // use default configuration 
    session = new Session.Builder.build();

    // initialize with a single node
    session = 
        new Session.Builder()
            .host(String host)
            .port(int port)
            .build();

    // initialize with multiple nodes
    session = 
        new Session.Builder()
            .nodeUrls(List<String> nodeUrls)
            .build();

    // other configurations
    session = 
        new Session.Builder()
            .fetchSize(int fetchSize)
            .username(String username)
            .password(String password)
            .thriftDefaultBufferSize(int thriftDefaultBufferSize)
            .thriftMaxFrameSize(int thriftMaxFrameSize)
            .enableCacheLeader(boolean enableCacheLeader)
            .build();
```

* Open a Session

```java
Session.open()
```

* Close a Session

```java
Session.close()
```

* Set storage group

```java
void setStorageGroup(String storageGroupId)    
```

* Delete one or several storage groups

```java
void deleteStorageGroup(String storageGroup)
void deleteStorageGroups(List<String> storageGroups)
```

* Create one or multiple timeseries

```java
void createTimeseries(String path, TSDataType dataType,
      TSEncoding encoding, CompressionType compressor, Map<String, String> props,
      Map<String, String> tags, Map<String, String> attributes, String measurementAlias)
      
void createMultiTimeseries(List<String> paths, List<TSDataType> dataTypes,
      List<TSEncoding> encodings, List<CompressionType> compressors,
      List<Map<String, String>> propsList, List<Map<String, String>> tagsList,
      List<Map<String, String>> attributesList, List<String> measurementAliasList)
```

* Create aligned timeseries
```
void createAlignedTimeseries(String prefixPath, List<String> measurements,
      List<TSDataType> dataTypes, List<TSEncoding> encodings,
      CompressionType compressor, List<String> measurementAliasList);
```

Attention: Alias of measurements are **not supported** currently.

* Delete one or several timeseries

```java
void deleteTimeseries(String path)
void deleteTimeseries(List<String> paths)
```

* Delete data before or equal to a timestamp of one or several timeseries

```java
void deleteData(String path, long time)
void deleteData(List<String> paths, long time)
```

* Insert a Record，which contains multiple measurement value of a device at a timestamp. Without type info the server has to do type inference, which may cost some time

```java
void insertRecord(String prefixPath, long time, List<String> measurements, List<String> values)
```

* Insert a Tablet，which is multiple rows of a device, each row has the same measurements

```java
void insertTablet(Tablet tablet)
```

* Insert multiple Tablets

```java
void insertTablets(Map<String, Tablet> tablet)
```

* Insert multiple Records. Without type info the server has to do type inference, which may cost some time

```java
void insertRecords(List<String> deviceIds, List<Long> times, 
                   List<List<String>> measurementsList, List<List<String>> valuesList)
```

* Insert a Record, which contains multiple measurement value of a device at a timestamp. With type info the server has no need to do type inference, which leads a better performance

```java
void insertRecord(String deviceId, long time, List<String> measurements,
   List<TSDataType> types, List<Object> values)
```

* Insert multiple Records. With type info the server has no need to do type inference, which leads a better performance

```java
void insertRecords(List<String> deviceIds, List<Long> times,
    List<List<String>> measurementsList, List<List<TSDataType>> typesList,
    List<List<Object>> valuesList)
```
* Insert multiple Records that belong to the same device. 
  With type info the server has no need to do type inference, which leads a better performance

```java
void insertRecordsOfOneDevice(String deviceId, List<Long> times,
    List<List<String>> measurementsList, List<List<TSDataType>> typesList,
    List<List<Object>> valuesList)
```

* Raw data query. Time interval include startTime and exclude endTime

```java
SessionDataSet executeRawDataQuery(List<String> paths, long startTime, long endTime)
```

* Execute query statement

```java
SessionDataSet executeQueryStatement(String sql)
```

* Execute non query statement

```java
void executeNonQueryStatement(String sql)
```

* Create a physical quantity template

```

* name: template name
* measurements: List of measurements, if it is a single measurement, just put it's name
*     into a list and add to measurements if it is a vector measurement, put all measurements of
*     the vector into a list and add to measurements
* dataTypes: List of datatypes, if it is a single measurement, just put it's type into a
*     list and add to dataTypes if it is a vector measurement, put all types of the vector
*     into a list and add to dataTypes
* encodings: List of encodings, if it is a single measurement, just put it's encoding into
*     a list and add to encodings if it is a vector measurement, put all encodings of the
*     vector into a list and add to encodings
* compressors: List of compressors                            
void createSchemaTemplate(
      String templateName,
      List<String> schemaName,
      List<List<String>> measurements,
      List<List<TSDataType>> dataTypes,
      List<List<TSEncoding>> encodings,
      List<CompressionType> compressors)
```

* Mount the physical quantity template named'templateName' to the path'prefixPath'. Before performing this step, you need to create a physical quantity template named'templateName'

```java
void setSchemaTemplate(String templateName, String prefixPath)
```



### Test APIs Description

* To test testInsertRecords, do not actually write data, just transmit the data to the server and then return.

```java
void testInsertRecords(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, List<List<String>> valuesList)
```
  or
```java
void testInsertRecords(List<String> deviceIds, List<Long> times,
    List<List<String>> measurementsList, List<List<TSDataType>> typesList,
    List<List<Object>> valuesList)
```

* Test insertRecord, do not actually write data, just transfer the data to the server and then return.

```java
void testInsertRecord(String deviceId, long time, List<String> measurements, List<String> values)
```
or
```java
void testInsertRecord(String deviceId, long time, List<String> measurements,
      List<TSDataType> types, List<Object> values)
```

* Test insertTablet, do not actually write data, only transfer the data to the server and then return.

```java
void testInsertTablet(Tablet tablet)
```



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

For examples of aligned timeseries and measurement template, you can refer to `example/session/src/main/java/org/apache/iotdb/AlignedTimeseriesSessionExample.java`



### Coding Examples

To get more information of the following interfaces, please view session/src/main/java/org/apache/iotdb/session/Session.java

The sample code of using these interfaces is in example/session/src/main/java/org/apache/iotdb/SessionExample.java，which provides an example of how to open an IoTDB session, execute a batch insertion.



### Cluster information related APIs (only works in the cluster mode)

Cluster information related APIs allow users get the cluster info like where a storage group will be 
partitioned to, the status of each node in the cluster.

To use the APIs, add dependency in your pom file:

```xml
<dependencies>
    <dependency>
      <groupId>org.apache.iotdb</groupId>
      <artifactId>iotdb-thrift-cluster</artifactId>
      <version>0.13.0-SNAPSHOT</version>
    </dependency>
</dependencies>
```

How to open a connection:

```java
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.iotdb.rpc.RpcTransportFactory;

    public class CluserInfoClient {
      TTransport transport;
      ClusterInfoService.Client client;
      public void connect() {
          transport =
              RpcTransportFactory.INSTANCE.getTransport(
                  new TSocket(
                      // the RPC address
                      IoTDBDescriptor.getInstance().getConfig().getRpcAddress(),
                      // the RPC port
                      ClusterDescriptor.getInstance().getConfig().getClusterRpcPort()));
          try {
            transport.open();
          } catch (TTransportException e) {
            Assert.fail(e.getMessage());
          }
          //get the client
          client = new ClusterInfoService.Client(new TBinaryProtocol(transport));
       }
      public void close() {
        transport.close();
      }  
    }
```

APIs in `ClusterInfoService.Client`:


* Get the physical hash ring of the cluster:

```java
list<Node> getRing();
```

* Get data partition information of input path and time range:

```java 
    /**
     * @param path input path (should contains a Storage group name as its prefix)
     * @return the data partition info. If the time range only covers one data partition, the the size
     * of the list is one.
     */
    list<DataPartitionEntry> getDataPartition(1:string path, 2:long startTime, 3:long endTime);
```

* Get metadata partition information of input path:
```java  
    /**
     * @param path input path (should contains a Storage group name as its prefix)
     * @return metadata partition information
     */
    list<Node> getMetaPartition(1:string path);
```

* Get the status (alive or not) of all nodes:
```java
    /**
     * @return key: node, value: live or not
     */
    map<Node, bool> getAllNodeStatus();
```

* get the raft group info (voteFor, term, etc..) of the connected node
  (Notice that this API is rarely used by users):
```java  
    /**
     * @return A multi-line string with each line representing the total time consumption, invocation
     *     number, and average time consumption.
     */
    string getInstrumentingInfo();
```

