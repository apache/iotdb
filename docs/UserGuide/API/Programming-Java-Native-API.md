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

# Java Native API

## Installation

### Dependencies

* JDK >= 1.8
* Maven >= 3.6

### How to install

In root directory:
> mvn clean install -pl iotdb-client/session -am -DskipTests

### Using IoTDB Java Native API with Maven

```xml
<dependencies>
    <dependency>
      <groupId>org.apache.iotdb</groupId>
      <artifactId>iotdb-session</artifactId>
      <version>1.0.0</version>
    </dependency>
</dependencies>
```

## Syntax Convention

- **IoTDB-SQL interface:** The input SQL parameter needs to conform to the [syntax conventions](../Syntax-Conventions/Literal-Values.md) and be escaped for JAVA strings. For example, you need to add a backslash before the double-quotes. (That is: after JAVA escaping, it is consistent with the SQL statement executed on the command line.)
- **Other interfaces:**
  - The node names in path or path prefix as parameter: The node names which should be escaped by backticks (`) in the SQL statement,  escaping is required here.
  - Identifiers (such as template names) as parameters: The identifiers which should be escaped by backticks (`) in the SQL statement, and escaping is not required here.
- **Code example for syntax convention could be found at:** `example/session/src/main/java/org/apache/iotdb/SyntaxConventionRelatedExample.java`

## Native APIs

Here we show the commonly used interfaces and their parameters in the Native API:

### Initialization

* Initialize a Session

``` java
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
        .enableRedirection(boolean enableRedirection)
        .version(Version version)
        .build();
```

Version represents the SQL semantic version used by the client, which is used to be compatible with the SQL semantics of 0.12 when upgrading 0.13. The possible values are: `V_0_12`, `V_0_13`, `V_1_0`.

* Open a Session

``` java
void open()
```

* Open a session, with a parameter to specify whether to enable RPC compression
  
``` java
void open(boolean enableRPCCompression)
```

Notice: this RPC compression status of client must comply with that of IoTDB server

* Close a Session

``` java
void close()
```

### Data Definition Interface (DDL Interface)

#### Database Management

* CREATE DATABASE

``` java
void setStorageGroup(String storageGroupId)    
```

* Delete one or several databases

``` java
void deleteStorageGroup(String storageGroup)
void deleteStorageGroups(List<String> storageGroups)
```

#### Timeseries Management

* Create one or multiple timeseries

``` java
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
      List <CompressionType> compressors, List<String> measurementAliasList);
```

Attention: Alias of measurements are **not supported** currently.

* Delete one or several timeseries

``` java
void deleteTimeseries(String path)
void deleteTimeseries(List<String> paths)
```

* Check whether the specific timeseries exists.

``` java
boolean checkTimeseriesExists(String path)
```

#### Schema Template


Create a schema template for massive identical devices will help to improve memory performance. You can use Template, InternalNode and MeasurementNode to depict the structure of the template, and use belowed interface to create it inside session.

``` java
public void createSchemaTemplate(Template template);

Class Template {
    private String name;
    private boolean directShareTime;
    Map<String, Node> children;
    public Template(String name, boolean isShareTime);
    
    public void addToTemplate(Node node);
    public void deleteFromTemplate(String name);
    public void setShareTime(boolean shareTime);
}

Abstract Class Node {
    private String name;
    public void addChild(Node node);
    public void deleteChild(Node node);
}

Class MeasurementNode extends Node {
    TSDataType dataType;
    TSEncoding encoding;
    CompressionType compressor;
    public MeasurementNode(String name, 
                           TSDataType dataType, 
                           TSEncoding encoding,
                          CompressionType compressor);
}
```

We strongly suggest you implement templates only with flat-measurement (like object 'flatTemplate' in belowed snippet), since tree-structured template may not be a long-term supported feature in further version of IoTDB.

A snippet of using above Method and Class：

``` java
MeasurementNode nodeX = new MeasurementNode("x", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);
MeasurementNode nodeY = new MeasurementNode("y", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);
MeasurementNode nodeSpeed = new MeasurementNode("speed", TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY);

// This is the template we suggest to implement
Template flatTemplate = new Template("flatTemplate");
template.addToTemplate(nodeX);
template.addToTemplate(nodeY);
template.addToTemplate(nodeSpeed);

createSchemaTemplate(flatTemplate);
```

To implement schema template, you can set the measurement template named 'templateName' at path 'prefixPath'.

**Please notice that, we strongly recommend not setting templates on the nodes above the database to accommodate future updates and collaboration between modules.**

``` java
void setSchemaTemplate(String templateName, String prefixPath)
```

Before setting template, you should firstly create the template using

``` java
void createSchemaTemplate(Template template)
```

After setting template to a certain path, you can use the template to create timeseries on given device paths through the following interface, or you can write data directly to trigger timeseries auto creation using schema template under target devices. 

``` java
void createTimeseriesUsingSchemaTemplate(List<String> devicePathList)
```

After setting template to a certain path, you can query for info about template using belowed interface in session:

``` java
/** @return All template names. */
public List<String> showAllTemplates();

/** @return All paths have been set to designated template. */
public List<String> showPathsTemplateSetOn(String templateName);

/** @return All paths are using designated template. */
public List<String> showPathsTemplateUsingOn(String templateName)
```

If you are ready to get rid of schema template, you can drop it with belowed interface. Make sure the template to drop has been unset from MTree.

``` java
void unsetSchemaTemplate(String prefixPath, String templateName);
public void dropSchemaTemplate(String templateName);
```

Unset the measurement template named 'templateName' from path 'prefixPath'. When you issue this interface, you should assure that there is a template named 'templateName' set at the path 'prefixPath'.

Attention: Unsetting the template named 'templateName' from node at path 'prefixPath' or descendant nodes which have already inserted records using template is **not supported**.


### Data Manipulation Interface (DML Interface)

#### Insert

It is recommended to use insertTablet to help improve write efficiency.

* Insert a Tablet，which is multiple rows of a device, each row has the same measurements
  * **Better Write Performance**
  * **Support null values**: fill the null value with any value, and then mark the null value via BitMap

``` java
void insertTablet(Tablet tablet)

public class Tablet {
  /** deviceId of this tablet */
  public String prefixPath;
  /** the list of measurement schemas for creating the tablet */
  private List<MeasurementSchema> schemas;
  /** timestamps in this tablet */
  public long[] timestamps;
  /** each object is a primitive type array, which represents values of one measurement */
  public Object[] values;
  /** each bitmap represents the existence of each value in the current column. */
  public BitMap[] bitMaps;
  /** the number of rows to include in this tablet */
  public int rowSize;
  /** the maximum number of rows for this tablet */
  private int maxRowNumber;
  /** whether this tablet store data of aligned timeseries or not */
  private boolean isAligned;
}
```

* Insert multiple Tablets

``` java
void insertTablets(Map<String, Tablet> tablet)
```

* Insert a Record, which contains multiple measurement value of a device at a timestamp. This method is equivalent to providing a common interface for multiple data types of values. Later, the value can be cast to the original type through TSDataType.

  The correspondence between the Object type and the TSDataType type is shown in the following table.

  | TSDataType | Object         |
  | ---------- | -------------- |
  | BOOLEAN    | Boolean        |
  | INT32      | Integer        |
  | INT64      | Long           |
  | FLOAT      | Float          |
  | DOUBLE     | Double         |
  | TEXT       | String, Binary |

``` java
void insertRecord(String deviceId, long time, List<String> measurements,
   List<TSDataType> types, List<Object> values)
```

* Insert multiple Records

``` java
void insertRecords(List<String> deviceIds, List<Long> times,
    List<List<String>> measurementsList, List<List<TSDataType>> typesList,
    List<List<Object>> valuesList)
```
* Insert multiple Records that belong to the same device. 
  With type info the server has no need to do type inference, which leads a better performance

``` java
void insertRecordsOfOneDevice(String deviceId, List<Long> times,
    List<List<String>> measurementsList, List<List<TSDataType>> typesList,
    List<List<Object>> valuesList)
```

#### Insert with type inference

When the data is of String type, we can use the following interface to perform type inference based on the value of the value itself. For example, if value is "true" , it can be automatically inferred to be a boolean type. If value is "3.2" , it can be automatically inferred as a flout type. Without type information, server has to do type inference, which may cost some time.

* Insert a Record, which contains multiple measurement value of a device at a timestamp

``` java
void insertRecord(String prefixPath, long time, List<String> measurements, List<String> values)
```

* Insert multiple Records

``` java
void insertRecords(List<String> deviceIds, List<Long> times, 
   List<List<String>> measurementsList, List<List<String>> valuesList)
```

* Insert multiple Records that belong to the same device.

``` java
void insertStringRecordsOfOneDevice(String deviceId, List<Long> times,
        List<List<String>> measurementsList, List<List<String>> valuesList)
```

#### Insert of Aligned Timeseries

The Insert of aligned timeseries uses interfaces like insertAlignedXXX, and others are similar to the above interfaces:

* insertAlignedRecord
* insertAlignedRecords
* insertAlignedRecordsOfOneDevice
* insertAlignedStringRecordsOfOneDevice
* insertAlignedTablet
* insertAlignedTablets

#### Delete

* Delete data before or equal to a timestamp of one or several timeseries

``` java
void deleteData(String path, long time)
void deleteData(List<String> paths, long time)
```

#### Query

* Time-series raw data query with time range:
  - The specified query time range is a left-closed right-open interval, including the start time but excluding the end time.

``` java
SessionDataSet executeRawDataQuery(List<String> paths, long startTime, long endTime);
```

* Last query: 
  - Query the last data, whose timestamp is greater than or equal LastTime.

``` java
SessionDataSet executeLastDataQuery(List<String> paths, long LastTime);
```

* Aggregation query:
  - Support specified query time range: The specified query time range is a left-closed right-open interval, including the start time but not the end time.
  - Support GROUP BY TIME.

``` java
SessionDataSet executeAggregationQuery(List<String> paths, List<Aggregation> aggregations);

SessionDataSet executeAggregationQuery(
    List<String> paths, List<Aggregation> aggregations, long startTime, long endTime);

SessionDataSet executeAggregationQuery(
    List<String> paths,
    List<Aggregation> aggregations,
    long startTime,
    long endTime,
    long interval);

SessionDataSet executeAggregationQuery(
    List<String> paths,
    List<Aggregation> aggregations,
    long startTime,
    long endTime,
    long interval,
    long slidingStep);
```

### IoTDB-SQL Interface

* Execute query statement

``` java
SessionDataSet executeQueryStatement(String sql)
```

* Execute non query statement

``` java
void executeNonQueryStatement(String sql)
```

### Write Test Interface (to profile network cost)

These methods **don't** insert data into database and server just return after accept the request.

* Test the network and client cost of insertRecord

``` java
void testInsertRecord(String deviceId, long time, List<String> measurements, List<String> values)

void testInsertRecord(String deviceId, long time, List<String> measurements,
        List<TSDataType> types, List<Object> values)
```

* Test the network and client cost of insertRecords

``` java
void testInsertRecords(List<String> deviceIds, List<Long> times,
        List<List<String>> measurementsList, List<List<String>> valuesList)
        
void testInsertRecords(List<String> deviceIds, List<Long> times,
        List<List<String>> measurementsList, List<List<TSDataType>> typesList
        List<List<Object>> valuesList)
```

* Test the network and client cost of insertTablet

``` java
void testInsertTablet(Tablet tablet)
```

* Test the network and client cost of insertTablets

``` java
void testInsertTablets(Map<String, Tablet> tablets)
```

### Coding Examples

To get more information of the following interfaces, please view session/src/main/java/org/apache/iotdb/session/Session.java

The sample code of using these interfaces is in example/session/src/main/java/org/apache/iotdb/SessionExample.java，which provides an example of how to open an IoTDB session, execute a batch insertion.

For examples of aligned timeseries and measurement template, you can refer to `example/session/src/main/java/org/apache/iotdb/AlignedTimeseriesSessionExample.java`


## Session Pool for Native API

We provide a connection pool (`SessionPool) for Native API.
Using the interface, you need to define the pool size.

If you can not get a session connection in 60 seconds, there is a warning log but the program will hang.

If a session has finished an operation, it will be put back to the pool automatically.
If a session connection is broken, the session will be removed automatically and the pool will try 
to create a new session and redo the operation.
You can also specify an url list of multiple reachable nodes when creating a SessionPool, just as you would when creating a Session. To ensure high availability of clients in distributed cluster.

For query operations:

1. When using SessionPool to query data, the result set is `SessionDataSetWrapper`;
2. Given a `SessionDataSetWrapper`, if you have not scanned all the data in it and stop to use it,
you have to call `SessionPool.closeResultSet(wrapper)` manually;
3. When you call `hasNext()` and `next()` of a `SessionDataSetWrapper` and there is an exception, then
you have to call `SessionPool.closeResultSet(wrapper)` manually;
4. You can call `getColumnNames()` of `SessionDataSetWrapper` to get the column names of query result;

Examples: ```session/src/test/java/org/apache/iotdb/session/pool/SessionPoolTest.java```

Or `example/session/src/main/java/org/apache/iotdb/SessionPoolExample.java`

