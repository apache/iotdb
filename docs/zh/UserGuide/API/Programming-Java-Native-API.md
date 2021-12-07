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

## Java 原生接口

### 依赖

* JDK >= 1.8
* Maven >= 3.6

### 安装方法

在根目录下运行：

```shell
mvn clean install -pl session -am -Dmaven.test.skip=true
```

### 在 MAVEN 中使用原生接口

```xml
<dependencies>
    <dependency>
      <groupId>org.apache.iotdb</groupId>
      <artifactId>iotdb-session</artifactId>
      <version>0.13.0-SNAPSHOT</version>
    </dependency>
</dependencies>
```

### 原生接口说明

下面将给出 Session 对应的接口的简要介绍和对应参数：

* 初始化 Session

```java
    // 全部使用默认配置
    session = new Session.Builder.build();

    // 指定一个可连接节点
    session = 
        new Session.Builder()
            .host(String host)
            .port(int port)
            .build();

    // 指定多个可连接节点
    session = 
        new Session.Builder()
            .nodeUrls(List<String> nodeUrls)
            .build();

    // 其他配置项
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

* 开启 Session

```java
Session.open()
```

* 关闭 Session

```java
Session.close()
```

* 设置存储组

```java
void setStorageGroup(String storageGroupId)
```

* 删除单个或多个存储组

```java
void deleteStorageGroup(String storageGroup)
void deleteStorageGroups(List<String> storageGroups)
```

* 创建单个或多个时间序列

```java
void createTimeseries(String path, TSDataType dataType,
      TSEncoding encoding, CompressionType compressor, Map<String, String> props,
      Map<String, String> tags, Map<String, String> attributes, String measurementAlias)
      
void createMultiTimeseries(List<String> paths, List<TSDataType> dataTypes,
      List<TSEncoding> encodings, List<CompressionType> compressors,
      List<Map<String, String>> propsList, List<Map<String, String>> tagsList,
      List<Map<String, String>> attributesList, List<String> measurementAliasList)
```

* 创建对齐时间序列

```
void createAlignedTimeseries(String prefixPath, List<String> measurements,
      List<TSDataType> dataTypes, List<TSEncoding> encodings,
      CompressionType compressor, List<String> measurementAliasList);
```

注意：目前**暂不支持**使用传感器别名。

* 删除一个或多个时间序列

```java
void deleteTimeseries(String path)
void deleteTimeseries(List<String> paths)
```

* 删除一个或多个时间序列在某个时间点前或这个时间点的数据

```java
void deleteData(String path, long endTime)
void deleteData(List<String> paths, long endTime)
```

* 插入一个 Record，一个 Record 是一个设备一个时间戳下多个测点的数据。服务器需要做类型推断，可能会有额外耗时

```java
void insertRecord(String prefixPath, long time, List<String> measurements, List<String> values)
```

* 插入一个 Tablet，Tablet 是一个设备若干行非空数据块，每一行的列都相同

```java
void insertTablet(Tablet tablet)
```

* 插入多个 Tablet

```java
void insertTablets(Map<String, Tablet> tablets)
```

* 插入多个 Record。服务器需要做类型推断，可能会有额外耗时

```java
void insertRecords(List<String> deviceIds, List<Long> times, 
                   List<List<String>> measurementsList, List<List<String>> valuesList)
```

* 插入一个 Record，一个 Record 是一个设备一个时间戳下多个测点的数据。提供数据类型后，服务器不需要做类型推断，可以提高性能

```java
void insertRecord(String prefixPath, long time, List<String> measurements,
   List<TSDataType> types, List<Object> values)
```

* 插入多个 Record。提供数据类型后，服务器不需要做类型推断，可以提高性能

```java
void insertRecords(List<String> deviceIds,
        List<Long> times,
        List<List<String>> measurementsList,
        List<List<TSDataType>> typesList,
        List<List<Object>> valuesList)
```

* 插入同属于一个 device 的多个 Record。

```java
void insertRecordsOfOneDevice(String deviceId, List<Long> times,
    List<List<String>> measurementsList, List<List<TSDataType>> typesList,
    List<List<Object>> valuesList)
```

* 原始数据查询。时间间隔包含开始时间，不包含结束时间

```java
SessionDataSet executeRawDataQuery(List<String> paths, long startTime, long endTime)
```

* 执行查询语句

```java
SessionDataSet executeQueryStatement(String sql)
```

* 执行非查询语句

```java
void executeNonQueryStatement(String sql)
```

* 物理量模板内部支持树状结构，可以通过先后创建 Template、InternalNode、MeasurementNode 三类的对象，并通过以下接口创建模板

```java
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

Class InternalNode extends Node {
    boolean shareTime;
    Map<String, Node> children;
    public void setShareTime(boolean shareTime);
    public InternalNode(String name, boolean isShareTime);
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

通过这种方式创建物理量模板的代码示例如下：

```java
MeasurementNode nodeX = new MeasurementNode("x", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);
MeasurementNode nodeY = new MeasurementNode("y", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);
MeasurementNode nodeSpeed = new MeasurementNode("speed", TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY);

InternalNode internalGPS = new InternalNode("GPS", true);
InternalNode internalVehicle = new InternalNode("vehicle", false);

internalGPS.addChild(nodeX);
internalGPS.addChild(nodeY);
internalVehicle.addChild(GPS);
internalVehicle.addChild(nodeSpeed);

Template template = new Template("treeTemplateExample");
template.addToTemplate(internalGPS);
template.addToTemplate(internalVehicle);
template.addToTemplate(nodeSpeed);

createSchemaTemplate(template);
```

* 在创建概念物理量模板以后，还可以通过以下接口增加或删除模板内的物理量。请注意，已经挂载的模板不能删除内部的物理量。

```java
// 为指定模板新增一组对齐的物理量，若其父节点在模板中已经存在，且不要求对齐，则报错
public void addAlignedMeasurementsInTemplate(String templateName,
    						  String[] measurementsPath,
                              TSDataType[] dataTypes,
                              TSEncoding[] encodings,
                              CompressionType[] compressors);

// 为指定模板新增一个对齐物理量, 若其父节点在模板中已经存在，且不要求对齐，则报错
public void addAlignedMeasurementInTemplate(String templateName,
                                String measurementPath,
                                TSDataType dataType,
                                TSEncoding encoding,
                                CompressionType compressor);


// 为指定模板新增一个不对齐物理量, 若其父节在模板中已经存在，且要求对齐，则报错
public void addUnalignedMeasurementInTemplate(String templateName,
                                String measurementPath,
                                TSDataType dataType,
                                TSEncoding encoding,
                                CompressionType compressor);
                                
// 为指定模板新增一组不对齐的物理量, 若其父节在模板中已经存在，且要求对齐，则报错
public void addUnalignedMeasurementsIntemplate(String templateName,
                                String[] measurementPaths,
                                TSDataType[] dataTypes,
                                TSEncoding[] encodings,
                                CompressionType[] compressors);

// 从指定模板中删除一个节点及其子树
public void deleteNodeInTemplate(String templateName, String path);
```

* 对于已经创建的物理量模板，还可以通过以下接口查询模板信息：

```java
// 查询返回目前模板中所有物理量的数量
public int countMeasurementsInTemplate(String templateName);

// 检查模板内指定路径是否为物理量
public boolean isMeasurementInTemplate(String templateName, String path);

// 检查在指定模板内是否存在某路径
public boolean isPathExistInTemplate(String templateName, String path);

// 返回指定模板内所有物理量的路径
public List<String> showMeasurementsInTemplate(String templateName);

// 返回指定模板内某前缀路径下的所有物理量的路径
public List<String> showMeasurementsInTemplate(String templateName, String pattern);
```

* 将名为'templateName'的物理量模板挂载到'prefixPath'路径下，在执行这一步之前，你需要创建名为'templateName'的物理量模板

``` java
void setSchemaTemplate(String templateName, String prefixPath)
```

``` java
void unsetSchemaTemplate(String prefixPath, String templateName)
```

* 请注意，如果一个子树中有多个孩子节点需要使用模板，可以在其共同父母节点上使用 setSchemaTemplate 。而只有在已有数据点插入模板对应的物理量时，模板才会被设置为激活状态，进而被 show timeseries 等查询检测到。
* 卸载'prefixPath'路径下的名为'templateName'的物理量模板。你需要保证给定的路径'prefixPath'下需要有名为'templateName'的物理量模板。

注意：目前不支持从曾经在'prefixPath'路径及其后代节点使用模板插入数据后（即使数据已被删除）卸载模板。

### 测试接口说明

* 测试 testInsertRecords，不实际写入数据，只将数据传输到 server 即返回。

```java
void testInsertRecords(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, List<List<String>> valuesList)
```
  或

```java
void testInsertRecords(List<String> deviceIds, List<Long> times,
      List<List<String>> measurementsList, List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
```

* 测试 insertRecord，不实际写入数据，只将数据传输到 server 即返回。

```java
void testInsertRecord(String deviceId, long time, List<String> measurements, List<String> values)
```
  或

```java
void testInsertRecord(String deviceId, long time, List<String> measurements,
      List<TSDataType> types, List<Object> values)
```

* 测试 insertTablet，不实际写入数据，只将数据传输到 server 即返回。

```java
void testInsertTablet(Tablet tablet)
```

### 针对原生接口的连接池

我们提供了一个针对原生接口的连接池 (`SessionPool`)，使用该接口时，你只需要指定连接池的大小，就可以在使用时从池中获取连接。
如果超过 60s 都没得到一个连接的话，那么会打印一条警告日志，但是程序仍将继续等待。

当一个连接被用完后，他会自动返回池中等待下次被使用；
当一个连接损坏后，他会从池中被删除，并重建一个连接重新执行用户的操作。

对于查询操作：

1. 使用 SessionPool 进行查询时，得到的结果集是`SessionDataSet`的封装类`SessionDataSetWrapper`;
2. 若对于一个查询的结果集，用户并没有遍历完且不再想继续遍历时，需要手动调用释放连接的操作`closeResultSet`;
3. 若对一个查询的结果集遍历时出现异常，也需要手动调用释放连接的操作`closeResultSet`.
4. 可以调用 `SessionDataSetWrapper` 的 `getColumnNames()` 方法得到结果集列名 

使用示例可以参见 `session/src/test/java/org/apache/iotdb/session/pool/SessionPoolTest.java`

或 `example/session/src/main/java/org/apache/iotdb/SessionPoolExample.java`

使用对齐时间序列和物理量模板的示例可以参见 `example/session/src/main/java/org/apache/iotdb/AlignedTimeseriesSessionExample.java`。

### 示例代码

浏览上述接口的详细信息，请参阅代码 ```session/src/main/java/org/apache/iotdb/session/Session.java```

使用上述接口的示例代码在 ```example/session/src/main/java/org/apache/iotdb/SessionExample.java```

### 集群信息相关的接口 （仅在集群模式下可用）

集群信息相关的接口允许用户获取如数据分区情况、节点是否当机等信息。
要使用该 API，需要增加依赖：

```xml
<dependencies>
    <dependency>
      <groupId>org.apache.iotdb</groupId>
      <artifactId>iotdb-thrift-cluster</artifactId>
      <version>0.13.0-SNAPSHOT</version>
    </dependency>
</dependencies>
```

建立连接与关闭连接的示例：

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

API 列表：

* 获取集群中的各个节点的信息（构成哈希环）

```java
list<Node> getRing();
```

* 给定一个路径（应包括一个 SG 作为前缀）和起止时间，获取其覆盖的数据分区情况：

```java 
    /**
     * @param path input path (should contains a Storage group name as its prefix)
     * @return the data partition info. If the time range only covers one data partition, the the size
     * of the list is one.
     */
    list<DataPartitionEntry> getDataPartition(1:string path, 2:long startTime, 3:long endTime);
```

* 给定一个路径（应包括一个 SG 作为前缀），获取其被分到了哪个节点上：
```java  
    /**
     * @param path input path (should contains a Storage group name as its prefix)
     * @return metadata partition information
     */
    list<Node> getMetaPartition(1:string path);
```

* 获取所有节点的死活状态：
```java
    /**
     * @return key: node, value: live or not
     */
    map<Node, bool> getAllNodeStatus();
```

* 获取当前连接节点的 Raft 组信息（投票编号等）（一般用户无需使用该接口）:
```java  
    /**
     * @return A multi-line string with each line representing the total time consumption, invocation
     *     number, and average time consumption.
     */
    string getInstrumentingInfo();
```
