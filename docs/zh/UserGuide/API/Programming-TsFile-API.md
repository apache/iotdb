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

## TsFile API 

TsFile 是在 IoTDB 中使用的时间序列的文件格式。在这个章节中，我们将介绍这种文件格式的用法。 

### 安装 TsFile library

在您自己的项目中有两种方法使用 TsFile .

* 使用 jar 包：编译源码生成 jar 包
	
```shell
git clone https://github.com/apache/iotdb.git
cd tsfile/
mvn clean package -Dmaven.test.skip=true
```

命令执行完成之后，所有的 jar 包都可以从 `target/` 目录下找到。之后您可以在自己的工程中导入 `target/tsfile-1.0.0.jar`.
	
* 使用 Maven 依赖：

编译源码并且部署到您的本地仓库中需要 3 步：

 1. 下载源码

 ```shell
git clone https://github.com/apache/iotdb.git
 ```
 2. 编译源码和部署到本地仓库

 ```shell
cd tsfile/
mvn clean install -Dmaven.test.skip=true
 ```
 3. 在您自己的工程中增加依赖：

 ```xml
 <dependency>
   <groupId>org.apache.iotdb</groupId>
   <artifactId>tsfile</artifactId>
   <version>0.12.0</version>
 </dependency>
 ```

或者，您可以直接使用官方的 Maven 仓库：

  1. 首先，在`${username}\.m2\settings.xml`目录下的`settings.xml`文件中`<profiles>`
     节中增加`<profile>`，内容如下：
     
 ```xml
<profile>
   <id>allow-snapshots</id>
      <activation><activeByDefault>true</activeByDefault></activation>
   <repositories>
     <repository>  
        <id>apache.snapshots</id>
        <name>Apache Development Snapshot Repository</name>
        <url>https://repository.apache.org/content/repositories/snapshots/</url>
        <releases>
            <enabled>false</enabled>
        </releases>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
      </repository>
   </repositories>
 </profile>
 ```
  2. 之后您可以在您的工程中增加如下依赖：

 ```xml
 <dependency>
   <groupId>org.apache.iotdb</groupId>
   <artifactId>tsfile</artifactId>
   <version>1.0.0</version>
 </dependency>
 ```

### TsFile 的使用

本章节演示 TsFile 的详细用法。

时序数据 (Time-series Data)
一个时序是由 4 个序列组成，分别是 device, measurement, time, value。

* **measurement**: 时间序列描述的是一个物理或者形式的测量 (measurement)，比如：城市的温度，一些商品的销售数量或者是火车在不同时间的速度。
传统的传感器（如温度计）也采用单次测量 (measurement) 并产生时间序列，我们将在下面交替使用测量 (measurement) 和传感器。

* **device**: 一个设备指的是一个正在进行多次测量（产生多个时间序列）的实体，例如，
  ​    ​    ​    一列正在运行的火车监控它的速度、油表、它已经运行的英里数，当前的乘客每个都被传送到一个时间序列。

**单行数据**: 在许多工业应用程序中，一个设备通常包含多个传感器，这些传感器可能同时具有多个值，这称为一行数据。

在形式上，一行数据包含一个`device_id`，它是一个时间戳，表示从 1970 年 1 月 1 日 00:00:00 开始的毫秒数，
以及由`measurement_id`和相应的`value`组成的几个数据对。一行中的所有数据对都属于这个`device_id`，并且具有相同的时间戳。
如果其中一个度量值`measurements`在某个时间戳`timestamp`没有值`value`，将使用一个空格表示（实际上 TsFile 并不存储 null 值）。
其格式如下：

```
device_id, timestamp, <measurement_id, value>...
```

示例数据如下所示。在本例中，两个度量值 (measurement) 的数据类型分别是`INT32`和`FLOAT`。

```
device_1, 1490860659000, m1, 10, m2, 12.12
```

#### 写入 TsFile

TsFile 可以通过以下三个步骤生成，完整的代码参见"写入 TsFile 示例"章节。

1. 构造一个`TsFileWriter`实例。
  
    以下是可用的构造函数：
    
    * 没有预定义 schema
    
    ```java
    public TsFileWriter(File file) throws IOException
    ```
    * 预定义 schema
    
    ```java
    public TsFileWriter(File file, Schema schema) throws IOException
    ```
    这个是用于使用 HDFS 文件系统的。`TsFileOutput`可以是`HDFSOutput`类的一个实例。
    
    ```java
    public TsFileWriter(TsFileOutput output, Schema schema) throws IOException 
    ```
    
    如果你想自己设置一些 TSFile 的配置，你可以使用`config`参数。比如：
    
    ```java
    TSFileConfig conf = new TSFileConfig();
    conf.setTSFileStorageFs("HDFS");
    TsFileWriter tsFileWriter = new TsFileWriter(file, schema, conf);
    ```

    在上面的例子中，数据文件将存储在 HDFS 中，而不是本地文件系统中。如果你想在本地文件系统中存储数据文件，你可以使用`conf.setTSFileStorageFs("LOCAL")`，这也是默认的配置。
    
    您还可以通过`config.setHdfsIp(...)`和`config.setHdfsPort(...)`来配置 HDFS 的 IP 和端口。默认的 IP 是`localhost`，默认的`RPC`端口是`9000`.
    
    **参数：**
    
    * file : 写入 TsFile 数据的文件
    * schema : 文件的 schemas，将在下章进行介绍
    * config : TsFile 的一些配置项

2. 添加测量值 (measurement)
  
    你也可以先创建一个`Schema`类的实例然后把它传递给`TsFileWriter`类的构造函数
    
    `Schema`类保存的是一个映射关系，key 是一个 measurement 的名字，value 是 measurement schema.
    
    下面是一系列接口：
    
    ```java
    // Create an empty Schema or from an existing map
    public Schema()
    public Schema(Map<String, MeasurementSchema> measurements)
    // Use this two interfaces to add measurements
    public void registerMeasurement(MeasurementSchema descriptor)
    public void registerMeasurements(Map<String, MeasurementSchema> measurements)
    // Some useful getter and checker
    public TSDataType getMeasurementDataType(String measurementId)
    public MeasurementSchema getMeasurementSchema(String measurementId)
    public Map<String, MeasurementSchema> getAllMeasurementSchema()
    public boolean hasMeasurement(String measurementId)
    ```
  
    你可以在`TsFileWriter`类中使用以下接口来添加额外的测量 (measurement):
    ​      
    ```java
    public void addMeasurement(MeasurementSchema measurementSchema) throws WriteProcessException
    ```
    
    `MeasurementSchema`类保存了一个测量 (measurement) 的信息，有几个构造函数：
    
    ```java
    public MeasurementSchema(String measurementId, TSDataType type, TSEncoding encoding)
    public MeasurementSchema(String measurementId, TSDataType type, TSEncoding encoding, CompressionType compressionType)
    public MeasurementSchema(String measurementId, TSDataType type, TSEncoding encoding, CompressionType compressionType, 
    Map<String, String> props)
    ```
    
    **参数：**
    ​    
    
    * measurementID: 测量的名称，通常是传感器的名称。
      
    * type: 数据类型，现在支持六种类型：`BOOLEAN`, `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `TEXT`;
    
    * encoding: 编码类型。
    
    * compression: 压缩方式。现在支持 `UNCOMPRESSED` 和 `SNAPPY`.
    
    * props: 特殊数据类型的属性。比如说`FLOAT`和`DOUBLE`可以设置`max_point_number`，`TEXT`可以设置`max_string_length`。
    可以使用 Map 来保存键值对，比如 ("max_point_number", "3")。
    
    > **注意：** 虽然一个测量 (measurement) 的名字可以被用在多个 deltaObjects 中，但是它的参数是不允许被修改的。比如：
        不允许多次为同一个测量 (measurement) 名添加不同类型的编码。下面是一个错误示例：
	
	```java
	// The measurement "sensor_1" is float type
	addMeasurement(new MeasurementSchema("sensor_1", TSDataType.FLOAT, TSEncoding.RLE));
	// This call will throw a WriteProcessException exception
	addMeasurement(new MeasurementSchema("sensor_1", TSDataType.INT32, TSEncoding.RLE));
	```
3. 插入和写入数据。
  
    使用这个接口创建一个新的`TSRecord`（时间戳和设备对）。
    
    ```java
    public TSRecord(long timestamp, String deviceId)
    ```
  
    然后创建一个`DataPoint`（度量 (measurement) 和值的对应），并使用 addTuple 方法将数据 DataPoint 添加正确的值到 TsRecord。
    
    用下面这种方法写

    ```java
    public void write(TSRecord record) throws IOException, WriteProcessException
    ```
    
4. 调用`close`方法来完成写入过程。

    ```java
    public void close() throws IOException
    ```

我们也支持将数据写入已关闭的 TsFile 文件中。

1. 使用`ForceAppendTsFileWriter`打开已经关闭的文件。

	```java
	public ForceAppendTsFileWriter(File file) throws IOException
	```
2. 调用 `doTruncate` 去掉文件的 Metadata 部分

3. 使用  `ForceAppendTsFileWriter` 构造另一个`TsFileWriter`

	```java
	public TsFileWriter(TsFileIOWriter fileWriter) throws IOException
	```
请注意 此时需要重新添加测量值 (measurement) 再进行上述写入操作。

#### 写入 TsFile 示例

您需要安装 TsFile 到本地的 Maven 仓库中。

```shell
mvn clean install -pl tsfile -am -DskipTests
```

如果存在**非对齐**的时序数据（比如：不是所有的传感器都有值），您可以通过构造** TSRecord **来写入。

更详细的例子可以在

```
/example/tsfile/src/main/java/org/apache/iotdb/tsfile/TsFileWriteWithTSRecord.java
```

中查看

如果所有时序数据都是**对齐**的，您可以通过构造** Tablet **来写入数据。

更详细的例子可以在

```
/example/tsfile/src/main/java/org/apache/iotdb/tsfile/TsFileWriteWithTablet.java
```
中查看

在已关闭的 TsFile 文件中写入新数据的详细例子可以在

```
/example/tsfile/src/main/java/org/apache/iotdb/tsfile/TsFileForceAppendWrite.java
```
中查看

#### 读取 TsFile 接口

 * 路径的定义

路径是一个点 (.) 分隔的字符串，它唯一地标识 TsFile 中的时间序列，例如："root.area_1.device_1.sensor_1"。
最后一部分"sensor_1"称为"measurementId"，其余部分"root.area_1.device_1"称为 deviceId。
正如之前提到的，不同设备中的相同测量 (measurement) 具有相同的数据类型和编码，设备也是唯一的。

在 read 接口中，参数`paths`表示要选择的测量值 (measurement)。
Path 实例可以很容易地通过类`Path`来构造。例如：

```java
Path p = new Path("device_1.sensor_1");
```

我们可以为查询传递一个 ArrayList 路径，以支持多个路径查询。

```java
List<Path> paths = new ArrayList<Path>();
paths.add(new Path("device_1.sensor_1"));
paths.add(new Path("device_1.sensor_3"));
```

> **注意：** 在构造路径时，参数的格式应该是一个点 (.) 分隔的字符串，最后一部分是 measurement，其余部分确认为 deviceId。

 * 定义 Filter

   * 使用条件过滤
在 TsFile 读取过程中使用 Filter 来选择满足一个或多个给定条件的数据。

   * IExpression
`IExpression`是一个过滤器表达式接口，它将被传递给系统查询时调用。
我们创建一个或多个筛选器表达式，并且可以使用`Binary Filter Operators`将它们连接形成最终表达式。

* **创建一个 Filter 表达式**
  
    有两种类型的过滤器。
    
     * TimeFilter: 使用时序数据中的`time`过滤。
    
    ```java
    IExpression timeFilterExpr = new GlobalTimeExpression(TimeFilter);
    ```

 使用以下关系获得一个`TimeFilter`对象（值是一个 long 型变量）。

|Relationship|Description|
|----|----|
|TimeFilter.eq(value)|选择时间等于值的数据|
|TimeFilter.lt(value)|选择时间小于值的数据|
|TimeFilter.gt(value)|选择时间大于值的数据|
|TimeFilter.ltEq(value)|选择时间小于等于值的数据|
|TimeFilter.gtEq(value)|选择时间大于等于值的数据|
|TimeFilter.notEq(value)|选择时间不等于值的数据|
|TimeFilter.not(TimeFilter)|选择时间不满足另一个时间过滤器的数据|

   * ValueFilter: 使用时序数据中的`value`过滤。
     

```java
IExpression valueFilterExpr = new SingleSeriesExpression(Path, ValueFilter);
```

 `ValueFilter`的用法与`TimeFilter`相同，只是需要确保值的类型等于 measurement（在路径中定义）的类型。

* **Binary Filter Operators**

    Binary filter operators 可以用来连接两个单独的表达式。

     * BinaryExpression.and(Expression, Expression): 选择同时满足两个表达式的数据。
     * BinaryExpression.or(Expression, Expression): 选择满足任意一个表达式值的数据。
    

Filter Expression 示例

* **TimeFilterExpression 示例**

```java
IExpression timeFilterExpr = new GlobalTimeExpression(TimeFilter.eq(15)); // series time = 15
```
```java
IExpression timeFilterExpr = new GlobalTimeExpression(TimeFilter.ltEq(15)); // series time <= 15
```
```java
IExpression timeFilterExpr = new GlobalTimeExpression(TimeFilter.lt(15)); // series time < 15
```
```java
IExpression timeFilterExpr = new GlobalTimeExpression(TimeFilter.gtEq(15)); // series time >= 15
```
```java
IExpression timeFilterExpr = new GlobalTimeExpression(TimeFilter.notEq(15)); // series time != 15
```
```java
IExpression timeFilterExpr = BinaryExpression.and(
		new GlobalTimeExpression(TimeFilter.gtEq(15L)),
    new GlobalTimeExpression(TimeFilter.lt(25L))); // 15 <= series time < 25
```
```java
IExpression timeFilterExpr = BinaryExpression.or(
    new GlobalTimeExpression(TimeFilter.gtEq(15L)),
    new GlobalTimeExpression(TimeFilter.lt(25L))); // series time >= 15 or series time < 25
```

* 读取接口

首先，我们打开 TsFile 并从文件路径`path`中获取一个`ReadOnlyTsFile`实例。

```java
TsFileSequenceReader reader = new TsFileSequenceReader(path);
ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);
```
接下来，我们准备路径数组和查询表达式，然后通过这个接口得到最终的`QueryExpression`对象：

```java
QueryExpression queryExpression = QueryExpression.create(paths, statement);
```

ReadOnlyTsFile 类有两个`query`方法来执行查询。

```java
public QueryDataSet query(QueryExpression queryExpression) throws IOException
public QueryDataSet query(QueryExpression queryExpression, long partitionStartOffset, long partitionEndOffset) throws IOException
```

此方法是为高级应用（如 TsFile-Spark 连接器）设计的。

* **参数** : 对于第二个方法，添加了两个额外的参数来支持部分查询 (Partial Query):
    *  `partitionStartOffset`: TsFile 的开始偏移量
    *  `partitionEndOffset`: TsFile 的结束偏移量
                        
>什么是部分查询？

> 在一些分布式文件系统中（比如：HDFS), 文件被分成几个部分，这些部分被称为"Blocks"并存储在不同的节点中。在涉及的每个节点上并行执行查询可以提高效率。因此需要部分查询 (Partial Query)。部分查询 (Partial Query) 仅支持查询 TsFile 中被`QueryConstant.PARTITION_START_OFFSET`和`QueryConstant.PARTITION_END_OFFSET`分割的部分。

* QueryDataset 接口

 上面执行的查询将返回一个`QueryDataset`对象。

 以下是一些用户常用的接口：

   * `bool hasNext();`

     如果该数据集仍然有数据，则返回 true。
   * `List<Path> getPaths()`

      获取这个数据集中的路径。
  * `List<TSDataType> getDataTypes();` 

     获取数据类型。

   * `RowRecord next() throws IOException;`

     获取下一条记录。
     
     `RowRecord`类包含一个`long`类型的时间戳和一个`List<Field>`，用于不同传感器中的数据，我们可以使用两个 getter 方法来获取它们。
     
     ```java
     long getTimestamp();
     List<Field> getFields();
     ```
    
     要从一个字段获取数据，请使用以下方法：
     
     ```java
     TSDataType getDataType();
     Object getObjectValue();
     ```

#### 读取现有 TsFile 示例

您需要安装 TsFile 到本地的 Maven 仓库中。

有关查询语句的更详细示例，请参见
`/example/tsfile/src/main/java/org/apache/iotdb/tsfile/TsFileRead.java`

```java
package org.apache.iotdb.tsfile;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

/**
 * The class is to show how to read TsFile file named "test.tsfile".
 * The TsFile file "test.tsfile" is generated from class TsFileWrite.
 * Run TsFileWrite to generate the test.tsfile first
 */
public class TsFileRead {
  private static final String DEVICE1 = "device_1";
  
  private static void queryAndPrint(ArrayList<Path> paths, ReadOnlyTsFile readTsFile, IExpression statement)
          throws IOException {
    QueryExpression queryExpression = QueryExpression.create(paths, statement);
    QueryDataSet queryDataSet = readTsFile.query(queryExpression);
    while (queryDataSet.hasNext()) {
      System.out.println(queryDataSet.next());
    }
    System.out.println("------------");
  }

  public static void main(String[] args) throws IOException {

    // file path
    String path = "test.tsfile";

    // create reader and get the readTsFile interface
    try (TsFileSequenceReader reader = new TsFileSequenceReader(path);
         ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader)){
	 
    // use these paths(all sensors) for all the queries
    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path(DEVICE1, "sensor_1"));
    paths.add(new Path(DEVICE1, "sensor_2"));
    paths.add(new Path(DEVICE1, "sensor_3"));

    // no filter, should select 1 2 3 4 6 7 8
    queryAndPrint(paths, readTsFile, null);

   // time filter : 4 <= time <= 10, should select 4 6 7 8
    IExpression timeFilter =
       BinaryExpression.and(
           new GlobalTimeExpression(TimeFilter.gtEq(4L)),
           new GlobalTimeExpression(TimeFilter.ltEq(10L)));
    queryAndPrint(paths, readTsFile, timeFilter);

   // value filter : device_1.sensor_2 <= 20, should select 1 2 4 6 7
    IExpression valueFilter =
        new SingleSeriesExpression(new Path(DEVICE1, "sensor_2"), ValueFilter.ltEq(20L));
    queryAndPrint(paths, readTsFile, valueFilter);

   // time filter : 4 <= time <= 10, value filter : device_1.sensor_3 >= 20, should select 4 7 8
    timeFilter =
        BinaryExpression.and(
            new GlobalTimeExpression(TimeFilter.gtEq(4L)),
            new GlobalTimeExpression(TimeFilter.ltEq(10L)));
    valueFilter =
        new SingleSeriesExpression(new Path(DEVICE1, "sensor_3"), ValueFilter.gtEq(20L));
    IExpression finalFilter = BinaryExpression.and(timeFilter, valueFilter);
    queryAndPrint(paths, readTsFile, finalFilter);
    }
  }
}
```

### 修改 TsFile 配置项

```java
TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
config.setXXX();
```
