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

# 用法

现在，您准备开始使用TsFile做一些很棒的事情。 本节演示TsFile的详细用法。

## 时间序列数据

时间序列被视为四倍序列。 四元组定义为（设备，测量，时间，值）。

* **测量**: 时间序列进行的物理或形式测量，例如城市温度，某些商品的销售数量或火车在不同时间的速度。 由于传统的传感器（例如温度计）也需要进行一次测量并生成一个时间序列，因此我们将在下面互换使用测量和传感器。

* **设备**: 设备是指正在执行多个测量（产生多个时间序列）的实体，例如，运行中的火车监视其速度，油表，行驶里程，当前乘客均被传送到一个时间序列。

表1说明了一组时间序列数据。 下表中显示的集合包含一个名为“ device_1”的设备以及三个名为“ sensor_1”，“ sensor_2”和“ sensor_3”的测量值。

<center>
<table style="text-align:center">
	<tr><th colspan="6">device_1</th></tr>
	<tr><th colspan="2">sensor_1</th><th colspan="2">sensor_2</th><th colspan="2">sensor_3</th></tr>
	<tr><th>time</th><th>value</th><th>time</th><th>value</th><th>time</th><th>value</th></tr>
	<tr><td>1</td><td>1.2</td><td>1</td><td>20</td><td>2</td><td>50</td></tr>
	<tr><td>3</td><td>1.4</td><td>2</td><td>20</td><td>4</td><td>51</td></tr>
	<tr><td>5</td><td>1.1</td><td>3</td><td>21</td><td>6</td><td>52</td></tr>
	<tr><td>7</td><td>1.8</td><td>4</td><td>20</td><td>8</td><td>53</td></tr>
</table>
<span>一组时间序列数据</span>
</center>

**一行数据**: 在许多工业应用中，设备通常包含多个传感器，并且这些传感器可能在同一时间戳上具有值，这称为一行数据。

形式上，一行数据由一个`device_id`，一个指示自1970年1月1日以来的毫秒数，00：00：00组成的时间戳，以及由`measurement_id`和对应的`value`组成的几对数据组成。 一行中的所有数据对都属于该`device_id`，并且具有相同的时间戳。 如果其中一个`measurements` 在`value` 中没有`value`，请改用空格（实际上，TsFile不存储空值）。 其格式如下所示：

```
device_id, timestamp, <measurement_id, value>...
```

一个示例如下所示。 在此示例中，两个测量的数据类型分别为INT32和FLOAT。

```
device_1, 1490860659000, m1, 10, m2, 12.12
```

## 编写TsFile

### 生成一个TsFile文件。

可以通过以下三个步骤来生成TsFile，完整的代码将在“编写TsFile的示例”部分中给出。

* 首先，构造一个`TsFileWriter`实例。

    以下是可用的构造函数：

    * 没有预定义的架构
    ```
    public TsFileWriter(File file) throws IOException
    ```
    * 使用预定义的架构
    ```
    public TsFileWriter(File file, FileSchema schema) throws IOException
    ```
    这是用于使用HDFS文件系统的。  `TsFileOutput`可以是`HDFSOutput`类的实例。

    ```
    public TsFileWriter(TsFileOutput output, FileSchema schema) throws IOException 
    ```
    **参量:**

    * 文件: 要写的TsFile

    * 模式: 文件模式将在下一部分中介绍。

* 二，添加测量

  或者，您可以先创建`FileSchema`类的实例，然后将其传递给`TsFileWriter`类的构造函数。

  `FileSchema`类包含一个映射，该映射的键是一个测量模式的名称，而值是该模式本身。

  这里是接口：
  ```
  // 创建一个空的FileSchema或从现有的映射
  public FileSchema()
  public FileSchema(Map<String, MeasurementSchema> measurements)
    
  // 使用这两个界面添加测量
  public void registerMeasurement(MeasurementSchema descriptor)
  public void registerMeasurements(Map<String, MeasurementSchema> measurements)
    
  // 一些有用的吸气剂和检查剂
  public TSDataType getMeasurementDataType(String measurementId)
  public MeasurementSchema getMeasurementSchema(String measurementId)
  public Map<String, MeasurementSchema> getAllMeasurementSchema()
  public boolean hasMeasurement(String measurementId)
  ```

  您可以始终在`TsFileWriter`类中使用以下接口来添加其他度量：
  ​      
    ```
    public void addMeasurement(MeasurementSchema measurementSchema) throws WriteProcessException
    ```

  `MeasurementSchema`类包含一种度量的信息，有几种构造函数：

  ```
  public MeasurementSchema(String measurementId, TSDataType type, TSEncoding encoding)
    
  public MeasurementSchema(String measurementId, TSDataType type, TSEncoding encoding, CompressionType compressionType)
  
  public MeasurementSchema(String measurementId, TSDataType type, TSEncoding encoding, CompressionType compressionType, 
  Map<String, String> props)
  ```

  **参数：**
    	

    * measurementID：此测量的名称，通常是传感器的名称。
    
    * 类型：数据类型，现在支持六种类型：`BOOLEAN`，`INT32`，`INT64`，`FLOAT`，`DOUBLE`，`TEXT`。
    
    * encoding：数据编码。 看到 [章节 2-3](../2-Concept%20Key%20Concepts%20and%20Terminology/3-Encoding.md).
    
    * compression：数据压缩。 现在支持`UNCOMPRESSED`和`SNAPPY`。
    
    * props：特殊数据类型的属性，例如`FLOAT`和`DOUBLE`的`max_point_number`，`TEXT`的`max_string_length`。 用作字符串对，例如（“ max_point_number”，“ 3”）。

    > **注意：**尽管一个度量名称可以在多个deltaObject中使用，但是不能更改属性。 即    不允许多次使用不同的类型或编码添加一个测量名称。
    >    这是一个不好的例子：

     	// 测量值“ sensor_1”为浮点型
     	addMeasurement(new MeasurementSchema("sensor_1", TSDataType.FLOAT, TSEncoding.RLE));
     	
     	// 此调用将引发WriteProcessException异常
     	addMeasurement(new MeasurementSchema("sensor_1", TSDataType.INT32, TSEncoding.RLE));
* 第三，连续插入和写入数据。

  使用此接口可以创建新的`TSRecord`（时间戳和设备对）。

  ```
  public TSRecord(long timestamp, String deviceId)
  ```
  然后创建一个`DataPoint`（一个测量值和一个值对），并使用`addTuple`方法将`DataPoint`添加到正确的TsRecord。

  用这种方法写

  ```
  public void write(TSRecord record) throws IOException, WriteProcessException
  ```

* 最后，调用`close`完成此编写过程。

  ```
  public void close() throws IOException
  ```

### 编写TsFile的示例

您应该将TsFile安装到本地Maven存储库。

```
mvn clean install -pl tsfile -am -DskipTests
```

可以在以下位置找到更详尽的示例`/tsfile/example/src/main/java/org/apache/iotdb/tsfile/TsFileWrite.java`

```java
package org.apache.iotdb.tsfile;

import java.io.File;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
/**
 * 将数据写入TsFile的示例
 * 它使用以下接口：
 * 公共无效addMeasurement（MeasurementSchema MeasurementSchema）抛出WriteProcessException
 */
public class TsFileWrite {

  public static void main(String args[]) {
    try {
      String path = "test.tsfile";
      File f = new File(path);
      if (f.exists()) {
        f.delete();
      }
      TsFileWriter tsFileWriter = new TsFileWriter(f);

      // 将度量添加到文件模式
      tsFileWriter
          .addMeasurement(new MeasurementSchema("sensor_1", TSDataType.INT64, TSEncoding.RLE));
      tsFileWriter
          .addMeasurement(new MeasurementSchema("sensor_2", TSDataType.INT64, TSEncoding.RLE));
      tsFileWriter
          .addMeasurement(new MeasurementSchema("sensor_3", TSDataType.INT64, TSEncoding.RLE));
            
      // 构造TSRecord
      TSRecord tsRecord = new TSRecord(1, "device_1");
      DataPoint dPoint1 = new LongDataPoint("sensor_1", 1);
      DataPoint dPoint2 = new LongDataPoint("sensor_2", 2);
      DataPoint dPoint3 = new LongDataPoint("sensor_3", 3);
      tsRecord.addTuple(dPoint1);
      tsRecord.addTuple(dPoint2);
      tsRecord.addTuple(dPoint3);
            
      // 写TSRecord
      tsFileWriter.write(tsRecord);
      
      // 关闭TsFile
      tsFileWriter.close();
    } catch (Throwable e) {
      e.printStackTrace();
      System.out.println(e.getMessage());
    }
  }
}

```

## 读取TsFile的接口

### 开始之前

“时间序列数据”部分中的时间序列数据集在此用于本节的具体介绍。 下表中显示的集合包含一个名为“ device \ _1”的deltaObject和三个名为“ sensor \ _1”，“ sensor \ _2”和“ sensor \ _3”的测量值。 并简化了测量以进行简单说明，每个仅包含4个时间值对。

|            关系            |               描述               |
| :------------------------: | :------------------------------: |
|    TimeFilter.eq(value)    |         选择等于值的时间         |
|    TimeFilter.lt(value)    |        选择小于时间的时间        |
|    TimeFilter.gt(value)    |         选择大于值的时间         |
|   TimeFilter.ltEq(value)   |     选择小于或等于该值的时间     |
|   TimeFilter.gtEq(value)   |     选择大于或等于该值的时间     |
|  TimeFilter.notEq(value)   |       选择不等于该值的时间       |
| TimeFilter.not(TimeFilter) | 选择不满足另一个TimeFilter的时间 |



### 路径定义

路径是用点分隔的字符串，该字符串在TsFile中唯一标识时间序列，例如“ root.area_1.device_1.sensor_1”。
最后一部分“ sensor_1”称为“ measurementId”，其余部分“ root.area_1.device_1”称为deviceId。
如上所述，不同设备中的相同测量具有相同的数据类型和编码，并且设备也是唯一的。

在读取接口中，参数`paths`指示要选择的测量。

路径实例可以通过`Path`类很容易地构造。 例如：

```
Path p = new Path("device_1.sensor_1");
```

我们将传递路径的ArrayList进行最终查询，以支持多个路径。

```
List<Path> paths = new ArrayList<Path>();
paths.add(new Path("device_1.sensor_1"));
paths.add(new Path("device_1.sensor_3"));
```

> **注意：**在构造路径时，参数的格式应为点分隔的字符串，最后一部分将被识别为MeasurementId，而其余部分将被识别为deviceId。

### 过滤器的定义

#### 使用场景

在TsFile读取过程中使用筛选器来选择满足一个或多个给定条件的数据。

#### IExpression

`IExpression`是一个过滤器表达式接口，它将传递给我们的最终查询调用。
   我们创建一个或多个过滤器表达式，并可以使用二进制过滤器运算符将它们链接到最终表达式。

* **创建一个过滤器表达式**

  有两种类型的过滤器。

   * 时间过滤器: 时间序列数据中时间的过滤器。
  	 	```
  	 	IExpression timeFilterExpr = new GlobalTimeExpression(TimeFilter);
  	```
  	 	使用以下关系获取`TimeFilter`对象（值是一个long int变量）。

  |            关系            |               描述               |
  | :------------------------: | :------------------------------: |
  |    TimeFilter.eq(value)    |         选择等于值的时间         |
  |    TimeFilter.lt(value)    |        选择小于时间的时间        |
  |    TimeFilter.gt(value)    |         选择大于值的时间         |
  |   TimeFilter.ltEq(value)   |     选择小于或等于该值的时间     |
  |   TimeFilter.gtEq(value)   |     选择大于或等于该值的时间     |
  |  TimeFilter.notEq(value)   |       选择不等于该值的时间       |
  | TimeFilter.not(TimeFilter) | 选择不满足另一个TimeFilter的时间 |

   * ValueFilter：时间序列数据中`value`的过滤器。

   	```
   	IExpression valueFilterExpr = new SingleSeriesExpression(Path, ValueFilter);
   	```
  ​	`ValueFilter`的用法与使用`TimeFilter`的用法相同，只是要确保值的类型等于测量值（在路径中定义）。

* **二元滤波器运算符**

  二进制过滤器运算符可用于链接两个单个表达式。

   * BinaryExpression.and（Expression，Expression）：选择两个表达式都满足的值。
   * BinaryExpression.or（Expression，Expression）：选择至少满足一个表达式的值。


#### 过滤器表达式示例

* **TimeFilterExpression示例**

	```
	IExpression timeFilterExpr = new GlobalTimeExpression(TimeFilter.eq(15)); // series time = 15

	```
	```
	IExpression timeFilterExpr = new GlobalTimeExpression(TimeFilter.ltEq(15)); // series time <= 15

	```
	```
	IExpression timeFilterExpr = new GlobalTimeExpression(TimeFilter.lt(15)); // series time < 15

	```
	```
	IExpression timeFilterExpr = new GlobalTimeExpression(TimeFilter.gtEq(15)); // series time >= 15

	```
	```
	IExpression timeFilterExpr = new GlobalTimeExpression(TimeFilter.notEq(15)); // series time != 15

	```
	```
	IExpression timeFilterExpr = BinaryExpression.and(new GlobalTimeExpression(TimeFilter.gtEq(15L)),
                                             new GlobalTimeExpression(TimeFilter.lt(25L))); // 15 <= series time < 25
	```
	```
	IExpression timeFilterExpr = BinaryExpression.or(new GlobalTimeExpression(TimeFilter.gtEq(15L)),
                                             new GlobalTimeExpression(TimeFilter.lt(25L))); // series time >= 15 or series time < 25
	```
### 读取界面

首先，我们打开TsFile并从文件路径字符串`path`获取一个`ReadOnlyTsFile`实例。

```
TsFileSequenceReader reader = new TsFileSequenceReader(path);
   
ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);
```
接下来，我们准备路径数组和查询表达式，然后通过此接口获取最终的`QueryExpression`对象：

```
QueryExpression queryExpression = QueryExpression.create(paths, statement);
```

ReadOnlyTsFile类具有两个`query`方法来执行查询。
* **方法1**

  ```
  public QueryDataSet query(QueryExpression queryExpression) throws IOException
  ```

* **方法二**

  ```
  public QueryDataSet query(QueryExpression queryExpression, long partitionStartOffset, long partitionEndOffset) throws IOException
  ```

  此方法设计用于高级应用程序，例如TsFile-Spark连接器。

  * **参数**: 对于方法2，添加了两个附加参数以支持部分查询：

    *  ```partitionStartOffset```:TsFile的起始偏移量
    *  ```partitionEndOffset```: TsFile的结束偏移量

    > **什么是部分查询？**在某些分布式文件系统（例如HDFS）中，文件被分成几个部分，这些部分称为“块”，并存储在不同的节点中。 在涉及的每个节点中并行执行查询可以提高效率。 因此，需要部分查询。  Paritial Query只选择存储在零件中的结果，该零件由TsFile的`QueryConstant.PARTITION_START_OFFSET`和`QueryConstant.PARTITION_END_OFFSET`分开。

### QueryDataset接口

上面执行的查询将返回一个`QueryDataset`对象。

这是对用户有用的界面。


* `bool hasNext();`

    如果此数据集仍包含元素，则返回true。
* `List<Path> getPaths()`

    获取此数据集中的路径。
* `List<TSDataType> getDataTypes();` 

   获取数据类型。  `TSDataType`类是一个枚举类，值将是以下值之一：
   
       BOOLEAN,
       INT32,
       INT64,
       FLOAT,
       DOUBLE,
       TEXT;
 * `RowRecord next() throws IOException;`

    获取下一条记录。

    `RowRecord`类由一个`long`时间戳和一个`List <Field>`组成，用于不同传感器中的数据，我们可以使用两种getter方法来获取它们。

    ```
    long getTimestamp();
    List<Field> getFields();
    ```

    要从一个字段获取数据，请使用以下方法：

    ```
    TSDataType getDataType();
    Object getObjectValue();
    ```

### 读取现有TsFile的示例


您应该将TsFile安装到本地Maven存储库。

```
mvn clean install -pl tsfile -am -DskipTests
```

有关查询语句的更详尽示例，请参见：
`/tsfile/example/src/main/java/org/apache/iotdb/tsfile/TsFileRead.java`

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
 * 该类将显示如何读取名为“ test.ts文件”的Ts文件文件。
 * TsFile文件“ test.tsfile”是从类TsFileWrite生成的。
 * 运行TsFileWrite首先生成test.tsfile
 */
public class TsFileRead {
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

    // 文件路径
    String path = "test.tsfile";

    // 创建阅读器并获取readTsFile接口
    TsFileSequenceReader reader = new TsFileSequenceReader(path);
    ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);
    // 使用这些路径（所有传感器）进行所有查询
    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("device_1.sensor_1"));
    paths.add(new Path("device_1.sensor_2"));
    paths.add(new Path("device_1.sensor_3"));

    // 没有查询语句
    queryAndPrint(paths, readTsFile, null);

    // 离开时关闭阅读器
    reader.close();
  }
}

```

## 用户指定的配置文件路径

默认配置文件`tsfile-format.properties.template`位于` / tsfile / src / main / resources`目录中。 如果要使用自己的路径，则可以：
```
System.setProperty(TsFileConstant.TSFILE_CONF, "your config file path");
```
然后调用：
```
TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
```


