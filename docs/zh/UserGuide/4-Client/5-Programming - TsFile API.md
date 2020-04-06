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

# 编程-TsFile API

TsFile是我们在IoTDB中使用的时间序列的文件格式。 在本节中，我们要介绍这种文件格式的用法。

## Ts文件库安装


在您自己的项目中有两种使用TsFile的方法。

* 用作jars：
  * 编译源代码并构建为jars

  	```
  	git clone https://github.com/apache/incubator-iotdb.git
  	cd tsfile/
  	mvn clean package -Dmaven.test.skip=true
  	```
  	然后，所有的jar都可以放在名为“ target /”的文件夹中。 将`target / tsfile-0.9.1-jar-with-dependencies.jar`导入您的项目。

* 用作Maven依赖项：

  编译源代码并通过三个步骤将其部署到本地存储库：

  * 获取源代码

  	```
  	git clone https://github.com/apache/incubator-iotdb.git
  	```
  * 编译源代码并部署
  	
  	```
  	cd tsfile/
  	mvn clean install -Dmaven.test.skip=true
  	```
  * 在项目中添加依赖项：

    ```
  	 <dependency>
  	   <groupId>org.apache.iotdb</groupId>
  	   <artifactId>tsfile</artifactId>
  	   <version>0.9.1</version>
  	 </dependency>
    ```
    

  或者，您可以从官方Maven存储库下载依赖项：

  * 第一, 在路径上找到您的Maven`settings.xml`: `${username}\.m2\settings.xml`
    , 将 `<profile>` 加入到 `<profiles>`:

    ```
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
  * 然后将依赖项添加到您的项目中：

    ```
     <dependency>
       <groupId>org.apache.iotdb</groupId>
       <artifactId>tsfile</artifactId>
       <version>0.9.1</version>
     </dependency>
    ```

## TSFile的用法
本节演示TsFile的详细用法。

### 时间序列数据
时间序列被视为四倍序列。 四元组定义为（设备，测量，时间，值）。

* **测量**: 时间序列进行的物理或形式测量，例如城市温度，某些商品的销售数量或火车在不同时间的速度。 由于传统的传感器（例如温度计）也需要进行一次测量并生成一个时间序列，因此我们将在下面互换使用测量和传感器。

* **设备**: 设备是指正在执行多个测量（产生多个时间序列）的实体，例如，运行中的火车监视其速度，油表，行驶里程，当前乘客均被传送到一个时间序列。

表1说明了一组时间序列数据。 下表中显示的集合包含一个名为“ device \ _1”的设备以及三个名为“ sensor \ _1”，“ sensor \ _2”和“ sensor \ _3”的测量值。

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



**一行数据**:在许多工业应用中，设备通常包含多个传感器，并且这些传感器可能在同一时间戳上具有值，这称为一行数据。

形式上，一行数据由一个`device_id`，一个指示自1970年1月1日以来的毫秒数，00：00：00组成的时间戳，以及由`measurement_id`和对应的`value`组成的几对数据组成。 一行中的所有数据对都属于该`device_id`，并且具有相同的时间戳。 如果其中一个`measurements` 在`timestamp`中没有`value` ，请改用空格（实际上，TsFile不存储空值）。 其格式如下所示：

```
device_id, timestamp, <measurement_id, value>...
```

一个示例如下所示。 在此示例中，两个测量的数据类型分别为“ INT32”和“ FLOAT”。

```
device_1, 1490860659000, m1, 10, m2, 12.12
```


### 编写TsFile

#### 生成一个TsFile文件
可以通过以下三个步骤来生成TsFile，完整的代码将在“编写TsFile的示例”部分中给出。

* 一，构造一个`TsFileWriter`实例。

    以下是可用的构造函数：

    * 没有预定义的架构
    ```
    public TsFileWriter(File file) throws IOException
    ```
    * 使用预定义的架构
    ```
    public TsFileWriter(File file, Schema schema) throws IOException
    ```
    这是用于使用HDFS文件系统的。  `TsFileOutput`可以是`HDFSOutput`类的实例。

    ```
    public TsFileWriter(TsFileOutput output, Schema schema) throws IOException 
    ```

    如果您想自己设置一些TSFile配置，则可以使用param`config`。 例如：
    ```
    TSFileConfig conf = new TSFileConfig();
    conf.setTSFileStorageFs("HDFS");
    TsFileWriter tsFileWriter = new TsFileWriter(file, schema, conf);
    ```
    在此示例中，数据文件将存储在HDFS中，而不是本地文件系统中。 如果要将数据文件存储在本地文件系统中，则可以使用`conf.setTSFileStorageFs（“ LOCAL”）`，这也是默认配置。

    您也可以通过`config.setHdfsIp（...）`和`config.setHdfsPort（...）`配置HDFS的IP和端口。 默认ip为`localhost`，默认端口为`9000`。

    **Parameters:**

    * file：要写入的TsFile

    * schema：文件模式将在下一部分中介绍。

    * config：TsFile的配置。

* 二，添加测量

    或者，您可以先创建`Schema`类的实例，然后将其传递给`TsFileWriter`类的构造函数。`Schema`类包含一个映射，该映射的键是一个度量模式的名称，而值是该模式本身。

    这里是接口：
    ```
    // 创建一个空的架构或从现有的映射
    public Schema()
    public Schema(Map<String, MeasurementSchema> measurements)
    // Use this two interfaces to add measurements
    public void registerMeasurement(MeasurementSchema descriptor)
    public void registerMeasurements(Map<String, MeasurementSchema> measurements)
    // 一些有用的吸气剂和检查剂
    public TSDataType getMeasurementDataType(String measurementId)
    public MeasurementSchema getMeasurementSchema(String measurementId)
    public Map<String, MeasurementSchema> getAllMeasurementSchema()
    public boolean hasMeasurement(String measurementId)
    ```

    您始终可以在`TsFileWriter`类中使用以下接口来添加其他度量：
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
    ​    

    * measurementID：此测量的名称，通常是传感器的名称。
      
    * 类型：数据类型，现在支持六种类型：`BOOLEAN`，`INT32`，`INT64`，`FLOAT`，`DOUBLE`，`TEXT`。

    * encoding：数据编码。 见 [Chapter 2-3](/document/V0.9.x/UserGuide/2-Concept/3-Encoding.html).

    * compression：数据压缩。 现在支持`UNCOMPRESSED`和`SNAPPY`。

    * props：特殊数据类型的属性，例如`FLOAT`和`DOUBLE`的`max_point_number`，`TEXT`的`max_string_length`。 用作字符串对，例如（“ max_point_number”，“ 3”）。

    > **注意：**尽管一个度量名称可以在多个deltaObject中使用，但是不能更改属性。 即      不允许多次使用不同的类型或编码添加一个测量名称。
    > ​     这是一个不好的例子：

        // 测量值“ sensor_1”为浮点型
        addMeasurement(new MeasurementSchema("sensor_1", TSDataType.FLOAT, TSEncoding.RLE));
        
        // 此调用将引发WriteProcessException异常
        addMeasurement(new MeasurementSchema("sensor_1", TSDataType.INT32, TSEncoding.RLE));

* 第三，连续插入和写入数据。
    ​    使用此接口可以创建新的`TSRecord`（时间戳和设备对）。

* ```
    public TSRecord(long timestamp, String deviceId)
    ```
    然后创建一个`DataPoint`（一个测量值和一个值对），并使用addTuple方法将DataPoint添加到正确的TsRecord。
    ​    用这种方法写

    ```
    public void write(TSRecord record) throws IOException, WriteProcessException
    ```

* 最后，调用`close`完成此编写过程。

    ```
    public void close() throws IOException
    ```

#### 编写TsFile的示例

您应该将TsFile安装到本地Maven存储库。

```
mvn clean install -pl tsfile -am -DskipTests
```

如果您具有**未对齐**（例如，并非所有传感器都包含值）的时间序列数据，则可以通过构造**TSRecord **来编写TsFile。

可以在以下位置找到更详尽的示例`/example/tsfile/src/main/java/org/apache/iotdb/tsfile/TsFileWriteWithTSRecord.java`

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
 * An example of writing data to TsFile
 * It uses the interface:
 * public void addMeasurement(MeasurementSchema MeasurementSchema) throws WriteProcessException
 */
public class TsFileWriteWithTSRecord {

  public static void main(String args[]) {
    try {
      String path = "test.tsfile";
      File f = new File(path);
      if (f.exists()) {
        f.delete();
      }
      TsFileWriter tsFileWriter = new TsFileWriter(f);

      // add measurements into file schema
      tsFileWriter
          .addMeasurement(new MeasurementSchema("sensor_1", TSDataType.INT64, TSEncoding.RLE));
      tsFileWriter
          .addMeasurement(new MeasurementSchema("sensor_2", TSDataType.INT64, TSEncoding.RLE));
      tsFileWriter
          .addMeasurement(new MeasurementSchema("sensor_3", TSDataType.INT64, TSEncoding.RLE));
            
      // construct TSRecord
      TSRecord tsRecord = new TSRecord(1, "device_1");
      DataPoint dPoint1 = new LongDataPoint("sensor_1", 1);
      DataPoint dPoint2 = new LongDataPoint("sensor_2", 2);
      DataPoint dPoint3 = new LongDataPoint("sensor_3", 3);
      tsRecord.addTuple(dPoint1);
      tsRecord.addTuple(dPoint2);
      tsRecord.addTuple(dPoint3);
            
      // write TSRecord
      tsFileWriter.write(tsRecord);
      
      // close TsFile
      tsFileWriter.close();
    } catch (Throwable e) {
      e.printStackTrace();
      System.out.println(e.getMessage());
    }
  }
}

```

如果您具有**对齐的**时间序列数据，则可以通过构造**RowBatch **来编写TsFile。
​    可以在`/example/tsfile/src/main/java/org/apache/iotdb/tsfile/TsFileWriteWithRowBatch.java`中找到更详尽的示例。

```java
package org.apache.iotdb.tsfile;

import java.io.File;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.record.RowBatch;
/**
 * An example of writing data with RowBatch to TsFile
 */
public class TsFileWriteWithRowBatch {

  public static void main(String[] args) {
    try {
      String path = "test.tsfile";
      File f = new File(path);
      if (f.exists()) {
        f.delete();
      }

      Schema schema = new Schema();

      // the number of rows to include in the row batch
      int rowNum = 1000000;
      // the number of values to include in the row batch
      int sensorNum = 10;

      // add measurements into file schema (all with INT64 data type)
      for (int i = 0; i < sensorNum; i++) {
        schema.registerMeasurement(
                new MeasurementSchema("sensor_" + (i + 1), TSDataType.INT64, TSEncoding.TS_2DIFF));
      }

      // add measurements into TSFileWriter
      TsFileWriter tsFileWriter = new TsFileWriter(f, schema);

      // construct the row batch
      RowBatch rowBatch = schema.createRowBatch("device_1");

      long[] timestamps = rowBatch.timestamps;
      Object[] values = rowBatch.values;

      long timestamp = 1;
      long value = 1000000L;

      for (int r = 0; r < rowNum; r++, value++) {
        int row = rowBatch.batchSize++;
        timestamps[row] = timestamp++;
        for (int i = 0; i < sensorNum; i++) {
          long[] sensor = (long[]) values[i];
          sensor[row] = value;
        }
        // write RowBatch to TsFile
        if (rowBatch.batchSize == rowBatch.getMaxBatchSize()) {
          tsFileWriter.write(rowBatch);
          rowBatch.reset();
        }
      }
      // write RowBatch to TsFile
      if (rowBatch.batchSize != 0) {
        tsFileWriter.write(rowBatch);
        rowBatch.reset();
      }

      // close TsFile
      tsFileWriter.close();
    } catch (Throwable e) {
      e.printStackTrace();
      System.out.println(e.getMessage());
    }
  }
}

```

### 读取TsFile的接口

#### 开始之前

“时间序列数据”部分中的时间序列数据集在此用于本节的具体介绍。 下表中显示的集合包含一个名为“ device \ _1”的deltaObject和三个名为“ sensor \ _1”，“ sensor \ _2”和“ sensor \ _3”的测量值。 并简化了测量以进行简单说明，每个仅包含4个时间值对。

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



#### 路径定义

路径是用点分隔的字符串，该字符串在TsFile中唯一标识时间序列，例如“ root.area_1.device_1.sensor_1”。
   最后一部分“ sensor_1”称为“ measurementId”，其余部分“ root.area_1.device_1”称为deviceId。
   如上所述，不同设备中的相同测量具有相同的数据类型和编码，并且设备也是唯一的。

在读取接口中，参数路径指示要选择的测量。
   通过类`Path`可以轻松构造`Path实`例。 例如： 

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


#### 过滤器的定义

##### 使用场景
在TsFile读取过程中使用筛选器来选择满足一个或多个给定条件的数据。

#### IExpression
`IExpression`是一个过滤器表达式接口，它将被传递给我们的最终查询调用。
   我们创建一个或多个过滤器表达式，并可以使用二进制过滤器运算符将它们链接到最终表达式。

* **创建一个过滤器表达式**

    有两种类型的过滤器。

     * TimeFilter：时间序列数据中`时间`的过滤器。
        ```
        IExpression timeFilterExpr = new GlobalTimeExpression(TimeFilter);
        ```
        使用以下关系获取`TimeFilter`对象（值是一个long int变量）。
        <center>
        <table style="text-align:center">
            <tr><th>Relationship</th><th>Description</th></tr>
            <tr><td>TimeFilter.eq(value)</td><td>Choose the time equal to the value</td></tr>
            <tr><td>TimeFilter.lt(value)</td><td>Choose the time less than the value</td></tr>
            <tr><td>TimeFilter.gt(value)</td><td>Choose the time greater than the value</td></tr>
            <tr><td>TimeFilter.ltEq(value)</td><td>Choose the time less than or equal to the value</td></tr>
            <tr><td>TimeFilter.gtEq(value)</td><td>Choose the time greater than or equal to the value</td></tr></tr>
            <tr><td>TimeFilter.notEq(value)</td><td>Choose the time not equal to the value</td></tr>
            <tr><td>TimeFilter.not(TimeFilter)</td><td>Choose the time not satisfy another TimeFilter</td></tr>
        </table>
        </center>

     * ValueFilter：时间序列数据中值的过滤器。

        ```
        IExpression valueFilterExpr = new SingleSeriesExpression(Path, ValueFilter);
        ```
        `ValueFilter`的用法与`TimeFilter`的用法相同，只是要确保值的类型等于度量值（在路径中定义）。

* **二元滤波器运算符**

    二进制过滤器运算符可用于链接两个单个表达式。

     * BinaryExpression.and（Expression，Expression）：选择两个表达式都满足的值。
     * BinaryExpression.or（Expression，Expression）：选择至少满足一个表达式的值。


##### 过滤器表达式示例

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
#### 读取界面

首先，我们打开TsFile并从文件路径字符串`path`获取一个`ReadOnlyTsFile`实例。

```
TsFileSequenceReader reader = new TsFileSequenceReader(path);
   
ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);
```
接下来，我们准备路径数组和查询表达式，然后通过此接口获取最终的`QueryExpression`对象：

```
QueryExpression queryExpression = QueryExpression.create(paths, statement);
```

ReadOnlyTsFile类具有两种查询方法来执行查询。
* **方法1**

    ```
    public QueryDataSet query(QueryExpression queryExpression) throws IOException
    ```

* **方法二**

    ```
    public QueryDataSet query(QueryExpression queryExpression, long partitionStartOffset, long partitionEndOffset) throws IOException
    ```

    此方法设计用于高级应用程序，例如TsFile-Spark连接器。

    * **参数** : 对于方法2，添加了两个附加参数以支持部分查询：

        *  ```partitionStartOffset```: TsFile的起始偏移量
        *  ```partitionEndOffset```: TsFile的结束偏移量

        > **什么是部分查询？**
        >
        > 在某些分布式文件系统（例如HDFS）中，文件被分成几个部分，这些部分称为“块”，并存储在不同的节点中。 在涉及的每个节点中并行执行查询可以提高效率。 因此，需要部分查询。  Paritial Query只选择存储在零件中的结果，该零件由TsFile的`QueryConstant.PARTITION_START_OFFSET`和`QueryConstant.PARTITION_END_OFFSET`分开。

### QueryDataset接口

上面执行的查询将返回一个`QueryDataset`对象。

这是对用户有用的界面。


* `bool hasNext();`

    如果此数据集仍包含元素，则返回true。
* `List<Path> getPaths()`

    获取此数据集中的路径。
* `List<TSDataType> getDataTypes();` 

   获取数据类型。  TSDataType类是一个枚举类，值将是以下值之一：
   
       BOOLEAN,
       INT32,
       INT64,
       FLOAT,
       DOUBLE,
       TEXT;
 * `RowRecord next() throws IOException;`

    获取下一条记录。

    `RowRecord`类由一个很长的时间戳和一个`List <Field>`组成，用于不同传感器中的数据，我们可以使用两种getter方法来获取它们。

    ```
    long getTimestamp();
    List<Field> getFields();
    ```

    要从一个字段获取数据，请使用以下方法：

    ```
    TSDataType getDataType();
    Object getObjectValue();
    ```

#### 读取现有TsFile的示例


您应该将TsFile安装到本地Maven存储库。

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

    //离开时关闭阅读器
    reader.close();
  }
}

```

## 修改 TsFile 配置项

```
TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
config.setXXX()
```

## 布隆过滤器

布隆过滤器在加载元数据之前检查给定的时间序列是否在ts文件中。 这样可以提高加载元数据的性能，并跳过不包含指定时间序列的tsfile。
  如果要了解有关其机制的更多信息，可以参考： [wiki page of bloom filter](https://en.wikipedia.org/wiki/Bloom_filter).

#### 配置
您可以通过`/ server / src / assembly / resources / conf`目录中的配置文件`iotdb-engine.properties`中的以下参数来控制Bloom过滤器的误报率。
```
# The acceptable error rate of bloom filter, should be in [0.01, 0.1], default is 0.05
bloom_filter_error_rate=0.05
```

