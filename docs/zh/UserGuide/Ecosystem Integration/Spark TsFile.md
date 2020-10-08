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

# TsFile的Spark连接器

## 1. About TsFile-Spark-Connector

TsFile-Spark-Connector对Tsfile类型的外部数据源实现Spark的支持。 这使用户可以通过Spark读取，写入和查询Tsfile。

使用此连接器，您可以

- 从本地文件系统或hdfs加载单个TsFile到Spark
- 将本地文件系统或hdfs中特定目录中的所有文件加载到Spark中
- 将数据从Spark写入TsFile

## 2. System Requirements

| Spark Version | Scala Version | Java Version | TsFile   |
| ------------- | ------------- | ------------ | -------- |
| `2.4.3`       | `2.11.8`      | `1.8`        | `0.10.0` |

> 注意：有关如何下载和使用TsFile的更多信息，请参见以下链接：https://github.com/apache/iotdb/tree/master/tsfile

## 3. 快速开始

### 本地模式

在本地模式下使用TsFile-Spark-Connector启动Spark：

```
./<spark-shell-path>  --jars  tsfile-spark-connector.jar,tsfile-0.10.0-jar-with-dependencies.jar
```

- \<spark-shell-path\>是您的spark-shell的真实路径。
- 多个jar包用逗号分隔，没有任何空格。
- 有关如何获取TsFile的信息，请参见https://github.com/apache/iotdb/tree/master/tsfile。

### 分布式模式

在分布式模式下使用TsFile-Spark-Connector启动Spark（即，Spark集群通过spark-shell连接）：

```
. /<spark-shell-path>   --jars  tsfile-spark-connector.jar,tsfile-{version}-jar-with-dependencies.jar  --master spark://ip:7077
```

注意：

- \<spark-shell-path\>是您的spark-shell的真实路径。
- 多个jar包用逗号分隔，没有任何空格。
- 有关如何获取TsFile的信息，请参见https://github.com/apache/iotdb/tree/master/tsfile。

## 4. 数据类型对应

| TsFile数据类型 | SparkSQL数据类型 |
| -------------- | ---------------- |
| BOOLEAN        | BooleanType      |
| INT32          | IntegerType      |
| INT64          | LongType         |
| FLOAT          | FloatType        |
| DOUBLE         | DoubleType       |
| TEXT           | StringType       |

## 5. 模式推断

显示TsFile的方式取决于架构。 以以下TsFile结构为例：TsFile模式中有三个度量：状态，温度和硬件。 这三种测量的基本信息如下：

<center>
<table style="text-align:center">
	<tr><th colspan="2">名称</th><th colspan="2">类型</th><th colspan="2">编码</th></tr>
	<tr><td colspan="2">状态</td><td colspan="2">Boolean</td><td colspan="2">PLAIN</td></tr>
	<tr><td colspan="2">温度</td><td colspan="2">Float</td><td colspan="2">RLE</td></tr>
	<tr><td colspan="2">硬件</td><td colspan="2">Text</td><td colspan="2">PLAIN</td></tr>
</table>
</center>

TsFile中的现有数据如下：

<center>
<table style="text-align:center">
	<tr><th colspan="4">device:root.ln.wf01.wt01</th><th colspan="4">device:root.ln.wf02.wt02</th></tr>
	<tr><th colspan="2">status</th><th colspan="2">temperature</th><th colspan="2">hardware</th><th colspan="2">status</th></tr>
	<tr><th>time</th><th>value</th><th>time</th><th>value</th><th>time</th><th>value</th><th>time</th><th>value</th></tr>
	<tr><td>1</td><td>True</td><td>1</td><td>2.2</td><td>2</td><td>"aaa"</td><td>1</td><td>True</td></tr>
	<tr><td>3</td><td>True</td><td>2</td><td>2.2</td><td>4</td><td>"bbb"</td><td>2</td><td>False</td></tr>
	<tr><td>5</td><td> False </td><td>3</td><td>2.1</td><td>6</td><td>"ccc"</td><td>4</td><td>True</td></tr>
</table>
</center>

相应的SparkSQL表如下：

| time | root.ln.wf02.wt02.temperature | root.ln.wf02.wt02.status | root.ln.wf02.wt02.hardware | root.ln.wf01.wt01.temperature | root.ln.wf01.wt01.status | root.ln.wf01.wt01.hardware |
| ---- | ----------------------------- | ------------------------ | -------------------------- | ----------------------------- | ------------------------ | -------------------------- |
| 1    | null                          | true                     | null                       | 2.2                           | true                     | null                       |
| 2    | null                          | false                    | aaa                        | 2.2                           | null                     | null                       |
| 3    | null                          | null                     | null                       | 2.1                           | true                     | null                       |
| 4    | null                          | true                     | bbb                        | null                          | null                     | null                       |
| 5    | null                          | null                     | null                       | null                          | false                    | null                       |
| 6    | null                          | null                     | ccc                        | null                          | null                     | null                       |

您还可以使用如下所示的窄表形式：（您可以参阅第6部分，了解如何使用窄表形式）

| time | device_name       | status | hardware | temperature |
| ---- | ----------------- | ------ | -------- | ----------- |
| 1    | root.ln.wf02.wt01 | true   | null     | 2.2         |
| 1    | root.ln.wf02.wt02 | true   | null     | null        |
| 2    | root.ln.wf02.wt01 | null   | null     | 2.2         |
| 2    | root.ln.wf02.wt02 | false  | aaa      | null        |
| 3    | root.ln.wf02.wt01 | true   | null     | 2.1         |
| 4    | root.ln.wf02.wt02 | true   | bbb      | null        |
| 5    | root.ln.wf02.wt01 | false  | null     | null        |
| 6    | root.ln.wf02.wt02 | null   | ccc      | null        |



## 6. Scala API

注意：请记住预先分配必要的读写权限。

### 示例1：从本地文件系统读取

```scala
import org.apache.iotdb.spark.tsfile._
val wide_df = spark.read.tsfile("test.tsfile")  
wide_df.show

val narrow_df = spark.read.tsfile("test.tsfile", true)  
narrow_df.show
```

### 示例2：从hadoop文件系统读取

```scala
import org.apache.iotdb.spark.tsfile._
val wide_df = spark.read.tsfile("hdfs://localhost:9000/test.tsfile") 
wide_df.show

val narrow_df = spark.read.tsfile("hdfs://localhost:9000/test.tsfile", true)  
narrow_df.show
```

### 示例3：从特定目录读取

```scala
import org.apache.iotdb.spark.tsfile._
val df = spark.read.tsfile("hdfs://localhost:9000/usr/hadoop") 
df.show
```

注1：现在不支持目录中所有TsFile的全局时间排序。

注2：具有相同名称的度量应具有相同的架构。

### 示例4：广泛形式的查询

```scala
import org.apache.iotdb.spark.tsfile._
val df = spark.read.tsfile("hdfs://localhost:9000/test.tsfile") 
df.createOrReplaceTempView("tsfile_table")
val newDf = spark.sql("select * from tsfile_table where `device_1.sensor_1`>0 and `device_1.sensor_2` < 22")
newDf.show
```

```scala
import org.apache.iotdb.spark.tsfile._
val df = spark.read.tsfile("hdfs://localhost:9000/test.tsfile") 
df.createOrReplaceTempView("tsfile_table")
val newDf = spark.sql("select count(*) from tsfile_table")
newDf.show
```

### 示例5：缩小形式的查询

```scala
import org.apache.iotdb.spark.tsfile._
val df = spark.read.tsfile("hdfs://localhost:9000/test.tsfile", true) 
df.createOrReplaceTempView("tsfile_table")
val newDf = spark.sql("select * from tsfile_table where device_name = 'root.ln.wf02.wt02' and temperature > 5")
newDf.show
```

```scala
import org.apache.iotdb.spark.tsfile._
val df = spark.read.tsfile("hdfs://localhost:9000/test.tsfile", true) 
df.createOrReplaceTempView("tsfile_table")
val newDf = spark.sql("select count(*) from tsfile_table")
newDf.show
```

### 例6：写宽格式

```scala
// we only support wide_form table to write
import org.apache.iotdb.spark.tsfile._

val df = spark.read.tsfile("hdfs://localhost:9000/test.tsfile") 
df.show
df.write.tsfile("hdfs://localhost:9000/output")

val newDf = spark.read.tsfile("hdfs://localhost:9000/output")
newDf.show
```

## 例6：写窄格式

```scala
// we only support wide_form table to write
import org.apache.iotdb.spark.tsfile._

val df = spark.read.tsfile("hdfs://localhost:9000/test.tsfile", true) 
df.show
df.write.tsfile("hdfs://localhost:9000/output", true)

val newDf = spark.read.tsfile("hdfs://localhost:9000/output", true)
newDf.show
```

## 附录A：模式推断的旧设计

显示TsFile的方式与TsFile Schema有关。 以以下TsFile结构为例：TsFile架构中有三个度量：状态，温度和硬件。 这三个度量的基本信息如下：

<center>
<table style="text-align:center">
	<tr><th colspan="2">名称</th><th colspan="2">类型</th><th colspan="2">编码</th></tr>
	<tr><td colspan="2">状态</td><td colspan="2">Boolean</td><td colspan="2">PLAIN</td></tr>
	<tr><td colspan="2">温度</td><td colspan="2">Float</td><td colspan="2">RLE</td></tr>
	<tr><td colspan="2">硬件</td><td colspan="2">Text</td><td colspan="2">PLAIN</td></tr>
</table>
<span>测量的基本信息</span>
</center>

文件中的现有数据如下：

<center>
<table style="text-align:center">
	<tr><th colspan="4">delta\_object:root.ln.wf01.wt01</th><th colspan="4">delta\_object:root.ln.wf02.wt02</th><th colspan="4">delta\_object:root.sgcc.wf03.wt01</th></tr>
	<tr><th colspan="2">status</th><th colspan="2">temperature</th><th colspan="2">hardware</th><th colspan="2">status</th><th colspan="2">status</th><th colspan="2">temperature</th></tr>
	<tr><th>time</th><th>value</td><th>time</th><th>value</td><th>time</th><th>value</th><th>time</th><th>value</td><th>time</th><th>value</td><th>time</th><th>value</th></tr>
	<tr><td>1</td><td>True</td><td>1</td><td>2.2</td><td>2</td><td>"aaa"</td><td>1</td><td>True</td><td>2</td><td>True</td><td>3</td><td>3.3</td></tr>
	<tr><td>3</td><td>True</td><td>2</td><td>2.2</td><td>4</td><td>"bbb"</td><td>2</td><td>False</td><td>3</td><td>True</td><td>6</td><td>6.6</td></tr>
	<tr><td>5</td><td> False </td><td>3</td><td>2.1</td><td>6</td><td>"ccc"</td><td>4</td><td>True</td><td>4</td><td>True</td><td>8</td><td>8.8</td></tr>
	<tr><td>7</td><td> True </td><td>4</td><td>2.0</td><td>8</td><td>"ddd"</td><td>5</td><td>False</td><td>6</td><td>True</td><td>9</td><td>9.9</td></tr>
</table>
<span>一组时间序列数据</span>
</center>

有两种显示方法：

#### 默认方式

将创建两列来存储设备的完整路径：time（LongType）和delta_object（StringType）。

- `time`：时间戳记，LongType
- `delta_object`：Delta_object ID，StringType

接下来，为每个度量创建一列以存储特定数据。 SparkSQL表结构如下：

<center>
	<table style="text-align:center">
	<tr><th>time(LongType)</th><th> delta\_object(StringType)</th><th>status(BooleanType)</th><th>temperature(FloatType)</th><th>hardware(StringType)</th></tr>
	<tr><td>1</td><td> root.ln.wf01.wt01 </td><td>True</td><td>2.2</td><td>null</td></tr>
	<tr><td>1</td><td> root.ln.wf02.wt02 </td><td>True</td><td>null</td><td>null</td></tr>
	<tr><td>2</td><td> root.ln.wf01.wt01 </td><td>null</td><td>2.2</td><td>null</td></tr>
	<tr><td>2</td><td> root.ln.wf02.wt02 </td><td>False</td><td>null</td><td>"aaa"</td></tr>
	<tr><td>2</td><td> root.sgcc.wf03.wt01 </td><td>True</td><td>null</td><td>null</td></tr>
	<tr><td>3</td><td> root.ln.wf01.wt01 </td><td>True</td><td>2.1</td><td>null</td></tr>
	<tr><td>3</td><td> root.sgcc.wf03.wt01 </td><td>True</td><td>3.3</td><td>null</td></tr>
	<tr><td>4</td><td> root.ln.wf01.wt01 </td><td>null</td><td>2.0</td><td>null</td></tr>
	<tr><td>4</td><td> root.ln.wf02.wt02 </td><td>True</td><td>null</td><td>"bbb"</td></tr>
	<tr><td>4</td><td> root.sgcc.wf03.wt01 </td><td>True</td><td>null</td><td>null</td></tr>
	<tr><td>5</td><td> root.ln.wf01.wt01 </td><td>False</td><td>null</td><td>null</td></tr>
	<tr><td>5</td><td> root.ln.wf02.wt02 </td><td>False</td><td>null</td><td>null</td></tr>
	<tr><td>5</td><td> root.sgcc.wf03.wt01 </td><td>True</td><td>null</td><td>null</td></tr>
	<tr><td>6</td><td> root.ln.wf02.wt02 </td><td>null</td><td>null</td><td>"ccc"</td></tr>
	<tr><td>6</td><td> root.sgcc.wf03.wt01 </td><td>null</td><td>6.6</td><td>null</td></tr>
	<tr><td>7</td><td> root.ln.wf01.wt01 </td><td>True</td><td>null</td><td>null</td></tr>
	<tr><td>8</td><td> root.ln.wf02.wt02 </td><td>null</td><td>null</td><td>"ddd"</td></tr>
	<tr><td>8</td><td> root.sgcc.wf03.wt01 </td><td>null</td><td>8.8</td><td>null</td></tr>
	<tr><td>9</td><td> root.sgcc.wf03.wt01 </td><td>null</td><td>9.9</td><td>null</td></tr>
	</table>
</center>



#### 展开delta_object列

通过“。”将设备列展开为多个列，忽略根目录“root”。方便进行更丰富的聚合操作。如果用户想使用这种显示方式，需要在表创建语句中设置参数“delta\_object\_name”(参考本手册5.1节中的示例5)，在本例中，将参数“delta\_object\_name”设置为“root.device.turbine”。路径层的数量必须是一对一的。此时，除了“根”层之外，为设备路径的每一层创建一列。列名是参数中的名称，值是设备相应层的名称。接下来，将为每个度量创建一个列来存储特定的数据。



那么SparkSQL表结构如下:

<center>
	<table style="text-align:center">
	<tr><th>time(LongType)</th><th> group(StringType)</th><th> field(StringType)</th><th> device(StringType)</th><th>status(BooleanType)</th><th>temperature(FloatType)</th><th>hardware(StringType)</th></tr>
	<tr><td>1</td><td> ln </td><td> wf01 </td><td> wt01 </td><td>True</td><td>2.2</td><td>null</td></tr>
	<tr><td>1</td><td> ln </td><td> wf02 </td><td> wt02 </td><td>True</td><td>null</td><td>null</td></tr>
	<tr><td>2</td><td> ln </td><td> wf01 </td><td> wt01 </td><td>null</td><td>2.2</td><td>null</td></tr>
	<tr><td>2</td><td> ln </td><td> wf02 </td><td> wt02 </td><td>False</td><td>null</td><td>"aaa"</td></tr>
	<tr><td>2</td><td> sgcc </td><td> wf03 </td><td> wt01 </td><td>True</td><td>null</td><td>null</td></tr>
	<tr><td>3</td><td> ln </td><td> wf01 </td><td> wt01 </td><td>True</td><td>2.1</td><td>null</td></tr>
	<tr><td>3</td><td> sgcc </td><td> wf03 </td><td> wt01 </td><td>True</td><td>3.3</td><td>null</td></tr>
	<tr><td>4</td><td> ln </td><td> wf01 </td><td> wt01 </td><td>null</td><td>2.0</td><td>null</td></tr>
	<tr><td>4</td><td> ln </td><td> wf02 </td><td> wt02 </td><td>True</td><td>null</td><td>"bbb"</td></tr>
	<tr><td>4</td><td> sgcc </td><td> wf03 </td><td> wt01 </td><td>True</td><td>null</td><td>null</td></tr>
	<tr><td>5</td><td> ln </td><td> wf01 </td><td> wt01 </td><td>False</td><td>null</td><td>null</td></tr>
	<tr><td>5</td><td> ln </td><td> wf02 </td><td> wt02 </td><td>False</td><td>null</td><td>null</td></tr>
	<tr><td>5</td><td> sgcc </td><td> wf03 </td><td> wt01 </td><td>True</td><td>null</td><td>null</td></tr>
	<tr><td>6</td><td> ln </td><td> wf02 </td><td> wt02 </td><td>null</td><td>null</td><td>"ccc"</td></tr>
	<tr><td>6</td><td> sgcc </td><td> wf03 </td><td> wt01 </td><td>null</td><td>6.6</td><td>null</td></tr>
	<tr><td>7</td><td> ln </td><td> wf01 </td><td> wt01 </td><td>True</td><td>null</td><td>null</td></tr>
	<tr><td>8</td><td> ln </td><td> wf02 </td><td> wt02 </td><td>null</td><td>null</td><td>"ddd"</td></tr>
	<tr><td>8</td><td> sgcc </td><td> wf03 </td><td> wt01 </td><td>null</td><td>8.8</td><td>null</td></tr>
	<tr><td>9</td><td> sgcc </td><td> wf03 </td><td> wt01 </td><td>null</td><td>9.9</td><td>null</td></tr>
	</table>

</center>

TsFile-Spark-Connector可以通过SparkSQL在SparkSQL中以表的形式显示一个或多个tsfile。它还允许用户指定一个目录或使用通配符来匹配多个目录。如果有多个tsfile，那么所有tsfile中的度量值的并集将保留在表中，并且具有相同名称的度量值在默认情况下具有相同的数据类型。注意，如果存在名称相同但数据类型不同的情况，TsFile-Spark-Connector将不能保证结果的正确性。



写入过程是将数据aframe写入一个或多个tsfile。默认情况下，需要包含两个列:time和delta_object。其余的列用作测量。如果用户希望将第二个表结构写回TsFile，可以设置“delta\_object\_name”参数(请参阅本手册5.1节的5.1节)。

## 附录B：旧注

注意：检查Spark根目录中的jar软件包，并将libthrift-0.9.2.jar和libfb303-0.9.2.jar分别替换为libthrift-0.9.1.jar和libfb303-0.9.1.jar。