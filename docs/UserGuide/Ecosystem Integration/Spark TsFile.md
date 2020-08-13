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

# TsFile-Spark-Connector User Guide

## 1. About TsFile-Spark-Connector

TsFile-Spark-Connector implements the support of Spark for external data sources of Tsfile type. This enables users to read, write and query Tsfile by Spark.

With this connector, you can
* load a single TsFile, from either the local file system or hdfs, into Spark
* load all files in a specific directory, from either the local file system or hdfs, into Spark
* write data from Spark into TsFile

## 2. System Requirements

|Spark Version | Scala Version | Java Version | TsFile |
|------------- | ------------- | ------------ |------------ |
| `2.4.3`        | `2.11.8`        | `1.8`        | `0.10.0`|

> Note: For more information about how to download and use TsFile, please see the following link: https://github.com/apache/incubator-iotdb/tree/master/tsfile.

## 3. Quick Start
### Local Mode

Start Spark with TsFile-Spark-Connector in local mode: 

```
./<spark-shell-path>  --jars  tsfile-spark-connector.jar,tsfile-0.10.0-jar-with-dependencies.jar
```

Note:

* \<spark-shell-path\> is the real path of your spark-shell.
* Multiple jar packages are separated by commas without any spaces.
* See https://github.com/apache/incubator-iotdb/tree/master/tsfile for how to get TsFile.


### Distributed Mode

Start Spark with TsFile-Spark-Connector in distributed mode (That is, the spark cluster is connected by spark-shell): 

```
. /<spark-shell-path>   --jars  tsfile-spark-connector.jar,tsfile-{version}-jar-with-dependencies.jar  --master spark://ip:7077
```

Note:

* \<spark-shell-path\> is the real path of your spark-shell.
* Multiple jar packages are separated by commas without any spaces.
* See https://github.com/apache/incubator-iotdb/tree/master/tsfile for how to get TsFile.

## 4. Data Type Correspondence

| TsFile data type | SparkSQL data type|
| --------------| -------------- |
| BOOLEAN       		   | BooleanType    |
| INT32       		   | IntegerType    |
| INT64       		   | LongType       |
| FLOAT       		   | FloatType      |
| DOUBLE      		   | DoubleType     |
| TEXT      				| StringType     |

## 5. Schema Inference

The way to display TsFile is dependent on the schema. Take the following TsFile structure as an example: There are three Measurements in the TsFile schema: status, temperature, and hardware. The basic information of these three measurements is as follows:

<center>
<table style="text-align:center">
	<tr><th colspan="2">Name</th><th colspan="2">Type</th><th colspan="2">Encode</th></tr>
	<tr><td colspan="2">status</td><td colspan="2">Boolean</td><td colspan="2">PLAIN</td></tr>
	<tr><td colspan="2">temperature</td><td colspan="2">Float</td><td colspan="2">RLE</td></tr>
	<tr><td colspan="2">hardware</td><td colspan="2">Text</td><td colspan="2">PLAIN</td></tr>
</table>
</center>

The existing data in the TsFile is as follows:

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



The corresponding SparkSQL table is as follows:

| time | root.ln.wf02.wt02.temperature | root.ln.wf02.wt02.status | root.ln.wf02.wt02.hardware | root.ln.wf01.wt01.temperature | root.ln.wf01.wt01.status | root.ln.wf01.wt01.hardware |
|------|-------------------------------|--------------------------|----------------------------|-------------------------------|--------------------------|----------------------------|
|    1 | null                          | true                     | null                       | 2.2                           | true                     | null                       |
|    2 | null                          | false                    | aaa                        | 2.2                           | null                     | null                       |
|    3 | null                          | null                     | null                       | 2.1                           | true                     | null                       |
|    4 | null                          | true                     | bbb                        | null                          | null                     | null                       |
|    5 | null                          | null                     | null                       | null                          | false                    | null                       |
|    6 | null                          | null                     | ccc                        | null                          | null                     | null                       |

You can also use narrow table form which as follows: (You can see part 6 about how to use narrow form)

| time | device_name                   | status                   | hardware                   | temperature |
|------|-------------------------------|--------------------------|----------------------------|-------------------------------|
|    1 | root.ln.wf02.wt01             | true                     | null                       | 2.2                           |
|    1 | root.ln.wf02.wt02             | true                     | null                       | null                          |
|    2 | root.ln.wf02.wt01             | null                     | null                       | 2.2                          |
|    2 | root.ln.wf02.wt02             | false                    | aaa                        | null                           |
|    3 | root.ln.wf02.wt01             | true                     | null                       | 2.1                           |
|    4 | root.ln.wf02.wt02             | true                     | bbb                        | null                          |
|    5 | root.ln.wf02.wt01             | false                    | null                       | null                          |
|    6 | root.ln.wf02.wt02             | null                     | ccc                        | null                          |



## 6. Scala API

NOTE: Remember to assign necessary read and write permissions in advance.

### Example 1: read from the local file system

```scala
import org.apache.iotdb.spark.tsfile._
val wide_df = spark.read.tsfile("test.tsfile")  
wide_df.show

val narrow_df = spark.read.tsfile("test.tsfile", true)  
narrow_df.show
```

### Example 2: read from the hadoop file system

```scala
import org.apache.iotdb.spark.tsfile._
val wide_df = spark.read.tsfile("hdfs://localhost:9000/test.tsfile") 
wide_df.show

val narrow_df = spark.read.tsfile("hdfs://localhost:9000/test.tsfile", true)  
narrow_df.show
```

### Example 3: read from a specific directory

```scala
import org.apache.iotdb.spark.tsfile._
val df = spark.read.tsfile("hdfs://localhost:9000/usr/hadoop") 
df.show
```

Note 1: Global time ordering of all TsFiles in a directory is not supported now.

Note 2: Measurements of the same name should have the same schema.

### Example 4: query in wide form

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

### Example 5: query in narrow form
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

### Example 6: write in wide form

```scala
// we only support wide_form table to write
import org.apache.iotdb.spark.tsfile._

val df = spark.read.tsfile("hdfs://localhost:9000/test.tsfile") 
df.show
df.write.tsfile("hdfs://localhost:9000/output")

val newDf = spark.read.tsfile("hdfs://localhost:9000/output")
newDf.show
```

## Example 6: write in narrow form

```scala
// we only support wide_form table to write
import org.apache.iotdb.spark.tsfile._

val df = spark.read.tsfile("hdfs://localhost:9000/test.tsfile", true) 
df.show
df.write.tsfile("hdfs://localhost:9000/output", true)

val newDf = spark.read.tsfile("hdfs://localhost:9000/output", true)
newDf.show
```


## Appendix A: Old Design of Schema Inference

The way to display TsFile is related to TsFile Schema. Take the following TsFile structure as an example: There are three Measurements in the Schema of TsFile: status, temperature, and hardware. The basic info of these three Measurements is as follows:

<center>
<table style="text-align:center">
	<tr><th colspan="2">Name</th><th colspan="2">Type</th><th colspan="2">Encode</th></tr>
	<tr><td colspan="2">status</td><td colspan="2">Boolean</td><td colspan="2">PLAIN</td></tr>
	<tr><td colspan="2">temperature</td><td colspan="2">Float</td><td colspan="2">RLE</td></tr>
	<tr><td colspan="2">hardware</td><td colspan="2">Text</td><td colspan="2">PLAIN</td></tr>
</table>
<span>Basic info of Measurements</span>
</center>

The existing data in the file is as follows:

<center>
<table style="text-align:center">
	<tr><th colspan="4">delta\_object:root.ln.wf01.wt01</th><th colspan="4">delta\_object:root.ln.wf02.wt02</th><th colspan="4">delta\_object:root.sgcc.wf03.wt01</th></tr>
	<tr><th colspan="2">status</th><th colspan="2">temperature</th><th colspan="2">hardware</th><th colspan="2">status</th><th colspan="2">status</th><th colspan="2">temperature</th></tr>
	<tr><th>time</th><th>value</th><th>time</th><th>value</th><th>time</th><th>value</th><th>time</th><th>value</th><th>time</th><th>value</th><th>time</th><th>value</th></tr>
	<tr><td>1</td><td>True</td><td>1</td><td>2.2</td><td>2</td><td>"aaa"</td><td>1</td><td>True</td><td>2</td><td>True</td><td>3</td><td>3.3</td></tr>
	<tr><td>3</td><td>True</td><td>2</td><td>2.2</td><td>4</td><td>"bbb"</td><td>2</td><td>False</td><td>3</td><td>True</td><td>6</td><td>6.6</td></tr>
	<tr><td>5</td><td> False </td><td>3</td><td>2.1</td><td>6</td><td>"ccc"</td><td>4</td><td>True</td><td>4</td><td>True</td><td>8</td><td>8.8</td></tr>
	<tr><td>7</td><td> True </td><td>4</td><td>2.0</td><td>8</td><td>"ddd"</td><td>5</td><td>False</td><td>6</td><td>True</td><td>9</td><td>9.9</td></tr>
</table>
<span>A set of time-series data</span>
</center>
There are two ways to show it out:

#### the default way

Two columns will be created to store the full path of the device: time(LongType) and delta_object(StringType).

- `time` : Timestamp, LongType
- `delta_object` : Delta_object ID, StringType

Next, a column is created for each Measurement to store the specific data. The SparkSQL table structure is as follows:

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


#### unfolding delta_object column

Expand the device column by "." into multiple columns, ignoring the root directory "root". Convenient for richer aggregation operations. If the user wants to use this display way, the parameter "delta\_object\_name" needs to be set in the table creation statement (refer to Example 5 in Section 5.1 of this manual), as in this example, parameter "delta\_object\_name" is set to "root.device.turbine". The number of path layers needs to be one-to-one. At this point, one column is created for each layer of the device path except the "root" layer. The column name is the name in the parameter and the value is the name of the corresponding layer of the device. Next, one column will be created for each Measurement to store the specific data.

Then The SparkSQL Table Structure is as follow:

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

TsFile-Spark-Connector can display one or more TsFiles as a table in SparkSQL By SparkSQL. It also allows users to specify a single directory or use wildcards to match multiple directories. If there are multiple TsFiles, the union of the measurements in all TsFiles will be retained in the table, and the measurement with the same name will have the same data type by default. Note that if there is a situation with the same name but different data types, TsFile-Spark-Connector will not guarantee the correctness of the results.

The writing process is to write a DataFrame as one or more TsFiles. By default, two columns need to be included: time and delta_object. The rest of the columns are used as Measurement. If user wants to write the second table structure back to TsFile, user can set the "delta\_object\_name" parameter(refer to Section 5.1 of Section 5.1 of this manual).

## Appendix B: Old Note
NOTE: Check the jar packages in the root directory  of your Spark and replace libthrift-0.9.2.jar and libfb303-0.9.2.jar with libthrift-0.9.1.jar and libfb303-0.9.1.jar respectively.
