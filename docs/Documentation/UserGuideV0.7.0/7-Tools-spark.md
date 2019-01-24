<!-- TOC -->

- [TsFile-Spark-Connector User Guide](#tsfile-spark-connector-user-guide)
    - [Dependencies & Version](#dependencies--version)
    - [Quick Start](#quick-start)
        - [Step1: Build TsFile-Spark-Connector](#step1-build-tsfile-spark-connector)
        - [Step2: Import Connector Lib into Spark](#step2-import-connector-lib-into-spark)
        - [Step3: Use Connector in Spark](#step3-use-connector-in-spark)
            - [5.2.1.1 Local Mode](#5211-local-mode)
            - [5.2.1.2 Distributed Mode](#5212-distributed-mode)
    - [Detail: Conversion between TsFile and Spark](#detail-conversion-between-tsfile-and-spark)
        - [TsFile Type <-> SparkSQL type](#tsfile-type---sparksql-type)
        - [TsFile Schema <-> SparkSQL Table Structure](#tsfile-schema---sparksql-table-structure)
            - [the default way](#the-default-way)
            - [unfolding delta_object column](#unfolding-delta_object-column)
    - [Example](#example)
        - [5.1 Scala API](#51-scala-api)

<!-- /TOC -->
# TsFile-Spark-Connector User Guide

TsFile-Spark-Connector implements the support of Spark for external data sources of Tsfile type. This enables users to read, write and query Tsfile by Spark.

## Dependencies & Version

The versions required for Spark and Java are as follow:

| Spark Version | Scala Version | Java Version | TsFile |
| ------------- | ------------- | ------------ |------- |
| `2.0+`        | `2.11`        | `1.8`        | `0.7.0`|

> Note: For more information about how to download and use TsFile, please see the following link: https://github.com/thulab/tsfile.git.

## Quick Start

### Step1: Build TsFile-Spark-Connector

To build TsFile-Spark-Connector, you can use the following command:

```
mvn clean scala:compile compile package
```

In addition, you can also choose to download the available lib package directly from our website. The download link will be comming soon.

### Step2: Import Connector Lib into Spark

* import tsfile-spark-connector.jar into spark lib
* replace libthrift-0.9.2.jar and libfb303-0.9.2.jar with libthrift-0.9.1.jar and libfb303-0.9.1.jar respectively.

### Step3: Use Connector in Spark
#### 5.2.1.1 Local Mode

Start Spark with TsFile-Spark-Connector in local mode: 

```
./<spark-shell-path>  --jars  tsfile-<tsfile-version>.jar,tsfile-spark-connector-<connector-version>.jar
```
Note:

* \<spark-shell-path> is the real path of your spark-shell.
* \<tsfile-version> is the tsfile version.
* \<connector-version> is the TsFile-Spark-Connector version. Note that, the version of TsFile-Spark-Connector and TsFile should be correspondence.
* Multiple jar packages are separated by commas without any spaces.
* See https://github.com/thulab/tsfile.git for how to get TsFile.

#### 5.2.1.2 Distributed Mode

Start Spark with TsFile-Spark-Connector in distributed mode (That is, the spark cluster is connected by spark-shell): 

```
. /<spark-shell-path>  --jars tsfile-<tsfile-version>.jar,tsfile-spark-connector-<connector-version>.jar  --master spark://ip:7077
```

Note:

* \<spark-shell-path> is the real path of your spark-shell.
* \<tsfile-version> is the tsfile version.
* \<connector-version> is the TsFile-Spark-Connector version. Note that, the version of TsFile-Spark-Connector and TsFile should be correspondence.
* Multiple jar packages are separated by commas without any spaces.
* See https://github.com/thulab/tsfile.git for how to get TsFile.

## Detail: Conversion between TsFile and Spark

### TsFile Type <-> SparkSQL type

This library uses the following mapping the data type from TsFile to SparkSQL:

| TsFile 		   | SparkSQL|
| --------------| -------------- |
| BOOLEAN       		   | BooleanType    |
| INT32       		   | IntegerType    |
| INT64       		   | LongType       |
| FLOAT       		   | FloatType      |
| DOUBLE      		   | DoubleType     |
| ENUMS     		 		| StringType     |
| TEXT      				| StringType     |

### TsFile Schema <-> SparkSQL Table Structure

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
	<tr><th>time</th><th>value</td><th>time</th><th>value</td><th>time</th><th>value</th><th>time</th><th>value</td><th>time</th><th>value</td><th>time</th><th>value</th></tr>
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

## Example

The path of 'test.tsfile' used in the following examples is "data/test.tsfile". Please upload 'test.tsfile' to hdfs in advance and the directory is "/test.tsfile".

### 5.1 Scala API

* **Example 1**

	```scala
	import cn.edu.tsinghua.tsfile._
	
	//read data in TsFile and create a table
	val df = spark.read.tsfile("/test.tsfile")
	df.createOrReplaceTempView("tsfile_table")
	
	//query with filter
	val newDf = spark.sql("select * from tsfile_table where temperature > 1.2").cache()
	
	newDf.show()
	```

* **Example 2**

	```scala
	val df = spark.read
       .format("cn.edu.tsinghua.tsfile")
       .load("/test.tsfile ")
	df.filter("time < 10").show()
	```

* **Example 3**

	```scala
	//create a table in SparkSQL and build relation with a TsFile
	spark.sql("create temporary view tsfile_table using cn.edu.tsinghua.tsfile options(path = \"test.ts\")")
	
	spark.sql("select * from tsfile_table where temperature > 1.2").show()
	```
	
* **Example 4(using options to read)**

	```scala
	import cn.edu.tsinghua.tsfile._
	
	val df = spark.read.option("delta_object_name", "root.group.field.device").tsfile("/test.tsfile")
	     
	//create a table in SparkSQL and build relation with a TsFile
	df.createOrReplaceTempView("tsfile_table")
	 
	spark.sql("select * from tsfile_table where device = 'wt01' and field = 'wf01' and group = 'ln' and time < 10").show()
	```

* **Example 5(write)**

	```scala
	import cn.edu.tsinghua.tsfile._
	
	val df = spark.read.tsfile("/test.tsfile").write.tsfile("/out")
	```
	
* **Example 6(using options to write)**

	```scala
	import cn.edu.tsinghua.tsfile._
	
	val df = spark.read.option("delta_object_name", "root.group.field.device").tsfile("/test.tsfile")
	     
	df.write.option("delta_object_name", "root.group.field.device").tsfile("/out")
	```

