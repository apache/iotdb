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

# TsFile-Spark-Connector Quick Guide

## About TsFile-Spark-Connector

TsFile-Spark-Connector provides integration between TsFile and Apache Spark.

With this connector, you can
* load a single TsFile into Spark
* load files in a specified directory into Spark
* write data from Spark to TsFile

Both the local file system and the hadoop file system are supported.

## 2. System Requirements

| Spark Version | Scala Version | Java Version | TsFile |
| ------------- | ------------- | ------------ |------------ |
| `2.0+`        | `2.11`        | `1.8`        | `0.8.0-SNAPSHOT`|

NOTE: Check the jar packages in the root directory  of your Spark and replace libthrift-0.9.2.jar and libfb303-0.9.2.jar with libthrift-0.9.1.jar and libfb303-0.9.1.jar respectively.

## 3. Data Type Correspondence

| TsFile data type | SparkSQL data type|
| --------------| -------------- |
| INT32       		   | IntegerType    |
| INT64       		   | LongType       |
| FLOAT       		   | FloatType      |
| DOUBLE      		   | DoubleType     |
| BYTE_ARRAY      		| StringType     |

## 4. Schema Inference

Illustrate the schema inference between TsFile and Apache Spark using the following example.

Given a TsFile:

<center>
<table style="text-align:center">
	<tr><th colspan="6">delta_object:root.car.turbine1</th></tr>
	<tr><th colspan="2">sensor_1</th><th colspan="2">sensor_2</th><th colspan="2">sensor_3</th></tr>
	<tr><th>time</th><th>value</td><th>time</th><th>value</td><th>time</th><th>value</td>
	<tr><td>1</td><td>1.2</td><td>1</td><td>20</td><td>2</td><td>50</td></tr>
	<tr><td>3</td><td>1.4</td><td>2</td><td>20</td><td>4</td><td>51</td></tr>
	<tr><td>5</td><td>1.1</td><td>3</td><td>21</td><td>6</td><td>52</td></tr>
	<tr><td>7</td><td>1.8</td><td>4</td><td>20</td><td>8</td><td>53</td></tr>
</table>
</center>

The corresponding SparkSQL table:

<center>
	<table style="text-align:center">
	<tr><th>time</th><th> root.car.turbine1.sensor_1</th><th>root.car.turbine1.sensor_2</th><th>root.car.turbine1.sensor_3</th></tr>
	<tr><td>1</td><td>1.2</td><td>20</td><td>null</td></tr>
	<tr><td>2</td><td>null</td><td>20</td><td>50</td></tr>
	<tr><td>3</td><td>1.4</td><td>21</td><td>null</td></tr>
	<tr><td>4</td><td>null</td><td>20</td><td>51</td></tr>
	<tr><td>5</td><td>1.1</td><td>null</td><td>null</td></tr>
	<tr><td>6</td><td>null</td><td>null</td><td>52</td></tr>
	<tr><td>7</td><td>1.8</td><td>null</td><td>null</td></tr>
	<tr><td>8</td><td>null</td><td>null</td><td>53</td></tr>
	</table>

</center>

## 5. Building

```
mvn clean scala:compile compile package
```


## 6. Examples

The path of 'test.tsfile' used in the following examples is "data/test.tsfile". Please upload 'test.tsfile' to hdfs in advance and the directory is "/test.tsfile".


### 6.1 Scala API

* **Example 1**

	```scala
	import org.apache.iotdb.tsfile._
	
	//read data in TsFile and create a table
	val df = spark.read.tsfile("/test.tsfile")
	df.createOrReplaceTempView("tsfile_table")
	
	//query with filter
	val newDf = spark.sql("select * from tsfile_table where s1 > 1.2").cache()
	
	newDf.show()
	```

* **Example 2**

	```scala
	val df = spark.read
       .format("org.apache.iotdb.tsfile")
       .load("/test.tsfile ")
	df.filter("time < 10").show()
	```

* **Example 3**

	```scala
	//create a table in SparkSQL and build relation with a TsFile
	spark.sql("create temporary view tsfile_table using org.apache.iotdb.tsfile options(path = \"test.ts\")")
	
	spark.sql("select * from tsfile_table where s1 > 1.2").show()
	```
	
* **Example 4(using options to read)**

	```scala
	import org.apache.iotdb.tsfile._
	
	val df = spark.read.option("delta_object_name", "root.device.turbine").tsfile("/test.tsfile")
	     
	//create a table in SparkSQL and build relation with a TsFile
	df.createOrReplaceTempView("tsfile_table")
	 
	spark.sql("select * from tsfile_table where turbine = 'd1' and device = 'car' and time < 10").show()
	```

* **Example 5(write)**

	```scala
	import org.apache.iotdb.tsfile._
	
	val df = spark.read.tsfile("/test.tsfile").write.tsfile("/out")
	```
	
* **Example 6(using options to write)**

	```scala
	import org.apache.iotdb.tsfile._
	
	val df = spark.read.option("delta_object_name", "root.device.turbine").tsfile("/test.tsfile")
	     
	df.write.option("delta_object_name", "root.device.turbine").tsfile("/out")
	```


### 6.2 spark-shell
#### 6.2.1 Start Spark
##### 6.2.1.1 Local Mode
```
./spark-2.0.1-bin-hadoop2.7/bin/spark-shell  --jars  tsfile-0.4.0.jar,tsfile-spark-connector-0.4.0.jar
```
ATTENTION:
* Please replace "spark-2.0.1-bin-hadoop2.7/bin/spark-shell" with the real path of your spark-shell.
* Multiple jar packages are separated by commas without any spaces.
* The latest version used is v0.4.0.
##### 6.2.1.2 Distributed Mode
```
. /spark-2.0.1-bin-hadoop2.7/bin/spark-shell  --jars  tsfile-0.4.0.jar,tsfile-spark-connector-0.4.0.jar  --master spark://ip:7077
```

## Appendix: Old Design of Schema Inference

The set of time-series data in section "Time-series Data" is used here to illustrate the mapping from TsFile Schema to SparkSQL Table Stucture.

<center>
<table style="text-align:center">
	<tr><th colspan="6">delta_object:root.car.turbine1</th></tr>
	<tr><th colspan="2">sensor_1</th><th colspan="2">sensor_2</th><th colspan="2">sensor_3</th></tr>
	<tr><th>time</th><th>value</td><th>time</th><th>value</td><th>time</th><th>value</td>
	<tr><td>1</td><td>1.2</td><td>1</td><td>20</td><td>2</td><td>50</td></tr>
	<tr><td>3</td><td>1.4</td><td>2</td><td>20</td><td>4</td><td>51</td></tr>
	<tr><td>5</td><td>1.1</td><td>3</td><td>21</td><td>6</td><td>52</td></tr>
	<tr><td>7</td><td>1.8</td><td>4</td><td>20</td><td>8</td><td>53</td></tr>
</table>
<span>A set of time-series data</span>
</center>

### using delta_object as reserved column

There are two reserved columns in Spark SQL Table:

- `time` : Timestamp, LongType
- `delta_object` : Delta_object ID, StringType

The SparkSQL Table Structure is as follow:

<center>
	<table style="text-align:center">
	<tr><th>time(LongType)</th><th> delta_object(StringType)</th><th>sensor_1(FloatType)</th><th>sensor_2(IntType)</th><th>sensor_3(IntType)</th></tr>
	<tr><td>1</td><td> root.car.turbine1 </td><td>1.2</td><td>20</td><td>null</td></tr>
	<tr><td>2</td><td> root.car.turbine1 </td><td>null</td><td>20</td><td>50</td></tr>
	<tr><td>3</td><td> root.car.turbine1 </td><td>1.4</td><td>21</td><td>null</td></tr>
	<tr><td>4</td><td> root.car.turbine1 </td><td>null</td><td>20</td><td>51</td></tr>
	<tr><td>5</td><td> root.car.turbine1 </td><td>1.1</td><td>null</td><td>null</td></tr>
	<tr><td>6</td><td> root.car.turbine1 </td><td>null</td><td>null</td><td>52</td></tr>
	<tr><td>7</td><td> root.car.turbine1 </td><td>1.8</td><td>null</td><td>null</td></tr>
	<tr><td>8</td><td> root.car.turbine1 </td><td>null</td><td>null</td><td>53</td></tr>
	</table>

</center>


### unfolding delta_object column

If you want to unfold the delta_object column into multi columns you should add an option when read and write:

e.g. 

option("delta_object_name" -> "root.device.turbine")

The "delta_object_name" is reserved key.


Then The SparkSQL Table Structure is as follow:

<center>
	<table style="text-align:center">
	<tr><th>time(LongType)</th><th> device(StringType)</th><th> turbine(StringType)</th><th>sensor_1(FloatType)</th><th>sensor_2(IntType)</th><th>sensor_3(IntType)</th></tr>
	<tr><td>1</td><td> car </td><td> turbine1 </td><td>1.2</td><td>20</td><td>null</td></tr>
	<tr><td>2</td><td> car </td><td> turbine1 </td><td>null</td><td>20</td><td>50</td></tr>
	<tr><td>3</td><td> car </td><td> turbine1 </td><td>1.4</td><td>21</td><td>null</td></tr>
	<tr><td>4</td><td> car </td><td> turbine1 </td><td>null</td><td>20</td><td>51</td></tr>
	<tr><td>5</td><td> car </td><td> turbine1 </td><td>1.1</td><td>null</td><td>null</td></tr>
	<tr><td>6</td><td> car </td><td> turbine1 </td><td>null</td><td>null</td><td>52</td></tr>
	<tr><td>7</td><td> car </td><td> turbine1 </td><td>1.8</td><td>null</td><td>null</td></tr>
	<tr><td>8</td><td> car </td><td> turbine1 </td><td>null</td><td>null</td><td>53</td></tr>
	</table>

</center>


Then you can group by any level in delta_object. And then with the same option you can write this dataframe to TsFile.
