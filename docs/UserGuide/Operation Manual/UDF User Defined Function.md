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



# UDF (User Defined Function)

IoTDB provides a variety of built-in functions to meet your computing needs, and you can also create user defined functions to meet more computing needs. 

This document describes how to write, register and use a UDF.



## UDF Types

In IoTDB, you can expand two types of UDF:

| UDF Class                                           | Description                                                  |
| --------------------------------------------------- | ------------------------------------------------------------ |
| UDTF（User Defined Timeseries Generating Function） | This type of function can take **multiple** time series as input, and output **one** time series, which can have any number of data points. |
| UDAF（User Defined Aggregation Function）           | Under development, please stay tuned.                        |



## UDF Development Dependencies

If you use [Maven](http://search.maven.org/), you can search for the development dependencies listed below from the [Maven repository](http://search.maven.org/) . Please note that you must select the same dependency version as the target IoTDB server version for development.

``` xml
<dependency>
  <groupId>org.apache.iotdb</groupId>
  <artifactId>iotdb-server</artifactId>
  <version>0.11.0-SNAPSHOT</version>
  <scope>provided</scope>
</dependency>
<dependency>
  <groupId>org.apache.iotdb</groupId>
  <artifactId>tsfile</artifactId>
  <version>0.11.0-SNAPSHOT</version>
  <scope>provided</scope>
</dependency>
```



## UDTF（User Defined Timeseries Generating Function）

To write a UDTF,  you need to inherit the `org.apache.iotdb.db.query.udf.api.UDTF` class, and at least implement the `beforeStart` method and a `transform` method.

The following table shows all the interfaces available for user implementation. 

| Interface definition                                         | Description                                                  | Required to Implement                                 |
| :----------------------------------------------------------- | :----------------------------------------------------------- | ----------------------------------------------------- |
| `void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) throws Exception` | The initialization method to call the user-defined initialization behavior before a UDTF processes the input data. Every time a user executes a UDTF query, the framework will construct a new worker, and `beforeStart` will be called. | Required                                              |
| `void beforeDestroy() `                                      | This method is called by the framework after the last input data is processed, and will only be called once in the life cycle of each worker. | Optional                                              |
| `void transform(Row row, PointCollector collector) throws Exception` | This method is called by the framework. This data processing method will be called when you choose to use the `OneByOneAccessStrategy` strategy (set in `beforeStart`) to consume raw data. Input data is passed in by `Row`, and the transformation result should be output by `PointCollector`. You need to call the data collection method provided by `collector`  to determine the output data. | Required to implement at least one `transform` method |
| `void transform(RowWindow rowWindow, PointCollector collector) throws Exception` | This method is called by the framework. This data processing method will be called when you choose to use the `TumblingWindowAccessStrategy` or `SlidingTimeWindowAccessStrategy` strategy (set in `beforeStart`) to consume raw data. Input data is passed in by `RowWindow`, and the transformation result should be output by `PointCollector`. You need to call the data collection method provided by `collector`  to determine the output data. | Required to implement at least one `transform` method |

Note that every time the framework executes a UDTF query, a new worker will be constructed. When the query ends, the corresponding worker will be destroyed. Therefore, the internal data of the workers in different UDTF queries (even in the same SQL statement) are isolated. You can maintain some state data in the UDTF without considering the influence of concurrency and other factors.

The usage of each interface will be described in detail below.



### `void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) throws Exception`

This method is mainly used to customize UDTF. In this method, the user can do the following things:

1. Use UDFParameters to get the time series paths and parse key-value pair attributes entered by the user.
2. Set the strategy to access the raw data and set the output data type in UDTFConfigurations.
3. Create resources, such as establishing external connections, opening files, etc.




#### `UDFParameters`

`UDFParameters` is used to parse UDF parameters in SQL statements (the part in parentheses after the UDF function name in SQL). The input parameters have two parts. The first part is the paths (measurements) of the time series that the UDF needs to process, and the second part is the key-value pair attributes for customization. Only the second part can be empty.


Example：

``` sql
SELECT UDF(s1, s2, 'key1'='iotdb', 'key2'='123.45') FROM root.sg.d;
```

Usage：

``` java
void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) throws Exception {
  // parameters
	for (PartialPath path : parameters.getPaths()) {
  	// do something
  }
  String stringValue = parameters.getString("key1"); // iotdb
  Float floatValue = parameters.getFloat("key2"); // 123.45
  Double doubleValue = parameters.getDouble("key3"); // null
  int intValue = parameters.getIntOrDefault("key4", 678); // 678
  // do something
  
  // configurations
  // ...
}
```



####  `UDTFConfigurations`

You must use `UDTFConfigurations` to specify the strategy used by UDF to access raw data and the type of output sequence.

Usage：

``` java
void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) throws Exception {
  // parameters
  // ...
  
  // configurations
  configurations
    .setAccessStrategy(new OneByOneAccessStrategy())
    .setOutputDataType(TSDataType.INT32);
}
```

The `setAccessStrategy` method is used to set the UDF's strategy for accessing the raw data, and the `setOutputDataType` method is used to set the data type of the output sequence.



##### `setAccessStrategy`

Note that the raw data access strategy you set here determines which `transform` method the framework will call. Please implement the `transform` method corresponding to the raw data access strategy. Of course, you can also dynamically decide which strategy to set based on the attribute parameters parsed by `UDFParameters`. Therefore, two `transform` methods are also allowed to be implemented in one UDF.

The following are the strategies you can set:

| Interface definition              | Description                                                  | The `transform` Method to Call                               |
| :-------------------------------- | :----------------------------------------------------------- | ------------------------------------------------------------ |
| `OneByOneAccessStrategy`          | Process raw data row by row. The framework calls the `transform` method once for each row of raw data input. When UDF has only one input sequence, a row of input is one data point in the input sequence. When UDF has multiple input sequences, one row of input is a result record of the raw query (aligned by time) on these input sequences. (In a row, there may be a column with a value of `null`, but not all of them are `null`) | `void transform(Row row, PointCollector collector) throws Exception` |
| `SlidingTimeWindowAccessStrategy` | Process a batch of data in a fixed time interval each time. We call the container of a data batch a window. The framework calls the `transform` method once for each raw data input window. There may be multiple rows of data in a window, and each row is a result record of the raw query (aligned by time) on these input sequences. (In a row, there may be a column with a value of `null`, but not all of them are `null`) | `void transform(RowWindow rowWindow, PointCollector collector) throws Exception` |
| `TumblingWindowAccessStrategy`    | The raw data is processed batch by batch, and each batch contains a fixed number of raw data rows (except the last batch). We call the container of a data batch a window. The framework calls the `transform` method once for each raw data input window. There may be multiple rows of data in a window, and each row is a result record of the raw query (aligned by time) on these input sequences. (In a row, there may be a column with a value of `null`, but not all of them are `null`) | `void transform(RowWindow rowWindow, PointCollector collector) throws Exception` |

`OneByOneAccessStrategy`: The construction of `OneByOneAccessStrategy` does not require any parameters.

`SlidingTimeWindowAccessStrategy`: `SlidingTimeWindowAccessStrategy` has 2 types of constructors, each of which requires you to provide 3 types of parameters:

- Parameter 1: The display window on the time axis
- Parameter 2: Time interval for dividing the time axis (should be positive)
- Parameter 3: Time sliding step (not required to be greater than or equal to the time interval, but must be a positive number)

The relationship between the three types of parameters can be seen in the figure below. Please see the Javadoc for more details. 

<div style="text-align: center;"><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/30497621/99787878-47b51480-2b5b-11eb-8ed3-84088c5c30f7.png"></div>

Note that the actual time interval of some of the last time windows may be less than the specified time interval parameter. In addition, there may be cases where the number of data rows in some time windows is 0. In these cases, the framework will also call the `transform` method for the empty windows.

`TumblingWindowAccessStrategy`: One parameter is required to construct `TumblingWindowAccessStrategy`. This parameter specifies the number of data rows contained in a data processing window. Note that the number of data rows in the last window may be less than the specified number of data rows.



##### `setOutputDataType`

Note that the type of output sequence you set here determines the type of data that the `PointCollector` can actually receive in the `transform` method. The relationship between the output data type set in `setOutputDataType` and the actual data output type that `PointCollector` can receive is as follows:

| Output Data Type Set in `setOutputDataType` | Data Type that `PointCollector` Can Receive                  |
| :------------------------------------------ | :----------------------------------------------------------- |
| `INT32`                                     | `int`                                                        |
| `INT64`                                     | `long`                                                       |
| `FLOAT`                                     | `float`                                                      |
| `DOUBLE`                                    | `double`                                                     |
| `BOOLEAN`                                   | `boolean`                                                    |
| `TEXT`                                      | `java.lang.String` and `org.apache.iotdb.tsfile.utils.Binary` |



### `void beforeDestroy() `

The method for terminating a UDF.

This method is called by the framework. For a worker, `beforeDestroy` will be called after the last record is processed. In the entire life cycle of the worker, `beforeDestroy` will only be called once.



### `void transform(Row row, PointCollector collector) throws Exception`

You need to implement this method when you specify the strategy of UDF to read the original data as `OneByOneAccessStrategy`.

This method processes the raw data one row at a time. The raw data is input from `Row` and output by `PointCollector`. You can output any number of data points in one `transform` method call. It should be noted that the type of output data points must be the same as you set in the `beforeStart` method, and the timestamps of output data points must be strictly monotonically increasing.

The following is a complete UDF example that implements the `void transform(Row row, PointCollector collector) throws Exception` method. It is an adder that receives two columns of time series as input. When two data points in a row are not `null`, this UDF will output the algebraic sum of these two data points.

``` java
import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.OneByOneAccessStrategy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class Adder implements UDTF {

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
    configurations
        .setOutputDataType(TSDataType.INT64)
        .setAccessStrategy(new OneByOneAccessStrategy());
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (row.isNull(0) || row.isNull(1)) {
      return;
    }
    collector.putLong(row.getTime(), row.getLong(0) + row.getLong(1));
  }
}
```



### `void transform(RowWindow rowWindow, PointCollector collector) throws Exception`

You need to implement this method when you specify the strategy of UDF to read the original data as `SlidingTimeWindowAccessStrategy` or `TumblingWindowAccessStrategy`.

This method processes a batch of data in a fixed number of rows or a fixed time interval each time, and we call the container containing this batch of data a window. The raw data is input from `RowWindow` and output by `PointCollector`. `RowWindow` can help you access a batch of `Row`, it provides a set of interfaces for random access and iterative access to this batch of `Row`. You can output any number of data points in one `transform` method call. It should be noted that the type of output data points must be the same as you set in the `beforeStart` method, and the timestamps of output data points must be strictly monotonically increasing.

Below is a complete UDF example that implements the `void transform(RowWindow rowWindow, PointCollector collector) throws Exception` method. It is a counter that receives any number of time series as input, and its function is to count and output the number of data rows in each time window within a specified time range.

```java
import java.io.IOException;
import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.RowWindow;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class Counter implements UDTF {

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
    configurations
        .setOutputDataType(TSDataType.INT32)
        .setAccessStrategy(new SlidingTimeWindowAccessStrategy(
            parameters.getLong("time_interval"),
            parameters.getLong("sliding_step"),
            parameters.getLong("display_window_begin"),
            parameters.getLong("display_window_end")));
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) {
    if (rowWindow.windowSize() != 0) {
      collector.putInt(rowWindow.getRow(0).getTime(), rowWindow.windowSize());
    }
  }
}
```



## Maven Project Example

If you use Maven, you can build your own UDF project referring to our **udf-example** module. You can find the project [here](https://github.com/apache/iotdb/tree/master/example/udf).



## UDF Registration

The process of registering a UDF in IoTDB is as follows:

1. Implement a complete UDF class, assuming the full class name of this class is `org.apache.iotdb.udf.ExampleUDTF`.
2. Package your project into a JAR. If you use Maven to manage your project, you can refer to the Maven project example above.
3. Place the JAR package in the directory `iotdb-server-0.11.0-SNAPSHOT/lib` .
4. Register the UDF with the SQL statement, assuming that the name given to the UDF is `example`.

The following shows the SQL syntax of how to register a UDF.

```sql
CREATE FUNCTION <UDF-NAME> AS <UDF-CLASS-FULL-PATHNAME>
```

Here is an example:

```sql
CREATE FUNCTION example AS "org.apache.iotdb.udf.ExampleUDTF"
```

Since UDF instances are dynamically loaded through reflection technology, you do not need to restart the server during the UDF registration process.

Note: Please ensure that the function name given to the UDF is different from all built-in function names. A UDF with the same name as a built-in function can be registered, but cannot be called.

Note: We recommend that you do not use classes that have the same class name but different function logic in different JAR packages. For example, in `UDF(UDAF/UDTF): udf1, udf2`, the JAR package of udf1 is `udf1.jar` and the JAR package of udf2 is `udf2.jar`. Assume that both JAR packages contain the `org.apache.iotdb.udf.ExampleUDTF` class. If you use two UDFs in the same SQL statement at the same time, the system will randomly load either of them and may cause inconsistency in UDF execution behavior.



## UDF Deregistration

The following shows the SQL syntax of how to deregister a UDF.

```sql
DROP FUNCTION <UDF-NAME>
```

Here is an example:

```sql
DROP FUNCTION example
```



## UDF Queries

The usage of UDF is similar to that of built-in aggregation functions.



### Basic SQL syntax support

* Support `SLIMIT` / `SOFFSET`
* Support `LIMIT` / `OFFSET`
* Support `NON ALIGN`
* Support queries with time filters
* Support queries with value filters



### Queries with `*` in `SELECT` Clauses

Assume that there are 2 time series (`root.sg.d1.s1` and `root.sg.d1.s2`)  in the system.

* **`SELECT example(*) from root.sg.d1`**

Then the result set will include the results of `example (root.sg.d1.s1)` and `example (root.sg.d1.s2)`.

* **`SELECT example(s1, *) from root.sg.d1`**

Then the result set will include the results of `example(root.sg.d1.s1, root.sg.d1.s1)` and `example(root.sg.d1.s1, root.sg.d1.s2)`.

* **`SELECT example(*, *) from root.sg.d1`**

Then the result set will include the results of  `example(root.sg.d1.s1, root.sg.d1.s1)`, `example(root.sg.d1.s2, root.sg.d1.s1)`, `example(root.sg.d1.s1, root.sg.d1.s2)` and `example(root.sg.d1.s2, root.sg.d1.s2)`.



### Queries with Key-value Attributes in UDF Parameters

You can pass any number of key-value pair parameters to the UDF when constructing a UDF query. The key and value in the key-value pair need to be enclosed in single or double quotes. Note that key-value pair parameters can only be passed in after all time series have been passed in. Here is a set of examples:

``` sql
SELECT example(s1, "key1"="value1", "key2"="value2"), example(*, "key3"="value3") FROM root.sg.d1;
SELECT example(s1, s2, "key1"="value1", "key2"="value2") FROM root.sg.d1;
```



### Hybrid Queries

Currently IoTDB supports hybrid queries of UDF queries and raw data queries, for example:

``` sql
SELECT s1, s2, example(s1, s2) FROM root.sg.d1;
SELECT *, example(*) FROM root.sg.d1 NON ALIGN;
```



## Show All Registered UDFs

``` sql
SHOW FUNCTIONS
```



## Configurable Properties

When querying by a UDF, IoTDB may prompt that there is insufficient memory. You can resolve the issue by configuring `udf_initial_byte_array_length_for_memory_control`, `udf_memory_budget_in_mb` and `udf_reader_transformer_collector_memory_proportion` in `iotdb-engine.properties` and restarting the server.

