# Chapter 3: Operation Manual

## Sample Data

To make this manual more practical, we will use a specific scenario example to illustrate how to operate IoTDB databases at all stages of use. See [this page](Material-SampleData) for a look. For your convenience, we also provide you with a sample data file in real scenario to import into the IoTDB system for trial and operation.

Download file: [IoTDB-SampleData.txt](sampledata文件下载链接).

## Data Model Selection

Before importing data to IoTDB, we first select the appropriate data storage model according to the [sample data](Material-SampleData), and then create the storage group and timeseries using [SET STORAGE GROUP](Chapter5,setstoragegroup) statement and [CREATE TIMESERIES](Chapter5,createtimeseries) statement respectively.

### Storage Model Selection
According to the data attribute layers described in [sample data](Material-SampleData), we can express it as an attribute hierarchy structure based on the coverage of attributes and the subordinate relationship between them, as shown in Figure 3.1 below. Its hierarchical relationship is: power group layer - power plant layer - device layer - sensor layer. ROOT is the root node, and each node of sensor layer is called a leaf node. In the process of using IoTDB, you can directly connect the attributes on the path from ROOT node to each leaf node with ".", thus forming the name of a timeseries in IoTDB. For example, The left-most path in Figure 3.1 can generate a timeseries named `ROOT.ln.wf01.wt01.status`.

<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577327-7aa50780-1ef4-11e9-9d75-cadabb62444e.jpg">

**Figure 3.1 Attribute hierarchy structure**</center>

After getting the name of the timeseries, we need to set up the storage group according to the actual scenario and scale of the data. Because in the scenario of this chapter data is usually arrived in the unit of groups (i.e., data may be across electric fields and devices), in order to avoid frequent switching of IO when writing data, and to meet the user's requirement of physical isolation of data in the unit of  groups, we set the storage group at the group layer.

### Storage Group Creation
After selecting the storage model, according to which we can set up the corresponding storage group. The SQL statements for creating storage groups are as follows:

```
IoTDB > set storage group to root.ln
IoTDB > set storage group to root.sgcc
```

We can thus create two storage groups using the above two SQL statements.

It is worth noting that when the path itself or the parent/child layer of the path is already set as a storage group, the path is then not allowed to be set as a storage group. For example, it is not feasible to set `root.ln.wf01` as a storage group when there exist two storage groups `root.ln` and `root.sgcc`. The system will give the corresponding error prompt as shown below:

```
IoTDB> set storage group to root.ln.wf01
error: The prefix of root.ln.wf01 has been set to the storage group.
```

### Show Storage Group
After the storage group is created, we can use the [SHOW STORAGE GROUP](Chapter5,showstoragegroup) statement to view all the storage groups. The SQL statement is as follows:

```
IoTDB> show storage group
```

The result is as follows:
<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577338-84c70600-1ef4-11e9-9dab-605b32c02836.jpg"></center>

### Timeseries Creation
According to the storage model selected before, we can create corresponding timeseries in the two storage groups respectively. The SQL statements for creating timeseries are as follows:

```
IoTDB > create timeseries root.ln.wf01.wt01.status with datatype=BOOLEAN,encoding=PLAIN
IoTDB > create timeseries root.ln.wf01.wt01.temperature with datatype=FLOAT,encoding=RLE
IoTDB > create timeseries root.ln.wf02.wt02.hardware with datatype=TEXT,encoding=PLAIN
IoTDB > create timeseries root.ln.wf02.wt02.status with datatype=BOOLEAN,encoding=PLAIN
IoTDB > create timeseries root.sgcc.wf03.wt01.status with datatype=BOOLEAN,encoding=PLAIN
IoTDB > create timeseries root.sgcc.wf03.wt01.temperature with datatype=FLOAT,encoding=RLE
```

It is worth noting that when in the CRATE TIMESERIES statement the encoding method conflicts with the data type, the system will give the corresponding error prompt as shown below:

```
IoTDB> create timeseries root.ln.wf02.wt02.status WITH DATATYPE=BOOLEAN, ENCODING=TS_2DIFF
error: encoding TS_2DIFF does not support BOOLEAN
```

Please refer to [Encoding](Chapter2,encoding) for correspondence between data type and encoding.

### Show Timeseries

Currently, IoTDB supports two ways of viewing timeseries:

* SHOW TIMESERIES statement presents all timeseries information in JSON form 
* SHOW TIMESERIES <`Path`> statement returns all timeseries information and the total number of timeseries under the given <`Path`>  in tabular form. timeseries information includes: timeseries path, storage group it belongs to, data type, encoding type.  <`Path`> needs to be a prefix path or a path with star or a timeseries path. SQL statements are as follows:

```
IoTDB> show timeseries root
IoTDB> show timeseries root.ln
```

The results are shown below respectly:

<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577347-8db7d780-1ef4-11e9-91d6-764e58c10e94.jpg"></center>
<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577359-97413f80-1ef4-11e9-8c10-53b291fc10a5.jpg"></center>

It is worth noting that when the path queries does not exist, the system will give the corresponding error prompt as shown below:

```
IoTDB> show timeseries root.ln.wf03
Msg: Failed to fetch timeseries root.ln.wf03's metadata because: Timeseries does not exist.
```

### Precautions

Version 0.7.0 imposes some limitations on the scale of data that users can operate:

Limit 1: Assuming that the JVM memory allocated to IoTDB at runtime is p and the user-defined size of data in memory written to disk ([group\_size\_in\_byte](Chap4group_size_in_byte)) is Q, then the number of storage groups should not exceed p/q.

Limit 2: The number of timeseries should not exceed the ratio of JVM memory allocated to IoTDB at run time to 20KB.

## Data Import
### Import Historical Data

This feature is not supported in version 0.7.0.

### Import Real-time Data

IoTDB provides users with a variety of ways to insert real-time data, such as directly inputting [INSERT SQL statement](chapter5.InsertRecordStatement) in [Cli/Shell tools](cli-page), or using [Java JDBC](Java-api-page,commingsoon) to perform single or batch execution of [INSERT SQL statement](chapter5.InsertRecordStatement).

This section mainly introduces the use of [INSERT SQL statement](chapter5.InsertRecordStatement) for real-time data import in the scenario. See Section 7.1.3.1 for a detailed syntax of [INSERT SQL statement](chapter5.InsertRecordStatement).

#### Use of INSERT Statements
The [INSERT SQL statement](chapter5.InsertRecordStatement) statement can be used to insert data into one or more specified timeseries that have been created. For each point of data inserted, it consists of a [timestamp](chap2,timestamp) and a sensor acquisition value of a numerical type (see [Data Type](Chapter2datatype)).

In the scenario of this section, take two timeseries `root.ln.wf02.wt02.status` and `root.ln.wf02.wt02.hardware` as an example, and their data types are BOOLEAN and TEXT, respectively.

The sample code for single column data insertion is as follows:
```
IoTDB > insert into root.ln.wf02.wt02(timestamp,status) values(1,true)
IoTDB > insert into root.ln.wf02.wt02(timestamp,hardware) values(1, "v1")
```

The above example code inserts the long integer timestamp and the value "true" into the timeseries `root.ln.wf02.wt02.status` and inserts the long integer timestamp and the value "v1" into the timeseries `root.ln.wf02.wt02.hardware`. When the execution is successful, a prompt "execute successfully" will appear to indicate that the data insertion has been completed.

> Note: In IoTDB, TEXT type data can be represented by single and double quotation marks. The insertion statement above uses double quotation marks for TEXT type data. The following example will use single quotation marks for TEXT type data.

The INSERT statement can also support the insertion of multi-column data at the same time point.  The sample code of  inserting the values of the two timeseries at the same time point '2' is as follows:

```
IoTDB > insert into root.ln.wf02.wt02(timestamp, status, hardware) VALUES (2, false, 'v2')
```

After inserting the data, we can simply query the inserted data using the SELECT statement:

```
IoTDB > select * from root.ln.wf02 where time < 3
```

The result is shown below. From the query results, it can be seen that the insertion statements of single column and multi column data are performed correctly.

<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577384-a3c59800-1ef4-11e9-96c5-f7ef6626ecd0.jpg"></center>

### Error Handling of INSERT Statements
If the user inserts data into a non-existent timeseries, for example, execute the following commands:

```
IoTDB > insert into root.ln.wf02.wt02(timestamp, temperature) values(1,"v1")
```

Because `root.ln.wf02.wt02. temperature` does not exist, the system will return the following ERROR information:

```
error: Timeseries root.ln.wf02.wt02.temperature does not exist.
```
If the data type inserted by the user is inconsistent with the corresponding data type of the timeseries, for example, execute the following command:

```
IoTDB > insert into root.ln.wf02.wt02(timestamp,hardware) values(1,100)
```
The system will return the following ERROR information:

```
error: The TEXT data type should be covered by " or '
```

## Data Query
### Time Slice Query

This chapter mainly introduces the relevant examples of time slice query using IoTDB SELECT statements. Detailed SQL syntax and usage specifications can be found in [SQL Documentation](Chpater5). You can also use the [Java JDBC](Java-api-page,commingsoon) standard interface to execute related queries.

#### Select a Column of Data Based on a Time Interval

The SQL statement is:

```
select temperature from root.ln.wf01.wt01 where time < 2017-11-01T00:08:00.000
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is the temperature sensor (temperature). The SQL statement requires that all temperature sensor values before the time point of "2017-11-01T00:08:00.000" be selected.

The execution result of this SQL statement is as follows:

<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577402-b049f080-1ef4-11e9-9741-e0055379baca.jpg"></center>

#### Select Multiple Columns of Data Based on a Time Interval

The SQL statement is:

```
select status, temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000;
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is "status" and "temperature". The SQL statement requires that the status and temperature sensor values between the time point of "2017-11-01T00:05:00.000" and "2017-11-01T00:12:00.000" be selected.

The execution result of this SQL statement is as follows:
<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577407-b8a22b80-1ef4-11e9-8e7a-c655fcd94912.jpg"></center>

#### Select Multiple Columns of Data for the Same Device According to Multiple Time Intervals
IoTDB supports specifying multiple time interval conditions in a query. Users can combine time interval conditions at will according to their needs. For example, the SQL statement is:

```
select status,temperature from root.ln.wf01.wt01 where (time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000) or (time >= 2017-11-01T16:35:00.000 and time <= 2017-11-01T16:37:00.000);
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is "status" and "temperature"; the statement specifies two different time intervals, namely "2017-11-01T00:05:00.000 to 2017-11-01T00:12:00.000" and "2017-11-01T16:35:00.000 to 2017-11-01T16:37:00.000". The SQL statement requires that the values of selected timeseries satisfying any time interval be selected.

The execution result of this SQL statement is as follows:
<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577418-c657b100-1ef4-11e9-8886-768ec3cda119.jpg"></center>


#### Choose Multiple Columns of Data for Different Devices According to Multiple Time Intervals
The system supports the selection of data in any column in a query, i.e., the selected columns can come from different devices. For example, the SQL statement is:

```
select wf01.wt01.status,wf02.wt02.hardware from root.ln where (time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000) or (time >= 2017-11-01T16:35:00.000 and time <= 2017-11-01T16:37:00.000);
```
which means:

The selected timeseries are "the power supply status of ln group wf01 plant wt01 device" and "the hardware version of ln group wf02 plant wt02 device"; the statement specifies two different time intervals, namely "2017-11-01T00:05:00.000 to 2017-11-01T00:12:00.000" and "2017-11-01T16:35:00.000 to 2017-11-01T16:37:00.000". The SQL statement requires that the values of selected timeseries satisfying any time interval be selected.

The execution result of this SQL statement is as follows:
<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577450-dcfe0800-1ef4-11e9-9399-4ba2b2b7fb73.jpg"></center>

### Down-Frequency Aggregate Query
This section mainly introduces the related examples of down-frequency aggregation query, using the [GROUP BY clause](Chap5-groupbystatement), which is used to partition the result set according to the user's given partitioning conditions and aggregate the partitioned result set. IoTDB supports partitioning result sets according to time intervals, and by default results are sorted by time in ascending order. You can also use the [Java JDBC](Java-api-page,commingsoon) standard interface to execute related queries.

The GROUP BY statement provides users with three types of specified parameters:

* Parameter 1: Time interval for dividing the time axis
* Parameter 2: Time axis origin position (optional)
* Parameter 3: The display window(s) (one or more) on the time axis

The actual meanings of the three types of parameters are shown in Figure 3.2 below. Among them, the paramter 2 is optional. Next we will give three typical examples of frequency reduction aggregation: parameter 2 specified, parameter 2 not specified, and time filtering conditions specified.

<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577465-e8513380-1ef4-11e9-84c6-d0690f2a8113.jpg">

**Figure 3.2 The actual meanings of the three types of parameters**</center>

#### Down-Frequency Aggregate Query without Specifying the Time Axis Origin Position
The SQL statement is:

```
select count(status), max_value(temperature) from root.ln.wf01.wt01 group by (1d, [2017-11-01T00:00:00, 2017-11-07T23:00:00]);
```
which means:

Since the user does not specify the time axis origin position, the GROUP BY statement will by default set the origin at 0 (+0 time zone) on January 1, 1970.

The first parameter of the GROUP BY statement above is the time interval for dividing the time axis. Taking this parameter (1d) as time interval and the default origin as the dividing origin, the time axis is divided into several continuous intervals, which are [0,1d], [1d, 2d], [2d, 3d], etc.

The second parameter of the GROUP BY statement above is the display window paramter, which determines the final display range is [2017-11-01T00:00:00, 2017-11-07T23:00:00].

Then the system will use the time and value filtering condition in the WHERE clause and the second parameter of the GROUP BY statement as the data filtering condition to obtain the data satisfying the filtering condition (which in this case is the data in the range of [2017-11-01T00:00:00, 2017-11-07 T23:00:00]), and map these data to the previously segmented time axis (in this case there are mapped data in every 1-day period from 2017-11-01T00:00:00 to 2017-11-07T23:00:00:00).

Since there is data for each time period in the result range to be displayed, the execution result of the SQL statement is shown below:

<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577537-277f8480-1ef5-11e9-9b0f-c477f3b71acb.jpg"></center>

#### Down-Frequency Aggregate Query Specifying the Time Axis Origin Position
The SQL statement is:

```
select count(status), max_value(temperature) from root.ln.wf01.wt01 group by (1d, 2017-11-03 00:00:00, [2017-11-01 00:00:00, 2017-11-07 23:00:00]);
```

which means:

Since the user specifies the time axis origin position parameter as 2017-11-03 00:00:00, the GROUP BY statement will set the origin at 0 (system default time zone) on November 3, 2017.

The first parameter of the GROUP BY statement above is the time interval for dividing the time axis. Taking this parameter (1d) as time interval and the speicified origin as the dividing origin, the time axis is divided into several continuous intervals, which are [2017-11-02T00:00:00, 2017-11-03T00:00:00], [2017-11-03T00:00:00, 2017-11-04T00:00:00], etc.

The third parameter of the GROUP BY statement above is the display window paramter, which determines the final display range is [2017-11-01T00:00:00, 2017-11-07T23:00:00].

hen the system will use the time and value filtering condition in the WHERE clause and the second parameter of the GROUP BY statement as the data filtering condition to obtain the data satisfying the filtering condition (which in this case is the data in the range of [2017-11-01T00:00:00, 2017-11-07T23:00:00]), and map these data to the previously segmented time axis (in this case there are mapped data in every 1-day period from 2017-11-01T00:00:00 to 2017-11-07T23:00:00:00).

Since there is data for each time period in the result range to be displayed, the execution result of the SQL statement is shown below:

<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577563-3a925480-1ef5-11e9-88da-2d7e3eb4c951.jpg"></center>

#### Down-Frequency Aggregate Query Specifying the Time Filtering Conditions
The SQL statement is:

```
select count(status), max_value(temperature) from root.ln.wf01.wt01 where time > 2017-11-03T06:00:00 and temperature > 20 group by(1h, [2017-11-03T00:00:00, 2017-11-03T23:00:00]);
```
which means:

Since the user does not specify the time axis origin position, the GROUP BY statement will by default set the origin at 0 (+0 time zone) on January 1, 1970.

The first parameter of the GROUP BY statement above is the time interval for dividing the time axis. Taking this parameter (1d) as time interval and the default origin as the dividing origin, the time axis is divided into several continuous intervals, which are [0,1d], [1d, 2d], [2d, 3d], etc.

The second parameter of the GROUP BY statement above is the display window paramter, which determines the final display range is [2017-11-03T00:00:00, 2017-11-03T23:00:00].

Then the system will use the time and value filtering condition in the WHERE clause and the second parameter of the GROUP BY statement as the data filtering condition to obtain the data satisfying the filtering condition (which in this case is the data in the range of (2017-11-03T06:00:00, 2017-11-03T23:00:00] and satisfying root.ln.wf01.wt01.temperature > 20), and map these data to the previously segmented time axis (in this case there are mapped data in every 1-day period from 2017-11-03T00:06:00 to 2017-11-03T23:00:00).

Since there is  no data in the result range [2017-11-03T00:00:00, 2017-11-03T00:06:00], the aggregation results of this segment will be null. There is data in all other time periods in the result range to be displayed. The execution result of the SQL statement is shown below:

<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577582-441bbc80-1ef5-11e9-8b54-3ad1f586bbc4.jpg"></center>

It is worth noting that the path after SELECT in GROUP BY statement must be aggregate function, otherwise the system will give the corresponding error prompt, as shown below:

<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577596-4d0c8e00-1ef5-11e9-9386-cc71f5d90905.jpg"></center>

### Automated Fill
In the actual use of IoTDB, when doing the query operation of timeseries, situations where the value is null at some time points may appear, which will obstruct the further analysis by users. In order to better reflect the degree of data change, users expect missing values to be automatically filled. Therefore, the IoTDB system introduces the function of Automated Fill.

Automated fill function refers to filling empty values according to the user's specified method and effective time range when performing timeseries queries for single or multiple columns. If the queried point's value is not null, the fill function will not work.

> Note: In the current version 0.7.0, IoTDB provides users with two methods: Previous and Linear. The previous method fills blanks with previous value. The linear method fills blanks through linear fitting. And the fill function can only be used when performing point-in-time queries.

#### Fill Function
* Previous Function

When the value of the queried timestamp is null, the value of the previous timestamp is used to fill the blank. The formalized previous method is as follows (see Section 7.1.3.6 for detailed syntax):

```
select <path> from <prefixPath> where time = <T> fill(<data_type>[previous, <before_range>], …)
```

Detailed descriptions of all parameters are given in Table 3-4.

<center>**Table 3-4 Previous fill paramter list**

|Parameter name (case insensitive)|Interpretation|
|:---|:---|
|path, prefixPath|query path; mandatory field|
|T|query timestamp (only one can be specified); mandatory field|
|data\_type|the type of data used by the fill method. Optional values are int32, int64, float, double, boolean, text; optional field|
|before\_range|represents the valid time range of the previous method. The previous method works when there are values in the [T-before\_range, T] range. When before\_range is not specified, before\_range takes the default value T; optional field|
</center>

Here we give an example of filling null values using the previous method. The SQL statement is as follows:

```
select temperature from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(float[previous, 1m]) 
```
which means:

Because the timeseries root.sgcc.wf03.wt01.temperature is null at 2017-11-01T16:37:50.000, the system uses the previous timestamp of 2017-11-01T16:37:50.000 (and the timestamp is in the [2017-11-01T16:36:50.000, 2017-11-01T16:37:50.000] time range) for fill and display.

On the [sample data](Material-SampleData), the execution result of this statement is shown below:
<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577616-67df0280-1ef5-11e9-9dff-2eb8342074eb.jpg"></center>

It is worth noting that if there is no value in the specified valid time range, the system will not fill the null value, as shown below:
<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577679-9f4daf00-1ef5-11e9-8d8b-06a58de6efc1.jpg"></center>

* Linear Method

When the value of the queried timestamp is null, the value of the previous and the next timestamp is used to fill the blank. The formalized linear method is as follows:

```
select <path> from <prefixPath> where time = <T> fill(<data_type>[linear, <before_range>, <after_range>]…)
```
Detailed descriptions of all parameters are given in Table 3-5.

<center>**Table 3-5 Linear fill paramter list**

|Parameter name (case insensitive)|Interpretation|
|:---|:---|
|path, prefixPath|query path; mandatory field|
|T|query timestamp (only one can be specified); mandatory field|
|data_type|the type of data used by the fill method. Optional values are int32, int64, float, double, boolean, text; optional field|
|before\_range, after\_range|represents the valid time range of the linear method. The previous method works when there are values in the [T-before\_range, T+after\_range] range. When before\_range and after\_range are not explicitly specified, both before\_range and after\_range default to infinity; optional field|
</center>

Here we give an example of filling null values using the linear method. The SQL statement is as follows:

```
select temperature from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(float [linear, 1m, 1m])
```
which means:

Because the timeseries root.sgcc.wf03.wt01.temperature is null at 2017-11-01T16:37:50.000, the system uses the previous timestamp 2017-11-01T16:37:00.000 (and the timestamp is in the [2017-11-01T16:36:50.000, 2017-11-01T16:37:50.000] time range) and its value 21.927326, the next timestamp 2017-11-01T16:39:00.000 (and the timestamp is in the [2017-11-01T16:36:50.000, 2017-11-01T16:37:50.000] time range) and its value 25.311783 to perform linear fitting calculation: 21.927326 + (25.311783-21.927326)/60s*50s = 24.747707

On the [sample data](Material-SampleData), the execution result of this statement is shown below:
<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577727-d4f29800-1ef5-11e9-8ff3-3bb519da3993.jpg"></center>

#### Correspondence between Data Type and Fill Method
Data types and the supported fill methods are shown in Table 3-6.

<center>**Table 3-6 Data types and the supported fill methods**

|Data Type|Supported Fill Methods|
|:---|:---|
|boolean|previous|
|int32|previous, linear|
|int64|previous, linear|
|float|previous, linear|
|double|previous, linear|
|text|previous|
</center>

It is worth noting that IoTDB will give error prompts for fill methods that are not supported by data types, as shown below:

<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577741-e340b400-1ef5-11e9-9238-a4eaf498ab84.jpg"></center>

When the fill method is not specified, each data type bears its own default fill methods and parameters. The corresponding relationship is shown in Table 3-7.

<center>**Table 3-7 Default fill methods and parameters for various data types**

|Data Type|Default Fill Methods and Parameters|
|:---|:---|
|boolean|previous, 0|
|int32|linear, 0, 0|
|int64|linear, 0, 0|
|float|linear, 0, 0|
|double|linear, 0, 0|
|text|previous, 0|
</center>

> Note: In version 0.7.0, at least one fill method should be specified in the Fill statement.

### Row and Column Control over Query Results

IoTDB provides [LIMIT/SLIMIT](Chpater5,LimitStatement) clause and [OFFSET/SOFFSET](Chpater5,LimitStatement) clause in order to make users have more control over query results. The use of LIMIT and SLIMIT clauses allows users to control the number of rows and columns of query results, and the use of OFFSET and SOFSET clauses allows users to set the starting position of the results for display.

This chapter mainly introduces related examples of row and column control of query results. You can also use the [Java JDBC](Java-api-page,commingsoon) standard interface to execute queries.

#### Row Control over Query Results
By using LIMIT and OFFSET clauses, users can control the query results in a row-related manner. We will demonstrate how to use LIMIT and OFFSET clauses through the following examples.

* Example 1: basic LIMIT clause

The SQL statement is:

```
select status, temperature from root.ln.wf01.wt01 limit 10
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is "status" and "temperature". The SQL statement requires the first 10 rows of the query result be returned.

The result is shown below:

<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577752-efc50c80-1ef5-11e9-9071-da2bbd8b9bdd.jpg"></center>


* Example 2: LIMIT clause with OFFSET

The SQL statement is:

```
select status, temperature from root.ln.wf01.wt01 limit 5 offset 3
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is "status" and "temperature". The SQL statement requires rows 3 to 7 of the query result be returned (with the first row numbered as row 0).

The result is shown below:

<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577773-08352700-1ef6-11e9-883f-8d353bef2bdc.jpg"></center>

* Example 3: LIMIT clause combined with WHERE clause

The SQL statement is:

```
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time< 2017-11-01T00:12:00.000 limit 2 offset 3
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is "status" and "temperature". The SQL statement requires rows 3 to 4 of  the status and temperature sensor values between the time point of "2017-11-01T00:05:00.000" and "2017-11-01T00:12:00.000" be returned (with the first row numbered as row 0).

The result is shown below:

<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577789-15521600-1ef6-11e9-86ca-d7b2c947367f.jpg"></center>

* Example 4: LIMIT clause combined with GROUP BY clause

The SQL statement is:

```
select count(status), max_value(temperature) from root.ln.wf01.wt01 group by (1d,[2017-11-01T00:00:00, 2017-11-07T23:00:00]) limit 5 offset 3
```
which means:

The SQL statement clause requires rows 3 to 7 of the query result be returned (with the first row numbered as row 0).

The result is shown below:

<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577796-1e42e780-1ef6-11e9-8987-be443000a77e.jpg"></center>

It is worth noting that because the current FILL clause can only fill in the missing value of timeseries at a certain time point, that is to say, the execution result of FILL clause is exactly one line, so LIMIT and OFFSET are not expected to be used in combination with FILL clause, otherwise errors will be prompted. For example, executing the following SQL statement:

```
select temperature from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(float[previous, 1m]) limit 10
```

The SQL statement will not be executed and the corresponding error prompt is given as follows:

<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577806-28fd7c80-1ef6-11e9-938a-f32ae6abdc55.jpg"></center>

#### Column Control over Query Results

By using SLIMIT and SOFFSET clauses, users can control the query results in a column-related manner. We will demonstrate how to use SLIMIT and SOFFSET clauses through the following examples.

* Example 1: basic SLIMIT clause

The SQL statement is:

```
select * from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 slimit 1
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is the first column under this device, i.e., the power supply status. The SQL statement requires the status sensor values between the time point of "2017-11-01T00:05:00.000" and "2017-11-01T00:12:00.000" be selected.

The result is shown below:

<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577813-30bd2100-1ef6-11e9-94ef-dbeb450cf319.jpg"></center>

* Example 2: SLIMIT clause with SOFFSET

The SQL statement is:

```
select * from root.ln.wf01.wt01 
where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 
slimit 1 soffset 1
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is the second column under this device, i.e., the temperature. The SQL statement requires the temperature sensor values between the time point of "2017-11-01T00:05:00.000" and "2017-11-01T00:12:00.000" be selected.

The result is shown below:

<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577827-39adf280-1ef6-11e9-81b5-876769607cd2.jpg"></center>

* Example 3: SLIMIT clause combined with GROUP BY clause

The SQL statement is:

```
select max_value(*) from root.ln.wf01.wt01 group by (1d, [2017-11-01T00:00:00, 2017-11-07T23:00:00]) slimit 1 soffset 1
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is the second column under this device, i.e., the temperature.

The result is shown below:

<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577840-44688780-1ef6-11e9-8abc-04ae78efa85b.jpg"></center>

* Example 4: SLIMIT clause combined with FILL clause

The SQL statement is:

```
select * from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(float[previous, 1m]) slimit 1 soffset 1
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is the second column under this device, i.e., the temperature.

The result is shown below:

<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577855-4d595900-1ef6-11e9-8541-a4accd714b75.jpg"></center>

It is worth noting that SLIMIT clause is expected to be used in conjunction with star path or prefix path, and the system will prompt errors when SLIMIT clause is used in conjunction with complete path query. For example, executing the following SQL statement:

```
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 slimit 1
```

The SQL statement will not be executed and the corresponding error prompt is given as follows:
<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577867-577b5780-1ef6-11e9-978c-e02c1294bcc5.jpg"></center>

#### Row and Column Control over Query Results

In addition to row or column control over query results, IoTDB allows users to control both rows and columns of query results. Here is a complete example with both LIMIT clauses and SLIMIT clauses.

The SQL statement is:

```
select * from root.ln.wf01.wt01 limit 10 offset 100 slimit 2 soffset 0
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is columns 0 to 1 under this device (with the first column numbered as column 0). The SQL statement clause requires rows 100 to 109 of the query result be returned (with the first row numbered as row 0).

The result is shown below:

<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51577879-64984680-1ef6-11e9-9d7b-57dd60fab60e.jpg"></center>

####  Error Handling

When the parameter N/SN of LIMIT/SLIMIT exceeds the size of the result set, IoTDB will return all the results as expected. For example, the query result of the original SQL statement consists of six rows, and we select the first 100 rows through the LIMIT clause:

```
select status,temperature from root.ln.wf01.wt01 
where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 
limit 100
```
The result is shown below:

<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51578187-ad9cca80-1ef7-11e9-897a-83e66a0f3d94.jpg"></center>

When the parameter N/SN of LIMIT/SLIMIT clause exceeds the allowable maximum value (N/SN is of type int32), the system will prompt errors. For example, executing the following SQL statement:

```
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 limit 1234567890123456789
```
The SQL statement will not be executed and the corresponding error prompt is given as follows:

<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51578206-b7263280-1ef7-11e9-8607-17ef3602338a.jpg"></center>

When the parameter N/SN of LIMIT/SLIMIT clause is not a positive intege, the system will prompt errors. For example, executing the following SQL statement:

```
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 limit 13.1
```

The SQL statement will not be executed and the corresponding error prompt is given as follows:

<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51578217-bdb4aa00-1ef7-11e9-9a60-1effd6b6196c.jpg"></center>

When the parameter OFFSET of LIMIT clause exceeds the size of the result set, IoTDB will return an empty result set. For example, executing the following SQL statement:

```
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 limit 2 offset 6
```
The result is shown below:
<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51578227-c60ce500-1ef7-11e9-98eb-175beb8d4086.jpg"></center>

When the parameter SOFFSET of SLIMIT clause is not smaller than the number of available timeseries, the system will prompt errors. For example, executing the following SQL statement:

```
select * from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 slimit 1 soffset 2
```
The SQL statement will not be executed and the corresponding error prompt is given as follows:
<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51578237-cd33f300-1ef7-11e9-9aef-2a717c56ab54.jpg"></center>

## Data Maintenance
### Data Update

Users can use [UPDATE statements](Chap5,updatestatement) to update data over a period of time in a specified timeseries. When updating data, users can select a timeseries to be updated (version 0.7.0 does not support multiple timeseries updates) and specify a time point or period to be updated (version 0.7.0 must have time filtering conditions).

In a JAVA programming environment, you can use the [Java JDBC](Java-api-page,commingsoon) to execute single or batch UPDATE statements.

#### Update Single Timeseries
Taking the power supply status of ln group wf02 plant wt02 device as an example, there exists such a usage scenario:

After data access and analysis, it is found that the power supply status from 2017-11-01 15:54:00 to 2017-11-01 16:00:00 is true, but the actual power supply status is abnormal. You need to update the status to false during this period. The SQL statement for this operation is:

```
update root.ln.wf02 SET wt02.status = false where time <=2017-11-01T16:00:00 and time >= 2017-11-01T15:54:00
```
It should be noted that when the updated data type does not match the actual data type, IoTDB will give the corresponding error prompt as shown below:

```
IoTDB> update root.ln.wf02 set wt02.status = 1205 where time < now()
error: The BOOLEAN data type should be true/TRUE or false/FALSE
```
When the updated path does not exist, IoTDB will give the corresponding error prompt as shown below:

```
IoTDB> update root.ln.wf02 set wt02.sta = false where time < now()
error: do not select any existing path
```
### Data Deletion

Users can delete data that meet the deletion condition in the specified timeseries by using the [DELETE statement](Chap5,deletestatement). When deleting data, users can select one or more timeseries paths, prefix paths, or paths with star  to delete data before a certain time (version 0.7.0 does not support the deletion of data within a closed time interval).

In a JAVA programming environment, you can use the [Java JDBC](Java-api-page,commingsoon) to execute single or batch UPDATE statements.

#### Delete Single Timeseries
Taking ln Group as an example, there exists such a usage scenario:

The wf02 plant's wt02 device has many segments of errors in its power supply status before 2017-11-01 16:26:00, and the data cannot be analyzed correctly. The erroneous data affected the correlation analysis with other devices. At this point, the data before this time point needs to be deleted. The SQL statement for this operation is

```
delete from root.ln.wf02.wt02.status where time<=2017-11-01T16:26:00;
```

#### Delete Multiple Timeseries
When both the power supply status and hardware version of the ln group wf02 plant wt02 device before 2017-11-01 16:26:00 need to be deleted, the prefix path with broader meaning (see Section 3.1.6 of this manual) or the path with star can be used to delete the data. The SQL statement for this operation is:

```
delete from root.ln.wf02.wt02 where time <= 2017-11-01T16:26:00;
```
or

```
delete from root.ln.wf02.wt02.* where time <= 2017-11-01T16:26:00;
```
It should be noted that when the deleted path does not exist, IoTDB will give the corresponding error prompt as shown below:

```
IoTDB> delete from root.ln.wf03.wt02.status where time < now()
error: TimeSeries does not exist and cannot be delete data
```

## Priviledge Management
IoTDB provides users with priviledge management operations, so as to ensure data security.

We will show you basic user priviledge management operations through the following specific examples. Detailed SQL syntax and usage details can be found in [Chapter5.SQL Documentation](chap5). At the same time, in the JAVA programming environment, you can use the [Java JDBC](Java-api-page,commingsoon) to execute priviledge management statements in a single or batch mode. 

### Basic Concepts
#### User
The user is the legal user of the database. A user corresponds to a unique username and has a password as a means of authentication. Before using a database, a person must first provide a legitimate username and password to make himself/herself a user.

#### Priviledge
The database provides a variety of operations, and not all users can perform all operations. If a user can perform an operation, the user is said to have the priviledge to perform the operation. Priviledges can be divided into data management priviledge (such as adding, deleting and modifying data) and authority management priviledge (such as creation and deletion of users and roles, granting and revoking of priviledges, etc.). Data management priviledge often needs a path to limit its effective range, which is a subtree rooted at the path's corresponding node.

#### Role
A role is a set of priviledges and has a unique role name as an identifier. A user usually corresponds to a real identity (such as a traffic dispatcher), while a real identity may correspond to multiple users. These users with the same real identity tend to have the same priviledges. Roles are abstractions that can unify the management of such priviledges.

#### Default User
There is a default user in IoTDB after the initial installation: root, and the default password is root. This user is an administrator user, who cannot be deleted and has all the priviledges. Neither can new priviledges be granted to the root user nor can priviledges owned by the root user be deleted.

### Priviledge Management Operation Examples
According to the [sample data](material-sampledata), the sample data of IoTDB may belong to different power generation groups such as ln, sgcc, etc. Different power generation groups do not want others to obtain their own database data, so we need to have data priviledge isolated at the group layer.

#### Create User

We can create two users for ln and sgcc groups, named ln\_write\_user and sgcc\_write\_user, with both passwords being write\_pwd. The SQL statement is:

```
CREATE USER ln_write_user write_pwd
CREATE USER sgcc_write_user write_pwd
```
Then use the following SQL statement to show the user:

```
LIST USER
```
As can be seen from the result shown below, the two users have been created:

<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51578263-e2a91d00-1ef7-11e9-94e8-28819b6fea87.jpg"></center>

#### Grant User Priviledge
At this point, although two users have been created, they do not have any priviledges, so they can not operate on the database. For example, we use ln_write_user to write data in the database, the SQL statement is:

```
INSERT INTO root.ln.wf01.wt01(timestamp,status) values(1509465600000,true)
```
The SQL statement will not be executed and the corresponding error prompt is given as follows:

<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51578935-2c930280-1efa-11e9-9e94-b562b7c69f48.jpg"></center>

Now, we grant the two users write priviledges to the corresponding storage groups, and try to write data again. The SQL statement is:

```
GRANT USER ln_write_user PRIVILEGES 'INSERT_TIMESERIES' on root.ln
GRANT USER sgcc_write_user PRIVILEGES 'INSERT_TIMESERIES' on root.sgcc
INSERT INTO root.ln.wf01.wt01(timestamp, status) values(1509465600000, true)
```
The execution result is as follows:
<center><img style="width:80%" src="https://user-images.githubusercontent.com/13203019/51578942-33ba1080-1efa-11e9-891c-09d69791aff1.jpg"></center>

### Other Instructions
#### The Relationship among Users, Priviledges and Roles

A Role is a set of priviledges, and priviledges and roles are both attributes of users. That is, a role can have several priviledges and a user can have several roles and priviledges (called the user's own priviledges).

At present, there is no conflicting priviledge in IoTDB, so the real priviledges of a user is the union of the user's own priviledges and the priviledges of the user's roles. That is to say, to determine whether a user can perform an operation, it depends on whether one of the user's own priviledges or the priviledges of the user's roles permits the operation. The user's own priviledges and priviledges of the user's roles may overlap, but it does not matter.

It should be noted that if users have a priviledge (corresponding to operation A) themselves and their roles contain the same priviledge, then revoking the priviledge from the users themselves alone can not prohibit the users from performing operation A, since it is necessary to revoke the priviledge from the role, or revoke the role from the user. Similarly, revoking the priviledge from the users's roles alone can not prohibit the users from performing operation A.

At the same time, changes to roles are immediately reflected on all users who own the roles. For example, adding certain priviledges to roles will immediately give all users who own the roles corresponding priviledges, and deleting certain priviledges will also deprive the corresponding users of the priviledges (unless the users themselves have the priviledges).

#### List of Priviledges Included in the System

<center>**Table 3-8 List of Priviledges Included in the System**

|Priviledge Name|Interpretation|
|:---|:---|
|SET\_STORAGE\_GROUP|create timeseries; set storage groups; path dependent|
|INSERT\_TIMESERIES|insert data; path dependent|
|UPDATE\_TIMESERIES|update data; path dependent|
|READ\_TIMESERIES|query data; path dependent|
|DELETE\_TIMESERIES|delete data or timeseries; path dependent|
|CREATE\_USER|create users; path independent|
|DELETE\_USER|delete users; path independent|
|MODIFY\_PASSWORD|modify passwords for all users; path independent; (Those who do not have this priviledge can still change their own asswords. )|
|LIST\_USER|list all users; list a user's priviledges; list a user's roles with three kinds of operation priviledges; path independent|
|GRANT\_USER\_PRIVILEGE|grant user priviledges; path independent|
|REVOKE\_USER\_PRIVILEGE|revoke user priviledges; path independent|
|GRANT\_USER\_ROLE|grant user roles; path independent|
|REVOKE\_USER\_ROLE|revoke user roles; path independent|
|CREATE\_ROLE|create roles; path independent|
|DELETE\_ROLE|delete roles; path independent|
|LIST\_ROLE|list all roles; list the priviledges of a role; list the three kinds of operation priviledges of all users owning a role; path independent|
|GRANT\_ROLE\_PRIVILEGE|grant role priviledges; path independent|
|REVOKE\_ROLE\_PRIVILEGE|revoke role priviledges; path independent|
</center>

#### Username Restrictions
IoTDB specifies that the character length of a username should not be less than 4, and the username cannot contain spaces.
#### Password Restrictions
IoTDB specifies that the character length of a password should not be less than 4, and the password cannot contain spaces. The password is encrypted with MD5.
#### Role Name Restrictions
IoTDB specifies that the character length of a role name should not be less than 4, and the role name cannot contain spaces.
