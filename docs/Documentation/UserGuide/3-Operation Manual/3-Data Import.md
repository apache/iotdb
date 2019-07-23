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

# Chapter 3: Operation Manual

## Data Import
### Import Historical Data

This feature is not supported in version 0.7.0.

### Import Real-time Data

IoTDB provides users with a variety of ways to insert real-time data, such as directly inputting [INSERT SQL statement](/#/Documents/latest/chap5/sec1) in [Cli/Shell tools](/#/Tools/Cli), or using [Java JDBC](/#/Documents/latest/chap6/sec1) to perform single or batch execution of [INSERT SQL statement](/#/Documents/latest/chap5/sec1).

This section mainly introduces the use of [INSERT SQL statement](/#/Documents/latest/chap5/sec1) for real-time data import in the scenario. See Section 5.1 for a detailed syntax of [INSERT SQL statement](/#/Documents/latest/chap5/sec1).

#### Use of INSERT Statements
The [INSERT SQL statement](/#/Documents/latest/chap5/sec1) statement can be used to insert data into one or more specified timeseries that have been created. For each point of data inserted, it consists of a [timestamp](/#/Documents/latest/chap2/sec1) and a sensor acquisition value of a numerical type (see [Data Type](/#/Documents/latest/chap2/sec2)).

In the scenario of this section, take two timeseries `root.ln.wf02.wt02.status` and `root.ln.wf02.wt02.hardware` as an example, and their data types are BOOLEAN and TEXT, respectively.

The sample code for single column data insertion is as follows:
```
IoTDB > insert into root.ln.wf02.wt02(timestamp,status) values(1,true)
IoTDB > insert into root.ln.wf02.wt02(timestamp,hardware) values(1, "v1")
```

The above example code inserts the long integer timestamp and the value "true" into the timeseries `root.ln.wf02.wt02.status` and inserts the long integer timestamp and the value "v1" into the timeseries `root.ln.wf02.wt02.hardware`. When the execution is successful, cost time is shown to indicate that the data insertion has been completed.

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

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51605021-c2ee1500-1f48-11e9-8f6b-ba9b48875a41.png"></center>

### Error Handling of INSERT Statements
If the user inserts data into a non-existent timeseries, for example, execute the following commands:

```
IoTDB > insert into root.ln.wf02.wt02(timestamp, temperature) values(1,"v1")
```

Because `root.ln.wf02.wt02. temperature` does not exist, the system will return the following ERROR information:

```
Msg: Current deviceId[root.ln.wf02.wt02] does not contains measurement:temperature
```
If the data type inserted by the user is inconsistent with the corresponding data type of the timeseries, for example, execute the following command:

```
IoTDB > insert into root.ln.wf02.wt02(timestamp,hardware) values(1,100)
```
The system will return the following ERROR information:

```
error: The TEXT data type should be covered by " or '
```
