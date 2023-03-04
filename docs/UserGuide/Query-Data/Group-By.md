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

# Group By Aggregate

## Aggregation By Level

Aggregation by level statement is used to group the query result whose name is the same at the given level. 

- Keyword `LEVEL` is used to specify the level that need to be grouped.  By convention, `level=0` represents *root* level. 
- All aggregation functions are supported. When using five aggregations: sum, avg, min_value, max_value and extreme, please make sure all the aggregated series have exactly the same data type. Otherwise, it will generate a syntax error.

**Example 1:** there are multiple series named `status` under different databases， like "root.ln.wf01.wt01.status", "root.ln.wf02.wt02.status", and "root.sgcc.wf03.wt01.status". If you need to count the number of data points of the `status` sequence under different databases, use the following query:

```sql
select count(status) from root.** group by level = 1
```

Result：

```
+-------------------------+---------------------------+
|count(root.ln.*.*.status)|count(root.sgcc.*.*.status)|
+-------------------------+---------------------------+
|                    20160|                      10080|
+-------------------------+---------------------------+
Total line number = 1
It costs 0.003s
```

**Example 2:** If you need to count the number of data points under different devices, you can specify level = 3,

```sql
select count(status) from root.** group by level = 3
```

Result：

```
+---------------------------+---------------------------+
|count(root.*.*.wt01.status)|count(root.*.*.wt02.status)|
+---------------------------+---------------------------+
|                      20160|                      10080|
+---------------------------+---------------------------+
Total line number = 1
It costs 0.003s
```

**Example 3:** Attention，the devices named `wt01` under databases `ln` and `sgcc` are grouped together, since they are regarded as devices with the same name. If you need to further count the number of data points in different devices under different databases, you can use the following query:

```sql
select count(status) from root.** group by level = 1, 3
```

Result：

```
+----------------------------+----------------------------+------------------------------+
|count(root.ln.*.wt01.status)|count(root.ln.*.wt02.status)|count(root.sgcc.*.wt01.status)|
+----------------------------+----------------------------+------------------------------+
|                       10080|                       10080|                         10080|
+----------------------------+----------------------------+------------------------------+
Total line number = 1
It costs 0.003s
```

**Example 4:** Assuming that you want to query the maximum value of temperature sensor under all time series, you can use the following query statement:

```sql
select max_value(temperature) from root.** group by level = 0
```

Result：

```
+---------------------------------+
|max_value(root.*.*.*.temperature)|
+---------------------------------+
|                             26.0|
+---------------------------------+
Total line number = 1
It costs 0.013s
```

**Example 5:** The above queries are for a certain sensor. In particular, **if you want to query the total data points owned by all sensors at a certain level**, you need to explicitly specify `*` is selected.

```sql
select count(*) from root.ln.** group by level = 2
```

Result：

```
+----------------------+----------------------+
|count(root.*.wf01.*.*)|count(root.*.wf02.*.*)|
+----------------------+----------------------+
|                 20160|                 20160|
+----------------------+----------------------+
Total line number = 1
It costs 0.013s
```

## Downsampling Aggregate Query

Segmentation aggregation is a typical query method for time series data. Data is collected at high frequency and needs to be aggregated and calculated at certain time intervals. For example, to calculate the daily average temperature, the sequence of temperature needs to be segmented by day, and then calculated. average value.

Downsampling query refers to a query method that uses a lower frequency than the time frequency of data collection, and is a special case of segmented aggregation. For example, the frequency of data collection is one second. If you want to display the data in one minute, you need to use downsampling query.

This section mainly introduces the related examples of downsampling aggregation query,  using the `GROUP BY` clause.  IoTDB supports partitioning result sets according to time interval and customized sliding step. And by default results are sorted by time in ascending order. 

The GROUP BY statement provides users with three types of specified parameters:

* Parameter 1: The display window on the time axis
* Parameter 2: Time interval for dividing the time axis(should be positive)
* Parameter 3: Time sliding step (optional and defaults to equal the time interval if not set)

The actual meanings of the three types of parameters are shown in Figure below. 
Among them, the parameter 3 is optional. 

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="/img/github/69109512-f808bc80-0ab2-11ea-9e4d-b2b2f58fb474.png">
    </center>

There are three typical examples of frequency reduction aggregation: 

### Downsampling Aggregate Query without Specifying the Sliding Step Length

The SQL statement is:

```sql
select count(status), max_value(temperature) from root.ln.wf01.wt01 group by ([2017-11-01T00:00:00, 2017-11-07T23:00:00),1d);
```
which means:

Since the sliding step length is not specified, the GROUP BY statement by default set the sliding step the same as the time interval which is `1d`.

The fist parameter of the GROUP BY statement above is the display window parameter, which determines the final display range is [2017-11-01T00:00:00, 2017-11-07T23:00:00).

The second parameter of the GROUP BY statement above is the time interval for dividing the time axis. Taking this parameter (1d) as time interval and startTime of the display window as the dividing origin, the time axis is divided into several continuous intervals, which are [0,1d), [1d, 2d), [2d, 3d), etc.

Then the system will use the time and value filtering condition in the WHERE clause and the first parameter of the GROUP BY statement as the data filtering condition to obtain the data satisfying the filtering condition (which in this case is the data in the range of [2017-11-01T00:00:00, 2017-11-07 T23:00:00]), and map these data to the previously segmented time axis (in this case there are mapped data in every 1-day period from 2017-11-01T00:00:00 to 2017-11-07T23:00:00:00).

Since there is data for each time period in the result range to be displayed, the execution result of the SQL statement is shown below:

```
+-----------------------------+-------------------------------+----------------------------------------+
|                         Time|count(root.ln.wf01.wt01.status)|max_value(root.ln.wf01.wt01.temperature)|
+-----------------------------+-------------------------------+----------------------------------------+
|2017-11-01T00:00:00.000+08:00|                           1440|                                    26.0|
|2017-11-02T00:00:00.000+08:00|                           1440|                                    26.0|
|2017-11-03T00:00:00.000+08:00|                           1440|                                   25.99|
|2017-11-04T00:00:00.000+08:00|                           1440|                                    26.0|
|2017-11-05T00:00:00.000+08:00|                           1440|                                    26.0|
|2017-11-06T00:00:00.000+08:00|                           1440|                                   25.99|
|2017-11-07T00:00:00.000+08:00|                           1380|                                    26.0|
+-----------------------------+-------------------------------+----------------------------------------+
Total line number = 7
It costs 0.024s
```

### Downsampling Aggregate Query Specifying the Sliding Step Length

The SQL statement is:

```sql
select count(status), max_value(temperature) from root.ln.wf01.wt01 group by ([2017-11-01 00:00:00, 2017-11-07 23:00:00), 3h, 1d);
```

which means:

Since the user specifies the sliding step parameter as 1d, the GROUP BY statement will move the time interval `1 day` long instead of `3 hours` as default.

That means we want to fetch all the data of 00:00:00 to 02:59:59 every day from 2017-11-01 to 2017-11-07.

The first parameter of the GROUP BY statement above is the display window parameter, which determines the final display range is [2017-11-01T00:00:00, 2017-11-07T23:00:00).

The second parameter of the GROUP BY statement above is the time interval for dividing the time axis. Taking this parameter (3h) as time interval and the startTime of the display window as the dividing origin, the time axis is divided into several continuous intervals, which are [2017-11-01T00:00:00, 2017-11-01T03:00:00), [2017-11-02T00:00:00, 2017-11-02T03:00:00), [2017-11-03T00:00:00, 2017-11-03T03:00:00), etc.

The third parameter of the GROUP BY statement above is the sliding step for each time interval moving.

Then the system will use the time and value filtering condition in the WHERE clause and the first parameter of the GROUP BY statement as the data filtering condition to obtain the data satisfying the filtering condition (which in this case is the data in the range of [2017-11-01T00:00:00, 2017-11-07T23:00:00]), and map these data to the previously segmented time axis (in this case there are mapped data in every 3-hour period for each day from 2017-11-01T00:00:00 to 2017-11-07T23:00:00:00).

Since there is data for each time period in the result range to be displayed, the execution result of the SQL statement is shown below:

```
+-----------------------------+-------------------------------+----------------------------------------+
|                         Time|count(root.ln.wf01.wt01.status)|max_value(root.ln.wf01.wt01.temperature)|
+-----------------------------+-------------------------------+----------------------------------------+
|2017-11-01T00:00:00.000+08:00|                            180|                                   25.98|
|2017-11-02T00:00:00.000+08:00|                            180|                                   25.98|
|2017-11-03T00:00:00.000+08:00|                            180|                                   25.96|
|2017-11-04T00:00:00.000+08:00|                            180|                                   25.96|
|2017-11-05T00:00:00.000+08:00|                            180|                                    26.0|
|2017-11-06T00:00:00.000+08:00|                            180|                                   25.85|
|2017-11-07T00:00:00.000+08:00|                            180|                                   25.99|
+-----------------------------+-------------------------------+----------------------------------------+
Total line number = 7
It costs 0.006s
```

The sliding step can be smaller than the interval, in which case there is overlapping time between the aggregation windows (similar to a sliding window).

The SQL statement is:

```sql
select count(status), max_value(temperature) from root.ln.wf01.wt01 group by ([2017-11-01 00:00:00, 2017-11-01 10:00:00), 4h, 2h);
```

The execution result of the SQL statement is shown below:

```
+-----------------------------+-------------------------------+----------------------------------------+
|                         Time|count(root.ln.wf01.wt01.status)|max_value(root.ln.wf01.wt01.temperature)|
+-----------------------------+-------------------------------+----------------------------------------+
|2017-11-01T00:00:00.000+08:00|                            180|                                   25.98|
|2017-11-01T02:00:00.000+08:00|                            180|                                   25.98|
|2017-11-01T04:00:00.000+08:00|                            180|                                   25.96|
|2017-11-01T06:00:00.000+08:00|                            180|                                   25.96|
|2017-11-01T08:00:00.000+08:00|                            180|                                    26.0|
+-----------------------------+-------------------------------+----------------------------------------+
Total line number = 5
It costs 0.006s
```

### Downsampling Aggregate Query by Natural Month

The SQL statement is:

```sql
select count(status) from root.ln.wf01.wt01 group by([2017-11-01T00:00:00, 2019-11-07T23:00:00), 1mo, 2mo);
```

which means:

Since the user specifies the sliding step parameter as `2mo`, the GROUP BY statement will move the time interval `2 months` long instead of `1 month` as default.

The first parameter of the GROUP BY statement above is the display window parameter, which determines the final display range is [2017-11-01T00:00:00, 2019-11-07T23:00:00).

The start time is 2017-11-01T00:00:00. The sliding step will increment monthly based on the start date, and the 1st day of the month will be used as the time interval's start time.

The second parameter of the GROUP BY statement above is the time interval for dividing the time axis. Taking this parameter (1mo) as time interval and the startTime of the display window as the dividing origin, the time axis is divided into several continuous intervals, which are [2017-11-01T00:00:00, 2017-12-01T00:00:00), [2018-02-01T00:00:00, 2018-03-01T00:00:00), [2018-05-03T00:00:00, 2018-06-01T00:00:00)), etc.

The third parameter of the GROUP BY statement above is the sliding step for each time interval moving.

Then the system will use the time and value filtering condition in the WHERE clause and the first parameter of the GROUP BY statement as the data filtering condition to obtain the data satisfying the filtering condition (which in this case is the data in the range of (2017-11-01T00:00:00, 2019-11-07T23:00:00], and map these data to the previously segmented time axis (in this case there are mapped data of the first month in every two month period from 2017-11-01T00:00:00 to 2019-11-07T23:00:00).

The SQL execution result is:

```
+-----------------------------+-------------------------------+
|                         Time|count(root.ln.wf01.wt01.status)|
+-----------------------------+-------------------------------+
|2017-11-01T00:00:00.000+08:00|                            259|
|2018-01-01T00:00:00.000+08:00|                            250|
|2018-03-01T00:00:00.000+08:00|                            259|
|2018-05-01T00:00:00.000+08:00|                            251|
|2018-07-01T00:00:00.000+08:00|                            242|
|2018-09-01T00:00:00.000+08:00|                            225|
|2018-11-01T00:00:00.000+08:00|                            216|
|2019-01-01T00:00:00.000+08:00|                            207|
|2019-03-01T00:00:00.000+08:00|                            216|
|2019-05-01T00:00:00.000+08:00|                            207|
|2019-07-01T00:00:00.000+08:00|                            199|
|2019-09-01T00:00:00.000+08:00|                            181|
|2019-11-01T00:00:00.000+08:00|                             60|
+-----------------------------+-------------------------------+
```

The SQL statement is:

```sql
select count(status) from root.ln.wf01.wt01 group by([2017-10-31T00:00:00, 2019-11-07T23:00:00), 1mo, 2mo);
```

which means:

Since the user specifies the sliding step parameter as `2mo`, the GROUP BY statement will move the time interval `2 months` long instead of `1 month` as default.

The first parameter of the GROUP BY statement above is the display window parameter, which determines the final display range is [2017-10-31T00:00:00, 2019-11-07T23:00:00).

Different from the previous example, the start time is set to 2017-10-31T00:00:00.  The sliding step will increment monthly based on the start date, and the 31st day of the month meaning the last day of the month will be used as the time interval's start time. If the start time is set to the 30th date, the sliding step will use the 30th or the last day of the month.

The start time is 2017-10-31T00:00:00. The sliding step will increment monthly based on the start time, and the 1st day of the month will be used as the time interval's start time.

The second parameter of the GROUP BY statement above is the time interval for dividing the time axis. Taking this parameter (1mo) as time interval and the startTime of the display window as the dividing origin, the time axis is divided into several continuous intervals, which are [2017-10-31T00:00:00, 2017-11-31T00:00:00), [2018-02-31T00:00:00, 2018-03-31T00:00:00), [2018-05-31T00:00:00, 2018-06-31T00:00:00), etc.

The third parameter of the GROUP BY statement above is the sliding step for each time interval moving.

Then the system will use the time and value filtering condition in the WHERE clause and the first parameter of the GROUP BY statement as the data filtering condition to obtain the data satisfying the filtering condition (which in this case is the data in the range of [2017-10-31T00:00:00, 2019-11-07T23:00:00) and map these data to the previously segmented time axis (in this case there are mapped data of the first month in every two month period from 2017-10-31T00:00:00 to 2019-11-07T23:00:00).

The SQL execution result is:

```
+-----------------------------+-------------------------------+
|                         Time|count(root.ln.wf01.wt01.status)|
+-----------------------------+-------------------------------+
|2017-10-31T00:00:00.000+08:00|                            251|
|2017-12-31T00:00:00.000+08:00|                            250|
|2018-02-28T00:00:00.000+08:00|                            259|
|2018-04-30T00:00:00.000+08:00|                            250|
|2018-06-30T00:00:00.000+08:00|                            242|
|2018-08-31T00:00:00.000+08:00|                            225|
|2018-10-31T00:00:00.000+08:00|                            216|
|2018-12-31T00:00:00.000+08:00|                            208|
|2019-02-28T00:00:00.000+08:00|                            216|
|2019-04-30T00:00:00.000+08:00|                            208|
|2019-06-30T00:00:00.000+08:00|                            199|
|2019-08-31T00:00:00.000+08:00|                            181|
|2019-10-31T00:00:00.000+08:00|                             69|
+-----------------------------+-------------------------------+
```

### Left Open And Right Close Range

The SQL statement is:

```sql
select count(status) from root.ln.wf01.wt01 group by ((2017-11-01T00:00:00, 2017-11-07T23:00:00],1d);
```

In this sql, the time interval is left open and right close, so we won't include the value of timestamp 2017-11-01T00:00:00 and instead we will include the value of timestamp 2017-11-07T23:00:00.

We will get the result like following:

```
+-----------------------------+-------------------------------+
|                         Time|count(root.ln.wf01.wt01.status)|
+-----------------------------+-------------------------------+
|2017-11-02T00:00:00.000+08:00|                           1440|
|2017-11-03T00:00:00.000+08:00|                           1440|
|2017-11-04T00:00:00.000+08:00|                           1440|
|2017-11-05T00:00:00.000+08:00|                           1440|
|2017-11-06T00:00:00.000+08:00|                           1440|
|2017-11-07T00:00:00.000+08:00|                           1440|
|2017-11-07T23:00:00.000+08:00|                           1380|
+-----------------------------+-------------------------------+
Total line number = 7
It costs 0.004s
```

## Downsampling Aggregate Query with Level Clause

Level could be defined to show count the number of points of each node at the given level in current Metadata Tree.

This could be used to query the number of points under each device.

The SQL statement is:

Get downsampling aggregate query by level.

```sql
select count(status) from root.ln.wf01.wt01 group by ((2017-11-01T00:00:00, 2017-11-07T23:00:00],1d), level=1;
```
Result:

```
+-----------------------------+-------------------------+
|                         Time|COUNT(root.ln.*.*.status)|
+-----------------------------+-------------------------+
|2017-11-02T00:00:00.000+08:00|                     1440|
|2017-11-03T00:00:00.000+08:00|                     1440|
|2017-11-04T00:00:00.000+08:00|                     1440|
|2017-11-05T00:00:00.000+08:00|                     1440|
|2017-11-06T00:00:00.000+08:00|                     1440|
|2017-11-07T00:00:00.000+08:00|                     1440|
|2017-11-07T23:00:00.000+08:00|                     1380|
+-----------------------------+-------------------------+
Total line number = 7
It costs 0.006s
```

Downsampling aggregate query with sliding step and by level.

```sql
select count(status) from root.ln.wf01.wt01 group by ([2017-11-01 00:00:00, 2017-11-07 23:00:00), 3h, 1d), level=1;
```

Result:

```
+-----------------------------+-------------------------+
|                         Time|COUNT(root.ln.*.*.status)|
+-----------------------------+-------------------------+
|2017-11-01T00:00:00.000+08:00|                      180|
|2017-11-02T00:00:00.000+08:00|                      180|
|2017-11-03T00:00:00.000+08:00|                      180|
|2017-11-04T00:00:00.000+08:00|                      180|
|2017-11-05T00:00:00.000+08:00|                      180|
|2017-11-06T00:00:00.000+08:00|                      180|
|2017-11-07T00:00:00.000+08:00|                      180|
+-----------------------------+-------------------------+
Total line number = 7
It costs 0.004s
```

## Aggregation By Tags

IotDB allows you to do aggregation query with the tags defined in timeseries through `GROUP BY TAGS` clause as well.

Firstly, we can put these example data into IoTDB, which will be used in the following feature introduction.

These are the temperature data of the workshops, which belongs to the factory `factory1` and locates in different cities. The time range is `[1000, 10000)`.

The device node of the timeseries path is the ID of the device. The information of city and workshop are modelled in the tags `city` and `workshop`.
The devices `d1` and `d2` belong to the workshop `d1` in `Beijing`.
`d3` and `d4` belong to the workshop `w2` in `Beijing`.
`d5` and `d6` belong to the workshop `w1` in `Shanghai`.
`d7` belongs to the workshop `w2` in `Shanghai`.
`d8` and `d9` are under maintenance, and don't belong to any workshops, so they have no tags.


```SQL
CREATE DATABASE root.factory1;
create timeseries root.factory1.d1.temperature with datatype=FLOAT tags(city=Beijing, workshop=w1);
create timeseries root.factory1.d2.temperature with datatype=FLOAT tags(city=Beijing, workshop=w1);
create timeseries root.factory1.d3.temperature with datatype=FLOAT tags(city=Beijing, workshop=w2);
create timeseries root.factory1.d4.temperature with datatype=FLOAT tags(city=Beijing, workshop=w2);
create timeseries root.factory1.d5.temperature with datatype=FLOAT tags(city=Shanghai, workshop=w1);
create timeseries root.factory1.d6.temperature with datatype=FLOAT tags(city=Shanghai, workshop=w1);
create timeseries root.factory1.d7.temperature with datatype=FLOAT tags(city=Shanghai, workshop=w2);
create timeseries root.factory1.d8.temperature with datatype=FLOAT;
create timeseries root.factory1.d9.temperature with datatype=FLOAT;

insert into root.factory1.d1(time, temperature) values(1000, 104.0);
insert into root.factory1.d1(time, temperature) values(3000, 104.2);
insert into root.factory1.d1(time, temperature) values(5000, 103.3);
insert into root.factory1.d1(time, temperature) values(7000, 104.1);

insert into root.factory1.d2(time, temperature) values(1000, 104.4);
insert into root.factory1.d2(time, temperature) values(3000, 103.7);
insert into root.factory1.d2(time, temperature) values(5000, 103.3);
insert into root.factory1.d2(time, temperature) values(7000, 102.9);

insert into root.factory1.d3(time, temperature) values(1000, 103.9);
insert into root.factory1.d3(time, temperature) values(3000, 103.8);
insert into root.factory1.d3(time, temperature) values(5000, 102.7);
insert into root.factory1.d3(time, temperature) values(7000, 106.9);

insert into root.factory1.d4(time, temperature) values(1000, 103.9);
insert into root.factory1.d4(time, temperature) values(5000, 102.7);
insert into root.factory1.d4(time, temperature) values(7000, 106.9);

insert into root.factory1.d5(time, temperature) values(1000, 112.9);
insert into root.factory1.d5(time, temperature) values(7000, 113.0);

insert into root.factory1.d6(time, temperature) values(1000, 113.9);
insert into root.factory1.d6(time, temperature) values(3000, 113.3);
insert into root.factory1.d6(time, temperature) values(5000, 112.7);
insert into root.factory1.d6(time, temperature) values(7000, 112.3);

insert into root.factory1.d7(time, temperature) values(1000, 101.2);
insert into root.factory1.d7(time, temperature) values(3000, 99.3);
insert into root.factory1.d7(time, temperature) values(5000, 100.1);
insert into root.factory1.d7(time, temperature) values(7000, 99.8);

insert into root.factory1.d8(time, temperature) values(1000, 50.0);
insert into root.factory1.d8(time, temperature) values(3000, 52.1);
insert into root.factory1.d8(time, temperature) values(5000, 50.1);
insert into root.factory1.d8(time, temperature) values(7000, 50.5);

insert into root.factory1.d9(time, temperature) values(1000, 50.3);
insert into root.factory1.d9(time, temperature) values(3000, 52.1);
```

### Aggregation query by one single tag

If the user wants to know the average temperature of each workshop, he can query like this

```SQL
SELECT AVG(temperature) FROM root.factory1.** GROUP BY TAGS(city);
```

The query will calculate the average of the temperatures of those timeseries which have the same tag value of the key `city`.
The results are

```
+--------+------------------+
|    city|  avg(temperature)|
+--------+------------------+
| Beijing|104.04666697184244|
|Shanghai|107.85000076293946|
|    NULL| 50.84999910990397|
+--------+------------------+
Total line number = 3
It costs 0.231s
```

From the results we can see that the differences between aggregation by tags query and aggregation by time or level query are:
1. Aggregation query by tags will no longer remove wildcard to raw timeseries, but do the aggregation through the data of multiple timeseries, which have the same tag value.
2. Except for the aggregate result column, the result set contains the key-value column of the grouped tag. The column name is the tag key, and the values in the column are tag values which present in the searched timeseries.
If some searched timeseries doesn't have the grouped tag, a `NULL` value in the key-value column of the grouped tag will be presented, which means the aggregation of all the timeseries lacking the tagged key.

### Aggregation query by multiple tags

Except for the aggregation query by one single tag, aggregation query by multiple tags in a particular order is allowed as well.

For example, a user wants to know the average temperature of the devices in each workshop. 
As the workshop names may be same in different city, it's not correct to aggregated by the tag `workshop` directly.
So the aggregation by the tag `city` should be done first, and then by the tag `workshop`.

SQL

```SQL
SELECT avg(temperature) FROM root.factory1.** GROUP BY TAGS(city, workshop);
```

The results

```
+--------+--------+------------------+
|    city|workshop|  avg(temperature)|
+--------+--------+------------------+
|    NULL|    NULL| 50.84999910990397|
|Shanghai|      w1|113.01666768391927|
| Beijing|      w2| 104.4000004359654|
|Shanghai|      w2|100.10000038146973|
| Beijing|      w1|103.73750019073486|
+--------+--------+------------------+
Total line number = 5
It costs 0.027s
```

We can see that in a multiple tags aggregation query, the result set will output the key-value columns of all the grouped tag keys, which have the same order with the one in `GROUP BY TAGS`.

### Downsampling Aggregation by tags based on Time Window

Downsampling aggregation by time window is one of the most popular features in a time series database. IoTDB supports to do aggregation query by tags based on time window.

For example, a user wants to know the average temperature of the devices in each workshop, in every 5 seconds, in the range of time `[1000, 10000)`.

SQL

```SQL
SELECT avg(temperature) FROM root.factory1.** GROUP BY ([1000, 10000), 5s), TAGS(city, workshop);
```

The results

```
+-----------------------------+--------+--------+------------------+
|                         Time|    city|workshop|  avg(temperature)|
+-----------------------------+--------+--------+------------------+
|1970-01-01T08:00:01.000+08:00|    NULL|    NULL| 50.91999893188476|
|1970-01-01T08:00:01.000+08:00|Shanghai|      w1|113.20000076293945|
|1970-01-01T08:00:01.000+08:00| Beijing|      w2|             103.4|
|1970-01-01T08:00:01.000+08:00|Shanghai|      w2| 100.1999994913737|
|1970-01-01T08:00:01.000+08:00| Beijing|      w1|103.81666692097981|
|1970-01-01T08:00:06.000+08:00|    NULL|    NULL|              50.5|
|1970-01-01T08:00:06.000+08:00|Shanghai|      w1| 112.6500015258789|
|1970-01-01T08:00:06.000+08:00| Beijing|      w2| 106.9000015258789|
|1970-01-01T08:00:06.000+08:00|Shanghai|      w2| 99.80000305175781|
|1970-01-01T08:00:06.000+08:00| Beijing|      w1|             103.5|
+-----------------------------+--------+--------+------------------+
```

Comparing to the pure tag aggregations, this kind of aggregation will divide the data according to the time window specification firstly, and do the aggregation query by the multiple tags in each time window secondly.
The result set will also contain a time column, which have the same meaning with the time column of the result in downsampling aggregation query by time window.

### Limitation of Aggregation by Tags

As this feature is still under development, some queries have not been completed yet and will be supported in the future.

> 1. Temporarily not support `HAVING` clause to filter the results.
> 2. Temporarily not support ordering by tag values.
> 3. Temporarily not support `LIMIT`，`OFFSET`，`SLIMIT`，`SOFFSET`.
> 4. Temporarily not support `ALIGN BY DEVICE`.
> 5. Temporarily not support expressions as aggregation function parameter，e.g. `count(s+1)`.
> 6. Not support the value filter, which stands the same with the `GROUP BY LEVEL` query.

## Aggregation By Variation
IoTDB supports grouping by continuous stable values through the `GROUP BY VARIATION` statement.

Group-By-Variation wil set the first point in group as the base point,  
then if the difference between the new data and base point is small than or equal to delta, 
the data point will be grouped together and execute aggregation query (The calculation of difference and the meaning of delte are introduced below). The groups won't overlap and there is no fixed start time and end time.
The syntax of clause is as follows:
```sql
group by variation(controlExpression[,delta][,ignoreNull=true/false])
```
The different parameters mean:
* controlExpression

The value that is used to calculate difference. It can be any columns or the expression of them.
* delta

The threshold that is used when grouping. The difference of controlExpression between the first data point and new data point should less than or equal to delta. 
When delta is zero, all the continuous data with equal expression value will be grouped into the same group.
* ignoreNull

Used to specify how to deal with the data when the value of controlExpression is null. When ignoreNull is false, null will be treated as a new value and when ignoreNull is true, the data point will be directly skipped.

The supported return types of controlExpression and how to deal with null value when ignoreNull is false are shown in the following table:

| delta    | Return Type Supported By controlExpression | The Handling of null when ignoreNull is False                                                                                                                                                                             |
|----------|--------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| delta!=0 | INT32、INT64、FLOAT、DOUBLE                   | If the processing group doesn't contains null, null value should be treated as infinity/infinitesimal and will end current group.<br/>Continuous null values are treated as stable values and assigned to the same group. | 
| delta=0  | TEXT、BINARY、INT32、INT64、FLOAT、DOUBLE       | Null is treated as a new value in a new group and continuous nulls belong to the same group.                                                                                                                              |            

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="/img/UserGuide/Process-Data/GroupBy/groupByVariation.jpeg" alt="groupByVariation">

### Precautions for Use
1. The result of controlExpression should be a unique value. If multiple columns appear after using wildcard stitching, an error will be reported.
2. For a group in resultSet, the time column output the start time of the group by default. __endTime can be used in select clause to output the endTime of groups in resultSet.
3. Each device is grouped separately when used with `ALIGN BY DEVICE`.
4. Delta is zero and ignoreNull is true by default.
5. Currently `GROUP BY VARIATION` is not supported with `GROUP BY LEVEL`.

Using the raw data below, several examples of `GROUP BY VARIAITON` queries will be given.
```
+-----------------------------+-------+-------+-------+--------+-------+-------+
|                         Time|     s1|     s2|     s3|      s4|     s5|     s6|
+-----------------------------+-------+-------+-------+--------+-------+-------+
|1970-01-01T08:00:00.000+08:00|    4.5|    9.0|    0.0|    45.0|    9.0|   8.25|
|1970-01-01T08:00:00.010+08:00|   null|   19.0|   10.0|   145.0|   19.0|   8.25|
|1970-01-01T08:00:00.020+08:00|   24.5|   29.0|   null|   245.0|   29.0|   null|
|1970-01-01T08:00:00.030+08:00|   34.5|   null|   30.0|   345.0|   null|   null|
|1970-01-01T08:00:00.040+08:00|   44.5|   49.0|   40.0|   445.0|   49.0|   8.25|
|1970-01-01T08:00:00.050+08:00|   null|   59.0|   50.0|   545.0|   59.0|   6.25|
|1970-01-01T08:00:00.060+08:00|   64.5|   69.0|   60.0|   645.0|   69.0|   null|
|1970-01-01T08:00:00.070+08:00|   74.5|   79.0|   null|    null|   79.0|   3.25|
|1970-01-01T08:00:00.080+08:00|   84.5|   89.0|   80.0|   845.0|   89.0|   3.25|
|1970-01-01T08:00:00.090+08:00|   94.5|   99.0|   90.0|   945.0|   99.0|   3.25|
|1970-01-01T08:00:00.150+08:00|   66.5|   77.0|   90.0|   945.0|   99.0|   9.25|
+-----------------------------+-------+-------+-------+--------+-------+-------+
```
### delta = 0
The sql is shown below:
```sql
select __endTime, avg(s1), count(s2), sum(s3) from root.sg.d group by variation(s6)
```
Get the result below which ignores the row with null value in `s6`.
```
+-----------------------------+-----------------------------+-----------------+-------------------+-----------------+
|                         Time|                    __endTime|avg(root.sg.d.s1)|count(root.sg.d.s2)|sum(root.sg.d.s3)|
+-----------------------------+-----------------------------+-----------------+-------------------+-----------------+
|1970-01-01T08:00:00.000+08:00|1970-01-01T08:00:00.040+08:00|             24.5|                  3|             50.0|
|1970-01-01T08:00:00.050+08:00|1970-01-01T08:00:00.050+08:00|             null|                  1|             50.0|
|1970-01-01T08:00:00.070+08:00|1970-01-01T08:00:00.090+08:00|             84.5|                  3|            170.0|
|1970-01-01T08:00:00.150+08:00|1970-01-01T08:00:00.150+08:00|             66.5|                  1|             90.0|
+-----------------------------+-----------------------------+-----------------+-------------------+-----------------+
```
when ignoreNull is false, the row with null value in `s6` will be considered.
```sql
select __endTime, avg(s1), count(s2), sum(s3) from root.sg.d group by variation(s6, ignoreNull=false)
```
Get the following result.
```
+-----------------------------+-----------------------------+-----------------+-------------------+-----------------+
|                         Time|                    __endTime|avg(root.sg.d.s1)|count(root.sg.d.s2)|sum(root.sg.d.s3)|
+-----------------------------+-----------------------------+-----------------+-------------------+-----------------+
|1970-01-01T08:00:00.000+08:00|1970-01-01T08:00:00.010+08:00|              4.5|                  2|             10.0|
|1970-01-01T08:00:00.020+08:00|1970-01-01T08:00:00.030+08:00|             29.5|                  1|             30.0|
|1970-01-01T08:00:00.040+08:00|1970-01-01T08:00:00.040+08:00|             44.5|                  1|             40.0|
|1970-01-01T08:00:00.050+08:00|1970-01-01T08:00:00.050+08:00|             null|                  1|             50.0|
|1970-01-01T08:00:00.060+08:00|1970-01-01T08:00:00.060+08:00|             64.5|                  1|             60.0|
|1970-01-01T08:00:00.070+08:00|1970-01-01T08:00:00.090+08:00|             84.5|                  3|            170.0|
|1970-01-01T08:00:00.150+08:00|1970-01-01T08:00:00.150+08:00|             66.5|                  1|             90.0|
+-----------------------------+-----------------------------+-----------------+-------------------+-----------------+
```
### delta !=0

The sql is shown below:
```sql
select __endTime, avg(s1), count(s2), sum(s3) from root.sg.d group by variation(s6, 4)
```
Get the result below:
```
+-----------------------------+-----------------------------+-----------------+-------------------+-----------------+
|                         Time|                    __endTime|avg(root.sg.d.s1)|count(root.sg.d.s2)|sum(root.sg.d.s3)|
+-----------------------------+-----------------------------+-----------------+-------------------+-----------------+
|1970-01-01T08:00:00.000+08:00|1970-01-01T08:00:00.050+08:00|             24.5|                  4|            100.0|
|1970-01-01T08:00:00.070+08:00|1970-01-01T08:00:00.090+08:00|             84.5|                  3|            170.0|
|1970-01-01T08:00:00.150+08:00|1970-01-01T08:00:00.150+08:00|             66.5|                  1|             90.0|
+-----------------------------+-----------------------------+-----------------+-------------------+-----------------+
```
The sql is shown below:

```sql
select __endTime, avg(s1), count(s2), sum(s3) from root.sg.d group by variation(s6+s5, 10)
```
Get the result below:
```
+-----------------------------+-----------------------------+-----------------+-------------------+-----------------+
|                         Time|                    __endTime|avg(root.sg.d.s1)|count(root.sg.d.s2)|sum(root.sg.d.s3)|
+-----------------------------+-----------------------------+-----------------+-------------------+-----------------+
|1970-01-01T08:00:00.000+08:00|1970-01-01T08:00:00.010+08:00|              4.5|                  2|             10.0|
|1970-01-01T08:00:00.040+08:00|1970-01-01T08:00:00.050+08:00|             44.5|                  2|             90.0|
|1970-01-01T08:00:00.070+08:00|1970-01-01T08:00:00.080+08:00|             79.5|                  2|             80.0|
|1970-01-01T08:00:00.090+08:00|1970-01-01T08:00:00.150+08:00|             80.5|                  2|            180.0|
+-----------------------------+-----------------------------+-----------------+-------------------+-----------------+
```

## Aggregation By Condition
When you need to filter the data according to a specific condition and group the continuous ones for an aggregation query.
`GROUP BY CONDITION` is suitable for you.The rows which don't meet the given condition will be simply ignored because they don't belong to any group.
Its syntax is defined below:
```sql
group by condition(predict,[keep>/>=/=/<=/<]threshold,[,ignoreNull=true/false])
```
* predict

Any legal expression return the type of boolean for filtering in grouping.
* [keep>/>=/=/<=/<]threshold

Keep expression is used to specify the number of continuous rows that meet the `predict` condition to form a group. Only the number of rows in group satisfy the keep condition, the result of group will be output.
Keep expression consists of a 'keep' string and a threshold of type `long` or a single 'long' type data.
* ignoreNull=true/false

Used to specify how to handle data rows that encounter null predict, skip the row when it's true and end current group when it's false.

### Precautions for Use
1. keep condition is required in the query, but you can omit the 'keep' string and given a `long` number which defaults to 'keep=long number' condition.
2. IgnoreNull defaults to true.
3. For a group in resultSet, the time column output the start time of the group by default. __endTime can be used in select clause to output the endTime of groups in resultSet.
4. Each device is grouped separately when used with `ALIGN BY DEVICE`.
5. Currently `GROUP BY CONDITION` is not supported with `GROUP BY LEVEL`.

For the following raw data, several query examples are given below:
```
+-----------------------------+-------------------------+-------------------------------------+------------------------------------+
|                         Time|root.sg.beijing.car01.soc|root.sg.beijing.car01.charging_status|root.sg.beijing.car01.vehicle_status|
+-----------------------------+-------------------------+-------------------------------------+------------------------------------+
|1970-01-01T08:00:00.001+08:00|                     14.0|                                    1|                                   1|
|1970-01-01T08:00:00.002+08:00|                     16.0|                                    1|                                   1|
|1970-01-01T08:00:00.003+08:00|                     16.0|                                    0|                                   1|
|1970-01-01T08:00:00.004+08:00|                     16.0|                                    0|                                   1|
|1970-01-01T08:00:00.005+08:00|                     18.0|                                    1|                                   1|
|1970-01-01T08:00:00.006+08:00|                     24.0|                                    1|                                   1|
|1970-01-01T08:00:00.007+08:00|                     36.0|                                    1|                                   1|
|1970-01-01T08:00:00.008+08:00|                     36.0|                                 null|                                   1|
|1970-01-01T08:00:00.009+08:00|                     45.0|                                    1|                                   1|
|1970-01-01T08:00:00.010+08:00|                     60.0|                                    1|                                   1|
+-----------------------------+-------------------------+-------------------------------------+------------------------------------+
```
The sql statement to query data with at least two continuous row shown below: 
```sql
select max_time(charging_status),count(vehicle_status),last_value(soc) from root.** group by condition(charging_status=1,KEEP>=2,ignoringNull=true)
```
Get the result below:
```
+-----------------------------+-----------------------------------------------+-------------------------------------------+-------------------------------------+
|                         Time|max_time(root.sg.beijing.car01.charging_status)|count(root.sg.beijing.car01.vehicle_status)|last_value(root.sg.beijing.car01.soc)|
+-----------------------------+-----------------------------------------------+-------------------------------------------+-------------------------------------+
|1970-01-01T08:00:00.001+08:00|                                              2|                                          2|                                 16.0|
|1970-01-01T08:00:00.005+08:00|                                             10|                                          5|                                 60.0|
+-----------------------------+-----------------------------------------------+-------------------------------------------+-------------------------------------+
```
When ignoreNull is false, the null value will be treated as a row that doesn't meet the condition.
```sql
select max_time(charging_status),count(vehicle_status),last_value(soc) from root.** group by condition(charging_status=1,KEEP>=2,ignoringNull=false)
```
Get the result below, the original group is split.
```
+-----------------------------+-----------------------------------------------+-------------------------------------------+-------------------------------------+
|                         Time|max_time(root.sg.beijing.car01.charging_status)|count(root.sg.beijing.car01.vehicle_status)|last_value(root.sg.beijing.car01.soc)|
+-----------------------------+-----------------------------------------------+-------------------------------------------+-------------------------------------+
|1970-01-01T08:00:00.001+08:00|                                              2|                                          2|                                 16.0|
|1970-01-01T08:00:00.005+08:00|                                              7|                                          3|                                 36.0|
|1970-01-01T08:00:00.009+08:00|                                             10|                                          2|                                 60.0|
+-----------------------------+-----------------------------------------------+-------------------------------------------+-------------------------------------+
```

## Aggregation By Session
`GROUP BY SESSION` can be used to group data according to the interval of the time. Data with a time interval less than or equal to the given threshold will be assigned to the same group.
For example, in industrial scenarios, devices don't always run continuously, `GROUP BY SESSION` will group the data generated by each access session of the device. 
Its syntax is defined as follows:
```sql
group by session(timeInterval)
```
* timeInterval

A given interval threshold to create a new group of data when the difference between the time of data is greater than the threshold.

The figure below is a grouping diagram under `GROUP BY SESSION`.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="/img/UserGuide/Process-Data/GroupBy/groupBySession.jpeg" alt="groupBySession">

### Precautions for Use
1. For a group in resultSet, the time column output the start time of the group by default. __endTime can be used in select clause to output the endTime of groups in resultSet.
2. Each device is grouped separately when used with `ALIGN BY DEVICE`.
3. Currently `GROUP BY SESSION` is not supported with `GROUP BY LEVEL`.

For the raw data below, a few query examples are given:
```
+-----------------------------+-----------------+-----------+--------+------+
|                         Time|           Device|temperature|hardware|status|
+-----------------------------+-----------------+-----------+--------+------+
|1970-01-01T08:00:01.000+08:00|root.ln.wf02.wt01|       35.7|      11| false|
|1970-01-01T08:00:02.000+08:00|root.ln.wf02.wt01|       35.8|      22|  true|
|1970-01-01T08:00:03.000+08:00|root.ln.wf02.wt01|       35.4|      33| false|
|1970-01-01T08:00:04.000+08:00|root.ln.wf02.wt01|       36.4|      44| false|
|1970-01-01T08:00:05.000+08:00|root.ln.wf02.wt01|       36.8|      55| false|
|1970-01-01T08:00:10.000+08:00|root.ln.wf02.wt01|       36.8|     110| false|
|1970-01-01T08:00:20.000+08:00|root.ln.wf02.wt01|       37.8|     220|  true|
|1970-01-01T08:00:30.000+08:00|root.ln.wf02.wt01|       37.5|     330| false|
|1970-01-01T08:00:40.000+08:00|root.ln.wf02.wt01|       37.4|     440| false|
|1970-01-01T08:00:50.000+08:00|root.ln.wf02.wt01|       37.9|     550| false|
|1970-01-01T08:01:40.000+08:00|root.ln.wf02.wt01|       38.0|     110| false|
|1970-01-01T08:02:30.000+08:00|root.ln.wf02.wt01|       38.8|     220|  true|
|1970-01-01T08:03:20.000+08:00|root.ln.wf02.wt01|       38.6|     330| false|
|1970-01-01T08:04:20.000+08:00|root.ln.wf02.wt01|       38.4|     440| false|
|1970-01-01T08:05:20.000+08:00|root.ln.wf02.wt01|       38.3|     550| false|
|1970-01-01T08:06:40.000+08:00|root.ln.wf02.wt01|       null|       0|  null|
|1970-01-01T08:07:50.000+08:00|root.ln.wf02.wt01|       null|       0|  null|
|1970-01-01T08:08:00.000+08:00|root.ln.wf02.wt01|       null|       0|  null|
|1970-01-02T08:08:01.000+08:00|root.ln.wf02.wt01|       38.2|     110| false|
|1970-01-02T08:08:02.000+08:00|root.ln.wf02.wt01|       37.5|     220|  true|
|1970-01-02T08:08:03.000+08:00|root.ln.wf02.wt01|       37.4|     330| false|
|1970-01-02T08:08:04.000+08:00|root.ln.wf02.wt01|       36.8|     440| false|
|1970-01-02T08:08:05.000+08:00|root.ln.wf02.wt01|       37.4|     550| false|
+-----------------------------+-----------------+-----------+--------+------+
```
TimeInterval can be set by different time units, the sql is shown below:
```sql
select __endTime,count(*) from root.** group by session(1d)
```
Get the result：
```
+-----------------------------+-----------------------------+------------------------------------+---------------------------------+-------------------------------+
|                         Time|                    __endTime|count(root.ln.wf02.wt01.temperature)|count(root.ln.wf02.wt01.hardware)|count(root.ln.wf02.wt01.status)|
+-----------------------------+-----------------------------+------------------------------------+---------------------------------+-------------------------------+
|1970-01-01T08:00:01.000+08:00|1970-01-01T08:08:00.000+08:00|                                  15|                               18|                             15|
|1970-01-02T08:08:01.000+08:00|1970-01-02T08:08:05.000+08:00|                                   5|                                5|                              5|
+-----------------------------+-----------------------------+------------------------------------+---------------------------------+-------------------------------+
```
It can be also used with `HAVING` and `ALIGN BY DEVICE` clauses.
```sql
select __endTime,sum(hardware) from root.ln.wf02.wt01 group by session(50s) having sum(hardware)>0 align by device
```
Get the result below:
```
+-----------------------------+-----------------+-----------------------------+-------------+
|                         Time|           Device|                    __endTime|sum(hardware)|
+-----------------------------+-----------------+-----------------------------+-------------+
|1970-01-01T08:00:01.000+08:00|root.ln.wf02.wt01|1970-01-01T08:03:20.000+08:00|       2475.0|
|1970-01-01T08:04:20.000+08:00|root.ln.wf02.wt01|1970-01-01T08:04:20.000+08:00|        440.0|
|1970-01-01T08:05:20.000+08:00|root.ln.wf02.wt01|1970-01-01T08:05:20.000+08:00|        550.0|
|1970-01-02T08:08:01.000+08:00|root.ln.wf02.wt01|1970-01-02T08:08:05.000+08:00|       1650.0|
+-----------------------------+-----------------+-----------------------------+-------------+
```