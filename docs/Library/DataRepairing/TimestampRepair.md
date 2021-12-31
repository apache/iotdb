# TimestampRepair

This function is used for timestamp repair. 
According to the given standard time interval, 
the method of minimizing the repair cost is adopted.
By fine-tuning the timestamps, 
the original data with unstable timestamp interval is repaired to strictly equispaced data.
If no standard time interval is given, 
this function will use the **median**, **mode** or **cluster** of the time interval to estimate the standard time interval.

**Name:** TIMESTAMPREPAIR

**Input Series:** Only support a single input series. The data type is INT32 / INT64 / FLOAT / DOUBLE.

**Parameters:** 

+ `interval`: The standard time interval whose unit is millisecond. It is a positive integer. By default, it will be estimated according to the given method.
+ `method`: The method to estimate the standard time interval, which is 'median', 'mode' or 'cluster'. This parameter is only valid when `interval` is not given. By default, median will be used.

**Output Series:** Output a single series. The type is the same as the input. This series is the input after repairing.

## Examples

### Manually Specify the Standard Time Interval

When `interval` is given, this function repairs according to the given standard time interval.

Input series:

```
+-----------------------------+---------------+
|                         Time|root.test.d2.s1|
+-----------------------------+---------------+
|2021-07-01T12:00:00.000+08:00|            1.0|
|2021-07-01T12:00:10.000+08:00|            2.0|
|2021-07-01T12:00:19.000+08:00|            3.0|
|2021-07-01T12:00:30.000+08:00|            4.0|
|2021-07-01T12:00:40.000+08:00|            5.0|
|2021-07-01T12:00:50.000+08:00|            6.0|
|2021-07-01T12:01:01.000+08:00|            7.0|
|2021-07-01T12:01:11.000+08:00|            8.0|
|2021-07-01T12:01:21.000+08:00|            9.0|
|2021-07-01T12:01:31.000+08:00|           10.0|
+-----------------------------+---------------+
```

SQL for query:

```sql
select timestamprepair(s1,'interval'='10000') from root.test.d2
```

Output series:


```
+-----------------------------+----------------------------------------------------+
|                         Time|timestamprepair(root.test.d2.s1, "interval"="10000")|
+-----------------------------+----------------------------------------------------+
|2021-07-01T12:00:00.000+08:00|                                                 1.0|
|2021-07-01T12:00:10.000+08:00|                                                 2.0|
|2021-07-01T12:00:20.000+08:00|                                                 3.0|
|2021-07-01T12:00:30.000+08:00|                                                 4.0|
|2021-07-01T12:00:40.000+08:00|                                                 5.0|
|2021-07-01T12:00:50.000+08:00|                                                 6.0|
|2021-07-01T12:01:00.000+08:00|                                                 7.0|
|2021-07-01T12:01:10.000+08:00|                                                 8.0|
|2021-07-01T12:01:20.000+08:00|                                                 9.0|
|2021-07-01T12:01:30.000+08:00|                                                10.0|
+-----------------------------+----------------------------------------------------+
```

### Automatically Estimate the Standard Time Interval

When `interval` is default, this function estimates the standard time interval.

Input series is the same as above, the SQL for query is shown below:

```sql
select timestamprepair(s1) from root.test.d2
```

Output series:

```
+-----------------------------+--------------------------------+
|                         Time|timestamprepair(root.test.d2.s1)|
+-----------------------------+--------------------------------+
|2021-07-01T12:00:00.000+08:00|                             1.0|
|2021-07-01T12:00:10.000+08:00|                             2.0|
|2021-07-01T12:00:20.000+08:00|                             3.0|
|2021-07-01T12:00:30.000+08:00|                             4.0|
|2021-07-01T12:00:40.000+08:00|                             5.0|
|2021-07-01T12:00:50.000+08:00|                             6.0|
|2021-07-01T12:01:00.000+08:00|                             7.0|
|2021-07-01T12:01:10.000+08:00|                             8.0|
|2021-07-01T12:01:20.000+08:00|                             9.0|
|2021-07-01T12:01:30.000+08:00|                            10.0|
+-----------------------------+--------------------------------+
```
