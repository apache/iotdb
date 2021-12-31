# ConsecutiveWindows

## Usage

This function is used to find consecutive windows of specified length in strictly equispaced multidimensional data.

Strictly equispaced data is the data whose time intervals are strictly equal. Missing data, including missing rows and missing values, is allowed in it, while data redundancy and timestamp drift is not allowed.

Consecutive window is the subsequence that is strictly equispaced with the standard time interval without any missing data. 

**Name:** CONSECUTIVEWINDOWS

**Input Series:** Support multiple input series. The type is arbitrary but the data is strictly equispaced.

**Parameters:** 

+ `gap`: The standard time interval which is a positive number with an unit. The unit is 'ms' for millisecond, 's' for second, 'm' for minute, 'h' for hour and 'd' for day. By default, it will be estimated by the mode of time intervals.
+ `length`: The length of the window which is a positive number with an unit. The unit is 'ms' for millisecond, 's' for second, 'm' for minute, 'h' for hour and 'd' for day. This parameter cannot be lacked.

**Output Series:** Output a single series. The type is INT32. Each data point in the output series corresponds to a consecutive window. The output timestamp is the starting timestamp of the window and the output value is the number of data points in the window.

**Note:** For input series that is not strictly equispaced, there is no guarantee on the output.

## Examples


Input series:

```
+-----------------------------+---------------+---------------+
|                         Time|root.test.d1.s1|root.test.d1.s2|
+-----------------------------+---------------+---------------+
|2020-01-01T00:00:00.000+08:00|            1.0|            1.0|
|2020-01-01T00:05:00.000+08:00|            1.0|            1.0|
|2020-01-01T00:10:00.000+08:00|            1.0|            1.0|
|2020-01-01T00:20:00.000+08:00|            1.0|            1.0|
|2020-01-01T00:25:00.000+08:00|            1.0|            1.0|
|2020-01-01T00:30:00.000+08:00|            1.0|            1.0|
|2020-01-01T00:35:00.000+08:00|            1.0|            1.0|
|2020-01-01T00:40:00.000+08:00|            1.0|           null|
|2020-01-01T00:45:00.000+08:00|            1.0|            1.0|
|2020-01-01T00:50:00.000+08:00|            1.0|            1.0|
+-----------------------------+---------------+---------------+
```

SQL for query:

```sql
select consecutivewindows(s1,s2,'length'='10m') from root.test.d1
```

Output series:
```
+-----------------------------+--------------------------------------------------------------------+
|                         Time|consecutivewindows(root.test.d1.s1, root.test.d1.s2, "length"="10m")|
+-----------------------------+--------------------------------------------------------------------+
|2020-01-01T00:00:00.000+08:00|                                                                   3|
|2020-01-01T00:20:00.000+08:00|                                                                   3|
|2020-01-01T00:25:00.000+08:00|                                                                   3|
+-----------------------------+--------------------------------------------------------------------+
```