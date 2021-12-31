# ACF

## Usage

This function is used to calculate the auto-correlation factor of the input time series, 
which equals to cross correlation between the same series. 
For more information, please refer to `XCorr` function.

**Name:** ACF

**Input Series:** Only support a single input numeric series. The type is INT32 / INT64 / FLOAT / DOUBLE.

**Output Series:** Output a single series. The type is DOUBLE. 
There are $2N-1$ data points in the series, and the values are interpreted in details in `XCorr` function.

**Note:**

+ `null` and `NaN` values in the input series will be ignored and treated as 0.

## Examples

Input series:
```
+-----------------------------+---------------+
|                         Time|root.test.d1.s1|
+-----------------------------+---------------+
|2020-01-01T00:00:01.000+08:00|              1|
|2020-01-01T00:00:02.000+08:00|           null|
|2020-01-01T00:00:03.000+08:00|              3|
|2020-01-01T00:00:04.000+08:00|            NaN|
|2020-01-01T00:00:05.000+08:00|              5|
+-----------------------------+---------------+
```

SQL for query:

```sql
select acf(s1) from root.test.d1 where time <= 2020-01-01 00:00:05
```

Output series:
```
+-----------------------------+--------------------+
|                         Time|acf(root.test.d1.s1)|
+-----------------------------+--------------------+
|1970-01-01T08:00:00.001+08:00|                 1.0|
|1970-01-01T08:00:00.002+08:00|                 0.0|
|1970-01-01T08:00:00.003+08:00|                 3.6|
|1970-01-01T08:00:00.004+08:00|                 0.0|
|1970-01-01T08:00:00.005+08:00|                 7.0|
|1970-01-01T08:00:00.006+08:00|                 0.0|
|1970-01-01T08:00:00.007+08:00|                 3.6|
|1970-01-01T08:00:00.008+08:00|                 0.0|
|1970-01-01T08:00:00.009+08:00|                 1.0|
+-----------------------------+--------------------+
```
