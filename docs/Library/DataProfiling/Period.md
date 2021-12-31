# Period

## Usage

The function is used to compute the period of a numeric time series.

**Name:** PERIOD

**Input Series:** Only support a single input series. The data type is INT32 / INT64 / FLOAT / DOUBLE.

**Output Series:** Output a single series. The type is INT32. There is only one data point in the series, whose timestamp is 0 and value is the period.

## Examples

Input series:


```
+-----------------------------+---------------+
|                         Time|root.test.d3.s1|
+-----------------------------+---------------+
|1970-01-01T08:00:00.001+08:00|            1.0|
|1970-01-01T08:00:00.002+08:00|            2.0|
|1970-01-01T08:00:00.003+08:00|            3.0|
|1970-01-01T08:00:00.004+08:00|            1.0|
|1970-01-01T08:00:00.005+08:00|            2.0|
|1970-01-01T08:00:00.006+08:00|            3.0|
|1970-01-01T08:00:00.007+08:00|            1.0|
|1970-01-01T08:00:00.008+08:00|            2.0|
|1970-01-01T08:00:00.009+08:00|            3.0|
+-----------------------------+---------------+
```

SQL for query:

```sql
select period(s1) from root.test.d3
```

Output series:

```
+-----------------------------+-----------------------+
|                         Time|period(root.test.d3.s1)|
+-----------------------------+-----------------------+
|1970-01-01T08:00:00.000+08:00|                      3|
+-----------------------------+-----------------------+
```
### examples on zeppelin
link: <http://101.6.15.213:18181/#/notebook/2GEJBUSZ9>
