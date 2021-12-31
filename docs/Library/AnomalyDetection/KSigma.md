# KSigma

## Usage

This function is used to detect anomalies based on the Dynamic K-Sigma Algorithm.
Within a sliding window, the input value with a deviation of more than k times the standard deviation from the average will be output as anomaly.

**Name:** KSIGMA

**Input Series:** Only support a single input series. The type is INT32 / INT64 / FLOAT / DOUBLE.

+ `k`: How many times to multiply on standard deviation to define anomaly, the default value is 3.
+ `window`: The window size of Dynamic K-Sigma Algorithm, the default value is 10000. 

**Output Series:** Output a single series. The type is same as input series.

**Note:** Only when is larger than 0, the anomaly detection will be performed. Otherwise, nothing will be output.

## Examples

### Assigning k

Input series:

```
+-----------------------------+---------------+
|                         Time|root.test.d1.s1|
+-----------------------------+---------------+
|2020-01-01T00:00:02.000+08:00|            0.0|
|2020-01-01T00:00:03.000+08:00|           50.0|
|2020-01-01T00:00:04.000+08:00|          100.0|
|2020-01-01T00:00:06.000+08:00|          150.0|
|2020-01-01T00:00:08.000+08:00|          200.0|
|2020-01-01T00:00:10.000+08:00|          200.0|
|2020-01-01T00:00:14.000+08:00|          200.0|
|2020-01-01T00:00:15.000+08:00|          200.0|
|2020-01-01T00:00:16.000+08:00|          200.0|
|2020-01-01T00:00:18.000+08:00|          200.0|
|2020-01-01T00:00:20.000+08:00|          150.0|
|2020-01-01T00:00:22.000+08:00|          100.0|
|2020-01-01T00:00:26.000+08:00|           50.0|
|2020-01-01T00:00:28.000+08:00|            0.0|
|2020-01-01T00:00:30.000+08:00|            NaN|
+-----------------------------+---------------+
```

SQL for query: 

```sql
select ksigma(s1,"k"="1.0") from root.test.d1 where time <= 2020-01-01 00:00:30
```

Output series:

```
+-----------------------------+---------------------------------+
|Time                         |ksigma(root.test.d1.s1,"k"="3.0")|
+-----------------------------+---------------------------------+
|2020-01-01T00:00:02.000+08:00|                              0.0|
|2020-01-01T00:00:03.000+08:00|                             50.0|
|2020-01-01T00:00:26.000+08:00|                             50.0|
|2020-01-01T00:00:28.000+08:00|                              0.0|
+-----------------------------+---------------------------------+
```

