# Pearson

## Usage

This function is used to calculate the Pearson Correlation Coefficient.

**Name:** PEARSON

**Input Series:** Only support two input series. The types are both INT32 / INT64 / FLOAT / DOUBLE.

**Output Series:** Output a single series. The type is DOUBLE. There is only one data point in the series, whose timestamp is 0 and value is the Pearson Correlation Coefficient.

**Note:** 

+ If a row contains missing points, null points or `NaN`, it will be ignored;
+ If all rows are ignored, `NaN` will be output.


## Examples
Input series:

```
+-----------------------------+---------------+---------------+
|                         Time|root.test.d2.s1|root.test.d2.s2|
+-----------------------------+---------------+---------------+
|2020-01-01T00:00:02.000+08:00|          100.0|          101.0|
|2020-01-01T00:00:03.000+08:00|          101.0|           null|
|2020-01-01T00:00:04.000+08:00|          102.0|          101.0|
|2020-01-01T00:00:06.000+08:00|          104.0|          102.0|
|2020-01-01T00:00:08.000+08:00|          126.0|          102.0|
|2020-01-01T00:00:10.000+08:00|          108.0|          103.0|
|2020-01-01T00:00:12.000+08:00|           null|          103.0|
|2020-01-01T00:00:14.000+08:00|          112.0|          104.0|
|2020-01-01T00:00:15.000+08:00|          113.0|           null|
|2020-01-01T00:00:16.000+08:00|          114.0|          104.0|
|2020-01-01T00:00:18.000+08:00|          116.0|          105.0|
|2020-01-01T00:00:20.000+08:00|          118.0|          105.0|
|2020-01-01T00:00:22.000+08:00|          100.0|          106.0|
|2020-01-01T00:00:26.000+08:00|          124.0|          108.0|
|2020-01-01T00:00:28.000+08:00|          126.0|          108.0|
|2020-01-01T00:00:30.000+08:00|            NaN|          108.0|
+-----------------------------+---------------+---------------+
```

SQL for query:

```sql
select pearson(s1,s2) from root.test.d2
```

Output series:

```
+-----------------------------+-----------------------------------------+
|                         Time|pearson(root.test.d2.s1, root.test.d2.s2)|
+-----------------------------+-----------------------------------------+
|1970-01-01T08:00:00.000+08:00|                       0.5630881927754872|
+-----------------------------+-----------------------------------------+
```