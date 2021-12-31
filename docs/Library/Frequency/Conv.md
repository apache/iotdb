# Conv

## Usage
This function is used to calculate the convolution, i.e. polynomial multiplication.

**Name:** CONV

**Input:** Only support two input series. The types are both INT32 / INT64 / FLOAT / DOUBLE.

**Output:** Output a single series. The type is DOUBLE. It is the result of convolution whose timestamps starting from 0 only indicate the order.

**Note:** `NaN` in the input series will be ignored. 

## Examples

Input series:

```
+-----------------------------+---------------+---------------+
|                         Time|root.test.d2.s1|root.test.d2.s2|
+-----------------------------+---------------+---------------+
|1970-01-01T08:00:00.000+08:00|            1.0|            7.0|
|1970-01-01T08:00:00.001+08:00|            0.0|            2.0|
|1970-01-01T08:00:00.002+08:00|            1.0|           null|
+-----------------------------+---------------+---------------+
```

SQL for query:

```sql
select conv(s1,s2) from root.test.d2
```

Output series:

```
+-----------------------------+--------------------------------------+
|                         Time|conv(root.test.d2.s1, root.test.d2.s2)|
+-----------------------------+--------------------------------------+
|1970-01-01T08:00:00.000+08:00|                                   7.0|
|1970-01-01T08:00:00.001+08:00|                                   2.0|
|1970-01-01T08:00:00.002+08:00|                                   7.0|
|1970-01-01T08:00:00.003+08:00|                                   2.0|
+-----------------------------+--------------------------------------+
```
