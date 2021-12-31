# Mad

## Usage

The function is used to compute the exact or approximate median absolute deviation (MAD) of a numeric time series. MAD is the median of the deviation of each element from the elements' median.

Take a dataset $\{1,3,3,5,5,6,7,8,9\}$ as an instance. Its median is 5 and the deviation of each element from the median is $\{0,0,1,2,2,2,3,4,4\}$, whose median is 2. Therefore, the MAD of the original dataset is 2.

**Name:** MAD

**Input Series:** Only support a single input series. The data type is INT32 / INT64 / FLOAT / DOUBLE.

**Parameter:**

+ `error`: The relative error of the approximate MAD. It should be within [0,1) and the default value is 0. Taking `error`=0.01 as an instance, suppose the exact MAD is $a$ and the approximate MAD is $b$, we have $0.99a \le b \le 1.01a$. With `error`=0, the output is the exact MAD.

**Output Series:** Output a single series. The type is DOUBLE. There is only one data point in the series, whose timestamp is 0 and value is the MAD.

**Note:** Missing points, null points and `NaN` in the input series will be ignored.

## Examples

### Exact Query

With the default `error`(`error`=0), the function queries the exact MAD.

Input series:

```
+-----------------------------+------------+
|                         Time|root.test.s0|
+-----------------------------+------------+
|2021-03-17T10:32:17.054+08:00|   0.5319929|
|2021-03-17T10:32:18.054+08:00|   0.9304316|
|2021-03-17T10:32:19.054+08:00|  -1.4800133|
|2021-03-17T10:32:20.054+08:00|   0.6114087|
|2021-03-17T10:32:21.054+08:00|   2.5163336|
|2021-03-17T10:32:22.054+08:00|  -1.0845392|
|2021-03-17T10:32:23.054+08:00|   1.0562582|
|2021-03-17T10:32:24.054+08:00|   1.3867859|
|2021-03-17T10:32:25.054+08:00| -0.45429882|
|2021-03-17T10:32:26.054+08:00|   1.0353678|
|2021-03-17T10:32:27.054+08:00|   0.7307929|
|2021-03-17T10:32:28.054+08:00|   2.3167255|
|2021-03-17T10:32:29.054+08:00|    2.342443|
|2021-03-17T10:32:30.054+08:00|   1.5809103|
|2021-03-17T10:32:31.054+08:00|   1.4829416|
|2021-03-17T10:32:32.054+08:00|   1.5800357|
|2021-03-17T10:32:33.054+08:00|   0.7124368|
|2021-03-17T10:32:34.054+08:00| -0.78597564|
|2021-03-17T10:32:35.054+08:00|   1.2058644|
|2021-03-17T10:32:36.054+08:00|   1.4215064|
|2021-03-17T10:32:37.054+08:00|   1.2808295|
|2021-03-17T10:32:38.054+08:00|  -0.6173715|
|2021-03-17T10:32:39.054+08:00|  0.06644377|
|2021-03-17T10:32:40.054+08:00|    2.349338|
|2021-03-17T10:32:41.054+08:00|   1.7335888|
|2021-03-17T10:32:42.054+08:00|   1.5872132|
............
Total line number = 10000
```

SQL for query:

```sql
select mad(s0) from root.test
```

Output series:

```
+-----------------------------+------------------+
|                         Time| mad(root.test.s0)|
+-----------------------------+------------------+
|1970-01-01T08:00:00.000+08:00|0.6806197166442871|
+-----------------------------+------------------+
```

### Approximate Query

By setting `error` within (0,1), the function queries the approximate MAD.

SQL for query:

```sql
select mad(s0, "error"="0.01") from root.test
```

Output series:

```
+-----------------------------+---------------------------------+
|                         Time|mad(root.test.s0, "error"="0.01")|
+-----------------------------+---------------------------------+
|1970-01-01T08:00:00.000+08:00|               0.6806616245859518|
+-----------------------------+---------------------------------+
```