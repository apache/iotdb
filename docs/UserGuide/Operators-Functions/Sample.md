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

# Sample Functions

## Equal Size Bucket Sample Function

### Equal Size Bucket Random Sample Function

This function samples the input sequence in equal size buckets, that is, according to the downsampling ratio and downsampling method given by the user, the input sequence is equally divided into several buckets according to a fixed number of points. Sampling by the given sampling method within each bucket.
- `proportion`: sample ratio, the value range is `(0, 1]`.
#### Equal Size Bucket Random Sample
Random sampling is performed on the equally divided buckets.

| Function Name | Allowed Input Series Data Types | Required Attributes                           | Output Series Data Type | Series Data Type  Description                 |
|----------|--------------------------------|---------------------------------------|------------|--------------------------------------------------|
| EQUAL_SIZE_BUCKET_RANDOM_SAMPLE   | INT32 / INT64 / FLOAT / DOUBLE | `proportion` The value range is `(0, 1]`, the default is `0.1` | INT32 / INT64 / FLOAT / DOUBLE | Returns a random sample of equal buckets that matches the sampling ratio |

Example data: `root.ln.wf01.wt01.temperature` has a total of `100` ordered data from `0.0-99.0`.
```
IoTDB> select temperature from root.ln.wf01.wt01;
+-----------------------------+-----------------------------+
|                         Time|root.ln.wf01.wt01.temperature|
+-----------------------------+-----------------------------+
|1970-01-01T08:00:00.000+08:00|                          0.0|
|1970-01-01T08:00:00.001+08:00|                          1.0|
|1970-01-01T08:00:00.002+08:00|                          2.0|
|1970-01-01T08:00:00.003+08:00|                          3.0|
|1970-01-01T08:00:00.004+08:00|                          4.0|
|1970-01-01T08:00:00.005+08:00|                          5.0|
|1970-01-01T08:00:00.006+08:00|                          6.0|
|1970-01-01T08:00:00.007+08:00|                          7.0|
|1970-01-01T08:00:00.008+08:00|                          8.0|
|1970-01-01T08:00:00.009+08:00|                          9.0|
|1970-01-01T08:00:00.010+08:00|                         10.0|
|1970-01-01T08:00:00.011+08:00|                         11.0|
|1970-01-01T08:00:00.012+08:00|                         12.0|
|.............................|.............................|            
|1970-01-01T08:00:00.089+08:00|                         89.0|
|1970-01-01T08:00:00.090+08:00|                         90.0|
|1970-01-01T08:00:00.091+08:00|                         91.0|
|1970-01-01T08:00:00.092+08:00|                         92.0|
|1970-01-01T08:00:00.093+08:00|                         93.0|
|1970-01-01T08:00:00.094+08:00|                         94.0|
|1970-01-01T08:00:00.095+08:00|                         95.0|
|1970-01-01T08:00:00.096+08:00|                         96.0|
|1970-01-01T08:00:00.097+08:00|                         97.0|
|1970-01-01T08:00:00.098+08:00|                         98.0|
|1970-01-01T08:00:00.099+08:00|                         99.0|
+-----------------------------+-----------------------------+
```
Sql:
```sql
select equal_size_bucket_random_sample(temperature,'proportion'='0.1') as random_sample from root.ln.wf01.wt01;
```
Result:
```
+-----------------------------+-------------+
|                         Time|random_sample|
+-----------------------------+-------------+
|1970-01-01T08:00:00.007+08:00|          7.0|
|1970-01-01T08:00:00.014+08:00|         14.0|
|1970-01-01T08:00:00.020+08:00|         20.0|
|1970-01-01T08:00:00.035+08:00|         35.0|
|1970-01-01T08:00:00.047+08:00|         47.0|
|1970-01-01T08:00:00.059+08:00|         59.0|
|1970-01-01T08:00:00.063+08:00|         63.0|
|1970-01-01T08:00:00.079+08:00|         79.0|
|1970-01-01T08:00:00.086+08:00|         86.0|
|1970-01-01T08:00:00.096+08:00|         96.0|
+-----------------------------+-------------+
Total line number = 10
It costs 0.024s
```

### Equal Size Bucket Aggregation Sample

The input sequence is sampled by the aggregation sampling method, and the user needs to provide an additional aggregation function parameter, namely
- `type`: Aggregate type, which can be `avg` or `max` or `min` or `sum` or `extreme` or `variance`. By default, `avg` is used. `extreme` represents the value with the largest absolute value in the equal bucket. `variance` represents the variance in the sampling equal buckets.

The timestamp of the sampling output of each bucket is the timestamp of the first point of the bucket.

| Function Name | Allowed Input Series Data Types | Required Attributes                           | Output Series Data Type | Series Data Type  Description                 |
|----------|--------------------------------|---------------------------------------|------------|--------------------------------------------------|
| EQUAL_SIZE_BUCKET_AGG_SAMPLE | INT32 / INT64 / FLOAT / DOUBLE | `proportion` The value range is `(0, 1]`, the default is `0.1`<br>`type`: The value types are `avg`, `max`, `min`, `sum`, `extreme`, `variance`, the default is `avg` | INT32 / INT64 / FLOAT / DOUBLE | Returns equal bucket aggregation samples that match the sampling ratio |

Example data: `root.ln.wf01.wt01.temperature` has a total of `100` ordered data from `0.0-99.0`, and the test data is randomly sampled in equal buckets.

Sql:
```sql
select equal_size_bucket_agg_sample(temperature, 'type'='avg','proportion'='0.1') as agg_avg, equal_size_bucket_agg_sample(temperature, 'type'='max','proportion'='0.1') as agg_max, equal_size_bucket_agg_sample(temperature,'type'='min','proportion'='0.1') as agg_min, equal_size_bucket_agg_sample(temperature, 'type'='sum','proportion'='0.1') as agg_sum, equal_size_bucket_agg_sample(temperature, 'type'='extreme','proportion'='0.1') as agg_extreme, equal_size_bucket_agg_sample(temperature, 'type'='variance','proportion'='0.1') as agg_variance from root.ln.wf01.wt01;
```
Result:
```
+-----------------------------+-----------------+-------+-------+-------+-----------+------------+
|                         Time|          agg_avg|agg_max|agg_min|agg_sum|agg_extreme|agg_variance|
+-----------------------------+-----------------+-------+-------+-------+-----------+------------+
|1970-01-01T08:00:00.000+08:00|              4.5|    9.0|    0.0|   45.0|        9.0|        8.25|
|1970-01-01T08:00:00.010+08:00|             14.5|   19.0|   10.0|  145.0|       19.0|        8.25|
|1970-01-01T08:00:00.020+08:00|             24.5|   29.0|   20.0|  245.0|       29.0|        8.25|
|1970-01-01T08:00:00.030+08:00|             34.5|   39.0|   30.0|  345.0|       39.0|        8.25|
|1970-01-01T08:00:00.040+08:00|             44.5|   49.0|   40.0|  445.0|       49.0|        8.25|
|1970-01-01T08:00:00.050+08:00|             54.5|   59.0|   50.0|  545.0|       59.0|        8.25|
|1970-01-01T08:00:00.060+08:00|             64.5|   69.0|   60.0|  645.0|       69.0|        8.25|
|1970-01-01T08:00:00.070+08:00|74.50000000000001|   79.0|   70.0|  745.0|       79.0|        8.25|
|1970-01-01T08:00:00.080+08:00|             84.5|   89.0|   80.0|  845.0|       89.0|        8.25|
|1970-01-01T08:00:00.090+08:00|             94.5|   99.0|   90.0|  945.0|       99.0|        8.25|
+-----------------------------+-----------------+-------+-------+-------+-----------+------------+
Total line number = 10
It costs 0.044s
```

### Equal Size Bucket M4 Sample

The input sequence is sampled using the M4 sampling method. That is to sample the head, tail, min and max values for each bucket.

| Function Name | Allowed Input Series Data Types | Required Attributes                           | Output Series Data Type | Series Data Type  Description                 |
|----------|--------------------------------|---------------------------------------|------------|--------------------------------------------------|
| EQUAL_SIZE_BUCKET_M4_SAMPLE | INT32 / INT64 / FLOAT / DOUBLE | `proportion` The value range is `(0, 1]`, the default is `0.1` | INT32 / INT64 / FLOAT / DOUBLE | Returns equal bucket M4 samples that match the sampling ratio |

Example data: `root.ln.wf01.wt01.temperature` has a total of `100` ordered data from `0.0-99.0`, and the test data is randomly sampled in equal buckets.

Sql:
```sql
select equal_size_bucket_m4_sample(temperature, 'proportion'='0.1') as M4_sample from root.ln.wf01.wt01;
```
Result:
```
+-----------------------------+---------+
|                         Time|M4_sample|
+-----------------------------+---------+
|1970-01-01T08:00:00.000+08:00|      0.0|
|1970-01-01T08:00:00.001+08:00|      1.0|
|1970-01-01T08:00:00.038+08:00|     38.0|
|1970-01-01T08:00:00.039+08:00|     39.0|
|1970-01-01T08:00:00.040+08:00|     40.0|
|1970-01-01T08:00:00.041+08:00|     41.0|
|1970-01-01T08:00:00.078+08:00|     78.0|
|1970-01-01T08:00:00.079+08:00|     79.0|
|1970-01-01T08:00:00.080+08:00|     80.0|
|1970-01-01T08:00:00.081+08:00|     81.0|
|1970-01-01T08:00:00.098+08:00|     98.0|
|1970-01-01T08:00:00.099+08:00|     99.0|
+-----------------------------+---------+
Total line number = 12
It costs 0.065s
```

#### Equal Size Bucket Outlier Sample

This function samples the input sequence with equal number of bucket outliers, that is, according to the downsampling ratio given by the user and the number of samples in the bucket, the input sequence is divided into several buckets according to a fixed number of points. Sampling by the given outlier sampling method within each bucket.

| Function Name | Allowed Input Series Data Types | Required Attributes                           | Output Series Data Type | Series Data Type  Description                 |
|----------|--------------------------------|---------------------------------------|------------|--------------------------------------------------|
| EQUAL_SIZE_BUCKET_OUTLIER_SAMPLE | INT32 / INT64 / FLOAT / DOUBLE | The value range of `proportion` is `(0, 1]`, the default is `0.1`<br> The value of `type` is `avg` or `stendis` or `cos` or `prenextdis`, the default is `avg` <br>The value of `number` should be greater than 0, the default is `3`| INT32 / INT64 / FLOAT / DOUBLE | Returns outlier samples in equal buckets that match the sampling ratio and the number of samples in the bucket |

Parameter Description
- `proportion`: sampling ratio
- `number`: the number of samples in each bucket, default `3`
- `type`: outlier sampling method, the value is
  - `avg`: Take the average of the data points in the bucket, and find the `top number` farthest from the average according to the sampling ratio
  - `stendis`: Take the vertical distance between each data point in the bucket and the first and last data points of the bucket to form a straight line, and according to the sampling ratio, find the `top number` with the largest distance
  - `cos`: Set a data point in the bucket as b, the data point on the left of b as a, and the data point on the right of b as c, then take the cosine value of the angle between the ab and bc vectors. The larger the angle, the more likely it is an outlier. Find the `top number` with the smallest cos value
  - `prenextdis`: Let a data point in the bucket be b, the data point to the left of b is a, and the data point to the right of b is c, then take the sum of the lengths of ab and bc as the yardstick, the larger the sum, the more likely it is to be an outlier, and find the `top number` with the largest sum value

Example data: `root.ln.wf01.wt01.temperature` has a total of `100` ordered data from `0.0-99.0`. Among them, in order to add outliers, we make the number modulo 5 equal to 0 increment by 100.
```
IoTDB> select temperature from root.ln.wf01.wt01;
+-----------------------------+-----------------------------+
|                         Time|root.ln.wf01.wt01.temperature|
+-----------------------------+-----------------------------+
|1970-01-01T08:00:00.000+08:00|                          0.0|
|1970-01-01T08:00:00.001+08:00|                          1.0|
|1970-01-01T08:00:00.002+08:00|                          2.0|
|1970-01-01T08:00:00.003+08:00|                          3.0|
|1970-01-01T08:00:00.004+08:00|                          4.0|
|1970-01-01T08:00:00.005+08:00|                        105.0|
|1970-01-01T08:00:00.006+08:00|                          6.0|
|1970-01-01T08:00:00.007+08:00|                          7.0|
|1970-01-01T08:00:00.008+08:00|                          8.0|
|1970-01-01T08:00:00.009+08:00|                          9.0|
|1970-01-01T08:00:00.010+08:00|                         10.0|
|1970-01-01T08:00:00.011+08:00|                         11.0|
|1970-01-01T08:00:00.012+08:00|                         12.0|
|1970-01-01T08:00:00.013+08:00|                         13.0|
|1970-01-01T08:00:00.014+08:00|                         14.0|
|1970-01-01T08:00:00.015+08:00|                        115.0|
|1970-01-01T08:00:00.016+08:00|                         16.0|
|.............................|.............................|
|1970-01-01T08:00:00.092+08:00|                         92.0|
|1970-01-01T08:00:00.093+08:00|                         93.0|
|1970-01-01T08:00:00.094+08:00|                         94.0|
|1970-01-01T08:00:00.095+08:00|                        195.0|
|1970-01-01T08:00:00.096+08:00|                         96.0|
|1970-01-01T08:00:00.097+08:00|                         97.0|
|1970-01-01T08:00:00.098+08:00|                         98.0|
|1970-01-01T08:00:00.099+08:00|                         99.0|
+-----------------------------+-----------------------------+
```
Sql:
```sql
select equal_size_bucket_outlier_sample(temperature, 'proportion'='0.1', 'type'='avg', 'number'='2') as outlier_avg_sample, equal_size_bucket_outlier_sample(temperature, 'proportion'='0.1', 'type'='stendis', 'number'='2') as outlier_stendis_sample, equal_size_bucket_outlier_sample(temperature, 'proportion'='0.1', 'type'='cos', 'number'='2') as outlier_cos_sample, equal_size_bucket_outlier_sample(temperature, 'proportion'='0.1', 'type'='prenextdis', 'number'='2') as outlier_prenextdis_sample from root.ln.wf01.wt01;
```
Result:
```
+-----------------------------+------------------+----------------------+------------------+-------------------------+
|                         Time|outlier_avg_sample|outlier_stendis_sample|outlier_cos_sample|outlier_prenextdis_sample|
+-----------------------------+------------------+----------------------+------------------+-------------------------+
|1970-01-01T08:00:00.005+08:00|             105.0|                 105.0|             105.0|                    105.0|
|1970-01-01T08:00:00.015+08:00|             115.0|                 115.0|             115.0|                    115.0|
|1970-01-01T08:00:00.025+08:00|             125.0|                 125.0|             125.0|                    125.0|
|1970-01-01T08:00:00.035+08:00|             135.0|                 135.0|             135.0|                    135.0|
|1970-01-01T08:00:00.045+08:00|             145.0|                 145.0|             145.0|                    145.0|
|1970-01-01T08:00:00.055+08:00|             155.0|                 155.0|             155.0|                    155.0|
|1970-01-01T08:00:00.065+08:00|             165.0|                 165.0|             165.0|                    165.0|
|1970-01-01T08:00:00.075+08:00|             175.0|                 175.0|             175.0|                    175.0|
|1970-01-01T08:00:00.085+08:00|             185.0|                 185.0|             185.0|                    185.0|
|1970-01-01T08:00:00.095+08:00|             195.0|                 195.0|             195.0|                    195.0|
+-----------------------------+------------------+----------------------+------------------+-------------------------+
Total line number = 10
It costs 0.041s
```

## M4 Function

M4 is used to sample the `first, last, bottom, top` points for each sliding window:

-   the first point is the point with the **m**inimal time;
-   the last point is the point with the **m**aximal time;
-   the bottom point is the point with the **m**inimal value (if there are multiple such points, M4 returns one of them);
-   the top point is the point with the **m**aximal value (if there are multiple such points, M4 returns one of them).

<img src="/img/github/198178733-a0919d17-0663-4672-9c4f-1efad6f463c2.png" alt="image" style="zoom:50%;" />

| Function Name | Allowed Input Series Data Types | Attributes                                                   | Output Series Data Type        | Series Data Type  Description                                |
| ------------- | ------------------------------- | ------------------------------------------------------------ | ------------------------------ | ------------------------------------------------------------ |
| M4            | INT32 / INT64 / FLOAT / DOUBLE  | Different attributes used by the size window and the time window. The size window uses attributes `windowSize` and `slidingStep`. The time window uses attributes `timeInterval`, `slidingStep`, `displayWindowBegin`, and `displayWindowEnd`. More details see below. | INT32 / INT64 / FLOAT / DOUBLE | Returns the `first, last, bottom, top` points in each sliding window. M4 sorts and deduplicates the aggregated points within the window before outputting them. |

### Attributes

**(1) Attributes for the size window:**

+ `windowSize`: The number of points in a window. Int data type. **Required**.
+ `slidingStep`: Slide a window by the number of points. Int data type. Optional. If not set, default to the same as `windowSize`.

<img src="/img/github/198181449-00d563c8-7bce-4ecd-a031-ec120ca42c3f.png" alt="image" style="zoom: 50%;" />

*(image source: https://iotdb.apache.org/UserGuide/Master/Process-Data/UDF-User-Defined-Function.html#udtf-user-defined-timeseries-generating-function)*

**(2) Attributes for the time window:**

+ `timeInterval`: The time interval length of a window. Long data type. **Required**.
+ `slidingStep`: Slide a window by the time length. Long data type. Optional. If not set, default to the same as `timeInterval`.
+ `displayWindowBegin`: The starting position of the window (included). Long data type. Optional. If not set, default to Long.MIN_VALUE, meaning using the time of the first data point of the input time series as the starting position of the window.
+ `displayWindowEnd`: End time limit (excluded, essentially playing the same role as `WHERE time < displayWindowEnd`). Long data type. Optional. If not set, default to Long.MAX_VALUE, meaning there is no additional end time limit other than the end of the input time series itself.

<img src="/img/github/198183015-93b56644-3330-4acf-ae9e-d718a02b5f4c.png" alt="groupBy window" style="zoom: 67%;" />

*(image source: https://iotdb.apache.org/UserGuide/Master/Query-Data/Aggregate-Query.html#downsampling-aggregate-query)*

### Examples

Input series:

```sql
+-----------------------------+------------------+
|                         Time|root.vehicle.d1.s1|
+-----------------------------+------------------+
|1970-01-01T08:00:00.001+08:00|               5.0|
|1970-01-01T08:00:00.002+08:00|              15.0|
|1970-01-01T08:00:00.005+08:00|              10.0|
|1970-01-01T08:00:00.008+08:00|               8.0|
|1970-01-01T08:00:00.010+08:00|              30.0|
|1970-01-01T08:00:00.020+08:00|              20.0|
|1970-01-01T08:00:00.025+08:00|               8.0|
|1970-01-01T08:00:00.027+08:00|              20.0|
|1970-01-01T08:00:00.030+08:00|              40.0|
|1970-01-01T08:00:00.033+08:00|               9.0|
|1970-01-01T08:00:00.035+08:00|              10.0|
|1970-01-01T08:00:00.040+08:00|              20.0|
|1970-01-01T08:00:00.045+08:00|              30.0|
|1970-01-01T08:00:00.052+08:00|               8.0|
|1970-01-01T08:00:00.054+08:00|              18.0|
+-----------------------------+------------------+
```

SQL for query1:

```sql
select M4(s1,'timeInterval'='25','displayWindowBegin'='0','displayWindowEnd'='100') from root.vehicle.d1
```

Output1:

```sql
+-----------------------------+-----------------------------------------------------------------------------------------------+
|                         Time|M4(root.vehicle.d1.s1, "timeInterval"="25", "displayWindowBegin"="0", "displayWindowEnd"="100")|
+-----------------------------+-----------------------------------------------------------------------------------------------+
|1970-01-01T08:00:00.001+08:00|                                                                                            5.0|
|1970-01-01T08:00:00.010+08:00|                                                                                           30.0|
|1970-01-01T08:00:00.020+08:00|                                                                                           20.0|
|1970-01-01T08:00:00.025+08:00|                                                                                            8.0|
|1970-01-01T08:00:00.030+08:00|                                                                                           40.0|
|1970-01-01T08:00:00.045+08:00|                                                                                           30.0|
|1970-01-01T08:00:00.052+08:00|                                                                                            8.0|
|1970-01-01T08:00:00.054+08:00|                                                                                           18.0|
+-----------------------------+-----------------------------------------------------------------------------------------------+
Total line number = 8
```

SQL for query2:

```sql
select M4(s1,'windowSize'='10') from root.vehicle.d1
```

Output2:

```sql
+-----------------------------+-----------------------------------------+
|                         Time|M4(root.vehicle.d1.s1, "windowSize"="10")|
+-----------------------------+-----------------------------------------+
|1970-01-01T08:00:00.001+08:00|                                      5.0|
|1970-01-01T08:00:00.030+08:00|                                     40.0|
|1970-01-01T08:00:00.033+08:00|                                      9.0|
|1970-01-01T08:00:00.035+08:00|                                     10.0|
|1970-01-01T08:00:00.045+08:00|                                     30.0|
|1970-01-01T08:00:00.052+08:00|                                      8.0|
|1970-01-01T08:00:00.054+08:00|                                     18.0|
+-----------------------------+-----------------------------------------+
Total line number = 7
```
 
### Suggested Use Cases

**(1) Use Case: Extreme-point-preserving downsampling**

As M4 aggregation selects the `first, last, bottom, top` points for each window, M4 usually preserves extreme points and thus patterns better than other downsampling methods such as Piecewise Aggregate Approximation (PAA). Therefore, if you want to downsample the time series while preserving extreme points, you may give M4 a try.

**(2) Use case: Error-free two-color line chart visualization of large-scale time series using reduced data**

Refer to paper: ["M4: A Visualization-Oriented Time Series Data Aggregation"](http://www.vldb.org/pvldb/vol7/p797-jugel.pdf).

Given a chart of `w*h` pixels, suppose the visualization time range of the time series root.vehicle.d1.s1 is `[tqs,tqe)`(in this use case please extend tqe to make sure (tqe-tqs) is divisible by w), the points that fall within the  `i`-th time span `Ii=[tqs+(tqe-tqs)/w*(i-1),tqs+(tqe-tqs)/w*i)` will be drawn on the `i`-th pixel column, i=1,2,...,w.

Therefore, from a visualization-driven perspective, use the sql: `"select M4(s1,'timeInterval'='(tqe-tqs)/w','displayWindowBegin'='tqs','displayWindowEnd'='tqe') from root.vehicle.d1"` to sample the `first, last, bottom, top` points for each time span. The resulting series has no more than `4*w` points, a big reduction compared to the original large-scale time series. The line chart drawn from the reduced data is identical that to that drawn from the original data (pixel-level consistency).

### Comparison with Other SQL

| SQL                                                          | Whether support M4 aggregation                               | Sliding window type                               | Example                                                      | Docs                                                         |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 1. native built-in aggregate functions with Group By clause  | No. Lack `BOTTOM_TIME` and `TOP_TIME`, which are respectively the time of the points that have the mininum and maximum value. | Time Window                                       | `select count(status), max_value(temperature) from root.ln.wf01.wt01 group by ([2017-11-01 00:00:00, 2017-11-07 23:00:00), 3h, 1d)` | https://iotdb.apache.org/UserGuide/Master/Query-Data/Aggregate-Query.html#built-in-aggregate-functions <br />https://iotdb.apache.org/UserGuide/Master/Query-Data/Aggregate-Query.html#downsampling-aggregate-query |
| 2. EQUAL_SIZE_BUCKET_M4_SAMPLE (built-in UDF)                | Yes*                                                         | Size Window. `windowSize = 4*(int)(1/proportion)` | `select equal_size_bucket_m4_sample(temperature, 'proportion'='0.1') as M4_sample from root.ln.wf01.wt01` | https://iotdb.apache.org/UserGuide/Master/Query-Data/Select-Expression.html#time-series-generating-functions |
| **3. M4 (built-in UDF)**                                     | Yes*                                                         | Size Window, Time Window                          | (1) Size Window: `select M4(s1,'windowSize'='10') from root.vehicle.d1` <br />(2) Time Window: `select M4(s1,'timeInterval'='25','displayWindowBegin'='0','displayWindowEnd'='100') from root.vehicle.d1` | refer to this doc                                            |
| 4. extend native built-in aggregate functions with Group By clause to support M4 aggregation | not implemented                                              | not implemented                                   | not implemented                                              | not implemented                                              |

Further compare `EQUAL_SIZE_BUCKET_M4_SAMPLE` and `M4`:

**(1) Different M4 aggregation definition:**

For each window, `EQUAL_SIZE_BUCKET_M4_SAMPLE` extracts the top and bottom points from points **EXCLUDING** the first and last points.

In contrast, `M4` extracts the top and bottom points from points **INCLUDING** the first and last points, which is more consistent with the semantics of `max_value` and `min_value` stored in metadata.

It is worth noting that both functions sort and deduplicate the aggregated points in a window before outputting them to the collectors.

**(2) Different sliding windows:** 

`EQUAL_SIZE_BUCKET_M4_SAMPLE` uses SlidingSizeWindowAccessStrategy and **indirectly** controls sliding window size by sampling proportion. The conversion formula is `windowSize = 4*(int)(1/proportion)`. 

`M4` supports two types of sliding window: SlidingSizeWindowAccessStrategy and SlidingTimeWindowAccessStrategy. `M4` **directly** controls the window point size or time length using corresponding parameters.