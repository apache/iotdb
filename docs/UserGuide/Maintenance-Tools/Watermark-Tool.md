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

## Watermark Tool

This tool has two functions: 1) watermark embedding of the IoTDB query result and 2) watermark detection of the suspected data.

### Watermark Embedding

#### Configuration

Watermark is disabled by default in IoTDB. To enable watermark embedding, the first thing is to modify the following fields in the configuration file `iotdb-datanode.properties`:

| Name                    | Example                                                | Explanation                                                  |
| ----------------------- | ------------------------------------------------------ | ------------------------------------------------------------ |
| watermark_module_opened | false                                                  | `true` to enable watermark embedding of the IoTDB server; `false` to disable |
| watermark_secret_key    | IoTDB*2019@Beijing                                     | self-defined secret key                                      |
| watermark_bit_string    | 100101110100                                           | 0-1 bit string to be embedded                                |
| watermark_method        | GroupBasedLSBMethod(embed_row_cycle=2,embed_lsb_num=5) | specifies the watermark algorithm and its paramters          |

Notes:

- `watermark_module_opened`: Set it to be true if you want to enable watermark embedding 
- `watermark_secret_key`: Character '&' is not allowed. There is no constraint on the length of the secret key. Generally, the longer the key is, the higher the bar to intruders.
- `watermark_bit_string`: There is no constraint on the length of the bit string (except that it should not be empty). But note that it is difficult to reach the required significance level at the watermark detection phase if the bit string is way too short.
- `watermark_method`: Now only GroupBasedLSBMethod is supported, so actually you can only tune the two parameters of this method, which are `embed_row_cycle` and `embed_lsb_num`. 
  - Both of them should be positive integers. 
  - `embed_row_cycle` controls the ratio of rows watermarked. The smaller the `embed_row_cycle`, the larger the ratio of rows watermarked. When `embed_row_cycle` equals 1, every row is watermarked. 
  - GroupBasedLSBMethod uses LSB embedding. `embed_lsb_num` controls the number of least significant bits available for watermark embedding. The biggger the `embed_lsb_num`, the larger the varying range of a data point.
- `watermark_secret_key`, `watermark_bit_string`  and `watermark_method` should be kept secret from possible attackers. That is, it is your responsiblity to take care of `iotdb-datanode.properties`.

#### Usage Example 

* step 1. Create a new user Alice, grant read privilege and query

A newly created user doesn't use watermark by default. So the query result is the original data.

```
.\start-cli.bat -u root -pw root
create user Alice 1234
grant user Alice privileges READ_TIMESERIES on root.vehicle
exit

.\start-cli.bat -u Alice -pw 1234
select * from root
+-----------------------------------+------------------+
|                               Time|root.vehicle.d0.s0|
+-----------------------------------+------------------+
|      1970-01-01T08:00:00.001+08:00|              21.5|
|      1970-01-01T08:00:00.002+08:00|              22.5|
|      1970-01-01T08:00:00.003+08:00|              23.5|
|      1970-01-01T08:00:00.004+08:00|              24.5|
|      1970-01-01T08:00:00.005+08:00|              25.5|
|      1970-01-01T08:00:00.006+08:00|              26.5|
|      1970-01-01T08:00:00.007+08:00|              27.5|
|      1970-01-01T08:00:00.008+08:00|              28.5|
|      1970-01-01T08:00:00.009+08:00|              29.5|
|      1970-01-01T08:00:00.010+08:00|              30.5|
|      1970-01-01T08:00:00.011+08:00|              31.5|
|      1970-01-01T08:00:00.012+08:00|              32.5|
|      1970-01-01T08:00:00.013+08:00|              33.5|
|      1970-01-01T08:00:00.014+08:00|              34.5|
|      1970-01-01T08:00:00.015+08:00|              35.5|
|      1970-01-01T08:00:00.016+08:00|              36.5|
|      1970-01-01T08:00:00.017+08:00|              37.5|
|      1970-01-01T08:00:00.018+08:00|              38.5|
|      1970-01-01T08:00:00.019+08:00|              39.5|
|      1970-01-01T08:00:00.020+08:00|              40.5|
|      1970-01-01T08:00:00.021+08:00|              41.5|
|      1970-01-01T08:00:00.022+08:00|              42.5|
|      1970-01-01T08:00:00.023+08:00|              43.5|
|      1970-01-01T08:00:00.024+08:00|              44.5|
|      1970-01-01T08:00:00.025+08:00|              45.5|
|      1970-01-01T08:00:00.026+08:00|              46.5|
|      1970-01-01T08:00:00.027+08:00|              47.5|
|      1970-01-01T08:00:00.028+08:00|              48.5|
|      1970-01-01T08:00:00.029+08:00|              49.5|
|      1970-01-01T08:00:00.030+08:00|              50.5|
|      1970-01-01T08:00:00.031+08:00|              51.5|
|      1970-01-01T08:00:00.032+08:00|              52.5|
|      1970-01-01T08:00:00.033+08:00|              53.5|
+-----------------------------------+------------------+
```

* step 2. grant watermark_embedding to Alice

Usage: `grant watermark_embedding to Alice` 

Note that you can use `grant watermark_embedding to user1,user2,...` to grant watermark_embedding to multiple users.

Only root can run this command. After root grants watermark_embedding to Alice, all query results of Alice are watermarked.

```
.\start-cli.bat -u root -pw root
grant watermark_embedding to Alice
exit

.\start-cli.bat -u Alice -pw '1234'
select * from root
+-----------------------------------+------------------+
|                               Time|root.vehicle.d0.s0|
+-----------------------------------+------------------+
|      1970-01-01T08:00:00.001+08:00|              21.5|
|      1970-01-01T08:00:00.002+08:00|              22.5|
|      1970-01-01T08:00:00.003+08:00|         23.500008|
|      1970-01-01T08:00:00.004+08:00|         24.500015|
|      1970-01-01T08:00:00.005+08:00|              25.5|
|      1970-01-01T08:00:00.006+08:00|         26.500015|
|      1970-01-01T08:00:00.007+08:00|              27.5|
|      1970-01-01T08:00:00.008+08:00|         28.500004|
|      1970-01-01T08:00:00.009+08:00|              29.5|
|      1970-01-01T08:00:00.010+08:00|              30.5|
|      1970-01-01T08:00:00.011+08:00|              31.5|
|      1970-01-01T08:00:00.012+08:00|              32.5|
|      1970-01-01T08:00:00.013+08:00|              33.5|
|      1970-01-01T08:00:00.014+08:00|              34.5|
|      1970-01-01T08:00:00.015+08:00|         35.500004|
|      1970-01-01T08:00:00.016+08:00|              36.5|
|      1970-01-01T08:00:00.017+08:00|              37.5|
|      1970-01-01T08:00:00.018+08:00|              38.5|
|      1970-01-01T08:00:00.019+08:00|              39.5|
|      1970-01-01T08:00:00.020+08:00|              40.5|
|      1970-01-01T08:00:00.021+08:00|              41.5|
|      1970-01-01T08:00:00.022+08:00|         42.500015|
|      1970-01-01T08:00:00.023+08:00|              43.5|
|      1970-01-01T08:00:00.024+08:00|         44.500008|
|      1970-01-01T08:00:00.025+08:00|          45.50003|
|      1970-01-01T08:00:00.026+08:00|         46.500008|
|      1970-01-01T08:00:00.027+08:00|         47.500008|
|      1970-01-01T08:00:00.028+08:00|              48.5|
|      1970-01-01T08:00:00.029+08:00|              49.5|
|      1970-01-01T08:00:00.030+08:00|              50.5|
|      1970-01-01T08:00:00.031+08:00|         51.500008|
|      1970-01-01T08:00:00.032+08:00|              52.5|
|      1970-01-01T08:00:00.033+08:00|              53.5|
+-----------------------------------+------------------+
```

* step 3. revoke watermark_embedding from Alice

Usage: `revoke watermark_embedding from Alice` 

Note that you can use `revoke watermark_embedding from user1,user2,...` to revoke watermark_embedding from multiple users.

Only root can run this command. After root revokes watermark_embedding from Alice, all query results of Alice are original again.

### Watermark Detection

`detect-watermark.sh` and `detect-watermark.bat` are provided for different platforms.

Usage: ./detect-watermark.sh [filePath] [secretKey] [watermarkBitString] [embed_row_cycle] [embed_lsb_num] [alpha] [columnIndex] [dataType: int/float/double]

Example: ./detect-watermark.sh /home/data/dump1.csv IoTDB*2019@Beijing 100101110100 2 5 0.05 1 float

| Args               | Example              | Explanation                                                  |
| ------------------ | -------------------- | ------------------------------------------------------------ |
| filePath           | /home/data/dump1.csv | suspected data file path                                     |
| secretKey          | IoTDB*2019@Beijing   | see watermark embedding section                              |
| watermarkBitString | 100101110100         | see watermark embedding section                              |
| embed_row_cycle    | 2                    | see watermark embedding section                              |
| embed_lsb_num      | 5                    | see watermark embedding section                              |
| alpha              | 0.05                 | significance level                                           |
| columnIndex        | 1                    | specifies one column of the data to detect                   |
| dataType           | float                | specifies the data type of the detected column; int/float/double |

Notes:

- `filePath`: You can use export-csv tool to generate such data file. The first row is header and the first column is time. Data in the file looks like this:

  | Time                          | root.vehicle.d0.s1 | root.vehicle.d0.s1 |
  | ----------------------------- | ------------------ | ------------------ |
  | 1970-01-01T08:00:00.001+08:00 | 100                | null               |
  | ...                           | ...                | ...                |

- `watermark_secret_key`, `watermark_bit_string`, `embed_row_cycle` and `embed_lsb_num` should be consistent with those used in the embedding phase.

- `alpha`: It should be in the range of [0,1]. The watermark detection is based on the significance test. The smaller the `alpha` is, the lower the probability that the data without the watermark is detected to be watermark embedded, and thus the higher the credibility of the result of detecting the existence of the watermark in data.

- `columnIndex`: It should be a positive integer.

