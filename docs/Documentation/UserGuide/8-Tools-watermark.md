<!--

```
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
```

-->

## Outline

<!-- MarkdownTOC -->

- [Watermark Tool](#watermark-tool)
  - [Watermark Embedding](#watermark-embedding)
    - [Configuration](#configuration)
    - [Usage Example](#usage-example)
      - [step 1. Create a new user Alice, grant read privilege and query](#step-1-create-a-new-user-alice-grant-read-privilege-and-query)
      - [step 2. grant watermark_embedding to Alice](#step-2-grant-watermark_embedding-to-alice)
      - [step 3. revoke watermark_embedding from Alice](#step-3-revoke-watermark_embedding-from-alice)
  - [Watermark Detection](#watermark-detection)

<!-- /MarkdownTOC -->


<a id="watermark-tool"></a>
# Watermark Tool

This tool has two functions: 1) watermark embedding of the IoTDB query result and 2) watermark detection of the suspected data.


<a id="watermark-embedding"></a>
## Watermark Embedding


<a id="configuration"></a>
### Configuration

Watermark is disabled by default in IoTDB. To enable watermark embedding, the first thing is to modify the following fields in the configuration file `iotdb-engine.properties`:

| Name                    | Example                                          | Explanation                                                  |
| ----------------------- | ------------------------------------------------ | ------------------------------------------------------------ |
| watermark_module_opened | false                                            | `true` to enable watermark embedding of the IoTDB server; `false` to disable |
| watermark_secret_key    | IoTDB*2019@Beijing                               | self-defined secret key                                      |
| watermark_bit_string    | 100101110100                                     | 0-1 bit string to be embedded                                |
| watermark_method        | GroupBasedLSBMethod(mark_rate=2,max_right_bit=5) | specifies the watermark algorithm and its paramters          |

Notes:

- `watermark_module_opened`: Set it to be true if you want to enable watermark embedding 
- `watermark_secret_key`: Character '&' is not allowed.
- `watermark_method`: Now only GroupBasedLSBMethod is supported, so actually you can only tune the two parameters of this method, which are `mark_rate` and `max_right_bit`. 
  - Both of them should be positive integers. 
  - `mark_rate` controls the ratio of rows watermarked. The smaller `mark_rate` is, the larger proportion of rows are watermarked. When `mark_rate` equals 1, every row is watermarked. 
  - `max_right_bit` controls the number of least significant bits for watermark embedding. The biggger `max_right_bit` is, the bigger range a data point can be varied.
- `watermark_secret_key`, `watermark_bit_string`  and `watermark_method` should be kept secret from possible attackers. That is, it is your responsiblity to take care of `iotdb-engine.properties`.

<a id="usage-example"></a>
### Usage Example 

<a id="step-1-create-a-new-user-alice-grant-read-privilege-and-query"></a>
#### step 1. Create a new user Alice, grant read privilege and query

A newly created user doesn't use watermark by default. So the query result is the original data.

```
.\start-client.bat -u root -pw root
create user Alice 1234
grant user Alice privileges 'READ_TIMESERIES' on root.vehicle
exit

.\start-client.bat -u Alice -pw 1234
select * from root

+-----------------------------------+------------------+------------------+
|                               Time|root.vehicle.d0.s0|root.vehicle.d0.s1|
+-----------------------------------+------------------+------------------+
|      1970-01-01T08:00:00.001+08:00|               101|              null|
|      1970-01-01T08:00:00.002+08:00|               102|              null|
|      1970-01-01T08:00:00.003+08:00|               103|              null|
|      1970-01-01T08:00:00.004+08:00|               104|               104|
|      1970-01-01T08:00:00.005+08:00|               105|              null|
|      1970-01-01T08:00:00.006+08:00|               106|              null|
|      1970-01-01T08:00:00.007+08:00|               107|              null|
|      1970-01-01T08:00:00.008+08:00|               108|              null|
|      1970-01-01T08:00:00.009+08:00|               109|              null|
|      1970-01-01T08:00:00.010+08:00|               110|              null|
|      1970-01-01T08:00:00.011+08:00|               111|              null|
|      1970-01-01T08:00:00.012+08:00|               112|              null|
|      1970-01-01T08:00:00.013+08:00|               113|              null|
|      1970-01-01T08:00:00.014+08:00|               114|              null|
|      1970-01-01T08:00:00.015+08:00|               115|              null|
|      1970-01-01T08:00:00.016+08:00|               116|              null|
|      1970-01-01T08:00:00.017+08:00|               117|              null|
|      1970-01-01T08:00:00.018+08:00|               118|              null|
|      1970-01-01T08:00:00.019+08:00|               119|              null|
|      1970-01-01T08:00:00.020+08:00|               120|              null|
|      1970-01-01T08:00:00.021+08:00|               121|              null|
|      1970-01-01T08:00:00.022+08:00|               122|              null|
|      1970-01-01T08:00:00.023+08:00|               123|              null|
+-----------------------------------+------------------+------------------+
```

<a id="step-2-grant-watermark_embedding-to-alice"></a>
#### step 2. grant watermark_embedding to Alice

Usage: `grant watermark_embedding to a,b` 

Only root can run this command. After root grants watermark_embedding to Alice, all query results of Alice are watermarked.

```
.\start-client.bat -u root -pw root
grant watermark_embedding to Alice
exit

.\start-client.bat -u Alice -pw 1234
select * from root

+-----------------------------------+------------------+------------------+
|                               Time|root.vehicle.d0.s0|root.vehicle.d0.s1|
+-----------------------------------+------------------+------------------+
|      1970-01-01T08:00:00.001+08:00|               100|              null|
|      1970-01-01T08:00:00.002+08:00|               102|              null|
|      1970-01-01T08:00:00.003+08:00|               103|              null|
|      1970-01-01T08:00:00.004+08:00|               104|               104|
|      1970-01-01T08:00:00.005+08:00|               105|              null|
|      1970-01-01T08:00:00.006+08:00|               106|              null|
|      1970-01-01T08:00:00.007+08:00|               107|              null|
|      1970-01-01T08:00:00.008+08:00|               108|              null|
|      1970-01-01T08:00:00.009+08:00|               109|              null|
|      1970-01-01T08:00:00.010+08:00|               110|              null|
|      1970-01-01T08:00:00.011+08:00|               111|              null|
|      1970-01-01T08:00:00.012+08:00|                96|              null|
|      1970-01-01T08:00:00.013+08:00|               113|              null|
|      1970-01-01T08:00:00.014+08:00|               114|              null|
|      1970-01-01T08:00:00.015+08:00|               115|              null|
|      1970-01-01T08:00:00.016+08:00|               116|              null|
|      1970-01-01T08:00:00.017+08:00|               113|              null|
|      1970-01-01T08:00:00.018+08:00|               118|              null|
|      1970-01-01T08:00:00.019+08:00|               119|              null|
|      1970-01-01T08:00:00.020+08:00|               121|              null|
|      1970-01-01T08:00:00.021+08:00|               121|              null|
|      1970-01-01T08:00:00.022+08:00|               122|              null|
|      1970-01-01T08:00:00.023+08:00|               123|              null|
+-----------------------------------+------------------+------------------+
```

<a id="step-3-revoke-watermark_embedding-from-alice"></a>
#### step 3. revoke watermark_embedding from Alice

Usage: `revoke watermark_embedding from a,b`

Only root can run this command. After root revokes watermark_embedding from Alice, all query results of Alice are original again.


<a id="watermark-detection"></a>
## Watermark Detection

`detect-watermark.sh` and `detect-watermark.bat` are provided for different platforms.

Usage: ./detect-watermark.sh [filePath] [secretKey] [watermarkBitString] [mark_rate] [max_right_bit] [alpha] [columnIndex]

Example: ./detect-watermark.sh /home/data/dump1.csv IoTDB*2019@Beijing 100101110100 2 5 0.05 1

| Args               | Example              | Explanation                                |
| ------------------ | -------------------- | ------------------------------------------ |
| filePath           | /home/data/dump1.csv | suspected data file path                   |
| secretKey          | IoTDB*2019@Beijing   | see watermark embedding section            |
| watermarkBitString | 100101110100         | see watermark embedding section            |
| mark_rate          | 2                    | see watermark embedding section            |
| max_right_bit      | 5                    | see watermark embedding section            |
| alpha              | 0.05                 | significance level                         |
| columnIndex        | 1                    | specifies one column of the data to detect |

Notes:

- `filePath`: You can use export-csv tool to generate such data file. The first row is header and the first column is time. Data in the file looks like this:

  | Time                          | root.vehicle.d0.s1 | root.vehicle.d0.s1 |
  | ----------------------------- | ------------------ | ------------------ |
  | 1970-01-01T08:00:00.001+08:00 | 100                | null               |
  | ...                           | ...                | ...                |

- `watermark_secret_key`, `watermark_bit_string`, `mark_rate` and `max_right_bit` should be consistent with those used in the embedding phase.

- `alpha`: It should be in the range of [0,1]. 

- `columnIndex`: It should be postive integer.