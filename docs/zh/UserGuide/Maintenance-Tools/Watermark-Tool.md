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
## 水印工具

这个工具提供了 1）IoTDB 查询结果水印嵌入功能，2）可疑数据的水印检测功能。

### 水印嵌入

#### 配置

IoTDB 默认关闭水印嵌入功能。为了使用这个功能，第一步要做的事情是修改配置文件`iotdb-datanode.properties`中的以下各项：

| 名称                    | 示例                                                   | 解释                                |
| ----------------------- | ------------------------------------------------------ | ----------------------------------- |
| watermark_module_opened | false                                                  | `true`打开水印嵌入功能；`false`关闭 |
| watermark_secret_key    | IoTDB*2019@Beijing                                     | 自定义密钥                          |
| watermark_bit_string    | 100101110100                                           | 要被嵌入的 0-1 比特串                 |
| watermark_method        | GroupBasedLSBMethod(embed_row_cycle=2,embed_lsb_num=5) | 指定水印算法及其参数                |

注意：

- `watermark_module_opened`: 如果您想使用水印嵌入功能，请将其设置成`true`。
- `watermark_secret_key`: 不能使用字符 '&'。密钥长度没有限制，一般来说密钥越长，攻击难度就越高。
- `watermark_bit_string`: 比特串长度没有限制（除了不能为空字符串），但是当长度过短时，水印检测可能达不到要求的显著性水平。
- `watermark_method`: 现在仅支持一种算法 GroupBasedLSBMethod，因此您实际上可以修改的只有这个算法的两个参数`embed_row_cycle`和`embed_lsb_num`的值：
  - 均是正整数
  - `embed_row_cycle`控制了被嵌入水印的行占总行数的比例。`embed_row_cycle`越小，被嵌入水印的行的比例就越大。当`embed_row_cycle`等于 1 的时候，所有的行都将嵌入水印。
  - GroupBasedLSBMethod 使用 LSB 嵌入。`embed_lsb_num`控制了允许嵌入水印的最低有效位的数量。`embed_lsb_num`越大，数值的可变化范围就越大。
- `watermark_secret_key`, `watermark_bit_string`和`watermark_method`都不应该被攻击者获得。您需要自己负责配置文件`iotdb-datanode.properties`的安全管理。

#### 使用示例

 * 第一步：创建一个新用户 Alice，授予读权限，然后查询

一个新创建的用户默认不使用水印。因此查询结果就是数据库中的原始数据。

```
.\start-cli.bat -u root -pw root
create user Alice '1234'
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

 * 第二步：给 Alice 施加水印嵌入

sql 用法：`grant watermark_embedding to Alice`

您可以使用`grant watermark_embedding to user1,user2,...`来同时给多个用户施加水印嵌入。

只有 root 用户有权限运行该指令。在 root 给 Alice 施加水印嵌入之后，Alice 的所有查询结果都将被嵌入水印。

```
.\start-cli.bat -u root -pw root
grant watermark_embedding to Alice
exit

.\start-cli.bat -u Alice -pw 1234
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

 * 第三步：撤销 Alice 的水印嵌入

sql 用法：`revoke watermark_embedding from Alice`

您可以使用`revoke watermark_embedding from user1,user2,...`来同时撤销多个用户的水印嵌入。

只有 root 用户有权限运行该指令。在 root 撤销 Alice 的水印嵌入之后，Alice 的所有查询结果就又是数据库中的原始数据了。

### 水印检测

`detect-watermark.sh` 和 `detect-watermark.bat` 是给不同平台提供的功能相同的工具脚本。

用法： ./detect-watermark.sh [filePath] [secretKey] [watermarkBitString] [embed_row_cycle] [embed_lsb_num] [alpha] [columnIndex] [dataType: int/float/double]

示例： ./detect-watermark.sh /home/data/dump1.csv IoTDB*2019@Beijing 100101110100 2 5 0.05 1 float

| Args               | 示例                 | 解释                                           |
| ------------------ | -------------------- | ---------------------------------------------- |
| filePath           | /home/data/dump1.csv | 可疑数据的文件路径                             |
| secretKey          | IoTDB*2019@Beijing   | 参见水印嵌入小节                               |
| watermarkBitString | 100101110100         | 参见水印嵌入小节                               |
| embed_row_cycle    | 2                    | 参见水印嵌入小节                               |
| embed_lsb_num      | 5                    | 参见水印嵌入小节                               |
| alpha              | 0.05                 | 显著性水平                                     |
| columnIndex        | 1                    | 指定可疑数据的某一列进行检测                   |
| dataType           | float                | 指定检测列的数据类型；int/float/double 任选其一 |

注意：

- `filePath`: 您可以使用 export-csv 工具来生成这样的数据文件。第一行是表头， 第一列是时间列。文件中的数据示例如下：

| Time                          | root.vehicle.d0.s1 | root.vehicle.d0.s1 |
| ----------------------------- | ------------------ | ------------------ |
| 1970-01-01T08:00:00.001+08:00 | 100                | null               |
| ...                           | ...                | ...                |

- `watermark_secret_key`, `watermark_bit_string`, `embed_row_cycle`和`embed_lsb_num`应该和水印嵌入过程使用的值保持一致。

- `alpha`: 取值范围 [0,1]。水印检测基于显著性检验，`alpha`越小，没有嵌入水印的数据被检测成嵌入水印的可能性越低，从而检测出嵌入水印的结果的可信度越高。

- `columnIndex`: 正整数
