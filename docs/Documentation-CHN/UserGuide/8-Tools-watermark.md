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

## 概览

<!-- MarkdownTOC -->

- [水印工具](#%E6%B0%B4%E5%8D%B0%E5%B7%A5%E5%85%B7)
  - [水印嵌入](#%E6%B0%B4%E5%8D%B0%E5%B5%8C%E5%85%A5)
    - [配置](#%E9%85%8D%E7%BD%AE)
    - [使用流程示例](#%E4%BD%BF%E7%94%A8%E6%B5%81%E7%A8%8B%E7%A4%BA%E4%BE%8B)
      - [第一步：创建一个新用户Alice，授予读权限，然后查询](#%E7%AC%AC%E4%B8%80%E6%AD%A5%EF%BC%9A%E5%88%9B%E5%BB%BA%E4%B8%80%E4%B8%AA%E6%96%B0%E7%94%A8%E6%88%B7alice%EF%BC%8C%E6%8E%88%E4%BA%88%E8%AF%BB%E6%9D%83%E9%99%90%EF%BC%8C%E7%84%B6%E5%90%8E%E6%9F%A5%E8%AF%A2)
      - [第二步：给Alice施加水印嵌入](#%E7%AC%AC%E4%BA%8C%E6%AD%A5%EF%BC%9A%E7%BB%99alice%E6%96%BD%E5%8A%A0%E6%B0%B4%E5%8D%B0%E5%B5%8C%E5%85%A5)
      - [第三步：撤销Alice的水印嵌入](#%E7%AC%AC%E4%B8%89%E6%AD%A5%EF%BC%9A%E6%92%A4%E9%94%80alice%E7%9A%84%E6%B0%B4%E5%8D%B0%E5%B5%8C%E5%85%A5)
  - [水印检测](#%E6%B0%B4%E5%8D%B0%E6%A3%80%E6%B5%8B)

<!-- /MarkdownTOC -->


<a id="%E6%B0%B4%E5%8D%B0%E5%B7%A5%E5%85%B7"></a>
# 水印工具

这个工具提供了 1）IoTDB查询结果水印嵌入功能，2）可疑数据的水印检测功能。


<a id="%E6%B0%B4%E5%8D%B0%E5%B5%8C%E5%85%A5"></a>
## 水印嵌入


<a id="%E9%85%8D%E7%BD%AE"></a>
### 配置

IoTDB默认关闭水印嵌入功能。为了使用这个功能，第一步要做的事情是修改配置文件`iotdb-engine.properties`中的以下各项：

| 名称                    | 示例                                             | 解释                                |
| ----------------------- | ------------------------------------------------ | ----------------------------------- |
| watermark_module_opened | false                                            | `true`打开水印嵌入功能; `false`关闭 |
| watermark_secret_key    | IoTDB*2019@Beijing                               | 自定义密钥                          |
| watermark_bit_string    | 100101110100                                     | 要被嵌入的0-1比特串                 |
| watermark_method        | GroupBasedLSBMethod(mark_rate=2,max_right_bit=5) | 指定水印算法及其参数                |

注意：

- `watermark_module_opened`: 如果您想使用水印嵌入功能，请将其设置成`true`。
- `watermark_secret_key`: 不能使用字符 '&'
- `watermark_method`: 现在仅支持一种算法GroupBasedLSBMethod，因此您实际上可以修改的只有这个算法的两个参数`mark_rate`和`max_right_bit`的值：
  - 均是正整数
  - `mark_rate`控制了被嵌入水印的行占总行数的比例。`mark_rate`越小，越多比例的行被嵌入水印。当`mark_rate`等于1的时候，所有的行都将嵌入水印。
  - `max_right_bit`控制了允许嵌入水印的最低有效位的范围。`max_right_bit`越大，数值的变化范围越大。
- `watermark_secret_key`, `watermark_bit_string`和`watermark_method`都不应该被攻击者获得。您需要自己负责配置文件`iotdb-engine.properties`的安全管理。


<a id="%E4%BD%BF%E7%94%A8%E6%B5%81%E7%A8%8B%E7%A4%BA%E4%BE%8B"></a>
### 使用流程示例


<a id="%E7%AC%AC%E4%B8%80%E6%AD%A5%EF%BC%9A%E5%88%9B%E5%BB%BA%E4%B8%80%E4%B8%AA%E6%96%B0%E7%94%A8%E6%88%B7alice%EF%BC%8C%E6%8E%88%E4%BA%88%E8%AF%BB%E6%9D%83%E9%99%90%EF%BC%8C%E7%84%B6%E5%90%8E%E6%9F%A5%E8%AF%A2"></a>
#### 第一步：创建一个新用户Alice，授予读权限，然后查询

一个新创建的用户默认不使用水印。因此查询结果就是数据库中的原始数据。

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

<a id="%E7%AC%AC%E4%BA%8C%E6%AD%A5%EF%BC%9A%E7%BB%99alice%E6%96%BD%E5%8A%A0%E6%B0%B4%E5%8D%B0%E5%B5%8C%E5%85%A5"></a>
#### 第二步：给Alice施加水印嵌入

sql用法：`grant watermark_embedding to a,b` 

只有root用户有权限运行该指令。在root给Alice施加水印嵌入之后，Alice的所有查询结果都将被嵌入水印。

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

<a id="%E7%AC%AC%E4%B8%89%E6%AD%A5%EF%BC%9A%E6%92%A4%E9%94%80alice%E7%9A%84%E6%B0%B4%E5%8D%B0%E5%B5%8C%E5%85%A5"></a>
#### 第三步：撤销Alice的水印嵌入

sql用法：`revoke watermark_embedding from a,b`

只有root用户有权限运行该指令。在root撤销Alice的水印嵌入之后，Alice的所有查询结果就又是数据库中的原始数据了。

<a id="%E6%B0%B4%E5%8D%B0%E6%A3%80%E6%B5%8B"></a>
## 水印检测

`detect-watermark.sh` and `detect-watermark.bat` are provided for different platforms.

用法： ./detect-watermark.sh [filePath] [secretKey] [watermarkBitString] [mark_rate] [max_right_bit] [alpha] [columnIndex]

示例： ./detect-watermark.sh /home/data/dump1.csv IoTDB*2019@Beijing 100101110100 2 5 0.05 1

| Args               | 示例                 | 解释                         |
| ------------------ | -------------------- | ---------------------------- |
| filePath           | /home/data/dump1.csv | 可疑数据的文件路径           |
| secretKey          | IoTDB*2019@Beijing   | 参见水印嵌入小节             |
| watermarkBitString | 100101110100         | 参见水印嵌入小节             |
| mark_rate          | 2                    | 参见水印嵌入小节             |
| max_right_bit      | 5                    | 参见水印嵌入小节             |
| alpha              | 0.05                 | 显著性水平                   |
| columnIndex        | 1                    | 指定可疑数据的某一列进行检测 |

注意：

- `filePath`: 您可以使用export-csv工具来生成这样的数据文件。第一行是表头， 第一列是时间列。文件中的数据示例如下：

  | Time                          | root.vehicle.d0.s1 | root.vehicle.d0.s1 |
  | ----------------------------- | ------------------ | ------------------ |
  | 1970-01-01T08:00:00.001+08:00 | 100                | null               |
  | ...                           | ...                | ...                |

- `watermark_secret_key`, `watermark_bit_string`, `mark_rate`和`max_right_bit`应该和水印嵌入过程使用的值保持一致。

- `alpha`: 取值范围是[0,1]

- `columnIndex`: 正整数