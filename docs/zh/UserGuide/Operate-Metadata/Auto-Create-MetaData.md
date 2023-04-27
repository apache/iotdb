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

## 自动创建元数据

自动创建元数据指的是根据写入数据的特征自动创建出用户未定义的时间序列，
这既能解决海量序列场景下设备及测点难以提前预测与建模的难题，又能为用户提供开箱即用的写入体验。

### 自动创建存储组的元数据

* enable\_auto\_create\_schema

| 名字 | enable\_auto\_create\_schema |
|:---:|:---|
| 描述 | 是否开启自动创建元数据功能 |
| 类型 | boolean |
| 默认值 | true |
| 改后生效方式 | 重启服务生效 |

* default\_storage\_group\_level

| 名字 | default\_storage\_group\_level |
|:---:|:---|
| 描述 | 指定存储组在时间序列所处的层级，默认为第 1 层（root为第 0 层） |
| 类型 | int |
| 默认值 | 1 |
| 改后生效方式 | 仅允许在第一次启动服务前修改 |

以下图为例：

* 当 default_storage_group_level=1 时，将使用 root.turbine1 和 root.turbine2 作为存储组。

* 当 default_storage_group_level=2 时，将使用 root.turbine1.d1、root.turbine1.d2、root.turbine2.d1 和 root.turbine2.d2 作为存储组。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Data-Concept/Auto-Create-MetaData/auto_create_sg_example.png?raw=true" alt="auto create storage group example">

### 自动创建序列的元数据（前端指定数据类型）

* 用户在写入时精确指定数据类型：

    * Session中的insertTablet接口。
    * Session中带有TSDataType的insert接口。
      ```
      public void insertRecord(String deviceId, long time, List<String> measurements, List<TSDataType> types, Object... values);
      public void insertRecords(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, List<List<TSDataType>> typesList, List<List<Object>> valuesList);
      ```
    * ......

* 插入数据的同时自动创建序列，效率较高。

### 自动创建序列的元数据（类型推断）

* 在写入时直接传入字符串，数据库推断数据类型：
  
    * CLI的insert命令。
    * Session中不带有TSDataType的insert接口。
      ```
      public void insertRecord(String deviceId, long time, List<String> measurements, List<TSDataType> types, List<Object> values);
      public void insertRecords(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, List<List<String>> valuesList);
      ```
    * ......

* 由于类型推断会增加写入时间，所以通过类型推断自动创建序列元数据的效率要低于通过前端指定数据类型自动创建序列元数据，建议用户在可行时先选用前端指定数据类型的方式自动创建序列的元数据。

#### 类型推断

| 数据(String) | 字符串格式 | iotdb-engine.properties配置项 | 默认值 |
|:---:|:---|:---|:---|
| true | boolean | boolean\_string\_infer\_type | BOOLEAN |
| 1 | integer | integer\_string\_infer\_type | FLOAT |
| 17000000（大于 2^24 的整数） | integer | long\_string\_infer\_type | DOUBLE |
| 1.2 | floating | floating\_string\_infer\_type | FLOAT |
| NaN | nan | nan\_string\_infer\_type | DOUBLE |
| 'I am text' | text | 无 | 无 |

* 可配置的数据类型包括：BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT

* long_string_infer_type 配置项的目的是防止使用 FLOAT 推断 integer_string_infer_type 而造成精度缺失。

#### 编码方式

| 数据类型 | iotdb-engine.properties配置项 | 默认值 |
|:---|:---|:---|
| BOOLEAN | default\_boolean\_encoding | RLE |
| INT32 | default\_int32\_encoding | RLE |
| INT64 | default\_int64\_encoding | RLE |
| FLOAT | default\_float\_encoding | GORILLA |
| DOUBLE | default\_double\_encoding | GORILLA |
| TEXT | default\_text\_encoding | PLAIN |

* 可配置的编码方式包括：PLAIN, RLE, TS_2DIFF, GORILLA, DICTIONARY

* 数据类型与编码方式的对应关系详见 [编码方式](../Data-Concept/Encoding.md)。