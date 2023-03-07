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

# Auto create metadata

Automatically creating schema means creating time series based on the characteristics of written data in case time series haven't defined by users themselves.
This function can not only solve the problem that entities and measurements are difficult to predict and model in advance under massive time series scenarios,
but also provide users with an out-of-the-box writing experience.

## Auto create database metadata

* enable\_auto\_create\_schema

| Name | enable\_auto\_create\_schema |
|:---:|:---|
| Description | whether creating schema automatically is enabled |
| Type | boolean |
| Default | true |
| Effective | After restarting system |

* default\_storage\_group\_level

| Name | default\_storage\_group\_level |
|:---:|:---|
| Description | Specify which level database is in the time series, the default level is 1 (root is on level 0) |
| Type | int |
| Default | 1 |
| Effective | Only allowed to be modified in first start up |

Illustrated as the following figure:

* When default_storage_group_level=1, root.turbine1 and root.turbine2 will be created as database.

* When default_storage_group_level=2, root.turbine1.d1, root.turbine1.d2, root.turbine2.d1 and root.turbine2.d2 will be created as database.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="/img/UserGuide/Data-Concept/Auto-Create-MetaData/auto_create_sg_example.png?raw=true" alt="auto create database example">

## Auto create time series metadata(specify data type in the frontend)

* Users should specify data type when writing:

    * insertTablet method in Session module.
    * insert method using TSDataType in Session module.
      ```
      public void insertRecord(String deviceId, long time, List<String> measurements, List<TSDataType> types, Object... values);
      public void insertRecords(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, List<List<TSDataType>> typesList, List<List<Object>> valuesList);
      ```
    * ......

* Efficient, time series are auto created when inserting data.

## Auto create time series metadata(infer data type in the backend)

* Just pass string, and the database will infer the data type:
  
    * insert command in CLI module.
    * insert method without using TSDataType in Session module.
      ```
      public void insertRecord(String deviceId, long time, List<String> measurements, List<TSDataType> types, List<Object> values);
      public void insertRecords(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, List<List<String>> valuesList);
      ```
    * ......

* Since type inference will increase the writing time, the efficiency of auto creating time series metadata through type inference is lower than that of auto creating time series metadata through specifying data type. We recommend users choose specifying data type in the frontend when possible.

### Type inference

| Data(String Format) | Format Type | iotdb-datanode.properties     | Default |
|:---:|:---|:------------------------------|:---|
| true | boolean | boolean\_string\_infer\_type  | BOOLEAN |
| 1 | integer | integer\_string\_infer\_type  | FLOAT |
| 17000000（integer > 2^24） | integer | long\_string\_infer\_type     | DOUBLE |
| 1.2 | floating | floating\_string\_infer\_type | FLOAT |
| NaN | nan | nan\_string\_infer\_type      | DOUBLE |
| 'I am text' | text | x                             | x |

* Data types can be configured as BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT.

* long_string_infer_type is used to avoid precision loss caused by using integer_string_infer_type=FLOAT to infer num > 2^24.

### Encoding Type

| Data Type | iotdb-datanode.properties  | Default |
|:---|:---------------------------|:---|
| BOOLEAN | default\_boolean\_encoding | RLE |
| INT32 | default\_int32\_encoding   | RLE |
| INT64 | default\_int64\_encoding   | RLE |
| FLOAT | default\_float\_encoding   | GORILLA |
| DOUBLE | default\_double\_encoding  | GORILLA |
| TEXT | default\_text\_encoding    | PLAIN |

* Encoding types can be configured as PLAIN, RLE, TS_2DIFF, GORILLA, DICTIONARY.

* The corresponding relationship between data types and encoding types is detailed in [Encoding](../Data-Concept/Encoding.md).