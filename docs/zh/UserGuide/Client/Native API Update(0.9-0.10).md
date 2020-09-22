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

## 0.9-0.10 版本IoTDB Session 接口更新总结

从0.9到0.10版本的IoTDB session接口有了较大改变。一部分接口名称和参数类型发生了变化，另外新增了大量可用接口。所有session接口抛出的异常类型 *IoTDBSessionExeception* 更改为 *IoTDBConnectionException* 和 *StatementExecutionExeception* 。下面详细介绍具体接口的变化。

### 接口名称更改

#### insert()

用于插入一行数据，需提供数据点的deviceId, time, 所有measurement和相应的value值。

```
void insert(String deviceId, long time, List<String> measurements, List<String> values)
```


该方法在0.10版本中方法名发生变化

```
void insertRecord(String deviceId, long time, List<String> measurements, List<String> values)
```

#### insertRowInBatch()

用于插入多行数据，需提供各行数据的deviceId, time, 所有measurement名称和相应的value值。

```
void insertRowInBatch(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList,   
                      List<List<String>> valuesList)
```


该方法在0.10版本中方法名发生变化

```
void insertRecords(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, 
                   List<List<String>> valuesList)
```

#### insertBatch()

在0.9版本中用于以"RowBatch"结构为单位插入数据

```
void insertBatch(RowBatch rowBatch)
```

在0.10版本中"RowBatch"类型更改为"Tablet"类型，因此方法名也随之改变。

```
void insertTablet(Tablet tablet)
```

#### testInsertRow()

用于测试插入一行接口的响应

```
void testInsertRow(String deviceId, long time, List<String> measurements, List<String> values)
```

在0.10版本中方法名改为testInsertRecord。

```
void testInsertRecord(String deviceId, long time, List<String> measurements, List<String> values)
```

#### testInsertRowInBatch()

用于测试插入多行数据接口的响应

```
void testInsertRowInBatch(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, 
                          List<List<String>> valuesList)
```

在0.10版本中方法名改为testInsertRecords

```
void testInsertRecords(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, 
                       List<List<String>> valuesList)
```

#### testInsertBatch

用于测试以RowBatch结构为单位插入数据的响应

```
void testInsertBatch(RowBatch rowBatch)
```

在0.10版本中RowBatch类型更改为Tabet类型，因此方法名也随之改变为testInsertTablet

```
void testInsertTablet(Tablet tablet)
```



### 新增接口

```
void open(boolean enableRPCCompression)
```

开启一个session，并指定是否启用RPC压缩。注意客户端开启PRC压缩的状态需与服务端保持一致。

```
void insertRecord(String deviceId, long time, List<String> measurements,
      List<TSDataType> types, List<Object> values)
```

插入一行数据，该方法和已有的insertRecord()方法不同在于需额外提供每个measurement的类型信息types，且参数values以原始类型的方式提供。写入速度相对于参数为String格式的insertRecord接口要快一些。

```
void insertRecords(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, 
                   List<List<TSDataType>> typesList, List<List<Object>> valuesList)
```

插入多行数据，该方法和已有的insertRecords()方法不同在于需额外提供每个measurement的类型信息typesList，且参数valuesList以原始类型的方式提供。写入速度相对于参数为String格式的insertRecords接口要快一些。

```
void insertTablet(Tablet tablet, boolean sorted)
```

提供额外的sorted参数，表示tablet是否内部已排好序，如sorted为真则会省去排序的过程从而提升处理速度。

```
void insertTablets(Map<String, Tablet> tablets)
```

新增insertTablets接口用于写入多个tablet结构，tablets参数为Map<device名, tablet数据>

```
void insertTablets(Map<String, Tablet> tablets, boolean sorted)
```

带额外sorted参数的insertTablets接口

```
void testInsertRecord(String deviceId, long time, List<String> measurements, List<TSDataType> types, 
                      List<Object> values)
void testInsertRecords(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, 
                      List<List<TSDataType>> typesList, List<List<Object>> valuesList)
void testInsertTablet(Tablet tablet, boolean sorted)
void testInsertTablets(Map<String, Tablet> tablets)
void testInsertTablets(Map<String, Tablet> tablets, boolean sorted)
```

以上接口均为新增的测试rpc响应的方法，用于测试新增的写入接口

```
void createTimeseries(String path, TSDataType dataType, TSEncoding encoding, CompressionType compressor, 	
                      Map<String, String> props, Map<String, String> tags, Map<String, String> attributes, 
                      String measurementAlias)
```

在原来createTimeseries接口的基础上，创建时间序列可以额外指定时间序列的props, tags, attributes和measurementAlias。如果不需要指定以上额外参数可以将参数设为null。

```
void createMultiTimeseries(List<String> paths, List<TSDataType> dataTypes, List<TSEncoding> encodings, 
                           List<CompressionType> compressors, List<Map<String, String>> propsList, 
                           List<Map<String, String>> tagsList, List<Map<String, String>> attributesList, 
                           List<String> measurementAliasList)
```

一次性创建多个时间序列，同时也可以指定多个时间序列的props, tags, attributes和measurementAlias。

```
boolean checkTimeseriesExists(String path)
```
用于检测时间序列是否存在
