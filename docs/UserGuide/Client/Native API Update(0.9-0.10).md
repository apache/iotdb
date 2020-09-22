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

## Summary for 0.9-0.10 IoTDB Session Interface Updates

Great changes have taken place in IoTDB session of version 0.10 compared to version 0.9.
We added a large numbers of new interfaces, and some old interfaces had new names or parameters.
Besides, all exceptions thrown by session interfaces are changed from *IoTDBSessionExeception* to *IoTDBConnectionException* or *StatementExecutionExeception*.
The detailed modifications are listed as follows.

### Method name modifications

#### insert()

Insert a Record，which contains deviceId, timestamp of the record and multiple measurement values

```
void insert(String deviceId, long time, List<String> measurements, List<String> values)
```

The method name has been changed to insertRecord() in 0.10 version

```
void insertRecord(String deviceId, long time, List<String> measurements, List<String> values)
```

#### insertRowInBatch()

Insert multiple Records, which contains all the deviceIds, timestamps of the records and multiple measurement values

```
void insertRowInBatch(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList,   
                      List<List<String>> valuesList)
```


The method name has been changed to insertRecords() in 0.10 version

```
void insertRecords(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, 
                   List<List<String>> valuesList)
```

#### insertBatch()

In 0.9, insertBatch is used for insertion in terms of "RowBatch" structure.

```
void insertBatch(RowBatch rowBatch)
```

As "Tablet" replaced "RowBatch" in 0.10, the name has been changed to insertTablet()

```
void insertTablet(Tablet tablet)
```

#### testInsertRow()

To test the responsiveness of insertRow()

```
void testInsertRow(String deviceId, long time, List<String> measurements, List<String> values)
```

The method name has been changed to testInsertRecord() in 0.10 version

```
void testInsertRecord(String deviceId, long time, List<String> measurements, List<String> values)
```

#### testInsertRowInBatch()

To test the responsiveness of insertRowInBatch()

```
void testInsertRowInBatch(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, 
                          List<List<String>> valuesList)
```

The method name has been changed to testInsertRecords() in 0.10 version

```
void testInsertRecords(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, 
                       List<List<String>> valuesList)
```

#### testInsertBatch

To test the responsiveness of insertBatch()

```
void testInsertBatch(RowBatch rowBatch)
```

The method name has been changed to testInsertTablet() in 0.10 version

```
void testInsertTablet(Tablet tablet)
```



### New Interfaces

```
void open(boolean enableRPCCompression)
```

Open a session, with a parameter to specify whether to enable RPC compression. 
Please pay attention that this RPC compression status of client must comply with the status of IoTDB server

```
void insertRecord(String deviceId, long time, List<String> measurements,
      List<TSDataType> types, List<Object> values)
```

Insert one record, in a way that user has to provide the type information of each measurement, which is different from the original insertRecord() interface.
The values should be provided in their primitive types. This interface is more proficient than the one without type parameters.

```
void insertRecords(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, 
                   List<List<TSDataType>> typesList, List<List<Object>> valuesList)
```

Insert multiple records with type parameters. This interface is more proficient than the one without type parameters.

```
void insertTablet(Tablet tablet, boolean sorted)
```

An additional insertTablet() interface that providing a "sorted" parameter indicating if the tablet is in order. A sorted tablet may accelerate the insertion process.

```
void insertTablets(Map<String, Tablet> tablets)
```

A new insertTablets() for inserting multiple tablets. 

```
void insertTablets(Map<String, Tablet> tablets, boolean sorted)
```

insertTablets() with an additional "sorted" parameter. 

```
void testInsertRecord(String deviceId, long time, List<String> measurements, List<TSDataType> types, 
                      List<Object> values)
void testInsertRecords(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, 
                      List<List<TSDataType>> typesList, List<List<Object>> valuesList)
void testInsertTablet(Tablet tablet, boolean sorted)
void testInsertTablets(Map<String, Tablet> tablets)
void testInsertTablets(Map<String, Tablet> tablets, boolean sorted)
```

The above interfaces are newly added to test responsiveness of new insert interfaces.

```
void createTimeseries(String path, TSDataType dataType, TSEncoding encoding, CompressionType compressor, 	
                      Map<String, String> props, Map<String, String> tags, Map<String, String> attributes, 
                      String measurementAlias)
```

Create a timeseries with path, datatype, encoding and compression. Additionally, users can provide props, tags, attributes and measurementAlias。

```
void createMultiTimeseries(List<String> paths, List<TSDataType> dataTypes, List<TSEncoding> encodings, 
                           List<CompressionType> compressors, List<Map<String, String>> propsList, 
                           List<Map<String, String>> tagsList, List<Map<String, String>> attributesList, 
                           List<String> measurementAliasList)
```

Create multiple timeseries with a single method. Users can provide props, tags, attributes and measurementAlias as well for detailed timeseries information.

```
boolean checkTimeseriesExists(String path)
```
Added a method to check whether the specific timeseries exists.
