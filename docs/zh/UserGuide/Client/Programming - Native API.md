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

# 编程 - 原生接口

## 依赖

* JDK >= 1.8
* Maven >= 3.1

## 安装到本地 maven 库

在根目录下运行:
> mvn clean install -pl session -am -Dmaven.test.skip=true

## 在 maven 中使用原生接口

```
<dependencies>
    <dependency>
      <groupId>org.apache.iotdb</groupId>
      <artifactId>iotdb-session</artifactId>
      <version>0.10.0</version>
    </dependency>
</dependencies>
```

## 原生接口使用示例

下面将给出Session对应的接口的简要介绍和对应参数：

* 初始化Session

  ```
  ​Session(String host, int port)
  ​Session(String host, String port, String username, String password)
  ​Session(String host, int port, String username, String password)
  ```
  
* 开启Session

  ```
  ​Session.open()
  ```
  
* 关闭Session
  ​
  ```
  Session.close()
  ```
  
* 设置存储组

  ```
  void setStorageGroup(String storageGroupId)
  ```

* 删除单个或多个存储组

  ```
  void deleteStorageGroup(String storageGroup)
  void deleteStorageGroups(List<String> storageGroups)
  ```

* 创建单个或多个时间序列

  ```
  void createTimeseries(String path, TSDataType dataType,
          TSEncoding encoding, CompressionType compressor, Map<String, String> props,
          Map<String, String> tags, Map<String, String> attributes, String measurementAlias)
          
  void createMultiTimeseries(List<String> paths, List<TSDataType> dataTypes,
          List<TSEncoding> encodings, List<CompressionType> compressors,
          List<Map<String, String>> propsList, List<Map<String, String>> tagsList,
          List<Map<String, String>> attributesList, List<String> measurementAliasList)
  ```

* 删除一个或多个时间序列

  ```
  void deleteTimeseries(String path)
  void deleteTimeseries(List<String> paths)
  ```

* 删除一个或多个时间序列在某个时间点前的数据

  ```
  void deleteData(String path, long time)
  void deleteData(List<String> paths, long time)
  ```

* 插入一个 Record，一个 Record 是一个设备一个时间戳下多个测点的数据。服务器需要做类型推断，可能会有额外耗时

  ```
  void insertRecord(String deviceId, long time, List<String> measurements, List<String> values)
  ```

* 插入一个 Tablet，Tablet 是一个设备若干行非空数据块，每一行的列都相同

  ```
  void insertTablet(Tablet tablet)
  ```

* 插入多个 Tablet

  ```
  void insertTablets(Map<String, Tablet> tablet)
  ```
  
* 插入多个 Record。服务器需要做类型推断，可能会有额外耗时

  ```
  void insertRecords(List<String> deviceIds, List<Long> times, 
                       List<List<String>> measurementsList, List<List<String>> valuesList)
  ```
  
* 插入一个 Record，一个 Record 是一个设备一个时间戳下多个测点的数据。提供数据类型后，服务器不需要做类型推断，可以提高性能

  ```
  void insertRecord(String deviceId, long time, List<String> measurements,
       List<TSDataType> types, List<Object> values)
  ```

* 插入多个 Record。提供数据类型后，服务器不需要做类型推断，可以提高性能

  ```
  void insertRecords(List<String> deviceIds, List<Long> times,
        List<List<String>> measurementsList, List<List<TSDataType>> typesList,
        List<List<Object>> valuesList)
  ```


## 测试客户端逻辑+网络传输代价的接口

* 测试 testInsertRecords，不实际写入数据，只将数据传输到 server 即返回。

   ```
   void testInsertRecords(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, List<List<String>> valuesList)
   ```
  或
  or
    ```
    void testInsertRecords(List<String> deviceIds, List<Long> times,
          List<List<String>> measurementsList, List<List<TSDataType>> typesList,
          List<List<Object>> valuesList)
    ```


* 测试 insertRecord，不实际写入数据，只将数据传输到 server 即返回。

  ```
  void testInsertRecord(String deviceId, long time, List<String> measurements, List<String> values)
  ```
  或
  ```
    void testInsertRecord(String deviceId, long time, List<String> measurements,
          List<TSDataType> types, List<Object> values)
  ```


* 测试 insertTablet，不实际写入数据，只将数据传输到 server 即返回。

  ```
  void testInsertTablet(Tablet tablet)
  ```
  
  
## 示例代码

浏览上述接口的详细信息，请参阅代码 ```session/src/main/java/org/apache/iotdb/session/Session.java```

使用上述接口的示例代码在 ```example/session/src/main/java/org/apache/iotdb/SessionExample.java```

## 针对原生接口的连接池

我们提供了一个针对原生接口的连接池(`SessionPool`)，使用该接口时，你只需要指定连接池的大小，就可以在使用时从池中获取连接。
如果超过60s都没得到一个连接的话，那么会打印一条警告日志，但是程序仍将继续等待。

当一个连接被用完后，他会自动返回池中等待下次被使用；
当一个连接损坏后，他会从池中被删除，并重建一个连接重新执行用户的操作。

对于查询操作：

1. 使用SessionPool进行查询时，得到的结果集是`SessionDataSet`的封装类`SessionDataSetWrapper`;
2. 若对于一个查询的结果集，用户并没有遍历完且不再想继续遍历时，需要手动调用释放连接的操作`closeResultSet`;
3. 若对一个查询的结果集遍历时出现异常，也需要手动调用释放连接的操作`closeResultSet`.
4. 可以调用 `SessionDataSetWrapper` 的 `getColumnNames()` 方法得到结果集列名 

使用示例可以参见 ```session/src/test/java/org/apache/iotdb/session/pool/SessionPoolTest.java```

或 `example/session/src/main/java/org/apache/iotdb/SessionPoolExample.java`