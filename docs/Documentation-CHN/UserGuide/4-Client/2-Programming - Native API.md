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

# 第4章: 客户端

# 编程 - 原生接口

## 使用

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

### 建立连接

* 初始化Session
  ​	Session(String host, int port)
  ​	Session(String host, String port, String username, String password)
  ​	Session(String host, int port, String username, String password)

* 开启Session
  ​	Session.open()

* 关闭Session
  ​	Session.close()

### 数据操作接口

* 设置存储组

  ​	TSStatus setStorageGroup(String storageGroupId)

* 删除单个或多个存储组

  ​	TSStatus deleteStorageGroup(String storageGroup)
  ​	TSStatus deleteStorageGroups(List<String> storageGroups)

* 创建单个时间序列

  ​	TSStatus createTimeseries(String path, TSDataType dataType, TSEncoding encoding, CompressionType compressor)

* 删除一个或多个时间序列

  ​	TSStatus deleteTimeseries(String path)
  ​	TSStatus deleteTimeseries(List<String> paths)

* 删除某一特定时间前的时间序列

  ​	TSStatus deleteData(String path, long time)
  ​	TSStatus deleteData(List<String> paths, long time)

* 插入时序数据

  ​	TSStatus insert(String deviceId, long time, List<String> measurements, List<String> values)

* 批量插入时序数据

  ​	TSExecuteBatchStatementResp insertBatch(RowBatch rowBatch)

### 示例代码

浏览上述接口的详细信息，请参阅代码 ```session/src/main/java/org/apache/iotdb/session/Session.java```

使用上述接口的示例代码在 ```example/session/src/main/java/org/apache/iotdb/SessionExample.java```