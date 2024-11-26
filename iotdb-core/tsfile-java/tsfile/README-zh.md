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

[English](./README.md) | [中文](./README-zh.md)
# TsFile Java Document
<pre>
___________    ___________.__.__          
\__    ___/____\_   _____/|__|  |   ____  
  |    | /  ___/|    __)  |  |  | _/ __ \ 
  |    | \___ \ |     \   |  |  |_\  ___/ 
  |____|/____  >\___  /   |__|____/\___  >  version 1.0.0
             \/     \/                 \/  
</pre>

## 使用

### 在 Maven 中添加 TsFile 依赖

当前发布版本是 `1.0.0`，可以这样引用

```xml  
<dependencies>
    <dependency>
      <groupId>org.apache.tsfile</groupId>
      <artifactId>tsfile</artifactId>
      <version>1.0.0</version>
    </dependency>
<dependencies>
```


### TsFile Java API

#### 数据写入

数据写入主要通过 TsFileWriter 完成。

1. 创建 TsFileWriter

   ```java
    File f = new File("test.tsfile");
    TsFileWriter tsFileWriter = new TsFileWriter(f);
    ```

2. 注册时间序列
    
   ```java
   List<MeasurementSchema> schema1 = new ArrayList<>();
   schemas.add(new MeasurementSchema("电压", TSDataType.FLOAT));
   schemas.add(new MeasurementSchema("电流", TSDataType.FLOAT));
   tsFileWriter.registerTimeseries(new Path("太阳能板1"), schema1);
   
   List<MeasurementSchema> schema2 = new ArrayList<>();
   schemas.add(new MeasurementSchema("电压", TSDataType.FLOAT));
   schemas.add(new MeasurementSchema("电流", TSDataType.FLOAT));
   schemas.add(new MeasurementSchema("风速", TSDataType.FLOAT));
   tsFileWriter.registerTimeseries(new Path("风机1"), schema2);
   ```

3. 写入数据
  
    ```java
   TSRecord tsRecord = new TSRecord(1, "太阳能板1");
   tsRecord.addTuple(DataPoint.getDataPoint(TSDataType.FLOAT, "电压", 1.1f));
   tsRecord.addTuple(DataPoint.getDataPoint(TSDataType.FLOAT, "电流", 2.2f));
   tsFileWriter.write(tsRecord);
   ```

4. 调用`close`方法来关闭文件，关闭后才能进行查询。

    ```java
    tsFileWriter.close();
    ```

写入 TsFile 完整示例

[构造 TSRecord 来写入数据](../examples/src/main/java/org/apache/tsfile/TsFileWriteAlignedWithTSRecord.java)。

[构造 Tablet 来写入数据](../examples/src/main/java/org/apache/tsfile/TsFileWriteAlignedWithTablet.java)。


#### 数据查询

数据查询主要通过 TsFileReader 完成。

1. 创建 TsFileReader

   ```java
   TsFileSequenceReader reader = new TsFileSequenceReader(path);
   TsFileReader tsFileReader = new TsFileReader(reader)；
   ```

2. 构造查询条件

   ```java
      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path("太阳能板1", "电压"));
      paths.add(new Path("太阳能板1", "电流"));
   
      IExpression timeFilter = BinaryExpression.and(
      new GlobalTimeExpression(TimeFilterApi.gtEq(1L)),
      new GlobalTimeExpression(TimeFilterApi.ltEq(10L)));
   
      QueryExpression queryExpression = QueryExpression.create(paths, timeFilter);
   ```

3. 查询数据

   ```java
   QueryDataSet queryDataSet = readTsFile.query(queryExpression);
   while (queryDataSet.hasNext()) {
        queryDataSet.next();
   }
   ```

4. 关闭文件

   ```java
   tsFileReader.close();
   ```

读取 TsFile 完整示例

[查询数据](../examples/src/main/java/org/apache/tsfile/TsFileRead.java)

[全文件读取](../examples/src/main/java/org/apache/tsfile/TsFileSequenceRead.java)


## 开发

### 前置条件

构建 Java 版的 TsFile，必须要安装以下依赖:

1. Java >= 1.8 (1.8, 11 到 17 都经过验证. 请确保设置了环境变量).
2. Maven >= 3.6.3 (如果要从源代码编译TsFile).


### 使用 maven 构建

```
mvn clean package -P with-java -DskipTests
```

### 安装到本地机器

```
mvn install -P with-java -DskipTests
```
