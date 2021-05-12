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

## TsFile 的 Hive 连接器

TsFile 的 Hive 连接器实现了通过 Hive 读取外部 TsFile 类型的文件格式的支持，使用户能够通过 Hive 操作 TsFile。

连接器的主要功能：

* 将单个 TsFile 文件加载进 Hive，不论文件是存储在本地文件系统或者是 HDFS 中
* 将某个特定目录下的所有文件加载进 Hive，不论文件是存储在本地文件系统或者是 HDFS 中
* 使用 HQL 查询 TsFile
* 到现在为止, 写操作在 hive-connector 中还不支持. 所以, HQL 中的 insert 操作是不被允许的

### 设计原理

Hive 连接器需要能够解析 TsFile 的文件格式，转化为 Hive 能够识别的按行返回的格式。也需要能够根据用户定义的 Table 的形式，格式化输出。所以，Hive 连接器的功能实现主要分成四个部分

* 将整个 TsFile 文件分片
* 从分片中读取数据，转化为 Hive 能够识别的数据类型
* 解析用户自定义的 Table
* 将数据反序列化为 Hive 的输出格式

### 具体实现类

上述的主要四个功能模块都有其对应的实现类，下面就分别介绍一下这四个实现类。

#### org.apache.iotdb.hive.TSFHiveInputFormat

该类主要负责对输入的 TsFile 文件的格式化操作，它继承了`FileInputFormat<NullWritable, MapWritable>`类，一些通用的格式化操作在`FileInputFormat`中已经有实现，这个类覆写了它的`getSplits(JobConf, int)`方法，自定义了对于 TsFile 文件的分片方式；以及`getRecordReader(InputSpli, JobConf, Reporter)`方法，用于生成具体从一个分片中读取数据的
`TSFHiveRecordReader`。

#### org.apache.iotdb.hive.TSFHiveRecordReader

该类主要负责从一个分片中读取 TsFile 的数据。

它实现了`IReaderSet`接口，这个接口里是一些设置类内部属性的方法，主要是为了抽出`TSRecordReader`和`TSHiveRecordReader`中重复的代码部分。

```
public interface IReaderSet {

  void setReader(TsFileSequenceReader reader);

  void setMeasurementIds(List<String> measurementIds);

  void setReadDeviceId(boolean isReadDeviceId);

  void setReadTime(boolean isReadTime);
}
```

下面先介绍一下这个类的一些重要字段

* ```
  private List<QueryDataSet> dataSetList = new ArrayList<>();
  ```

  这个分片所生成的所有的 QueryDataSet

* ```
  private List<String> deviceIdList = new ArrayList<>();
  ```

  设备名列表，这个顺序与 dataSetList 的顺序一致，即 deviceIdList[i] 是 dataSetList[i] 的设备名.

* private int currentIndex = 0;

  当前正在被处理的 QueryDataSet 的下标


这个类在构造函数里，调用了`TSFRecordReader`的`initialize(TSFInputSplit, Configuration, IReaderSet, List<QueryDataSet>, List<String>)`方法去初始化上面提到的一些类字段。它覆写了`RecordReader`的`next()`方法，用以返回从 TsFile 里读出的数据。

##### next(NullWritable, MapWritable)

我们注意到它从 TsFile 读取出来数据之后，是以`MapWritable`的形式返回的，这里的`MapWritable`其实就是一个`Map`，只不过它的 key 与 value 都做了序列化与反序列化的特殊适配，它的读取流程如下

1. 首先判断`dataSetList`当前位置的`QueryDataSet`还有没有值，如果没有值，则将`currentIndex`递增1，直到找到第一个有值的`QueryDataSet`
2. 然后调用`QueryDataSet`的`next()`方法获得`RowRecord`
3. 最后调用`TSFRecordReader`的`getCurrentValue()`方法，将`RowRecord`中的值放入`MapWritable`里


#### org.apache.iotdb.hive.TsFileSerDe

这个类继承了`AbstractSerDe`，也是我们实现Hive从自定义输入格式中读取数据所必须的。

它覆写了`AbstractSerDe`的`initialize()`方法，在这个方法里，从用户的建表 sql 里，解析出相应的设备名，传感器名以及传感器对应的类型。还要构建出`ObjectInspector`对象，这个对象主要负责数据类型的转化，由于 TsFile 只支持原始数据类型，所以当出现其他数据类型时，需要抛出异常，具体的构建过程在`createObjectInspectorWorker()`方法中可以看到。

这个类的最主要职责就是序列化和反序列化不同文件格式的数据，由于我们的 Hive 连接器暂时只支持读取操作，并不支持 insert 操作，所以只有反序列化的过程，所以仅覆写了`deserialize(Writable)`方法，该方法里调用了`TsFileDeserializer`的`deserialize()`方法。


#### org.apache.iotdb.hive.TsFileDeserializer

这个类就是将数据反序列化为 Hive 的输出格式，仅有一个`deserialize()`方法。

```
public Object deserialize(List<String>, List<TypeInfo>, Writable, String)
```

这个方法的`Writable`参数就是`TSFHiveRecordReader`的`next()`生成的`MapWritable`。

首先判断`Writable`参数是不是`MapWritable`类型，如果不是，则抛出异常。

接着依次从`MapWritable`中取出该设备的传感器的值，如果遇到类型不匹配则抛异常，最后返回生成的结果集。
