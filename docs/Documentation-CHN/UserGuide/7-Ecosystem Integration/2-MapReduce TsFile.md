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

# MapReduce TsFile

<!-- TOC -->
## 概要

- TsFile的Hadoop连接器使用手册
    - 什么是TsFile的Hadoop连接器
    - 系统环境要求
    - 数据类型对应关系
    - 关于TSFInputFormat的说明
    - 使用示例
        - 读示例: 求和
        - 写示例: 计算平均数并写入Tsfile中

<!-- /TOC -->
# TsFile的Hadoop连接器使用手册

## 什么是TsFile的Hadoop连接器

TsFile的Hadoop连接器实现了对Hadoop读取外部Tsfile类型的文件格式的支持。让用户可以使用Hadoop的map、reduce等操作对Tsfile文件进行读取、写入和查询。

有了这个连接器，用户可以
* 将单个Tsfile文件加载进Hadoop，不论文件是存储在本地文件系统或者是HDFS中
* 将某个特定目录下的所有文件加载进Hadoop，不论文件是存储在本地文件系统或者是HDFS中
* 将Hadoop处理完后的结果以Tsfile的格式保存

## 系统环境要求

|Hadoop 版本     | Java 版本     | TsFile 版本 |
|-------------  | ------------ |------------ |
| `2.7.3`       | `1.8`        | `0.10.0`|

>注意：关于如何下载和使用Tsfile, 请参考以下链接: https://github.com/apache/incubator-iotdb/tree/master/tsfile.

## 数据类型对应关系

| TsFile 数据类型    | Hadoop writable |
| ---------------- | --------------- |
| BOOLEAN          | BooleanWritable |
| INT32            | IntWritable     |
| INT64            | LongWritable    |
| FLOAT            | FloatWritable   |
| DOUBLE           | DoubleWritable  |
| TEXT             | Text            |

## 关于TSFInputFormat的说明

TSFInputFormat继承了Hadoop中FileInputFormat类，重写了其中切片的方法。

目前的切片方法是根据每个ChunkGroup的中点的offset是否属于Hadoop所切片的startOffset和endOffset之间，来判断是否将该ChunkGroup放入此切片。

TSFInputFormat将tsfile中的数据以多个`MapWritable`记录的形式返回给用户。

假设我们想要从Tsfile中获得名为`d1`的设备的数据，该设备有三个传感器，名称分别为`s1`, `s2`, `s3`。

`s1`的类型是`BOOLEAN`, `s2`的类型是 `DOUBLE`, `s3`的类型是`TEXT`.

`MapWritable`的结构如下所示：
```
{
    "time_stamp": 10000000,
    "device_id":  d1,
    "s1":         true,
    "s2":         3.14,
    "s3":         "middle"
}
```

在Hadoop的Map job中，你可以采用如下方法获得你想要的任何值

`mapwritable.get(new Text("s1"))`
> 注意: `MapWritable`中所有的键值类型都是`Text`。

## 使用示例

### 读示例: 求和

首先，我们需要在TSFInputFormat中配置我们需要哪些数据

```
    // configure reading time enable
    TSFInputFormat.setReadTime(job, true); 
    // configure reading deviceId enable
    TSFInputFormat.setReadDeviceId(job, true); 
    // configure reading which deltaObjectIds
    String[] deviceIds = {"device_1"};
    TSFInputFormat.setReadDeviceIds(job, deltaObjectIds);
    // configure reading which measurementIds
    String[] measurementIds = {"sensor_1", "sensor_2", "sensor_3"};
    TSFInputFormat.setReadMeasurementIds(job, measurementIds);
```

然后，必须指定mapper和reducer输出的键和值类型

```
    // set inputformat and outputformat
    job.setInputFormatClass(TSFInputFormat.class);
    // set mapper output key and value
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    // set reducer output key and value
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
```
接着，就可以编写包含具体的处理数据逻辑的`mapper`和`reducer`类了。

```
  public static class TSMapper extends Mapper<NullWritable, MapWritable, Text, DoubleWritable> {

    @Override
    protected void map(NullWritable key, MapWritable value,
        Mapper<NullWritable, MapWritable, Text, DoubleWritable>.Context context)
        throws IOException, InterruptedException {

      Text deltaObjectId = (Text) value.get(new Text("device_id"));
      context.write(deltaObjectId, (DoubleWritable) value.get(new Text("sensor_3")));
    }
  }

  public static class TSReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values,
        Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
        throws IOException, InterruptedException {

      double sum = 0;
      for (DoubleWritable value : values) {
        sum = sum + value.get();
      }
      context.write(key, new DoubleWritable(sum));
    }
  }
```

> 注意: 完整的代码示例可以在如下链接中找到：https://github.com/apache/incubator-iotdb/blob/master/example/hadoop/src/main/java/org/apache/iotdb/hadoop/tsfile/TSFMRReadExample.java


### 写示例: 计算平均数并写入Tsfile中

除了`OutputFormatClass`，剩下的配置代码跟上面的读示例是一样的

```
   job.setOutputFormatClass(TSFOutputFormat.class);
   // set reducer output key and value
   job.setOutputKeyClass(NullWritable.class);
   job.setOutputValueClass(HDFSTSRecord.class);
```

然后，是包含具体的处理数据逻辑的`mapper`和`reducer`类。

```
    public static class TSMapper extends Mapper<NullWritable, MapWritable, Text, MapWritable> {

        @Override
        protected void map(NullWritable key, MapWritable value,
                           Mapper<NullWritable, MapWritable, Text, MapWritable>.Context context)
                throws IOException, InterruptedException {

            Text deltaObjectId = (Text) value.get(new Text("device_id"));
            long timestamp = ((LongWritable)value.get(new Text("timestamp"))).get();
            if (timestamp % 100000 == 0) {
                context.write(deltaObjectId, new MapWritable(value));
            }
        }
    }

    /**
     * This reducer calculate the average value.
     */
    public static class TSReducer extends Reducer<Text, MapWritable, NullWritable, HDFSTSRecord> {

        @Override
        protected void reduce(Text key, Iterable<MapWritable> values,
                              Reducer<Text, MapWritable, NullWritable, HDFSTSRecord>.Context context) throws IOException, InterruptedException {
            long sensor1_value_sum = 0;
            long sensor2_value_sum = 0;
            double sensor3_value_sum = 0;
            long num = 0;
            for (MapWritable value : values) {
                num++;
                sensor1_value_sum += ((LongWritable)value.get(new Text("sensor_1"))).get();
                sensor2_value_sum += ((LongWritable)value.get(new Text("sensor_2"))).get();
                sensor3_value_sum += ((DoubleWritable)value.get(new Text("sensor_3"))).get();
            }
            HDFSTSRecord tsRecord = new HDFSTSRecord(1L, key.toString());
            DataPoint dPoint1 = new LongDataPoint("sensor_1", sensor1_value_sum / num);
            DataPoint dPoint2 = new LongDataPoint("sensor_2", sensor2_value_sum / num);
            DataPoint dPoint3 = new DoubleDataPoint("sensor_3", sensor3_value_sum / num);
            tsRecord.addTuple(dPoint1);
            tsRecord.addTuple(dPoint2);
            tsRecord.addTuple(dPoint3);
            context.write(NullWritable.get(), tsRecord);
        }
    }
```
> 注意: 完整的代码示例可以在如下链接中找到：https://github.com/apache/incubator-iotdb/blob/master/example/hadoop/src/main/java/org/apache/iotdb/hadoop/tsfile/TSMRWriteExample.java