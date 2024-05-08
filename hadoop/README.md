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
# TsFile-Hadoop-Connector
<!-- TOC -->
## Outline

- TsFile-Hadoop-Connector User Guide
    - About TsFile-Hadoop-Connector
    - System Requirements
    - Data Type Correspondence
    - TSFInputFormat Explanation
    - Examples
        - Read Example: calculate the sum
        - Write Example: write the average into Tsfile

<!-- /TOC -->
# TsFile-Hadoop-Connector User Guide

## About TsFile-Hadoop-Connector

TsFile-Hadoop-Connector implements the support of Hadoop for external data sources of Tsfile type. This enables users to read, write and query Tsfile by Hadoop.

With this connector, you can
* load a single TsFile, from either the local file system or hdfs, into Hadoop
* load all files in a specific directory, from either the local file system or hdfs, into hadoop
* write data from Hadoop into TsFile

## System Requirements

|Hadoop Version | Java Version | TsFile Version|
|-------------  | ------------ |------------ |
| `2.7.3`       | `1.8`        | `1.0.0`|

> Note: For more information about how to download and use TsFile, please see the following link: https://github.com/apache/incubator-iotdb/tree/master/tsfile.

## Data Type Correspondence

| TsFile data type | Hadoop writable |
| ---------------- | --------------- |
| BOOLEAN          | BooleanWritable |
| INT32            | IntWritable     |
| INT64            | LongWritable    |
| FLOAT            | FloatWritable   |
| DOUBLE           | DoubleWritable  |
| TEXT             | Text            |

## TSFInputFormat Explanation

TSFInputFormat extract data from tsfile and format them into records of `MapWritable`.

Supposing that we want to extract data of the device named `d1` which has three measurements named `s1`, `s2`, `s3`.

`s1`'s type is `BOOLEAN`, `s2`'s type is `DOUBLE`, `s3`'s type is `TEXT`.

The `MapWritable` struct will be like:
```
{
    "time_stamp": 10000000,
    "device_id":  d1,
    "s1":         true,
    "s2":         3.14,
    "s3":         "middle"
}
```

In the Map job of Hadoop, you can get any value you want by key as following:

`mapwritable.get(new Text("s1"))`
> Note: All the keys in `MapWritable` have type of `Text`.

## Examples

### Read Example: calculate the sum

First of all, we should tell InputFormat what kind of data we want from tsfile.

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

And then,the output key and value of mapper and reducer should be specified

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

Then, the `mapper` and `reducer` class is how you deal with the `MapWritable` produced by `TSFInputFormat` class.

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

> Note: For the complete code, please see the following link: https://github.com/apache/incubator-iotdb/blob/master/example/hadoop/src/main/java/org/apache/iotdb//hadoop/tsfile/TSFMRReadExample.java


### Write Example: write the average into Tsfile

Except for the `OutputFormatClass`, the rest of configuration code for hadoop map-reduce job is almost same as above.

```
   job.setOutputFormatClass(TSFOutputFormat.class);
   // set reducer output key and value
   job.setOutputKeyClass(NullWritable.class);
   job.setOutputValueClass(HDFSTSRecord.class);
```

Then, the `mapper` and `reducer` class is how you deal with the `MapWritable` produced by `TSFInputFormat` class.

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
> Note: For the complete code, please see the following link: https://github.com/apache/incubator-iotdb/blob/master/example/hadoop/src/main/java/org/apache/iotdb//hadoop/tsfile/TSMRWriteExample.java