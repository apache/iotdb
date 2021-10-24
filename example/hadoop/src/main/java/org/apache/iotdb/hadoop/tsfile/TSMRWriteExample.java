/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.hadoop.tsfile;

import org.apache.iotdb.hadoop.tsfile.record.HDFSTSRecord;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/** One example for writing TsFile with MapReduce. */
public class TSMRWriteExample {

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, TSFHadoopException {

    if (args.length != 3) {
      System.out.println("Please give hdfs url, input path, output path");
      return;
    }

    Schema schema = new Schema();
    // the number of values to include in the row record
    int sensorNum = 3;

    // add measurements into file schema (all with INT64 data type)
    for (int i = 0; i < 2; i++) {
      schema.registerTimeseries(
          new org.apache.iotdb.tsfile.read.common.Path(
              Constant.DEVICE_1, Constant.SENSOR_PREFIX + (i + 1)),
          new UnaryMeasurementSchema(
              Constant.SENSOR_PREFIX + (i + 1), TSDataType.INT64, TSEncoding.TS_2DIFF));
    }

    for (int i = 2; i < sensorNum; i++) {
      schema.registerTimeseries(
          new org.apache.iotdb.tsfile.read.common.Path(
              Constant.DEVICE_1, Constant.SENSOR_PREFIX + (i + 1)),
          new UnaryMeasurementSchema(
              Constant.SENSOR_PREFIX + (i + 1), TSDataType.DOUBLE, TSEncoding.TS_2DIFF));
    }
    TSFOutputFormat.setSchema(schema);

    Path inputPath = new Path(args[1]);
    Path outputPath = new Path(args[2]);

    Configuration configuration = new Configuration();
    // set file system configuration
    // configuration.set("fs.defaultFS", HDFSURL);
    Job job = Job.getInstance(configuration);

    FileSystem fs = FileSystem.get(configuration);
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }

    job.setJobName("TsFile write jar");
    job.setJarByClass(TSMRWriteExample.class);
    // set mapper and reducer
    job.setMapperClass(TSMapper.class);
    job.setReducerClass(TSReducer.class);

    // set mapper output key and value
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(MapWritable.class);
    // set reducer output key and value
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(HDFSTSRecord.class);

    // set input format and output format
    job.setInputFormatClass(TSFInputFormat.class);
    job.setOutputFormatClass(TSFOutputFormat.class);

    // set input file path
    TSFInputFormat.setInputPaths(job, inputPath);
    // set output file path
    TSFOutputFormat.setOutputPath(job, outputPath);

    /** special configuration for reading tsfile with TSFInputFormat */
    TSFInputFormat.setReadTime(job, true); // configure reading time enable
    TSFInputFormat.setReadDeviceId(job, true); // configure reading deltaObjectId enable
    String[] deviceIds = {Constant.DEVICE_1}; // configure reading which deviceIds
    TSFInputFormat.setReadDeviceIds(job, deviceIds);
    String[] measurementIds = {
      Constant.SENSOR_1, Constant.SENSOR_2, Constant.SENSOR_3
    }; // configure reading which measurementIds
    TSFInputFormat.setReadMeasurementIds(job, measurementIds);
    boolean isSuccess = false;
    try {
      isSuccess = job.waitForCompletion(true);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e.getMessage());
    }
    if (isSuccess) {
      System.out.println("Execute successfully");
    } else {
      System.out.println("Execute unsuccessfully");
    }
  }

  public static class TSMapper extends Mapper<NullWritable, MapWritable, Text, MapWritable> {

    @Override
    protected void map(
        NullWritable key,
        MapWritable value,
        Mapper<NullWritable, MapWritable, Text, MapWritable>.Context context)
        throws IOException, InterruptedException {

      Text deltaObjectId = (Text) value.get(new Text("device_id"));
      long timestamp = ((LongWritable) value.get(new Text("time_stamp"))).get();
      if (timestamp % 100000 == 0) {
        context.write(deltaObjectId, new MapWritable(value));
      }
    }
  }

  /** This reducer calculate the average value. */
  public static class TSReducer extends Reducer<Text, MapWritable, NullWritable, HDFSTSRecord> {

    @Override
    protected void reduce(
        Text key,
        Iterable<MapWritable> values,
        Reducer<Text, MapWritable, NullWritable, HDFSTSRecord>.Context context)
        throws IOException, InterruptedException {
      long sensor1_value_sum = 0;
      long sensor2_value_sum = 0;
      double sensor3_value_sum = 0;
      long num = 0;
      for (MapWritable value : values) {
        num++;
        sensor1_value_sum += ((LongWritable) value.get(new Text(Constant.SENSOR_1))).get();
        sensor2_value_sum += ((LongWritable) value.get(new Text(Constant.SENSOR_2))).get();
        sensor3_value_sum += ((DoubleWritable) value.get(new Text(Constant.SENSOR_3))).get();
      }
      HDFSTSRecord tsRecord = new HDFSTSRecord(1L, key.toString());
      if (num != 0) {
        DataPoint dPoint1 = new LongDataPoint(Constant.SENSOR_1, sensor1_value_sum / num);
        DataPoint dPoint2 = new LongDataPoint(Constant.SENSOR_2, sensor2_value_sum / num);
        DataPoint dPoint3 = new DoubleDataPoint(Constant.SENSOR_3, sensor3_value_sum / num);
        tsRecord.addTuple(dPoint1);
        tsRecord.addTuple(dPoint2);
        tsRecord.addTuple(dPoint3);
      }
      context.write(NullWritable.get(), tsRecord);
    }
  }
}
