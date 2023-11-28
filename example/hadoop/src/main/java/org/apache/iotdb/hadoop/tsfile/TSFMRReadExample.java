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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * One example for reading TsFile with MapReduce. This MR Job is used to get the result of
 * sum("device_1.sensor_3") in the tsfile. The source of tsfile can be generated by <code>
 * TsFileHelper</code>.
 */
public class TSFMRReadExample {

  private static final Logger LOGGER = LoggerFactory.getLogger(TSFMRReadExample.class);

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, TSFHadoopException {

    if (args.length != 3) {
      LOGGER.info("Please give hdfs url, input path, output path");
      return;
    }
    Path inputPath = new Path(args[1]);
    Path outputPath = new Path(args[2]);

    Configuration configuration = new Configuration();
    // set file system configuration
    Job job = Job.getInstance(configuration);

    FileSystem fs = FileSystem.get(configuration);
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }

    job.setJobName("TsFile read jar");
    job.setJarByClass(TSFMRReadExample.class);
    // set mapper and reducer
    job.setMapperClass(TSMapper.class);
    job.setReducerClass(TSReducer.class);
    // set inputformat and outputformat
    job.setInputFormatClass(TSFInputFormat.class);
    // set mapper output key and value
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    // set reducer output key and value
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    // set input file path
    TSFInputFormat.setInputPaths(job, inputPath);
    // set output file path
    TSFOutputFormat.setOutputPath(job, outputPath);

    /** special configuration for reading tsfile with TSFInputFormat */
    TSFInputFormat.setReadTime(job, true); // configure reading time enable
    TSFInputFormat.setReadDeviceId(job, true); // configure reading deltaObjectId enable
    String[] deviceIds = {"device_1"}; // configure reading which deviceIds
    TSFInputFormat.setReadDeviceIds(job, deviceIds);
    String[] measurementIds = {
      "sensor_1", "sensor_2", "sensor_3"
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
      LOGGER.info("Execute successfully");
    } else {
      LOGGER.info("Execute unsuccessfully");
    }
  }

  public static class TSMapper extends Mapper<NullWritable, MapWritable, Text, DoubleWritable> {

    @Override
    protected void map(
        NullWritable key,
        MapWritable value,
        Mapper<NullWritable, MapWritable, Text, DoubleWritable>.Context context)
        throws IOException, InterruptedException {

      Text deltaObjectId = (Text) value.get(new Text("device_id"));
      context.write(deltaObjectId, (DoubleWritable) value.get(new Text("sensor_3")));
    }
  }

  public static class TSReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(
        Text key,
        Iterable<DoubleWritable> values,
        Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
        throws IOException, InterruptedException {

      double sum = 0;
      for (DoubleWritable value : values) {
        sum = sum + value.get();
      }
      context.write(key, new DoubleWritable(sum));
    }
  }
}
