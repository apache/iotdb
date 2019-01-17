/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.hadoop.example;

import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.iotdb.tsfile.hadoop.TSFHadoopException;
import org.apache.iotdb.tsfile.hadoop.TSFInputFormat;
import org.apache.iotdb.tsfile.hadoop.TSFOutputFormat;

/**
 * One example for reading TsFile with MapReduce.
 * This MR Job is used to get the result of count("root.car.d1") in the tsfile.
 * The source of tsfile can be generated by <code>TsFileHelper</code>.
 * @author liukun
 *
 */
public class TSFMRReadExample {

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, TSFHadoopException, URISyntaxException {

    if (args.length != 3) {
      System.out.println("Please give hdfs url, input path, output path");
      return;
    }
    String HDFSURL = args[0];
    Path inputPath = new Path(args[1]);
    Path outputPath = new Path(args[2]);

    Configuration configuration = new Configuration();
    // set file system configuration
    //configuration.set("fs.defaultFS", HDFSURL);
    Job job = Job.getInstance(configuration);
    job.setJobName("TsFile read jar");
    job.setJarByClass(TSFMRReadExample.class);
    // set mapper and reducer
    job.setMapperClass(TSMapper.class);
    job.setReducerClass(TSReducer.class);
    // set inputformat and outputformat
    job.setInputFormatClass(TSFInputFormat.class);
    // set mapper output key and value
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    // set reducer output key and value
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    // set input file path
    TSFInputFormat.setInputPaths(job, inputPath);
    // set output file path
    TSFOutputFormat.setOutputPath(job, outputPath);
    /**
     * special configuration for reading tsfile with TSFInputFormat
     */
    TSFInputFormat.setReadTime(job, true); // configure reading time enable
    TSFInputFormat.setReadDeltaObjectId(job, true); // configure reading deltaObjectId enable
    String[] deltaObjectIds = {"device_1"};// configure reading which deltaObjectIds
    TSFInputFormat.setReadDeltaObjectIds(job, deltaObjectIds);
    String[] measurementIds = {"sensor_1",};// configure reading which measurementIds
    TSFInputFormat.setReadMeasurementIds(job, measurementIds);
    boolean isSuccess = false;
    try {
      isSuccess = job.waitForCompletion(true);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    if (isSuccess) {
      System.out.println("Execute successfully");
    } else {
      System.out.println("Execute unsuccessfully");
    }
  }

  public static class TSMapper extends Mapper<NullWritable, ArrayWritable, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1);

    @Override
    protected void map(NullWritable key, ArrayWritable value,
        Mapper<NullWritable, ArrayWritable, Text, IntWritable>.Context context)
        throws IOException, InterruptedException {

      Text deltaObjectId = (Text) value.get()[1];
      context.write(deltaObjectId, one);
    }
  }

  public static class TSReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
        Reducer<Text, IntWritable, Text, IntWritable>.Context context)
        throws IOException, InterruptedException {

      int sum = 0;
      for (IntWritable intWritable : values) {
        sum = sum + intWritable.get();
      }
      context.write(key, new IntWritable(sum));
    }
  }
}
