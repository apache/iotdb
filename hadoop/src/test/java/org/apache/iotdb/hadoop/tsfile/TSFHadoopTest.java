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

import org.apache.iotdb.hadoop.fileSystem.HDFSInput;
import org.apache.iotdb.hadoop.tsfile.constant.TestConstant;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class TSFHadoopTest {

  private TSFInputFormat inputFormat = null;

  private String tsfilePath = TestConstant.BASE_OUTPUT_PATH.concat("example_mr.tsfile");

  @Before
  public void setUp() {

    TsFileTestHelper.deleteTsFile(tsfilePath);
    inputFormat = new TSFInputFormat();
  }

  @After
  public void tearDown() {

    TsFileTestHelper.deleteTsFile(tsfilePath);
  }

  @Test
  public void staticMethodTest() {
    Job job = null;
    try {
      job = Job.getInstance();
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    //
    // columns
    //
    String[] value = {"s1", "s2", "s3"};
    try {
      TSFInputFormat.setReadMeasurementIds(job, value);
      Set<String> getValue =
          new HashSet<>(
              Objects.requireNonNull(TSFInputFormat.getReadMeasurementIds(job.getConfiguration())));
      assertEquals(new HashSet<>(Arrays.asList(value)), getValue);

    } catch (TSFHadoopException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    //
    // deviceid
    //

    TSFInputFormat.setReadDeviceId(job, true);
    assertTrue(TSFInputFormat.getReadDeviceId(job.getConfiguration()));

    //
    // time
    //

    TSFInputFormat.setReadTime(job, true);
    assertTrue(TSFInputFormat.getReadTime(job.getConfiguration()));

    //
    // filter
    //
    TSFInputFormat.setHasFilter(job, true);
    assertTrue(TSFInputFormat.getHasFilter(job.getConfiguration()));

    String filterType = "singleFilter";
    TSFInputFormat.setFilterType(job, filterType);
    assertEquals(filterType, TSFInputFormat.getFilterType(job.getConfiguration()));

    String filterExpr = "s1>100";
    TSFInputFormat.setFilterExp(job, filterExpr);
    assertEquals(filterExpr, TSFInputFormat.getFilterExp(job.getConfiguration()));
  }

  @Test
  public void InputFormatTest() {

    //
    // test getinputsplit method
    //
    TsFileTestHelper.writeTsFile(tsfilePath);
    try {
      Job job = Job.getInstance();
      // set input path to the job
      TSFInputFormat.setInputPaths(job, tsfilePath);
      List<InputSplit> inputSplits = inputFormat.getSplits(job);
      TsFileSequenceReader reader =
          new TsFileSequenceReader(new HDFSInput(tsfilePath, job.getConfiguration()));
      System.out.println(reader.readFileMetadata());
      // assertEquals(tsFile.getRowGroupPosList().size(), inputSplits.size());
      for (InputSplit inputSplit : inputSplits) {
        System.out.println(inputSplit);
      }
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void RecordReaderTest() {
    TsFileTestHelper.writeTsFile(tsfilePath);
    try {
      Job job = Job.getInstance();
      // set input path to the job
      TSFInputFormat.setInputPaths(job, tsfilePath);
      String[] devices = {"device_1"};
      TSFInputFormat.setReadDeviceIds(job, devices);
      String[] sensors = {"sensor_1", "sensor_2", "sensor_3", "sensor_4", "sensor_5", "sensor_6"};
      TSFInputFormat.setReadMeasurementIds(job, sensors);
      TSFInputFormat.setReadDeviceId(job, false);
      TSFInputFormat.setReadTime(job, false);
      List<InputSplit> inputSplits = inputFormat.getSplits(job);
      TsFileSequenceReader reader =
          new TsFileSequenceReader(new HDFSInput(tsfilePath, job.getConfiguration()));

      reader.close();
      // read one split
      TSFRecordReader recordReader = new TSFRecordReader();
      TaskAttemptContextImpl attemptContextImpl =
          new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());
      recordReader.initialize(inputSplits.get(0), attemptContextImpl);
      System.out.println(inputSplits.get(0));
      long value = 1000000L;
      while (recordReader.nextKeyValue()) {
        assertEquals(recordReader.getCurrentValue().size(), sensors.length);
        for (Writable writable : recordReader.getCurrentValue().values()) {
          if (writable instanceof IntWritable) {
            assertEquals("1", writable.toString());
          } else if (writable instanceof LongWritable) {
            assertEquals(String.valueOf(value), writable.toString());
          } else if (writable instanceof FloatWritable) {
            assertEquals("0.1", writable.toString());
          } else if (writable instanceof DoubleWritable) {
            assertEquals("0.1", writable.toString());
          } else if (writable instanceof BooleanWritable) {
            assertEquals("true", writable.toString());
          } else if (writable instanceof Text) {
            assertEquals("tsfile", writable.toString());
          } else {
            fail(String.format("Not support type %s", writable.getClass().getName()));
          }
        }
        value++;
      }
      assertEquals(2000000L, value);
      recordReader.close();
    } catch (IOException | TSFHadoopException | InterruptedException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
