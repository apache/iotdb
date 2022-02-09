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
package org.apache.iotdb.hive;

import org.apache.iotdb.hadoop.tsfile.TSFInputSplit;
import org.apache.iotdb.hive.constant.TestConstant;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.fileSystem.FSType;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TSFHiveInputFormatTest {

  private TSFInputSplit inputSplit;
  private TSFHiveInputFormat inputFormat;
  private JobConf job;
  private FSType beforeFSType;
  private final String filePath =
      TestConstant.BASE_OUTPUT_PATH
          .concat("data")
          .concat(File.separator)
          .concat("data")
          .concat(File.separator)
          .concat("sequence")
          .concat(File.separator)
          .concat("root.sg1")
          .concat(File.separator)
          .concat("0")
          .concat(File.separator)
          .concat("0")
          .concat(File.separator)
          .concat("1-0-0-0.tsfile");

  @Before
  public void setUp() {
    TsFileTestHelper.writeTsFile(filePath);
    inputFormat = new TSFHiveInputFormat();
    // in windows
    String jobPath = filePath.replaceAll("\\\\", "/");
    job = new JobConf();
    job.set(FileInputFormat.INPUT_DIR, jobPath);
    Path path = new Path(jobPath);
    String[] hosts = {"127.0.0.1"};
    inputSplit = new TSFInputSplit(path, hosts, 0, 3727688L);
    beforeFSType = TSFileDescriptor.getInstance().getConfig().getTSFileStorageFs();
    TSFileDescriptor.getInstance().getConfig().setTSFileStorageFs(FSType.HDFS);
  }

  @After
  public void tearDown() {
    TsFileTestHelper.deleteTsFile(filePath);
    TSFileDescriptor.getInstance().getConfig().setTSFileStorageFs(beforeFSType);
  }

  @Test
  public void testGetRecordReader() {
    try {
      RecordReader<NullWritable, MapWritable> recordReader =
          inputFormat.getRecordReader(inputSplit, job, null);
      assertTrue(recordReader instanceof TSFHiveRecordReader);
    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testGetSplits() {
    try {
      InputSplit[] inputSplits = inputFormat.getSplits(job, 0);
      assertEquals(1, inputSplits.length);
      assertTrue(inputSplits[0] instanceof TSFInputSplit);
      TSFInputSplit inputSplit = (TSFInputSplit) inputSplits[0];
    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }
  }
}
