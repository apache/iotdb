/**
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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.iotdb.hadoop.tsfile.TSFInputSplit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.*;


public class TSFHiveInputFormatTest {

  private String deviceId;
  private TSFInputSplit inputSplit;
  private TSFHiveInputFormat inputFormat;
  private JobConf job;
  private long length;
  private long startOffset;
  private long endOffset;
  private String[] measurementIds;
  private String filePath = "test.tsfile";

  @Before
  public void setUp() throws IOException {
    TsFileTestHelper.writeTsFile(filePath);
    inputFormat = new TSFHiveInputFormat();
    job = new JobConf();
    job.set(FileInputFormat.INPUT_DIR, filePath);
    Path path = new Path(filePath);
    String[] hosts = {"127.0.0.1"};
    List<TSFInputSplit.ChunkGroupInfo> chunkGroupInfoList = new ArrayList<>();
    deviceId = "device_1";
    measurementIds = new String[10];
    for (int i = 0; i < measurementIds.length; i++) {
      measurementIds[i] = "sensor_" + (i + 1);
    }
    startOffset = 12L;
    endOffset = 3727528L;
    length = endOffset - startOffset;
    TSFInputSplit.ChunkGroupInfo chunkGroupInfo = new TSFInputSplit.ChunkGroupInfo(deviceId, measurementIds, startOffset, endOffset);
    chunkGroupInfoList.add(chunkGroupInfo);
    inputSplit = new TSFInputSplit(path, hosts, length, chunkGroupInfoList);

  }

  @After
  public void tearDown() {
    TsFileTestHelper.deleteTsFile(filePath);
  }

  @Test
  public void testGetRecordReader() {
    try {
      RecordReader<NullWritable, MapWritable> recordReader = inputFormat.getRecordReader(inputSplit, job, null);
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
      assertEquals(length, inputSplit.getLength());
      assertEquals(1, inputSplit.getChunkGroupInfoList().size());
      TSFInputSplit.ChunkGroupInfo chunkGroupInfo = inputSplit.getChunkGroupInfoList().get(0);
      assertEquals(deviceId, chunkGroupInfo.getDeviceId());
      assertEquals(startOffset, chunkGroupInfo.getStartOffset());
      assertEquals(endOffset, chunkGroupInfo.getEndOffset());
      assertEquals(Arrays.stream(measurementIds).collect(toSet()), Arrays.stream(chunkGroupInfo.getMeasurementIds()).collect(toSet()));
    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }
  }
}
