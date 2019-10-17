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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iotdb.tsfile.hadoop.TSFInputSplit;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TSFHiveRecordReaderTest {

  private TSFHiveRecordReader tsfHiveRecordReader;
  private String deviceId;

  @Before
  public void setUp() throws IOException {
    JobConf job = new JobConf();
    Path path = new Path("src/test/resources/test.tsfile");
    String[] hosts = {"127.0.0.1"};
    List<TSFInputSplit.ChunkGroupInfo> chunkGroupInfoList = new ArrayList<>();
    deviceId = "device_1";
    String[] measurementIds = new String[10];
    for (int i = 0; i < measurementIds.length; i++) {
      measurementIds[i] = "sensor_" + (i + 1);
    }
    long startOffset = 12L;
    long endOffset = 3727528L;
    long length = endOffset - startOffset;
    TSFInputSplit.ChunkGroupInfo chunkGroupInfo = new TSFInputSplit.ChunkGroupInfo(deviceId, measurementIds, startOffset, endOffset);
    chunkGroupInfoList.add(chunkGroupInfo);
    TSFInputSplit inputSplit = new TSFInputSplit(path, hosts, length, chunkGroupInfoList);

    tsfHiveRecordReader = new TSFHiveRecordReader(inputSplit, job);
  }

  @Test
  public void testNext() {
    NullWritable key = tsfHiveRecordReader.createKey();
    MapWritable value = tsfHiveRecordReader.createValue();
    try {
      assertTrue(tsfHiveRecordReader.next(key, value));
      assertEquals(1L, ((LongWritable)value.get(new Text("time_stamp"))).get());
      assertEquals(deviceId, value.get(new Text("device_id")).toString());
      assertEquals(1000000L, ((LongWritable)value.get(new Text("sensor_1"))).get());
      assertEquals(1000000L, ((LongWritable)value.get(new Text("sensor_2"))).get());
      assertEquals(1000000L, ((LongWritable)value.get(new Text("sensor_3"))).get());
      assertEquals(1000000L, ((LongWritable)value.get(new Text("sensor_4"))).get());
      assertEquals(1000000L, ((LongWritable)value.get(new Text("sensor_5"))).get());
      assertEquals(1000000L, ((LongWritable)value.get(new Text("sensor_6"))).get());
      assertEquals(1000000L, ((LongWritable)value.get(new Text("sensor_7"))).get());
      assertEquals(1000000L, ((LongWritable)value.get(new Text("sensor_8"))).get());
      assertEquals(1000000L, ((LongWritable)value.get(new Text("sensor_9"))).get());
      assertEquals(1000000L, ((LongWritable)value.get(new Text("sensor_10"))).get());

      for (int i = 0; i < 100; i++) {
        assertTrue(tsfHiveRecordReader.next(key, value));
      }

      assertEquals(101L, ((LongWritable)value.get(new Text("time_stamp"))).get());
      assertEquals(deviceId, value.get(new Text("device_id")).toString());
      assertEquals(1000100L, ((LongWritable)value.get(new Text("sensor_1"))).get());
      assertEquals(1000100L, ((LongWritable)value.get(new Text("sensor_2"))).get());
      assertEquals(1000100L, ((LongWritable)value.get(new Text("sensor_3"))).get());
      assertEquals(1000100L, ((LongWritable)value.get(new Text("sensor_4"))).get());
      assertEquals(1000100L, ((LongWritable)value.get(new Text("sensor_5"))).get());
      assertEquals(1000100L, ((LongWritable)value.get(new Text("sensor_6"))).get());
      assertEquals(1000100L, ((LongWritable)value.get(new Text("sensor_7"))).get());
      assertEquals(1000100L, ((LongWritable)value.get(new Text("sensor_8"))).get());
      assertEquals(1000100L, ((LongWritable)value.get(new Text("sensor_9"))).get());
      assertEquals(1000100L, ((LongWritable)value.get(new Text("sensor_10"))).get());

      for (int i = 0; i < 999899; i++) {
        assertTrue(tsfHiveRecordReader.next(key, value));
      }

      assertEquals(1000000L, ((LongWritable)value.get(new Text("time_stamp"))).get());
      assertEquals(deviceId, value.get(new Text("device_id")).toString());
      assertEquals(1999999L, ((LongWritable)value.get(new Text("sensor_1"))).get());
      assertEquals(1999999L, ((LongWritable)value.get(new Text("sensor_2"))).get());
      assertEquals(1999999L, ((LongWritable)value.get(new Text("sensor_3"))).get());
      assertEquals(1999999L, ((LongWritable)value.get(new Text("sensor_4"))).get());
      assertEquals(1999999L, ((LongWritable)value.get(new Text("sensor_5"))).get());
      assertEquals(1999999L, ((LongWritable)value.get(new Text("sensor_6"))).get());
      assertEquals(1999999L, ((LongWritable)value.get(new Text("sensor_7"))).get());
      assertEquals(1999999L, ((LongWritable)value.get(new Text("sensor_8"))).get());
      assertEquals(1999999L, ((LongWritable)value.get(new Text("sensor_9"))).get());
      assertEquals(1999999L, ((LongWritable)value.get(new Text("sensor_10"))).get());

      // reach the end of the file
      assertFalse(tsfHiveRecordReader.next(key, value));
    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }
  }
}
