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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.iotdb.hadoop.tsfile.TSFInputFormat.READ_DELTAOBJECTS;
import static org.apache.iotdb.hadoop.tsfile.TSFInputFormat.READ_MEASUREMENTID;
import static org.junit.Assert.*;

public class TSFHiveRecordReaderTest {

  private TSFHiveRecordReader tsfHiveRecordReader;
  private String filePath = TestConstant.BASE_OUTPUT_PATH.concat("test.tsfile");

  @Before
  public void setUp() throws IOException {
    TsFileTestHelper.writeTsFile(filePath);
    JobConf job = new JobConf();
    Path path = new Path(filePath);
    String[] hosts = {"127.0.0.1"};
    TSFInputSplit inputSplit = new TSFInputSplit(path, hosts, 0, 3727528L);
    String[] deviceIds = {"device_1"}; // configure reading which deviceIds
    job.set(READ_DELTAOBJECTS, String.join(",", deviceIds));
    String[] measurementIds = {
      "sensor_1",
      "sensor_2",
      "sensor_3",
      "sensor_4",
      "sensor_5",
      "sensor_6",
      "sensor_7",
      "sensor_8",
      "sensor_9",
      "sensor_10"
    }; // configure reading which measurementIds
    job.set(READ_MEASUREMENTID, String.join(",", measurementIds));
    tsfHiveRecordReader = new TSFHiveRecordReader(inputSplit, job);
  }

  @After
  public void tearDown() {
    TsFileTestHelper.deleteTsFile(filePath);
  }

  @Test
  public void testNext() {
    NullWritable key = tsfHiveRecordReader.createKey();
    MapWritable value = tsfHiveRecordReader.createValue();
    try {
      assertTrue(tsfHiveRecordReader.next(key, value));
      assertEquals(1L, ((LongWritable) value.get(new Text("time_stamp"))).get());
      assertEquals("device_1", value.get(new Text("device_id")).toString());
      assertEquals(1000000L, ((LongWritable) value.get(new Text("sensor_1"))).get());
      assertEquals(1000000L, ((LongWritable) value.get(new Text("sensor_2"))).get());
      assertEquals(1000000L, ((LongWritable) value.get(new Text("sensor_3"))).get());
      assertEquals(1000000L, ((LongWritable) value.get(new Text("sensor_4"))).get());
      assertEquals(1000000L, ((LongWritable) value.get(new Text("sensor_5"))).get());
      assertEquals(1000000L, ((LongWritable) value.get(new Text("sensor_6"))).get());
      assertEquals(1000000L, ((LongWritable) value.get(new Text("sensor_7"))).get());
      assertEquals(1000000L, ((LongWritable) value.get(new Text("sensor_8"))).get());
      assertEquals(1000000L, ((LongWritable) value.get(new Text("sensor_9"))).get());
      assertEquals(1000000L, ((LongWritable) value.get(new Text("sensor_10"))).get());

      for (int i = 0; i < 100; i++) {
        assertTrue(tsfHiveRecordReader.next(key, value));
      }

      assertEquals(101L, ((LongWritable) value.get(new Text("time_stamp"))).get());
      assertEquals("device_1", value.get(new Text("device_id")).toString());
      assertEquals(1000100L, ((LongWritable) value.get(new Text("sensor_1"))).get());
      assertEquals(1000100L, ((LongWritable) value.get(new Text("sensor_2"))).get());
      assertEquals(1000100L, ((LongWritable) value.get(new Text("sensor_3"))).get());
      assertEquals(1000100L, ((LongWritable) value.get(new Text("sensor_4"))).get());
      assertEquals(1000100L, ((LongWritable) value.get(new Text("sensor_5"))).get());
      assertEquals(1000100L, ((LongWritable) value.get(new Text("sensor_6"))).get());
      assertEquals(1000100L, ((LongWritable) value.get(new Text("sensor_7"))).get());
      assertEquals(1000100L, ((LongWritable) value.get(new Text("sensor_8"))).get());
      assertEquals(1000100L, ((LongWritable) value.get(new Text("sensor_9"))).get());
      assertEquals(1000100L, ((LongWritable) value.get(new Text("sensor_10"))).get());

      for (int i = 0; i < 999899; i++) {
        assertTrue(tsfHiveRecordReader.next(key, value));
      }

      assertEquals(1000000L, ((LongWritable) value.get(new Text("time_stamp"))).get());
      assertEquals("device_1", value.get(new Text("device_id")).toString());
      assertEquals(1999999L, ((LongWritable) value.get(new Text("sensor_1"))).get());
      assertEquals(1999999L, ((LongWritable) value.get(new Text("sensor_2"))).get());
      assertEquals(1999999L, ((LongWritable) value.get(new Text("sensor_3"))).get());
      assertEquals(1999999L, ((LongWritable) value.get(new Text("sensor_4"))).get());
      assertEquals(1999999L, ((LongWritable) value.get(new Text("sensor_5"))).get());
      assertEquals(1999999L, ((LongWritable) value.get(new Text("sensor_6"))).get());
      assertEquals(1999999L, ((LongWritable) value.get(new Text("sensor_7"))).get());
      assertEquals(1999999L, ((LongWritable) value.get(new Text("sensor_8"))).get());
      assertEquals(1999999L, ((LongWritable) value.get(new Text("sensor_9"))).get());
      assertEquals(1999999L, ((LongWritable) value.get(new Text("sensor_10"))).get());

      // reach the end of the file
      assertFalse(tsfHiveRecordReader.next(key, value));
    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }
  }
}
