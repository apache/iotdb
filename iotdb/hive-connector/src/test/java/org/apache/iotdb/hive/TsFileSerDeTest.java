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

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

public class TsFileSerDeTest {

  private TsFileSerDe tsFileSerDer;
  private List<String> columnNames;
  private List<TypeInfo> columnTypes;
  private JobConf job;
  private Properties tbl;

  @Before
  public void setUp() {
    tsFileSerDer = new TsFileSerDe();
    columnNames = Arrays.asList("time_stamp", "sensor_1");
    columnTypes = new ArrayList<>();
    PrimitiveTypeInfo typeInfo1 = new PrimitiveTypeInfo();
    typeInfo1.setTypeName("bigint");
    columnTypes.add(typeInfo1);
    PrimitiveTypeInfo typeInfo2 = new PrimitiveTypeInfo();
    typeInfo2.setTypeName("bigint");
    columnTypes.add(typeInfo2);
    tbl = new Properties();
    String delimiter = ",";
    tbl.setProperty(serdeConstants.COLUMN_NAME_DELIMITER, delimiter);
    tbl.setProperty(serdeConstants.LIST_COLUMNS, String.join(delimiter, columnNames));
    tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES, "bigint,bigint");
    tbl.setProperty(TsFileSerDe.DEVICE_ID, "device_1");
    job = new JobConf();
    try {
      tsFileSerDer.initialize(job, tbl);
    } catch (SerDeException e) {
      e.printStackTrace();
    }
  }

  @After
  public void tearDown() {
    columnNames = null;
    columnTypes = null;
    job = null;
    tbl = null;
    tsFileSerDer = null;
  }

  @Test
  public void testDeserialize() {
    try {
      tsFileSerDer.initialize(job, tbl);
    } catch (SerDeException e) {
      e.printStackTrace();
      fail();
    }
    assertEquals(
        PrimitiveObjectInspector.PrimitiveCategory.LONG,
        ((PrimitiveTypeInfo) columnTypes.get(0)).getPrimitiveCategory());

    Writable worryWritable1 = new Text();
    try {
      tsFileSerDer.deserialize(worryWritable1);
      fail("Expect a TsFileSerDeException to be thrown!");
    } catch (SerDeException e) {
      assertEquals("Expecting a MapWritable", e.getMessage());
    }

    MapWritable worryWritable2 = new MapWritable();
    worryWritable2.put(new Text("device_id"), new Text("device_2"));
    worryWritable2.put(new Text("time_stamp"), new LongWritable(1L));
    worryWritable2.put(new Text("sensor_1"), new LongWritable(1L));
    try {
      assertNull(tsFileSerDer.deserialize(worryWritable2));
    } catch (SerDeException e) {
      fail("Don't expect a TsFileSerDeException to be Thrown!");
    }

    MapWritable worryWritable3 = new MapWritable();
    worryWritable3.put(new Text("device_id"), new Text("device_1"));
    worryWritable3.put(new Text("time_stamp"), new LongWritable(1L));
    worryWritable3.put(new Text("sensor_1"), new IntWritable(1));
    try {
      tsFileSerDer.deserialize(worryWritable3);
      fail("Expect a TsFileSerDeException to be thrown!");
    } catch (SerDeException e) {
      assertEquals(
          "Unexpected data type: "
              + worryWritable3.get(new Text("sensor_1")).getClass().getName()
              + " for Date TypeInfo: "
              + PrimitiveObjectInspector.PrimitiveCategory.LONG,
          e.getMessage());
    }

    MapWritable writable = new MapWritable();
    writable.put(new Text("device_id"), new Text("device_1"));
    writable.put(new Text("time_stamp"), new LongWritable(1L));
    writable.put(new Text("sensor_1"), new LongWritable(1000000L));
    try {
      Object result = tsFileSerDer.deserialize(writable);
      assertTrue(result instanceof List);
      List<Object> row = (List<Object>) result;
      assertEquals(columnNames.size(), row.size());
      assertEquals(1L, row.get(0));
      assertEquals(1000000L, row.get(1));
    } catch (SerDeException e) {
      fail("Don't expect a TsFileSerDeException to be Thrown!");
    }
  }
}
