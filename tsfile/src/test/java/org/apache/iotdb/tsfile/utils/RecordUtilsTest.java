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
package org.apache.iotdb.tsfile.utils;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class RecordUtilsTest {

  Schema schema;

  private static Schema generateTestData() {
    Schema schema = new Schema();
    TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
    schema.registerTimeseries(
        new Path("d1", "s1"),
        new UnaryMeasurementSchema(
            "s1", TSDataType.INT32, TSEncoding.valueOf(conf.getValueEncoder())));
    schema.registerTimeseries(
        new Path("d1", "s2"),
        new UnaryMeasurementSchema(
            "s2", TSDataType.INT64, TSEncoding.valueOf(conf.getValueEncoder())));
    schema.registerTimeseries(
        new Path("d1", "s3"),
        new UnaryMeasurementSchema(
            "s3", TSDataType.FLOAT, TSEncoding.valueOf(conf.getValueEncoder())));
    schema.registerTimeseries(
        new Path("d1", "s4"),
        new UnaryMeasurementSchema(
            "s4", TSDataType.DOUBLE, TSEncoding.valueOf(conf.getValueEncoder())));
    schema.registerTimeseries(
        new Path("d1", "s5"),
        new UnaryMeasurementSchema("s5", TSDataType.BOOLEAN, TSEncoding.PLAIN));
    schema.registerTimeseries(
        new Path("d1", "s6"), new UnaryMeasurementSchema("s6", TSDataType.TEXT, TSEncoding.PLAIN));
    return schema;
  }

  @Before
  public void prepare() {
    schema = new Schema();
    schema = generateTestData();
  }

  @Test
  public void testParseSimpleTupleRecordInt() {
    String testString = "d1,1471522347000,s1,1";
    TSRecord record = RecordUtils.parseSimpleTupleRecord(testString, schema);
    assertEquals(1471522347000l, record.time);
    assertEquals("d1", record.deviceId);
    List<DataPoint> tuples = record.dataPointList;
    assertEquals(1, tuples.size());
    DataPoint tuple = tuples.get(0);
    assertEquals("s1", tuple.getMeasurementId());
    assertEquals(TSDataType.INT32, tuple.getType());
    assertEquals(1, tuple.getValue());

    testString = "d1,1471522347000,s1,1,";
    record = RecordUtils.parseSimpleTupleRecord(testString, schema);
    assertEquals(1471522347000l, record.time);
    assertEquals("d1", record.deviceId);
    tuples = record.dataPointList;
    assertEquals(1, tuples.size());
    tuple = tuples.get(0);
    assertEquals("s1", tuple.getMeasurementId());
    assertEquals(TSDataType.INT32, tuple.getType());
    assertEquals(1, tuple.getValue());

    testString = "d1,1471522347000,s1,1,s2";
    record = RecordUtils.parseSimpleTupleRecord(testString, schema);
    assertEquals(1471522347000l, record.time);
    assertEquals("d1", record.deviceId);
    tuples = record.dataPointList;
    assertEquals(1, tuples.size());
    tuple = tuples.get(0);
    assertEquals("s1", tuple.getMeasurementId());
    assertEquals(TSDataType.INT32, tuple.getType());
    assertEquals(1, tuple.getValue());
  }

  @Test
  public void testParseSimpleTupleRecordNull() {
    String testString = "d1,1471522347000,s1,1,s2,,s3,";
    TSRecord record = RecordUtils.parseSimpleTupleRecord(testString, schema);
    assertEquals(1471522347000l, record.time);
    List<DataPoint> tuples = record.dataPointList;
    assertEquals(1, tuples.size());
    DataPoint tuple = tuples.get(0);
    assertEquals("s1", tuple.getMeasurementId());
    assertEquals(TSDataType.INT32, tuple.getType());
    assertEquals(1, tuple.getValue());
  }

  @Test
  public void testParseSimpleTupleRecordAll() {
    String testString = "d1,1471522347000,s1,1,s2,134134287192587,s3,1.4,s4,1.128794817,s5,true";
    TSRecord record = RecordUtils.parseSimpleTupleRecord(testString, schema);
    assertEquals(1471522347000l, record.time);
    assertEquals("d1", record.deviceId);
    List<DataPoint> tuples = record.dataPointList;
    assertEquals(5, tuples.size()); // enum type is omitted.
    DataPoint tuple = tuples.get(0);
    assertEquals("s1", tuple.getMeasurementId());
    assertEquals(TSDataType.INT32, tuple.getType());
    assertEquals(1, tuple.getValue());
    tuple = tuples.get(1);
    assertEquals("s2", tuple.getMeasurementId());
    assertEquals(TSDataType.INT64, tuple.getType());
    assertEquals(134134287192587l, tuple.getValue());
    tuple = tuples.get(2);
    assertEquals("s3", tuple.getMeasurementId());
    assertEquals(TSDataType.FLOAT, tuple.getType());
    assertEquals(1.4f, tuple.getValue());
    tuple = tuples.get(3);
    assertEquals("s4", tuple.getMeasurementId());
    assertEquals(TSDataType.DOUBLE, tuple.getType());
    assertEquals(1.128794817d, tuple.getValue());
    tuple = tuples.get(4);
    assertEquals("s5", tuple.getMeasurementId());
    assertEquals(TSDataType.BOOLEAN, tuple.getType());
    assertEquals(true, tuple.getValue());
  }

  @Test
  public void testError() {
    String testString = "d1,1471522347000,s1,1,s2,s123";
    TSRecord record = RecordUtils.parseSimpleTupleRecord(testString, schema);
    assertEquals(1471522347000l, record.time);
    List<DataPoint> tuples = record.dataPointList;
    assertEquals(1, tuples.size());
    DataPoint tuple = tuples.get(0);
    assertEquals("s1", tuple.getMeasurementId());
    assertEquals(TSDataType.INT32, tuple.getType());
    assertEquals(1, tuple.getValue());
  }

  @Test
  public void testErrorMeasurementAndTimeStamp() {
    String testString = "d1,1471522347000,s1,1,s123,1";
    TSRecord record = RecordUtils.parseSimpleTupleRecord(testString, schema);
    assertEquals(1471522347000l, record.time);
    List<DataPoint> tuples = record.dataPointList;
    assertEquals(1, tuples.size());
    DataPoint tuple = tuples.get(0);
    assertEquals("s1", tuple.getMeasurementId());
    assertEquals(TSDataType.INT32, tuple.getType());
    assertEquals(1, tuple.getValue());

    testString = "d1,1dsjhk,s1,1,s123,1";
    record = RecordUtils.parseSimpleTupleRecord(testString, schema);
    assertEquals(record.time, -1);
    tuples = record.dataPointList;
    assertEquals(0, tuples.size());

    testString = "d1,1471522347000,s8,1";
    record = RecordUtils.parseSimpleTupleRecord(testString, schema);
    assertEquals(1471522347000l, record.time);
    tuples = record.dataPointList;
    assertEquals(0, tuples.size());
  }
}
