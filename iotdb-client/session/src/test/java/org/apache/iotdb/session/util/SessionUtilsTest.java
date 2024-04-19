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

package org.apache.iotdb.session.util;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.rpc.IoTDBConnectionException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SessionUtilsTest {

  @Test
  public void testGetTimeBuffer() {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    MeasurementSchema schema = new MeasurementSchema();
    schema.setMeasurementId("pressure");
    schema.setType(TSDataType.BOOLEAN);
    schema.setCompressor(CompressionType.SNAPPY.serialize());
    schema.setEncoding(TSEncoding.PLAIN.serialize());
    schemas.add(schema);
    long[] timestamp = new long[] {1l, 2l};
    Object[] values = new Object[] {true, false};
    BitMap[] partBitMap = new BitMap[2];
    Tablet tablet = new Tablet("device1", schemas, timestamp, values, partBitMap, 2);
    ByteBuffer timeBuffer = SessionUtils.getTimeBuffer(tablet);
    Assert.assertNotNull(timeBuffer);
  }

  @Test
  public void testGetValueBuffer() {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    MeasurementSchema schema = new MeasurementSchema();
    schema.setMeasurementId("pressure");
    schema.setType(TSDataType.INT32);
    schema.setCompressor(CompressionType.SNAPPY.serialize());
    schema.setEncoding(TSEncoding.PLAIN.serialize());
    schemas.add(schema);
    schema = new MeasurementSchema();
    schema.setMeasurementId("pressure");
    schema.setType(TSDataType.INT64);
    schema.setCompressor(CompressionType.SNAPPY.serialize());
    schema.setEncoding(TSEncoding.PLAIN.serialize());
    schemas.add(schema);
    schema = new MeasurementSchema();
    schema.setMeasurementId("pressure");
    schema.setType(TSDataType.FLOAT);
    schema.setCompressor(CompressionType.SNAPPY.serialize());
    schema.setEncoding(TSEncoding.PLAIN.serialize());
    schemas.add(schema);
    schema = new MeasurementSchema();
    schema.setMeasurementId("pressure");
    schema.setType(TSDataType.DOUBLE);
    schema.setCompressor(CompressionType.SNAPPY.serialize());
    schema.setEncoding(TSEncoding.PLAIN.serialize());
    schemas.add(schema);
    schema = new MeasurementSchema();
    schema.setMeasurementId("pressure");
    schema.setType(TSDataType.TEXT);
    schema.setCompressor(CompressionType.SNAPPY.serialize());
    schema.setEncoding(TSEncoding.PLAIN.serialize());
    schemas.add(schema);
    schema = new MeasurementSchema();
    schema.setMeasurementId("pressure");
    schema.setType(TSDataType.BOOLEAN);
    schema.setCompressor(CompressionType.SNAPPY.serialize());
    schema.setEncoding(TSEncoding.PLAIN.serialize());
    schemas.add(schema);

    long[] timestamp = new long[] {1l};
    Object[] values = new Object[6];
    values[0] = new int[] {1, 2};
    values[1] = new long[] {1l, 2l};
    values[2] = new float[] {1.1f, 1.2f};
    values[3] = new double[] {0.707, 0.708};
    values[4] =
        new Binary[] {new Binary(new byte[] {(byte) 8}), new Binary(new byte[] {(byte) 16})};
    values[5] = new boolean[] {true, false};
    BitMap[] partBitMap = new BitMap[6];
    Tablet tablet = new Tablet("device1", schemas, timestamp, values, partBitMap, 2);
    ByteBuffer timeBuffer = SessionUtils.getValueBuffer(tablet);
    Assert.assertNotNull(timeBuffer);
  }

  @Test
  public void testGetValueBuffer2() throws IoTDBConnectionException {
    List<Object> valueList = Arrays.asList(12, 13l, 1.2f, 0.707, "test", false);
    List<TSDataType> typeList =
        Arrays.asList(
            TSDataType.INT32,
            TSDataType.INT64,
            TSDataType.FLOAT,
            TSDataType.DOUBLE,
            TSDataType.TEXT,
            TSDataType.BOOLEAN);
    ByteBuffer timeBuffer = SessionUtils.getValueBuffer(typeList, valueList);
    Assert.assertNotNull(timeBuffer);

    valueList = new ArrayList<>();
    valueList.add(null);
    typeList = Arrays.asList(TSDataType.INT32);
    timeBuffer = SessionUtils.getValueBuffer(typeList, valueList);
    Assert.assertNotNull(timeBuffer);

    valueList = Arrays.asList(false);
    typeList = Arrays.asList(TSDataType.UNKNOWN);
    try {
      timeBuffer = SessionUtils.getValueBuffer(typeList, valueList);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IoTDBConnectionException);
    }
  }

  @Test
  public void testParseSeedNodeUrls() {
    List<String> nodeUrls = Arrays.asList("127.0.0.1:1234");
    List<TEndPoint> tEndPoints = SessionUtils.parseSeedNodeUrls(nodeUrls);
    Assert.assertEquals(tEndPoints.size(), 1);

    try {
      tEndPoints = SessionUtils.parseSeedNodeUrls(null);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof NumberFormatException);
    }

    nodeUrls = Arrays.asList("127.0.0.1:1234:0");
    try {
      tEndPoints = SessionUtils.parseSeedNodeUrls(nodeUrls);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof NumberFormatException);
    }

    nodeUrls = Arrays.asList("127.0.0.1:test");
    try {
      tEndPoints = SessionUtils.parseSeedNodeUrls(nodeUrls);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof NumberFormatException);
    }
  }

  @Test
  public void testParseSeedNodeUrlsException() {
    List<String> nodeUrls = Arrays.asList("127.0.0.1:1234");
    List<TEndPoint> tEndPoints = SessionUtils.parseSeedNodeUrls(nodeUrls);
    Assert.assertEquals(tEndPoints.size(), 1);
  }
}
