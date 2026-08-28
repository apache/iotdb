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

import org.apache.tsfile.enums.ColumnCategory;
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
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertThrows;

public class SessionUtilsTest {

  @Test
  public void testGetTimeBuffer() {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    MeasurementSchema schema = new MeasurementSchema();
    schema.setMeasurementName("pressure");
    schema.setDataType(TSDataType.BOOLEAN);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    long[] timestamp = new long[] {1L, 2L};
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
    schema.setMeasurementName("pressure");
    schema.setDataType(TSDataType.INT32);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    schema = new MeasurementSchema();
    schema.setMeasurementName("pressure");
    schema.setDataType(TSDataType.INT64);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    schema = new MeasurementSchema();
    schema.setMeasurementName("pressure");
    schema.setDataType(TSDataType.FLOAT);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    schema = new MeasurementSchema();
    schema.setMeasurementName("pressure");
    schema.setDataType(TSDataType.DOUBLE);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    schema = new MeasurementSchema();
    schema.setMeasurementName("pressure");
    schema.setDataType(TSDataType.TEXT);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    schema = new MeasurementSchema();
    schema.setMeasurementName("pressure");
    schema.setDataType(TSDataType.BOOLEAN);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);

    long[] timestamp = new long[] {1L};
    Object[] values = new Object[6];
    values[0] = new int[] {1, 2};
    values[1] = new long[] {1L, 2L};
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
    List<Object> valueList = Arrays.asList(12, 13L, 1.2f, 0.707, "test", false);
    List<TSDataType> typeList =
        Arrays.asList(
            TSDataType.INT32,
            TSDataType.INT64,
            TSDataType.FLOAT,
            TSDataType.DOUBLE,
            TSDataType.TEXT,
            TSDataType.BOOLEAN);
    List<String> measurements = Arrays.asList("s1", "s2", "s3", "s4", "s5", "s6");
    ByteBuffer timeBuffer = SessionUtils.getValueBuffer(typeList, valueList, measurements);
    Assert.assertNotNull(timeBuffer);

    valueList = new ArrayList<>();
    valueList.add(null);
    typeList = Collections.singletonList(TSDataType.INT32);
    timeBuffer = SessionUtils.getValueBuffer(typeList, valueList, measurements);
    Assert.assertNotNull(timeBuffer);

    valueList = Collections.singletonList(false);
    typeList = Collections.singletonList(TSDataType.UNKNOWN);
    try {
      SessionUtils.getValueBuffer(typeList, valueList, measurements);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IoTDBConnectionException);
    }
  }

  @Test
  public void testGetValueBuffer3() {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    MeasurementSchema schema = new MeasurementSchema();
    schema.setMeasurementName("pressure0");
    schema.setDataType(TSDataType.INT32);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    schema = new MeasurementSchema();
    schema.setMeasurementName("pressure1");
    schema.setDataType(TSDataType.INT64);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    schema = new MeasurementSchema();
    schema.setMeasurementName("pressure2");
    schema.setDataType(TSDataType.FLOAT);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    schema = new MeasurementSchema();
    schema.setMeasurementName("pressure3");
    schema.setDataType(TSDataType.DOUBLE);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    schema = new MeasurementSchema();
    schema.setMeasurementName("pressure4");
    schema.setDataType(TSDataType.TEXT);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    schema = new MeasurementSchema();
    schema.setMeasurementName("pressure5");
    schema.setDataType(TSDataType.BOOLEAN);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    schema = new MeasurementSchema();
    schema.setMeasurementName("pressure6");
    schema.setDataType(TSDataType.DATE);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);

    Tablet tablet = new Tablet("device1", schemas, 2);
    tablet.setTimestamps(new long[] {1L});
    tablet.getValues()[0] = new int[] {1, 2};
    tablet.getValues()[1] = new long[] {1L, 2L};
    tablet.getValues()[2] = new float[] {1.1f, 1.2f};
    tablet.getValues()[3] = new double[] {0.707, 0.708};
    tablet.getValues()[4] = new Binary[] {null, new Binary(new byte[] {(byte) 16})};
    tablet.getValues()[5] = new boolean[] {true, false};
    tablet.getValues()[6] = new LocalDate[] {null, LocalDate.of(2024, 4, 1)};
    tablet.setRowSize(tablet.getRowSize() + 2);

    ByteBuffer timeBuffer = SessionUtils.getValueBuffer(tablet);
    Assert.assertNotNull(timeBuffer);
  }

  @Test
  public void testGetValueBufferWithWrongType() {
    List<Object> valueList = Arrays.asList(12L, 13, 1.2, 0.707f, false, "false");
    List<TSDataType> typeList =
        Arrays.asList(
            TSDataType.INT32,
            TSDataType.INT64,
            TSDataType.FLOAT,
            TSDataType.DOUBLE,
            TSDataType.TEXT,
            TSDataType.BOOLEAN);
    List<String> measurements = Arrays.asList("s1", "s2", "s3", "s4", "s5", "s6");
    assertThrows(
        ClassCastException.class,
        () -> SessionUtils.getValueBuffer(typeList, valueList, measurements));
  }

  @Test
  public void testFilterNullColumns() {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    schemas.add(new MeasurementSchema("s1", TSDataType.INT32));
    schemas.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemas.add(new MeasurementSchema("s3", TSDataType.FLOAT));
    schemas.add(new MeasurementSchema("s4", TSDataType.DOUBLE));
    schemas.add(new MeasurementSchema("s5", TSDataType.BOOLEAN));

    Tablet tablet = new Tablet("root.sg.d1", schemas, 1);
    tablet.addTimestamp(0, 1000L);
    tablet.addValue("s1", 0, 1);
    tablet.addValue("s2", 0, null);
    tablet.addValue("s3", 0, 1.5f);
    tablet.addValue("s4", 0, null);
    tablet.addValue("s5", 0, null);

    Tablet filtered = SessionUtils.filterNullColumns(tablet);
    Assert.assertNotNull(filtered);
    Assert.assertNotSame(tablet, filtered);
    Assert.assertEquals(2, filtered.getSchemas().size());
    Assert.assertEquals("s1", filtered.getSchemas().get(0).getMeasurementName());
    Assert.assertEquals("s3", filtered.getSchemas().get(1).getMeasurementName());
    Assert.assertEquals(1, ((int[]) filtered.getValues()[0])[0]);
    Assert.assertEquals(1.5f, ((float[]) filtered.getValues()[1])[0], 0.0001f);

    // no BitMap -> same instance
    long[] timestamps = new long[] {1L};
    Object[] values = new Object[] {new int[] {1}};
    List<IMeasurementSchema> singleSchema =
        Collections.singletonList(new MeasurementSchema("s1", TSDataType.INT32));
    Tablet noBitMapTablet = new Tablet("root.sg.d1", singleSchema, timestamps, values, null, 1);
    Assert.assertSame(noBitMapTablet, SessionUtils.filterNullColumns(noBitMapTablet));

    // all columns null -> null
    Tablet allNull = new Tablet("root.sg.d1", schemas, 1);
    allNull.addTimestamp(0, 2000L);
    allNull.addValue("s1", 0, null);
    allNull.addValue("s2", 0, null);
    allNull.addValue("s3", 0, null);
    allNull.addValue("s4", 0, null);
    allNull.addValue("s5", 0, null);
    Assert.assertNull(SessionUtils.filterNullColumns(allNull));

    // table model: keep TAG when all FIELD columns are null
    List<IMeasurementSchema> tableSchemas = new ArrayList<>();
    tableSchemas.add(new MeasurementSchema("tag1", TSDataType.STRING));
    tableSchemas.add(new MeasurementSchema("s1", TSDataType.INT32));
    tableSchemas.add(new MeasurementSchema("s2", TSDataType.INT32));
    List<ColumnCategory> columnCategories = new ArrayList<>();
    columnCategories.add(ColumnCategory.TAG);
    columnCategories.add(ColumnCategory.FIELD);
    columnCategories.add(ColumnCategory.FIELD);
    Tablet tableModelTablet = new Tablet("table1", tableSchemas, columnCategories, 1);
    tableModelTablet.addTimestamp(0, 3000L);
    tableModelTablet.addValue("tag1", 0, "d1");
    tableModelTablet.addValue("s1", 0, null);
    tableModelTablet.addValue("s2", 0, null);
    Tablet tableModelFiltered = SessionUtils.filterNullColumns(tableModelTablet);
    Assert.assertNotNull(tableModelFiltered);
    Assert.assertNotSame(tableModelTablet, tableModelFiltered);
    Assert.assertEquals(1, tableModelFiltered.getSchemas().size());
    Assert.assertEquals("tag1", tableModelFiltered.getSchemas().get(0).getMeasurementName());
    Assert.assertEquals(ColumnCategory.TAG, tableModelFiltered.getColumnTypes().get(0));
  }

  @Test
  public void testParseSeedNodeUrls() {
    List<String> nodeUrls = Collections.singletonList("127.0.0.1:1234");
    List<TEndPoint> tEndPoints = SessionUtils.parseSeedNodeUrls(nodeUrls);
    Assert.assertEquals(tEndPoints.size(), 1);

    try {
      SessionUtils.parseSeedNodeUrls(null);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof NumberFormatException);
    }

    nodeUrls = Collections.singletonList("127.0.0.1:1234:0");
    try {
      SessionUtils.parseSeedNodeUrls(nodeUrls);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof NumberFormatException);
    }

    nodeUrls = Collections.singletonList("127.0.0.1:test");
    try {
      SessionUtils.parseSeedNodeUrls(nodeUrls);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof NumberFormatException);
    }
  }

  @Test
  public void testParseSeedNodeUrlsException() {
    List<String> nodeUrls = Collections.singletonList("127.0.0.1:1234");
    List<TEndPoint> tEndPoints = SessionUtils.parseSeedNodeUrls(nodeUrls);
    Assert.assertEquals(tEndPoints.size(), 1);
  }
}
