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
package org.apache.iotdb.session.it;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.OperationType;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.BatchExecutionException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
public class IoTDBSessionSimpleIT {

  private static Logger LOGGER = LoggerFactory.getLogger(IoTDBSessionSimpleIT.class);

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertPartialTabletTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      List<MeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
      schemaList.add(new MeasurementSchema("s2", TSDataType.DOUBLE));
      schemaList.add(new MeasurementSchema("s3", TSDataType.TEXT));

      Tablet tablet = new Tablet("root.sg.d", schemaList, 10);

      long timestamp = System.currentTimeMillis();

      for (long row = 0; row < 15; row++) {
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, timestamp);
        tablet.addValue("s1", rowIndex, 1L);
        tablet.addValue("s2", rowIndex, 1D);
        tablet.addValue("s3", rowIndex, new Binary("1"));
        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          session.insertTablet(tablet, true);
          tablet.reset();
        }
        timestamp++;
      }

      if (tablet.rowSize != 0) {
        session.insertTablet(tablet);
        tablet.reset();
      }

      SessionDataSet dataSet = session.executeQueryStatement("select count(*) from root");
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(15L, rowRecord.getFields().get(0).getLongV());
        assertEquals(15L, rowRecord.getFields().get(1).getLongV());
        assertEquals(15L, rowRecord.getFields().get(2).getLongV());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertByStrAndInferTypeTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String deviceId = "root.sg1.d1";
      List<String> measurements = new ArrayList<>();
      measurements.add("s1");
      measurements.add("s2");
      measurements.add("s3");
      measurements.add("s4");

      List<String> values = new ArrayList<>();
      values.add("1");
      values.add("1.2");
      values.add("true");
      values.add("dad");
      session.insertRecord(deviceId, 1L, measurements, values);

      Set<String> expected = new HashSet<>();
      expected.add(IoTDBDescriptor.getInstance().getConfig().getIntegerStringInferType().name());
      expected.add(IoTDBDescriptor.getInstance().getConfig().getFloatingStringInferType().name());
      expected.add(IoTDBDescriptor.getInstance().getConfig().getBooleanStringInferType().name());
      expected.add(TSDataType.TEXT.name());

      Set<String> actual = new HashSet<>();
      SessionDataSet dataSet = session.executeQueryStatement("show timeseries root.**");
      while (dataSet.hasNext()) {
        actual.add(dataSet.next().getFields().get(3).getStringValue());
      }

      assertEquals(expected, actual);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertWrongPathByStrAndInferTypeTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String deviceId = "root.sg1..d1";
      List<String> measurements = new ArrayList<>();
      measurements.add("s1");
      measurements.add("s2");
      measurements.add("s3");
      measurements.add("s4");

      List<String> values = new ArrayList<>();
      values.add("1");
      values.add("1.2");
      values.add("true");
      values.add("dad");
      try {
        session.insertRecord(deviceId, 1L, measurements, values);
      } catch (Exception e) {
        LOGGER.error("", e);
      }

      SessionDataSet dataSet = session.executeQueryStatement("show timeseries root");
      assertFalse(dataSet.hasNext());

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertIntoIllegalTimeseriesTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String deviceId = "root.sg1.d1\n";
      List<String> measurements = new ArrayList<>();
      measurements.add("s1");
      measurements.add("s2");
      measurements.add("s3");
      measurements.add("s4");

      List<String> values = new ArrayList<>();
      values.add("1");
      values.add("1.2");
      values.add("true");
      values.add("dad");
      try {
        session.insertRecord(deviceId, 1L, measurements, values);
      } catch (Exception e) {
        LOGGER.error("", e);
      }

      SessionDataSet dataSet = session.executeQueryStatement("show timeseries root");
      assertFalse(dataSet.hasNext());

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertByObjAndNotInferTypeTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String deviceId = "root.sg1.d1";
      List<String> measurements = new ArrayList<>();
      measurements.add("s1");
      measurements.add("s2");
      measurements.add("s3");
      measurements.add("s4");

      List<TSDataType> dataTypes = new ArrayList<>();
      dataTypes.add(TSDataType.INT64);
      dataTypes.add(TSDataType.DOUBLE);
      dataTypes.add(TSDataType.TEXT);
      dataTypes.add(TSDataType.TEXT);

      List<Object> values = new ArrayList<>();
      values.add(1L);
      values.add(1.2d);
      values.add("true");
      values.add("dad");
      session.insertRecord(deviceId, 1L, measurements, dataTypes, values);

      Set<String> expected = new HashSet<>();
      expected.add(TSDataType.INT64.name());
      expected.add(TSDataType.DOUBLE.name());
      expected.add(TSDataType.TEXT.name());
      expected.add(TSDataType.TEXT.name());

      Set<String> actual = new HashSet<>();
      SessionDataSet dataSet = session.executeQueryStatement("show timeseries root.**");
      while (dataSet.hasNext()) {
        actual.add(dataSet.next().getFields().get(3).getStringValue());
      }

      assertEquals(expected, actual);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void createMultiTimeseriesTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      List<String> paths = new ArrayList<>();
      paths.add("root.sg1.d1.s1");
      paths.add("root.sg1.d1.s2");
      List<TSDataType> tsDataTypes = new ArrayList<>();
      tsDataTypes.add(TSDataType.DOUBLE);
      tsDataTypes.add(TSDataType.DOUBLE);
      List<TSEncoding> tsEncodings = new ArrayList<>();
      tsEncodings.add(TSEncoding.RLE);
      tsEncodings.add(TSEncoding.RLE);
      List<CompressionType> compressionTypes = new ArrayList<>();
      compressionTypes.add(CompressionType.SNAPPY);
      compressionTypes.add(CompressionType.SNAPPY);

      List<Map<String, String>> tagsList = new ArrayList<>();
      Map<String, String> tags = new HashMap<>();
      tags.put("tag1", "v1");
      tagsList.add(tags);
      tagsList.add(tags);

      session.createMultiTimeseries(
          paths, tsDataTypes, tsEncodings, compressionTypes, null, tagsList, null, null);

      assertTrue(session.checkTimeseriesExists("root.sg1.d1.s1"));
      assertTrue(session.checkTimeseriesExists("root.sg1.d1.s2"));
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void chineseCharacterTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      if (!System.getProperty("sun.jnu.encoding").contains("UTF-8")) {
        LOGGER.error("The system does not support UTF-8, so skip Chinese test...");
        session.close();
        return;
      }
      String storageGroup = "root.存储组1";
      String[] devices = new String[] {"设备1.指标1", "设备1.s2", "d2.s1", "d2.指标2"};
      session.setStorageGroup(storageGroup);
      for (String path : devices) {
        String fullPath = storageGroup + TsFileConstant.PATH_SEPARATOR + path;
        session.createTimeseries(
            fullPath, TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
      }

      for (String path : devices) {
        for (int i = 0; i < 10; i++) {
          String[] ss = path.split("\\.");
          StringBuilder deviceId = new StringBuilder(storageGroup);
          for (int j = 0; j < ss.length - 1; j++) {
            deviceId.append(TsFileConstant.PATH_SEPARATOR).append(ss[j]);
          }
          String sensorId = ss[ss.length - 1];
          List<String> measurements = new ArrayList<>();
          List<Object> values = new ArrayList<>();
          List<TSDataType> types = new ArrayList<>();

          measurements.add(sensorId);
          types.add(TSDataType.INT64);
          values.add(100L);
          session.insertRecord(deviceId.toString(), i, measurements, types, values);
        }
      }

      SessionDataSet dataSet = session.executeQueryStatement("select * from root.存储组1.*");
      int count = 0;
      while (dataSet.hasNext()) {
        count++;
      }
      assertEquals(10, count);
      session.deleteStorageGroup(storageGroup);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertTabletWithAlignedTimeseriesTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      List<MeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
      schemaList.add(new MeasurementSchema("s2", TSDataType.INT32));
      schemaList.add(new MeasurementSchema("s3", TSDataType.TEXT));

      Tablet tablet = new Tablet("root.sg1.d1", schemaList);
      long timestamp = System.currentTimeMillis();

      for (long row = 0; row < 10; row++) {
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, timestamp);
        tablet.addValue(
            schemaList.get(0).getMeasurementId(), rowIndex, new SecureRandom().nextLong());
        tablet.addValue(
            schemaList.get(1).getMeasurementId(), rowIndex, new SecureRandom().nextInt());
        tablet.addValue(schemaList.get(2).getMeasurementId(), rowIndex, new Binary("test"));
        timestamp++;
      }

      if (tablet.rowSize != 0) {
        session.insertAlignedTablet(tablet);
        tablet.reset();
      }

      SessionDataSet dataSet = session.executeQueryStatement("select count(*) from root");
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(10L, rowRecord.getFields().get(0).getLongV());
        assertEquals(10L, rowRecord.getFields().get(1).getLongV());
        assertEquals(10L, rowRecord.getFields().get(2).getLongV());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertTabletWithNullValuesTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      List<MeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("s0", TSDataType.DOUBLE, TSEncoding.RLE));
      schemaList.add(new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
      schemaList.add(new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
      schemaList.add(new MeasurementSchema("s3", TSDataType.INT32, TSEncoding.RLE));
      schemaList.add(new MeasurementSchema("s4", TSDataType.BOOLEAN, TSEncoding.RLE));
      schemaList.add(new MeasurementSchema("s5", TSDataType.TEXT, TSEncoding.RLE));

      Tablet tablet = new Tablet("root.sg1.d1", schemaList);
      for (long time = 0; time < 10; time++) {
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, time);

        tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, (double) time);
        tablet.addValue(schemaList.get(1).getMeasurementId(), rowIndex, (float) time);
        tablet.addValue(schemaList.get(2).getMeasurementId(), rowIndex, time);
        tablet.addValue(schemaList.get(3).getMeasurementId(), rowIndex, (int) time);
        tablet.addValue(schemaList.get(4).getMeasurementId(), rowIndex, time % 2 == 0);
        tablet.addValue(
            schemaList.get(5).getMeasurementId(), rowIndex, new Binary(String.valueOf(time)));
      }

      BitMap[] bitMaps = new BitMap[schemaList.size()];
      for (int i = 0; i < schemaList.size(); i++) {
        if (bitMaps[i] == null) {
          bitMaps[i] = new BitMap(10);
        }
        bitMaps[i].mark(i);
      }
      tablet.bitMaps = bitMaps;

      if (tablet.rowSize != 0) {
        session.insertTablet(tablet);
        tablet.reset();
      }

      SessionDataSet dataSet = session.executeQueryStatement("select count(*) from root");
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(6L, rowRecord.getFields().size());
        for (Field field : rowRecord.getFields()) {
          assertEquals(9L, field.getLongV());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertTabletWithStringValuesTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      List<MeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("s0", TSDataType.DOUBLE, TSEncoding.RLE));
      schemaList.add(new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
      schemaList.add(new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
      schemaList.add(new MeasurementSchema("s3", TSDataType.INT32, TSEncoding.RLE));
      schemaList.add(new MeasurementSchema("s4", TSDataType.BOOLEAN, TSEncoding.RLE));
      schemaList.add(new MeasurementSchema("s5", TSDataType.TEXT, TSEncoding.RLE));
      schemaList.add(new MeasurementSchema("s6", TSDataType.TEXT, TSEncoding.RLE));

      Tablet tablet = new Tablet("root.sg1.d1", schemaList);
      for (long time = 0; time < 10; time++) {
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, time);

        tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, (double) time);
        tablet.addValue(schemaList.get(1).getMeasurementId(), rowIndex, (float) time);
        tablet.addValue(schemaList.get(2).getMeasurementId(), rowIndex, time);
        tablet.addValue(schemaList.get(3).getMeasurementId(), rowIndex, (int) time);
        tablet.addValue(schemaList.get(4).getMeasurementId(), rowIndex, time % 2 == 0);
        tablet.addValue(schemaList.get(5).getMeasurementId(), rowIndex, new Binary("Text" + time));
        tablet.addValue(schemaList.get(6).getMeasurementId(), rowIndex, "Text" + time);
      }

      if (tablet.rowSize != 0) {
        session.insertTablet(tablet);
        tablet.reset();
      }

      SessionDataSet dataSet = session.executeQueryStatement("select * from root.sg1.d1");
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        List<Field> fields = rowRecord.getFields();
        assertEquals(fields.get(5).getBinaryV(), fields.get(6).getBinaryV());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void createTimeSeriesWithDoubleTicksTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      if (!System.getProperty("sun.jnu.encoding").contains("UTF-8")) {
        LOGGER.error("The system does not support UTF-8, so skip Chinese test...");
        session.close();
        return;
      }
      String storageGroup = "root.sg";
      session.setStorageGroup(storageGroup);

      session.createTimeseries(
          "root.sg.`my.device.with.colon:`.s",
          TSDataType.INT64,
          TSEncoding.RLE,
          CompressionType.SNAPPY);

      final SessionDataSet dataSet = session.executeQueryStatement("SHOW TIMESERIES");
      assertTrue(dataSet.hasNext());

      session.deleteStorageGroup(storageGroup);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void createWrongTimeSeriesTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      if (!System.getProperty("sun.jnu.encoding").contains("UTF-8")) {
        LOGGER.error("The system does not support UTF-8, so skip Chinese test...");
        session.close();
        return;
      }
      String storageGroup = "root.sg";
      session.setStorageGroup(storageGroup);

      try {
        session.createTimeseries(
            "root.sg.d1..s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        LOGGER.error("", e);
      }

      try {
        session.createTimeseries("", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        LOGGER.error("", e);
      }

      final SessionDataSet dataSet = session.executeQueryStatement("SHOW TIMESERIES");
      assertFalse(dataSet.hasNext());

      session.deleteStorageGroup(storageGroup);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({ClusterIT.class})
  public void deleteNonExistTimeSeriesTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.insertRecord(
          "root.sg1.d1", 0, Arrays.asList("t1", "t2", "t3"), Arrays.asList("123", "333", "444"));
      try {
        session.deleteTimeseries(
            Arrays.asList("root.sg1.d1.t6", "root.sg1.d1.t2", "root.sg1.d1.t3"));
      } catch (BatchExecutionException e) {
        assertTrue(e.getMessage().contains("Path [root.sg1.d1.t6] does not exist;"));
      }
      assertTrue(session.checkTimeseriesExists("root.sg1.d1.t1"));
      assertFalse(session.checkTimeseriesExists("root.sg1.d1.t2"));
      assertFalse(session.checkTimeseriesExists("root.sg1.d1.t3"));
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertOneDeviceRecordsTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      List<Long> times = new ArrayList<>();
      List<List<String>> measurements = new ArrayList<>();
      List<List<TSDataType>> datatypes = new ArrayList<>();
      List<List<Object>> values = new ArrayList<>();

      addLine(
          times,
          measurements,
          datatypes,
          values,
          3L,
          "s1",
          "s2",
          TSDataType.INT32,
          TSDataType.INT32,
          1,
          2);
      addLine(
          times,
          measurements,
          datatypes,
          values,
          2L,
          "s2",
          "s3",
          TSDataType.INT32,
          TSDataType.INT64,
          3,
          4L);
      addLine(
          times,
          measurements,
          datatypes,
          values,
          1L,
          "s4",
          "s5",
          TSDataType.FLOAT,
          TSDataType.BOOLEAN,
          5.0f,
          Boolean.TRUE);
      session.insertRecordsOfOneDevice("root.sg.d1", times, measurements, datatypes, values);
      checkResult(session);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void addLine(
      List<Long> times,
      List<List<String>> measurements,
      List<List<TSDataType>> datatypes,
      List<List<Object>> values,
      long time,
      String s1,
      String s2,
      TSDataType s1type,
      TSDataType s2type,
      Object value1,
      Object value2) {
    List<String> tmpMeasurements = new ArrayList<>();
    List<TSDataType> tmpDataTypes = new ArrayList<>();
    List<Object> tmpValues = new ArrayList<>();
    tmpMeasurements.add(s1);
    tmpMeasurements.add(s2);
    tmpDataTypes.add(s1type);
    tmpDataTypes.add(s2type);
    tmpValues.add(value1);
    tmpValues.add(value2);
    times.add(time);
    measurements.add(tmpMeasurements);
    datatypes.add(tmpDataTypes);
    values.add(tmpValues);
  }

  private void checkResult(ISession session)
      throws StatementExecutionException, IoTDBConnectionException {
    SessionDataSet dataSet = session.executeQueryStatement("select * from root.sg.d1");
    dataSet.getColumnNames();
    assertArrayEquals(
        dataSet.getColumnNames().toArray(new String[0]),
        new String[] {
          "Time",
          "root.sg.d1.s3",
          "root.sg.d1.s4",
          "root.sg.d1.s5",
          "root.sg.d1.s1",
          "root.sg.d1.s2"
        });
    assertArrayEquals(
        dataSet.getColumnTypes().toArray(new String[0]),
        new String[] {
          String.valueOf(TSDataType.INT64),
          String.valueOf(TSDataType.INT64),
          String.valueOf(TSDataType.FLOAT),
          String.valueOf(TSDataType.BOOLEAN),
          String.valueOf(TSDataType.INT32),
          String.valueOf(TSDataType.INT32)
        });
    long time = 1L;
    //
    assertTrue(dataSet.hasNext());
    RowRecord record = dataSet.next();
    assertEquals(time, record.getTimestamp());
    time++;
    assertNulls(record, new int[] {0, 3, 4});
    assertEquals(5.0f, record.getFields().get(1).getFloatV(), 0.01);
    assertEquals(Boolean.TRUE, record.getFields().get(2).getBoolV());

    assertTrue(dataSet.hasNext());
    record = dataSet.next();
    assertEquals(time, record.getTimestamp());
    time++;
    assertNulls(record, new int[] {1, 2, 3});
    assertEquals(4L, record.getFields().get(0).getLongV());
    assertEquals(3, record.getFields().get(4).getIntV());

    assertTrue(dataSet.hasNext());
    record = dataSet.next();
    assertEquals(time, record.getTimestamp());
    assertNulls(record, new int[] {0, 1, 2});
    assertEquals(1, record.getFields().get(3).getIntV());
    assertEquals(2, record.getFields().get(4).getIntV());

    assertFalse(dataSet.hasNext());
    dataSet.closeOperationHandle();
  }

  private void assertNulls(RowRecord record, int[] index) {
    for (int i : index) {
      assertNull(record.getFields().get(i).getDataType());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertOneDeviceRecordsWithOrderTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      List<Long> times = new ArrayList<>();
      List<List<String>> measurements = new ArrayList<>();
      List<List<TSDataType>> datatypes = new ArrayList<>();
      List<List<Object>> values = new ArrayList<>();

      addLine(
          times,
          measurements,
          datatypes,
          values,
          1L,
          "s4",
          "s5",
          TSDataType.FLOAT,
          TSDataType.BOOLEAN,
          5.0f,
          Boolean.TRUE);
      addLine(
          times,
          measurements,
          datatypes,
          values,
          2L,
          "s2",
          "s3",
          TSDataType.INT32,
          TSDataType.INT64,
          3,
          4L);
      addLine(
          times,
          measurements,
          datatypes,
          values,
          3L,
          "s1",
          "s2",
          TSDataType.INT32,
          TSDataType.INT32,
          1,
          2);

      session.insertRecordsOfOneDevice("root.sg.d1", times, measurements, datatypes, values, true);
      checkResult(session);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertOneDeviceRecordsWithIncorrectOrderTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      List<Long> times = new ArrayList<>();
      List<List<String>> measurements = new ArrayList<>();
      List<List<TSDataType>> datatypes = new ArrayList<>();
      List<List<Object>> values = new ArrayList<>();

      addLine(
          times,
          measurements,
          datatypes,
          values,
          2L,
          "s2",
          "s3",
          TSDataType.INT32,
          TSDataType.INT64,
          3,
          4L);
      addLine(
          times,
          measurements,
          datatypes,
          values,
          3L,
          "s1",
          "s2",
          TSDataType.INT32,
          TSDataType.INT32,
          1,
          2);
      addLine(
          times,
          measurements,
          datatypes,
          values,
          1L,
          "s4",
          "s5",
          TSDataType.FLOAT,
          TSDataType.BOOLEAN,
          5.0f,
          Boolean.TRUE);

      session.insertRecordsOfOneDevice("root.sg.d1", times, measurements, datatypes, values, true);
      checkResult(session);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertStringRecordsOfOneDeviceWithOrderTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      List<Long> times = new ArrayList<>();
      List<List<String>> measurements = new ArrayList<>();
      List<List<String>> values = new ArrayList<>();

      addStringLine(times, measurements, values, 1L, "s4", "s5", "5.0", "true");
      addStringLine(times, measurements, values, 2L, "s2", "s3", "3", "4");
      addStringLine(times, measurements, values, 3L, "s1", "s2", "false", "6");

      session.insertStringRecordsOfOneDevice("root.sg.d1", times, measurements, values, true);
      checkResultForInsertStringRecordsOfOneDevice(session);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void checkResultForInsertStringRecordsOfOneDevice(ISession session)
      throws StatementExecutionException, IoTDBConnectionException {
    SessionDataSet dataSet = session.executeQueryStatement("select * from root.sg.d1");
    dataSet.getColumnNames();
    assertArrayEquals(
        dataSet.getColumnNames().toArray(new String[0]),
        new String[] {
          "Time",
          "root.sg.d1.s3",
          "root.sg.d1.s4",
          "root.sg.d1.s5",
          "root.sg.d1.s1",
          "root.sg.d1.s2"
        });
    assertArrayEquals(
        dataSet.getColumnTypes().toArray(new String[0]),
        new String[] {
          String.valueOf(TSDataType.INT64),
          String.valueOf(TSDataType.FLOAT),
          String.valueOf(TSDataType.FLOAT),
          String.valueOf(TSDataType.BOOLEAN),
          String.valueOf(TSDataType.BOOLEAN),
          String.valueOf(TSDataType.FLOAT)
        });
    long time = 1L;

    assertTrue(dataSet.hasNext());
    RowRecord record = dataSet.next();
    assertEquals(time, record.getTimestamp());
    time++;

    assertNulls(record, new int[] {0, 3, 4});
    assertEquals(5.0f, record.getFields().get(1).getFloatV(), 0.01);
    assertEquals(Boolean.TRUE, record.getFields().get(2).getBoolV());

    assertTrue(dataSet.hasNext());
    record = dataSet.next();
    assertEquals(time, record.getTimestamp());
    time++;

    assertNulls(record, new int[] {1, 2, 3});
    assertEquals(4, record.getFields().get(0).getFloatV(), 0.01);
    assertEquals(3, record.getFields().get(4).getFloatV(), 0.01);

    assertTrue(dataSet.hasNext());
    record = dataSet.next();
    assertEquals(time, record.getTimestamp());

    assertNulls(record, new int[] {0, 1, 2});
    assertFalse(record.getFields().get(3).getBoolV());
    assertEquals(6, record.getFields().get(4).getFloatV(), 0.01);

    assertFalse(dataSet.hasNext());
    dataSet.closeOperationHandle();
  }

  private void addStringLine(
      List<Long> times,
      List<List<String>> measurements,
      List<List<String>> values,
      long time,
      String s1,
      String s2,
      String value1,
      String value2) {
    List<String> tmpMeasurements = new ArrayList<>();
    List<String> tmpValues = new ArrayList<>();
    tmpMeasurements.add(s1);
    tmpMeasurements.add(s2);
    tmpValues.add(value1);
    tmpValues.add(value2);
    times.add(time);
    measurements.add(tmpMeasurements);
    values.add(tmpValues);
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertStringRecordsOfOneDeviceWithIncorrectOrderTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      List<Long> times = new ArrayList<>();
      List<List<String>> measurements = new ArrayList<>();
      List<List<String>> values = new ArrayList<>();

      addStringLine(times, measurements, values, 2L, "s2", "s3", "3", "4");
      addStringLine(times, measurements, values, 1L, "s4", "s5", "5.0", "true");
      addStringLine(times, measurements, values, 3L, "s1", "s2", "false", "6");

      session.insertStringRecordsOfOneDevice("root.sg.d1", times, measurements, values);
      checkResultForInsertStringRecordsOfOneDevice(session);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertAlignedStringRecordsOfOneDeviceWithOrderTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      List<Long> times = new ArrayList<>();
      List<List<String>> measurements = new ArrayList<>();
      List<List<String>> values = new ArrayList<>();

      addStringLine(times, measurements, values, 1L, "s4", "s5", "5.0", "true");
      addStringLine(times, measurements, values, 2L, "s2", "s3", "3", "4");
      addStringLine(times, measurements, values, 3L, "s1", "s2", "false", "6");

      session.insertAlignedStringRecordsOfOneDevice(
          "root.sg.d1", times, measurements, values, true);
      checkResultForInsertStringRecordsOfOneDevice(session);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertAlignedStringRecordsOfOneDeviceWithIncorrectOrderTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      List<Long> times = new ArrayList<>();
      List<List<String>> measurements = new ArrayList<>();
      List<List<String>> values = new ArrayList<>();

      addStringLine(times, measurements, values, 2L, "s2", "s3", "3", "4");
      addStringLine(times, measurements, values, 1L, "s4", "s5", "5.0", "true");
      addStringLine(times, measurements, values, 3L, "s1", "s2", "false", "6");

      session.insertAlignedStringRecordsOfOneDevice("root.sg.d1", times, measurements, values);
      checkResultForInsertStringRecordsOfOneDevice(session);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertIllegalPathTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String msg = "[%s] Exception occurred: %s failed. %s is not a legal path";
      String deviceId = "root.sg..d1";
      List<String> deviceIds = Arrays.asList("root.sg..d1", "root.sg.d2");
      List<Long> timestamps = Arrays.asList(1L, 1L);
      List<String> measurements = Arrays.asList("s1", "s2", "s3");
      List<List<String>> allMeasurements = Arrays.asList(measurements, measurements);
      List<TSDataType> tsDataTypes =
          Arrays.asList(TSDataType.INT32, TSDataType.FLOAT, TSDataType.TEXT);
      List<List<TSDataType>> allTsDataTypes = Arrays.asList(tsDataTypes, tsDataTypes);
      List<TSEncoding> tsEncodings =
          Arrays.asList(TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN);
      List<CompressionType> compressionTypes =
          Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY, CompressionType.SNAPPY);
      List<Object> values = Arrays.asList(1, 2f, "3");
      List<List<Object>> allValues = Arrays.asList(values, values);
      List<String> stringValues = Arrays.asList("1", "2", "3");
      List<List<String>> allstringValues = Arrays.asList(stringValues, stringValues);

      try {
        session.insertRecords(deviceIds, timestamps, allMeasurements, allTsDataTypes, allValues);
        fail("Exception expected");
      } catch (StatementExecutionException e) {
        assertTrue(
            e.getMessage()
                .contains(
                    String.format(
                        msg, TSStatusCode.ILLEGAL_PATH, OperationType.INSERT_RECORDS, deviceId)));
      }

      try {
        session.insertRecords(deviceIds, Arrays.asList(2L, 2L), allMeasurements, allstringValues);
        fail("Exception expected");
      } catch (StatementExecutionException e) {
        assertTrue(
            e.getMessage()
                .contains(
                    String.format(
                        msg,
                        TSStatusCode.ILLEGAL_PATH,
                        OperationType.INSERT_STRING_RECORDS,
                        deviceIds.get(0))));
      }

      try {
        session.insertRecord(deviceId, 3L, measurements, tsDataTypes, values);
        fail("Exception expected");
      } catch (StatementExecutionException e) {
        assertTrue(
            e.getMessage()
                .contains(
                    String.format(
                        msg, TSStatusCode.ILLEGAL_PATH, OperationType.INSERT_RECORD, deviceId)));
      }

      try {
        session.insertRecord(deviceId, 4L, measurements, stringValues);
        fail("Exception expected");
      } catch (StatementExecutionException e) {
        assertTrue(
            e.getMessage()
                .contains(
                    String.format(
                        msg,
                        TSStatusCode.ILLEGAL_PATH,
                        OperationType.INSERT_STRING_RECORD,
                        deviceId)));
      }

      try {
        session.insertRecordsOfOneDevice(
            deviceId, Arrays.asList(5L, 6L), allMeasurements, allTsDataTypes, allValues);
        fail("Exception expected");
      } catch (StatementExecutionException e) {
        assertTrue(
            e.getMessage()
                .contains(
                    String.format(
                        msg,
                        TSStatusCode.ILLEGAL_PATH,
                        OperationType.INSERT_RECORDS_OF_ONE_DEVICE,
                        deviceId)));
      }

      try {
        session.deleteData(deviceId + ".s1", 6L);
        fail("Exception expected");
      } catch (StatementExecutionException e) {
        assertTrue(
            e.getMessage()
                .contains(
                    String.format(
                        msg,
                        TSStatusCode.ILLEGAL_PATH,
                        OperationType.DELETE_DATA,
                        deviceId + ".s1")));
      }

      try {
        Tablet tablet =
            new Tablet(
                deviceId,
                Arrays.asList(
                    new MeasurementSchema("s1", TSDataType.INT32),
                    new MeasurementSchema("s2", TSDataType.FLOAT)),
                5);
        long ts = 7L;
        for (long row = 0; row < 8; row++) {
          int rowIndex = tablet.rowSize++;
          tablet.addTimestamp(rowIndex, ts);
          tablet.addValue("s1", rowIndex, 1);
          tablet.addValue("s2", rowIndex, 1.0F);
          if (tablet.rowSize == tablet.getMaxRowNumber()) {
            session.insertTablet(tablet, true);
            tablet.reset();
          }
          ts++;
        }
        fail("Exception expected");
      } catch (StatementExecutionException e) {
        assertTrue(
            e.getMessage()
                .contains(
                    String.format(
                        msg, TSStatusCode.ILLEGAL_PATH, OperationType.INSERT_TABLET, deviceId)));
      }

      try {
        Tablet tablet1 =
            new Tablet(
                deviceId,
                Arrays.asList(
                    new MeasurementSchema("s1", TSDataType.INT32),
                    new MeasurementSchema("s2", TSDataType.FLOAT)),
                5);
        Tablet tablet2 =
            new Tablet(
                "root.sg.d2",
                Arrays.asList(
                    new MeasurementSchema("s1", TSDataType.INT32),
                    new MeasurementSchema("s2", TSDataType.FLOAT)),
                5);
        HashMap<String, Tablet> tablets = new HashMap<>();
        tablets.put(deviceId, tablet1);
        tablets.put("root.sg.d2", tablet2);
        long ts = 16L;
        for (long row = 0; row < 8; row++) {
          int row1 = tablet1.rowSize++;
          int row2 = tablet2.rowSize++;
          tablet1.addTimestamp(row1, ts);
          tablet2.addTimestamp(row2, ts);
          tablet1.addValue("s1", row1, 1);
          tablet1.addValue("s2", row1, 1.0F);
          tablet2.addValue("s1", row2, 1);
          tablet2.addValue("s2", row2, 1.0F);
          if (tablet1.rowSize == tablet1.getMaxRowNumber()) {
            session.insertTablets(tablets, true);
            tablet1.reset();
            tablet2.reset();
          }
          ts++;
        }
        fail("Exception expected");
      } catch (StatementExecutionException e) {
        assertTrue(
            e.getMessage()
                .contains(
                    String.format(
                        msg, TSStatusCode.ILLEGAL_PATH, OperationType.INSERT_TABLETS, deviceId)));
      }

      try {
        session.setStorageGroup("root..sg");
        fail("Exception expected");
      } catch (StatementExecutionException e) {
        assertTrue(
            e.getMessage()
                .contains(
                    String.format(
                        msg,
                        TSStatusCode.ILLEGAL_PATH,
                        OperationType.SET_STORAGE_GROUP,
                        "root..sg")));
      }

      try {
        session.createTimeseries(
            "root.sg..d1.s1", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY);
        fail("Exception expected");
      } catch (StatementExecutionException e) {
        assertTrue(
            e.getMessage()
                .contains(
                    String.format(
                        msg,
                        TSStatusCode.ILLEGAL_PATH,
                        OperationType.CREATE_TIMESERIES,
                        "root.sg..d1.s1")));
      }

      try {
        session.createAlignedTimeseries(
            deviceId,
            measurements,
            tsDataTypes,
            tsEncodings,
            compressionTypes,
            Arrays.asList("alias1", "alias2", "alias3"),
            null,
            null);

        fail("Exception expected");
      } catch (StatementExecutionException e) {
        assertTrue(
            e.getMessage()
                .contains(
                    String.format(
                        msg,
                        TSStatusCode.ILLEGAL_PATH,
                        OperationType.CREATE_ALIGNED_TIMESERIES,
                        deviceId)));
      }

      try {
        session.createMultiTimeseries(
            Arrays.asList("root.sg.d1..s1", "root.sg.d1.s2", "root.sg.d1.s3"),
            tsDataTypes,
            tsEncodings,
            compressionTypes,
            null,
            null,
            null,
            null);
        fail("Exception expected");
      } catch (StatementExecutionException e) {
        assertTrue(
            e.getMessage()
                .contains(
                    String.format(
                        msg,
                        TSStatusCode.ILLEGAL_PATH,
                        OperationType.CREATE_MULTI_TIMESERIES,
                        "root.sg.d1..s1")));
      }

      try {
        session.deleteTimeseries("root.sg.d1..s1");
        fail("Exception expected");
      } catch (StatementExecutionException e) {
        assertTrue(
            e.getMessage()
                .contains(
                    String.format(
                        msg,
                        TSStatusCode.ILLEGAL_PATH,
                        OperationType.DELETE_TIMESERIES,
                        "root.sg.d1..s1")));
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void conversionFunctionTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      // prepare data
      String deviceId = "root.sg.d1";
      List<Long> times = new ArrayList<>();
      List<List<String>> measurementsList = new ArrayList<>();
      List<List<TSDataType>> typesList = new ArrayList<>();
      List<List<Object>> valuesList = new ArrayList<>();

      for (int i = 0; i <= 4; i++) {
        times.add((long) i * 2);
        measurementsList.add(Arrays.asList("s1", "s2", "s3", "s4", "s5", "s6"));
        typesList.add(
            Arrays.asList(
                TSDataType.INT32,
                TSDataType.INT64,
                TSDataType.FLOAT,
                TSDataType.DOUBLE,
                TSDataType.BOOLEAN,
                TSDataType.TEXT));
        valuesList.add(
            Arrays.asList(
                i,
                (long) i + 1,
                (float) i,
                (double) i * 2,
                new boolean[] {true, false}[i % 2],
                String.valueOf(i)));
      }
      // insert data
      session.insertRecordsOfOneDevice(deviceId, times, measurementsList, typesList, valuesList);

      // excepted
      String[] targetTypes = {"INT64", "INT32", "DOUBLE", "FLOAT", "TEXT"};
      String[] excepted = {
        "0\t0\t1\t0.0\t0.0\ttrue",
        "2\t1\t2\t1.0\t2.0\tfalse",
        "4\t2\t3\t2.0\t4.0\ttrue",
        "6\t3\t4\t3.0\t6.0\tfalse",
        "8\t4\t5\t4.0\t8.0\ttrue"
      };

      // query
      String[] casts = new String[targetTypes.length];
      StringBuffer buffer = new StringBuffer();
      buffer.append("select ");
      for (int i = 0; i < targetTypes.length; i++) {
        casts[i] = String.format("cast(s%s, 'type'='%s')", i + 1, targetTypes[i]);
      }
      buffer.append(StringUtils.join(casts, ","));
      buffer.append(" from root.sg.d1");
      String sql = buffer.toString();
      SessionDataSet sessionDataSet = session.executeQueryStatement(sql);

      // compare types
      List<String> columnTypes = sessionDataSet.getColumnTypes();
      columnTypes.remove(0);
      for (int i = 0; i < columnTypes.size(); i++) {
        assertEquals(targetTypes[i], columnTypes.get(i));
      }

      // compare results
      int index = 0;
      while (sessionDataSet.hasNext()) {
        RowRecord next = sessionDataSet.next();
        assertEquals(excepted[index], next.toString());
        index++;
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertPartialTablet2Test() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      if (!session.checkTimeseriesExists("root.sg.d.s1")) {
        session.createTimeseries(
            "root.sg.d.s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
      }
      if (!session.checkTimeseriesExists("root.sg.d.s2")) {
        session.createTimeseries(
            "root.sg.d.s2", TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY);
      }
      if (!session.checkTimeseriesExists("root.sg.d.s3")) {
        session.createTimeseries(
            "root.sg.d.s3", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
      }
      List<MeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
      schemaList.add(new MeasurementSchema("s2", TSDataType.DOUBLE));
      schemaList.add(new MeasurementSchema("s3", TSDataType.TEXT));

      Tablet tablet = new Tablet("root.sg.d", schemaList, 10);

      long timestamp = System.currentTimeMillis();

      for (long row = 0; row < 15; row++) {
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, timestamp);
        tablet.addValue("s1", rowIndex, 1L);
        tablet.addValue("s2", rowIndex, 1D);
        tablet.addValue("s3", rowIndex, new Binary("1"));
        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          try {
            session.insertTablet(tablet, true);
          } catch (StatementExecutionException e) {
            assertTrue(e.getMessage().contains("insert measurements [s3] caused by"));
            assertTrue(e.getMessage().contains("data type of root.sg.d.s3 is not consistent"));
          }
          tablet.reset();
        }
        timestamp++;
      }

      if (tablet.rowSize != 0) {
        try {
          session.insertTablet(tablet);
        } catch (StatementExecutionException e) {
          assertTrue(e.getMessage().contains("insert measurements [s3] caused by"));
          assertTrue(e.getMessage().contains("data type of root.sg.d.s3 is not consistent"));
        }
        tablet.reset();
      }

      SessionDataSet dataSet =
          session.executeQueryStatement("select count(s1), count(s2), count(s3) from root.sg.d");
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(15L, rowRecord.getFields().get(0).getLongV());
        assertEquals(15L, rowRecord.getFields().get(1).getLongV());
        assertEquals(0L, rowRecord.getFields().get(2).getLongV());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertBinaryAsTextTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      // prepare binary data
      List<Object> bytesData = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
        byte[] bytes = new byte[128];
        for (int j = 0; j < 128; j++) {
          bytes[j] = Byte.valueOf("" + (j - i), 10);
        }
        bytesData.add(bytes);
      }
      // insert data using insertRecord
      for (int i = 0; i < bytesData.size(); i++) {
        byte[] data = (byte[]) bytesData.get(i);
        Binary dataBinary = new Binary(data);
        session.insertRecord(
            "root.sg1.d1",
            i,
            Collections.singletonList("s0"),
            Collections.singletonList(TSDataType.TEXT),
            dataBinary);
      }
      // insert data using insertTablet
      List<MeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("s1", TSDataType.TEXT));
      Tablet tablet = new Tablet("root.sg1.d1", schemaList, 100);
      for (int i = 0; i < bytesData.size(); i++) {
        byte[] data = (byte[]) bytesData.get(i);
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, i);
        Binary dataBinary = new Binary(data);
        tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, dataBinary);
      }
      session.insertTablet(tablet);
      // check result
      SessionDataSet dataSet = session.executeQueryStatement("select ** from root.sg1.d1");
      assertArrayEquals(
          dataSet.getColumnNames().toArray(new String[0]),
          new String[] {"Time", "root.sg1.d1.s0", "root.sg1.d1.s1"});
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        for (int i = 0; i < 2; i++) {
          // get Binary value from SessionDataSet
          byte[] actualBytes = rowRecord.getFields().get(i).getBinaryV().getValues();
          // compare Binary value to origin bytesData
          byte[] expectedBytes = (byte[]) bytesData.get((int) rowRecord.getTimestamp());
          assertArrayEquals(expectedBytes, actualBytes);
        }
      }
      dataSet.closeOperationHandle();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void illegalDatabaseNameTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.createDatabase("root.db");
      try {
        session.createDatabase("");
        fail();
      } catch (StatementExecutionException e) {
        Assert.assertTrue(e.getMessage().contains(" is not a legal path"));
      }

      try {
        session.deleteDatabases(Arrays.asList("root.db", ""));
        fail();
      } catch (StatementExecutionException e) {
        Assert.assertTrue(e.getMessage().contains(" is not a legal path"));
      }

      session.deleteDatabase("root.db");

      final SessionDataSet dataSet = session.executeQueryStatement("SHOW DATABASES");
      assertFalse(dataSet.hasNext());

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
