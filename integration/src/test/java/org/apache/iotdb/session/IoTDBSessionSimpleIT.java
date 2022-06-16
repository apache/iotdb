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
package org.apache.iotdb.session;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.OperationType;
import org.apache.iotdb.db.metadata.LocalSchemaProcessor;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.rpc.BatchExecutionException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.template.InternalNode;
import org.apache.iotdb.session.template.MeasurementNode;
import org.apache.iotdb.session.template.Template;
import org.apache.iotdb.session.template.TemplateNode;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({LocalStandaloneTest.class})
public class IoTDBSessionSimpleIT {

  private static Logger logger = LoggerFactory.getLogger(IoTDBSessionSimpleIT.class);

  private Session session;

  @Before
  public void setUp() {
    System.setProperty(IoTDBConstant.IOTDB_CONF, "src/test/resources/");
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    session.close();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testInsertPartialTablet()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

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
      Assert.assertEquals(15L, rowRecord.getFields().get(0).getLongV());
      Assert.assertEquals(15L, rowRecord.getFields().get(1).getLongV());
      Assert.assertEquals(15L, rowRecord.getFields().get(2).getLongV());
    }
    session.close();
  }

  @Test
  public void testInsertByStrAndInferType()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

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

    Assert.assertEquals(expected, actual);

    session.close();
  }

  @Test
  public void testInsertWrongPathByStrAndInferType()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

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
      logger.error("", e);
    }

    SessionDataSet dataSet = session.executeQueryStatement("show timeseries root");
    Assert.assertFalse(dataSet.hasNext());

    session.close();
  }

  @Test
  public void testInsertIntoIllegalTimeseries()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

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
      logger.error("", e);
    }

    SessionDataSet dataSet = session.executeQueryStatement("show timeseries root");
    Assert.assertFalse(dataSet.hasNext());

    session.close();
  }

  @Test
  public void testInsertByObjAndNotInferType()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

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

    Assert.assertEquals(expected, actual);

    session.close();
  }

  @Test
  public void testCreateMultiTimeseries()
      throws IoTDBConnectionException, StatementExecutionException, MetadataException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

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
    IMeasurementMNode mNode =
        LocalSchemaProcessor.getInstance().getMeasurementMNode(new PartialPath("root.sg1.d1.s1"));
    assertNull(mNode.getSchema().getProps());

    session.close();
  }

  @Test
  public void testChineseCharacter() throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    if (!System.getProperty("sun.jnu.encoding").contains("UTF-8")) {
      logger.error("The system does not support UTF-8, so skip Chinese test...");
      session.close();
      return;
    }
    String storageGroup = "root.存储组1";
    String[] devices = new String[] {"设备1.指标1", "设备1.s2", "d2.s1", "d2.指标2"};
    session.setStorageGroup(storageGroup);
    for (String path : devices) {
      String fullPath = storageGroup + TsFileConstant.PATH_SEPARATOR + path;
      session.createTimeseries(fullPath, TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
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
    Assert.assertEquals(10, count);
    session.deleteStorageGroup(storageGroup);
    session.close();
  }

  @Test
  public void testInsertTabletWithAlignedTimeseries()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
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
      tablet.addValue(schemaList.get(1).getMeasurementId(), rowIndex, new SecureRandom().nextInt());
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
      Assert.assertEquals(10L, rowRecord.getFields().get(0).getLongV());
      Assert.assertEquals(10L, rowRecord.getFields().get(1).getLongV());
      Assert.assertEquals(10L, rowRecord.getFields().get(2).getLongV());
    }
    session.close();
  }

  @Test
  public void testInsertTabletWithNullValues()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
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
      Assert.assertEquals(6L, rowRecord.getFields().size());
      for (Field field : rowRecord.getFields()) {
        Assert.assertEquals(9L, field.getLongV());
      }
    }
    session.close();
  }

  @Test
  public void testInsertTabletWithStringValues()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
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
      Assert.assertEquals(fields.get(5).getBinaryV(), fields.get(6).getBinaryV());
    }
    session.close();
  }

  @Test
  public void createTimeSeriesWithDoubleTicks()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    if (!System.getProperty("sun.jnu.encoding").contains("UTF-8")) {
      logger.error("The system does not support UTF-8, so skip Chinese test...");
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
    session.close();
  }

  @Test
  public void createWrongTimeSeries() throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    if (!System.getProperty("sun.jnu.encoding").contains("UTF-8")) {
      logger.error("The system does not support UTF-8, so skip Chinese test...");
      session.close();
      return;
    }
    String storageGroup = "root.sg";
    session.setStorageGroup(storageGroup);

    try {
      session.createTimeseries(
          "root.sg.d1..s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      logger.error("", e);
    }

    try {
      session.createTimeseries("", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      logger.error("", e);
    }

    final SessionDataSet dataSet = session.executeQueryStatement("SHOW TIMESERIES");
    assertFalse(dataSet.hasNext());

    session.deleteStorageGroup(storageGroup);
    session.close();
  }

  @Test
  public void testDeleteNonExistTimeSeries()
      throws StatementExecutionException, IoTDBConnectionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    session.insertRecord(
        "root.sg1.d1", 0, Arrays.asList("t1", "t2", "t3"), Arrays.asList("123", "333", "444"));
    try {
      session.deleteTimeseries(Arrays.asList("root.sg1.d1.t6", "root.sg1.d1.t2", "root.sg1.d1.t3"));
    } catch (BatchExecutionException e) {
      assertTrue(e.getMessage().contains("Path [root.sg1.d1.t6] does not exist;"));
    }
    assertTrue(session.checkTimeseriesExists("root.sg1.d1.t1"));
    assertFalse(session.checkTimeseriesExists("root.sg1.d1.t2"));
    assertFalse(session.checkTimeseriesExists("root.sg1.d1.t3"));

    session.close();
  }

  @Test
  public void testInsertOneDeviceRecords()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
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
    session.close();
  }

  @Test
  public void testFillAll() throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    List<String> paths = new ArrayList<>();
    paths.add("root.sg.d.s1");
    paths.add("root.sg.d.s2");
    paths.add("root.sg.d.s3");
    paths.add("root.sg.d.s4");
    paths.add("root.sg.d.s5");
    paths.add("root.sg.d.s6");
    List<TSDataType> tsDataTypes = new ArrayList<>();
    tsDataTypes.add(TSDataType.BOOLEAN);
    tsDataTypes.add(TSDataType.INT32);
    tsDataTypes.add(TSDataType.INT64);
    tsDataTypes.add(TSDataType.FLOAT);
    tsDataTypes.add(TSDataType.DOUBLE);
    tsDataTypes.add(TSDataType.TEXT);
    List<TSEncoding> tsEncodings = new ArrayList<>();
    tsEncodings.add(TSEncoding.RLE);
    tsEncodings.add(TSEncoding.RLE);
    tsEncodings.add(TSEncoding.RLE);
    tsEncodings.add(TSEncoding.RLE);
    tsEncodings.add(TSEncoding.RLE);
    tsEncodings.add(TSEncoding.PLAIN);
    List<CompressionType> compressionTypes = new ArrayList<>();
    compressionTypes.add(CompressionType.SNAPPY);
    compressionTypes.add(CompressionType.SNAPPY);
    compressionTypes.add(CompressionType.SNAPPY);
    compressionTypes.add(CompressionType.SNAPPY);
    compressionTypes.add(CompressionType.SNAPPY);
    compressionTypes.add(CompressionType.SNAPPY);
    session.createMultiTimeseries(
        paths, tsDataTypes, tsEncodings, compressionTypes, null, null, null, null);

    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    measurements.add("s4");
    measurements.add("s5");
    measurements.add("s6");
    List<Object> values = new ArrayList<>();
    values.add(false);
    values.add(1);
    values.add((long) 1);
    values.add((float) 1.0);
    values.add(1.0);
    values.add("1");
    session.insertRecord("root.sg.d", 1, measurements, tsDataTypes, values);

    SessionDataSet dataSet =
        session.executeQueryStatement(
            "select * from root.sg.d where time=70 fill(all[previous, 1m])");
    RowRecord record = dataSet.next();
    assertEquals(70, record.getTimestamp());
    for (Field field : record.getFields()) {
      switch (field.getDataType()) {
        case TEXT:
          break;
        case FLOAT:
          assertEquals(1.0, field.getFloatV(), 0);
          break;
        case INT32:
          assertEquals(1, field.getIntV());
          break;
        case INT64:
          assertEquals(1, field.getLongV());
          break;
        case DOUBLE:
          assertEquals(1.0, field.getDoubleV(), 0);
          break;
        case BOOLEAN:
          assertEquals(false, field.getBoolV());
          break;
      }
    }
    session.close();
  }

  @Test
  public void testInsertOneDeviceRecordsWithOrder()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
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
    session.close();
  }

  @Test
  public void testInsertOneDeviceRecordsWithIncorrectOrder()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
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
    session.close();
  }

  @Test
  public void testInsertStringRecordsOfOneDeviceWithOrder()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    List<Long> times = new ArrayList<>();
    List<List<String>> measurements = new ArrayList<>();
    List<List<String>> values = new ArrayList<>();

    addStringLine(times, measurements, values, 1L, "s4", "s5", "5.0", "true");
    addStringLine(times, measurements, values, 2L, "s2", "s3", "3", "4");
    addStringLine(times, measurements, values, 3L, "s1", "s2", "false", "6");

    session.insertStringRecordsOfOneDevice("root.sg.d1", times, measurements, values, true);
    checkResultForInsertStringRecordsOfOneDevice(session);
    session.close();
  }

  @Test
  public void testInsertStringRecordsOfOneDeviceWithIncorrectOrder()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    List<Long> times = new ArrayList<>();
    List<List<String>> measurements = new ArrayList<>();
    List<List<String>> values = new ArrayList<>();

    addStringLine(times, measurements, values, 2L, "s2", "s3", "3", "4");
    addStringLine(times, measurements, values, 1L, "s4", "s5", "5.0", "true");
    addStringLine(times, measurements, values, 3L, "s1", "s2", "false", "6");

    session.insertStringRecordsOfOneDevice("root.sg.d1", times, measurements, values);
    checkResultForInsertStringRecordsOfOneDevice(session);
    session.close();
  }

  @Test
  public void testInsertAlignedStringRecordsOfOneDeviceWithOrder()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    List<Long> times = new ArrayList<>();
    List<List<String>> measurements = new ArrayList<>();
    List<List<String>> values = new ArrayList<>();

    addStringLine(times, measurements, values, 1L, "s4", "s5", "5.0", "true");
    addStringLine(times, measurements, values, 2L, "s2", "s3", "3", "4");
    addStringLine(times, measurements, values, 3L, "s1", "s2", "false", "6");

    session.insertAlignedStringRecordsOfOneDevice("root.sg.d1", times, measurements, values, true);
    checkResultForInsertStringRecordsOfOneDevice(session);
    session.close();
  }

  @Test
  public void testInsertAlignedStringRecordsOfOneDeviceWithIncorrectOrder()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    List<Long> times = new ArrayList<>();
    List<List<String>> measurements = new ArrayList<>();
    List<List<String>> values = new ArrayList<>();

    addStringLine(times, measurements, values, 2L, "s2", "s3", "3", "4");
    addStringLine(times, measurements, values, 1L, "s4", "s5", "5.0", "true");
    addStringLine(times, measurements, values, 3L, "s1", "s2", "false", "6");

    session.insertAlignedStringRecordsOfOneDevice("root.sg.d1", times, measurements, values);
    checkResultForInsertStringRecordsOfOneDevice(session);
    session.close();
  }

  @Test
  public void testInsertIlligalPath() throws IoTDBConnectionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

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
                      msg, TSStatusCode.PATH_ILLEGAL, OperationType.INSERT_RECORDS, deviceId)));
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
                      TSStatusCode.PATH_ILLEGAL,
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
                      msg, TSStatusCode.PATH_ILLEGAL, OperationType.INSERT_RECORD, deviceId)));
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
                      TSStatusCode.PATH_ILLEGAL,
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
                      TSStatusCode.PATH_ILLEGAL,
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
                      TSStatusCode.PATH_ILLEGAL,
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
                      msg, TSStatusCode.PATH_ILLEGAL, OperationType.INSERT_TABLET, deviceId)));
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
                      msg, TSStatusCode.PATH_ILLEGAL, OperationType.INSERT_TABLETS, deviceId)));
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
                      TSStatusCode.PATH_ILLEGAL,
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
                      TSStatusCode.PATH_ILLEGAL,
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
                      TSStatusCode.PATH_ILLEGAL,
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
                      TSStatusCode.PATH_ILLEGAL,
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
                      TSStatusCode.PATH_ILLEGAL,
                      OperationType.DELETE_TIMESERIES,
                      "root.sg.d1..s1")));
    }

    try {
      session.deleteStorageGroup("root..sg");
      fail("Exception expected");
    } catch (StatementExecutionException e) {
      assertTrue(
          e.getMessage()
              .contains(
                  String.format(
                      msg,
                      TSStatusCode.PATH_ILLEGAL,
                      OperationType.DELETE_STORAGE_GROUPS,
                      "root..sg")));
    }

    session.close();
  }

  @Test
  public void testConversionFunction()
      throws IoTDBConnectionException, StatementExecutionException {
    // connect
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
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
    String[] targetTypes = {"INT64", "INT32", "DOUBLE", "FLOAT", "TEXT", "BOOLEAN"};
    String[] excepted = {
      "0\t0\t1\t0.0\t0.0\ttrue\ttrue",
      "2\t1\t2\t1.0\t2.0\tfalse\ttrue",
      "4\t2\t3\t2.0\t4.0\ttrue\ttrue",
      "6\t3\t4\t3.0\t6.0\tfalse\ttrue",
      "8\t4\t5\t4.0\t8.0\ttrue\ttrue"
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
    session.close();
  }

  @Test
  public void testInsertDeleteWithTemplate()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    initTreeTemplate("root.sg.loc1");
    List<String> measurements = new ArrayList<>();
    List<String> values = new ArrayList<>();
    String deviceId = "root.sg.loc1.sector.GPS";
    Set<String> checkSet = new HashSet<>();
    SessionDataSet dataSet;

    // insert record set using template

    measurements.add("x");
    measurements.add("y");
    values.add("1.0");
    values.add("2.0");

    checkSet.add("root.sg.loc1.sector.GPS.x");
    checkSet.add("root.sg.loc1.sector.GPS.y");
    checkSet.add("root.sg.loc1.sector.y");
    checkSet.add("root.sg.loc1.sector.x");
    checkSet.add("root.sg.loc1.sector.vehicle.x");
    checkSet.add("root.sg.loc1.sector.vehicle.y");
    checkSet.add("root.sg.loc1.sector.vehicle.GPS.x");
    checkSet.add("root.sg.loc1.sector.vehicle.GPS.y");

    session.insertRecord(deviceId, 1L, measurements, values);
    dataSet = session.executeQueryStatement("show timeseries");
    while (dataSet.hasNext()) {
      checkSet.remove(dataSet.next().getFields().get(0).toString());
    }
    assertTrue(checkSet.isEmpty());

    // insert aligned under unaligned node
    try {
      session.insertAlignedRecord("root.sg.loc1.sector.GPS", 3L, measurements, values);
    } catch (StatementExecutionException e) {
      assertEquals(
          "303: Timeseries under path [root.sg.loc1.sector.GPS] is not aligned , please set InsertPlan.isAligned() = false",
          e.getMessage());
    }

    // insert overlap unmatched series
    measurements.set(1, "speed");
    try {
      session.insertRecord(deviceId, 5L, measurements, values);
      fail();
    } catch (StatementExecutionException e) {
      assertEquals(
          "327: Path [root.sg.loc1.sector.GPS.speed] overlaps with [treeTemplate] on [GPS]",
          e.getMessage());
    }

    // insert tablets
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("x", TSDataType.FLOAT));
    schemaList.add(new MeasurementSchema("y", TSDataType.FLOAT));
    Tablet tablet = new Tablet("root.sg.loc1.sector", schemaList);

    long timestamp = System.currentTimeMillis();

    for (long row = 0; row < 10; row++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue(
          schemaList.get(0).getMeasurementId(), rowIndex, new SecureRandom().nextFloat());
      tablet.addValue(
          schemaList.get(1).getMeasurementId(), rowIndex, new SecureRandom().nextFloat());
      timestamp++;
    }

    if (tablet.rowSize != 0) {
      session.insertAlignedTablet(tablet);
      tablet.reset();
    }

    dataSet = session.executeQueryStatement("select count(*) from root");

    while (dataSet.hasNext()) {
      RowRecord rowRecord = dataSet.next();
      Assert.assertEquals(10L, rowRecord.getFields().get(0).getLongV());
      Assert.assertEquals(10L, rowRecord.getFields().get(1).getLongV());
    }

    // delete series inside template
    try {
      session.deleteTimeseries("root.sg.loc1.sector.x");
      fail();
    } catch (StatementExecutionException e) {
      assertTrue(
          e.getMessage()
              .contains("Cannot delete a timeseries inside a template: root.sg.loc1.sector.x;"));
    }

    session.close();
  }

  @Test
  public void testCountWithTemplate()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    initTreeTemplate("root.sg.loc");

    List<String> measurements = new ArrayList<>();
    List<String> values = new ArrayList<>();
    Set<String> checkSet = new HashSet<>();
    SessionDataSet dataSet;

    // invoke template template

    measurements.add("x");
    measurements.add("y");
    values.add("1.0");
    values.add("2.0");

    session.insertAlignedRecord("root.sg.loc.area", 1L, measurements, values);
    session.insertAlignedRecord("root.sg.loc", 1L, measurements, values);

    dataSet = session.executeQueryStatement("show timeseries");

    checkSet.add("root.sg.loc.x");
    checkSet.add("root.sg.loc.y");
    checkSet.add("root.sg.loc.GPS.x");
    checkSet.add("root.sg.loc.GPS.y");
    checkSet.add("root.sg.loc.vehicle.GPS.x");
    checkSet.add("root.sg.loc.vehicle.GPS.y");
    checkSet.add("root.sg.loc.vehicle.x");
    checkSet.add("root.sg.loc.vehicle.x");

    checkSet.add("root.sg.loc.area.x");
    checkSet.add("root.sg.loc.area.y");
    checkSet.add("root.sg.loc.area.GPS.x");
    checkSet.add("root.sg.loc.area.GPS.y");
    checkSet.add("root.sg.loc.area.vehicle.GPS.x");
    checkSet.add("root.sg.loc.area.vehicle.GPS.y");
    checkSet.add("root.sg.loc.area.vehicle.x");
    checkSet.add("root.sg.loc.area.vehicle.x");

    while (dataSet.hasNext()) {
      checkSet.remove(dataSet.next().getFields().get(0).toString());
    }

    assertTrue(checkSet.isEmpty());

    dataSet = session.executeQueryStatement("show devices");

    checkSet.add("root.sg.loc,true");
    checkSet.add("root.sg.loc.GPS,false");
    checkSet.add("root.sg.loc.vehicle,true");
    checkSet.add("root.sg.loc.vehicle.GPS,false");
    checkSet.add("root.sg.loc.area,true");
    checkSet.add("root.sg.loc.area.GPS,false");
    checkSet.add("root.sg.loc.area.vehicle,true");
    checkSet.add("root.sg.loc.area.vehicle.GPS,false");

    while (dataSet.hasNext()) {
      List<Field> fields = dataSet.next().getFields();
      checkSet.remove(fields.get(0).toString() + "," + fields.get(1).toString());
    }

    assertTrue(checkSet.isEmpty());
  }

  @Test
  public void testInsertPartialTablet2()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

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
          Assert.assertEquals(313, e.getStatusCode());
        }
        tablet.reset();
      }
      timestamp++;
    }

    if (tablet.rowSize != 0) {
      try {
        session.insertTablet(tablet);
      } catch (StatementExecutionException e) {
        Assert.assertEquals(313, e.getStatusCode());
      }
      tablet.reset();
    }

    SessionDataSet dataSet =
        session.executeQueryStatement("select count(s1), count(s2), count(s3) from root.sg.d");
    while (dataSet.hasNext()) {
      RowRecord rowRecord = dataSet.next();
      Assert.assertEquals(15L, rowRecord.getFields().get(0).getLongV());
      Assert.assertEquals(15L, rowRecord.getFields().get(1).getLongV());
      Assert.assertEquals(0L, rowRecord.getFields().get(2).getLongV());
    }
    session.close();
  }

  private void initTreeTemplate(String path)
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    Template sessionTemplate = new Template("treeTemplate", true);
    TemplateNode iNodeGPS = new InternalNode("GPS", false);
    TemplateNode iNodeV = new InternalNode("vehicle", true);
    TemplateNode mNodeX =
        new MeasurementNode("x", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);
    TemplateNode mNodeY =
        new MeasurementNode("y", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);
    ByteArrayOutputStream stream = new ByteArrayOutputStream();

    iNodeGPS.addChild(mNodeX);
    iNodeGPS.addChild(mNodeY);

    iNodeV.addChild(mNodeX);
    iNodeV.addChild(mNodeY);
    iNodeV.addChild(iNodeGPS);

    sessionTemplate.addToTemplate(iNodeGPS);
    sessionTemplate.addToTemplate(iNodeV);
    sessionTemplate.addToTemplate(mNodeX);
    sessionTemplate.addToTemplate(mNodeY);

    session.createSchemaTemplate(sessionTemplate);
    session.setSchemaTemplate("treeTemplate", path);
  }

  private void checkResult(Session session)
      throws StatementExecutionException, IoTDBConnectionException {
    SessionDataSet dataSet = session.executeQueryStatement("select * from root.sg.d1");
    dataSet.getColumnNames();
    Assert.assertArrayEquals(
        dataSet.getColumnNames().toArray(new String[0]),
        new String[] {
          "Time",
          "root.sg.d1.s3",
          "root.sg.d1.s4",
          "root.sg.d1.s5",
          "root.sg.d1.s1",
          "root.sg.d1.s2"
        });
    Assert.assertArrayEquals(
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
    Assert.assertTrue(dataSet.hasNext());
    RowRecord record = dataSet.next();
    Assert.assertEquals(time, record.getTimestamp());
    time++;
    assertNulls(record, new int[] {0, 3, 4});
    Assert.assertEquals(5.0f, record.getFields().get(1).getFloatV(), 0.01);
    Assert.assertEquals(Boolean.TRUE, record.getFields().get(2).getBoolV());

    Assert.assertTrue(dataSet.hasNext());
    record = dataSet.next();
    Assert.assertEquals(time, record.getTimestamp());
    time++;
    assertNulls(record, new int[] {1, 2, 3});
    Assert.assertEquals(4L, record.getFields().get(0).getLongV());
    Assert.assertEquals(3, record.getFields().get(4).getIntV());

    Assert.assertTrue(dataSet.hasNext());
    record = dataSet.next();
    Assert.assertEquals(time, record.getTimestamp());
    time++;
    assertNulls(record, new int[] {0, 1, 2});
    Assert.assertEquals(1, record.getFields().get(3).getIntV());
    Assert.assertEquals(2, record.getFields().get(4).getIntV());

    Assert.assertFalse(dataSet.hasNext());
    dataSet.closeOperationHandle();
  }

  private void checkResultForInsertStringRecordsOfOneDevice(Session session)
      throws StatementExecutionException, IoTDBConnectionException {
    SessionDataSet dataSet = session.executeQueryStatement("select * from root.sg.d1");
    dataSet.getColumnNames();
    Assert.assertArrayEquals(
        dataSet.getColumnNames().toArray(new String[0]),
        new String[] {
          "Time",
          "root.sg.d1.s3",
          "root.sg.d1.s4",
          "root.sg.d1.s5",
          "root.sg.d1.s1",
          "root.sg.d1.s2"
        });
    Assert.assertArrayEquals(
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

    Assert.assertTrue(dataSet.hasNext());
    RowRecord record = dataSet.next();
    Assert.assertEquals(time, record.getTimestamp());
    time++;

    assertNulls(record, new int[] {0, 3, 4});
    Assert.assertEquals(5.0f, record.getFields().get(1).getFloatV(), 0.01);
    Assert.assertEquals(Boolean.TRUE, record.getFields().get(2).getBoolV());

    Assert.assertTrue(dataSet.hasNext());
    record = dataSet.next();
    Assert.assertEquals(time, record.getTimestamp());
    time++;

    assertNulls(record, new int[] {1, 2, 3});
    Assert.assertEquals(4, record.getFields().get(0).getFloatV(), 0.01);
    Assert.assertEquals(3, record.getFields().get(4).getFloatV(), 0.01);

    Assert.assertTrue(dataSet.hasNext());
    record = dataSet.next();
    Assert.assertEquals(time, record.getTimestamp());
    time++;

    assertNulls(record, new int[] {0, 1, 2});
    Assert.assertEquals(false, record.getFields().get(3).getBoolV());
    Assert.assertEquals(6, record.getFields().get(4).getFloatV(), 0.01);

    Assert.assertFalse(dataSet.hasNext());
    dataSet.closeOperationHandle();
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

  private void assertNulls(RowRecord record, int[] index) {
    for (int i : index) {
      Assert.assertNull(record.getFields().get(i).getDataType());
    }
  }
}
