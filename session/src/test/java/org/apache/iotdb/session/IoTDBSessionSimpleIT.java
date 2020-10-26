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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.BatchExecutionException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBSessionSimpleIT {

  private static Logger logger = LoggerFactory.getLogger(IoTDBSessionSimpleIT.class);

  private Session session;

  @Before
  public void setUp() {
    System.setProperty(IoTDBConstant.IOTDB_CONF, "src/test/resources/");
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    session.close();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testInsertByBlankStrAndInferType()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    String deviceId = "root.sg1.d1";
    List<String> measurements = new ArrayList<>();
    measurements.add("s1 ");

    List<String> values = new ArrayList<>();
    values.add("1.0");
    session.insertRecord(deviceId, 1L, measurements, values);

    String[] expected = new String[]{"root.sg1.d1.s1 "};

    assertFalse(session.checkTimeseriesExists("root.sg1.d1.s1 "));
    SessionDataSet dataSet = session.executeQueryStatement("show timeseries");
    int i = 0;
    while (dataSet.hasNext()) {
      assertEquals(expected[i], dataSet.next().getFields().get(0).toString());
      i++;
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
    SessionDataSet dataSet = session.executeQueryStatement("show timeseries root");
    while (dataSet.hasNext()) {
      actual.add(dataSet.next().getFields().get(3).getStringValue());
    }

    Assert.assertEquals(expected, actual);

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
    SessionDataSet dataSet = session.executeQueryStatement("show timeseries root");
    while (dataSet.hasNext()) {
      actual.add(dataSet.next().getFields().get(3).getStringValue());
    }

    Assert.assertEquals(expected, actual);

    session.close();
  }

  @Test
  public void testCreateMultiTimeseries()
      throws IoTDBConnectionException, BatchExecutionException, StatementExecutionException, MetadataException {
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

    session
        .createMultiTimeseries(paths, tsDataTypes, tsEncodings, compressionTypes, null, tagsList,
            null, null);

    assertTrue(session.checkTimeseriesExists("root.sg1.d1.s1"));
    assertTrue(session.checkTimeseriesExists("root.sg1.d1.s2"));
    MeasurementMNode mNode = (MeasurementMNode) MManager
        .getInstance().getNodeByPath(new PartialPath("root.sg1.d1.s1"));
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
    String[] devices = new String[]{
        "设备1.指标1",
        "设备1.s2",
        "d2.s1",
        "d2.指标2"
    };
    session.setStorageGroup(storageGroup);
    for (String path : devices) {
      String fullPath = storageGroup + TsFileConstant.PATH_SEPARATOR + path;
      session.createTimeseries(fullPath, TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    }

    for (String path : devices) {
      for (int i = 0; i < 10; i++) {
        String[] ss = path.split("\\.");
        String deviceId = storageGroup;
        for (int j = 0; j < ss.length - 1; j++) {
          deviceId += (TsFileConstant.PATH_SEPARATOR + ss[j]);
        }
        String sensorId = ss[ss.length - 1];
        List<String> measurements = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        List<TSDataType> types = new ArrayList<>();

        measurements.add(sensorId);
        types.add(TSDataType.INT64);
        values.add(100L);
        session.insertRecord(deviceId, i, measurements, types, values);
      }
    }

    SessionDataSet dataSet = session.executeQueryStatement("select * from root.存储组1");
    int count = 0;
    while (dataSet.hasNext()) {
      count++;
    }
    Assert.assertEquals(10, count);
    session.deleteStorageGroup(storageGroup);
    session.close();
  }

  @Test
  public void createTimeSeriesWithDoubleTicks() throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    if (!System.getProperty("sun.jnu.encoding").contains("UTF-8")) {
      logger.error("The system does not support UTF-8, so skip Chinese test...");
      session.close();
      return;
    }
    String storageGroup = "root.sg";
    session.setStorageGroup(storageGroup);

    session.createTimeseries("root.sg.\"my.device.with.colon:\".s", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);

    final SessionDataSet dataSet = session.executeQueryStatement("SHOW TIMESERIES");
    assertTrue(dataSet.hasNext());

    session.deleteStorageGroup(storageGroup);
    session.close();
  }
}
