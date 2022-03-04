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

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.template.InternalNode;
import org.apache.iotdb.session.template.MeasurementNode;
import org.apache.iotdb.session.template.Template;
import org.apache.iotdb.session.template.TemplateNode;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SessionTest {

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
  public void testSortTablet() {
    /*
    To test sortTablet in Class Session
    !!!
    Before testing, change the sortTablet from private method to public method
    !!!
     */
    session = new Session("127.0.0.1", 6667, "root", "root", null);
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new UnaryMeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    // insert three rows data
    Tablet tablet = new Tablet("root.sg1.d1", schemaList, 3);
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    /*
    inorder data before inserting
    timestamp   s1
    2           0
    0           1
    1           2
     */
    // inorder timestamps
    timestamps[0] = 2;
    timestamps[1] = 0;
    timestamps[2] = 1;
    // just one column INT64 data
    long[] sensor = (long[]) values[0];
    sensor[0] = 0;
    sensor[1] = 1;
    sensor[2] = 2;
    tablet.rowSize = 3;

    session.sortTablet(tablet);

    /*
    After sorting, if the tablet data is sorted according to the timestamps,
    data in tablet will be
    timestamp   s1
    0           1
    1           2
    2           0

    If the data equal to above tablet, test pass, otherwise test fialed
     */
    long[] resTimestamps = tablet.timestamps;
    long[] resValues = (long[]) tablet.values[0];
    long[] expectedTimestamps = new long[] {0, 1, 2};
    long[] expectedValues = new long[] {1, 2, 0};
    try {
      assertArrayEquals(expectedTimestamps, resTimestamps);
      assertArrayEquals(expectedValues, resValues);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testInsertByStrAndSelectFailedData()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root", null);
    session.open();

    String deviceId = "root.sg1.d1";

    session.createTimeseries(
        deviceId + ".s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.UNCOMPRESSED);
    session.createTimeseries(
        deviceId + ".s2", TSDataType.INT64, TSEncoding.RLE, CompressionType.UNCOMPRESSED);
    session.createTimeseries(
        deviceId + ".s3", TSDataType.INT64, TSEncoding.RLE, CompressionType.UNCOMPRESSED);
    session.createTimeseries(
        deviceId + ".s4", TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.UNCOMPRESSED);

    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new UnaryMeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new UnaryMeasurementSchema("s2", TSDataType.DOUBLE, TSEncoding.RLE));
    schemaList.add(new UnaryMeasurementSchema("s3", TSDataType.TEXT, TSEncoding.PLAIN));
    schemaList.add(new UnaryMeasurementSchema("s4", TSDataType.INT64, TSEncoding.PLAIN));

    Tablet tablet = new Tablet("root.sg1.d1", schemaList, 10);

    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    for (long time = 0; time < 10; time++) {
      int row = tablet.rowSize++;
      timestamps[row] = time;
      long[] sensor = (long[]) values[0];
      sensor[row] = time;
      double[] sensor2 = (double[]) values[1];
      sensor2[row] = 0.1 + time;
      Binary[] sensor3 = (Binary[]) values[2];
      sensor3[row] = Binary.valueOf("ha" + time);
      long[] sensor4 = (long[]) values[3];
      sensor4[row] = time;
    }

    try {
      session.insertTablet(tablet);
      fail();
    } catch (StatementExecutionException e) {
      // ignore
    }

    SessionDataSet dataSet = session.executeQueryStatement("select * from root.sg1.d1");
    int i = 0;
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      int nullCount = 0;
      for (int j = 0; j < 4; ++j) {
        if (record.getFields().get(j) == null || record.getFields().get(j).getDataType() == null) {
          ++nullCount;
        } else {
          assertEquals(i, record.getFields().get(j).getLongV());
        }
      }
      assertEquals(3, nullCount);
      i++;
    }
  }

  @Test
  public void testSetTimeZone() throws StatementExecutionException, IoTDBConnectionException {
    session = new Session("127.0.0.1", 6667, "root", "root", ZoneId.of("+05:00"));
    session.open();
    assertEquals("+05:00", session.getTimeZone());
    session.setTimeZone("+09:00");
    assertEquals("+09:00", session.getTimeZone());
  }

  @Test
  public void setTimeout() throws StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root", 10000, 20000);
    Assert.assertEquals(20000, session.getQueryTimeout());
    session.setQueryTimeout(60000);
    Assert.assertEquals(60000, session.getQueryTimeout());
  }

  @Test
  public void createSchemaTemplateWithTreeStructure()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    session = new Session("127.0.0.1", 6667, "root", "root", ZoneId.of("+05:00"));
    session.open();

    Template template = new Template("treeTemplate", true);
    TemplateNode iNodeGPS = new InternalNode("GPS", false);
    TemplateNode iNodeV = new InternalNode("vehicle", true);
    TemplateNode mNodeX =
        new MeasurementNode("x", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);
    TemplateNode mNodeY =
        new MeasurementNode("y", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);

    template.addToTemplate(mNodeX);
    iNodeGPS.addChild(mNodeX);
    iNodeGPS.addChild(mNodeY);
    iNodeV.addChild(mNodeX);
    iNodeV.addChild(iNodeGPS);
    iNodeV.addChild(mNodeY);
    template.addToTemplate(iNodeGPS);
    template.addToTemplate(iNodeV);
    template.addToTemplate(mNodeY);

    session.createSchemaTemplate(template);
    assertEquals(
        "[vehicle.GPS.y, x, vehicle.GPS.x, y, GPS.x, vehicle.x, GPS.y, vehicle.y]",
        session.showMeasurementsInTemplate("treeTemplate").toString());
    assertEquals(8, session.countMeasurementsInTemplate("treeTemplate"));

    session.deleteNodeInTemplate("treeTemplate", "vehicle.GPS");
    assertEquals(6, session.countMeasurementsInTemplate("treeTemplate"));

    session.addAlignedMeasurementInTemplate(
        "treeTemplate",
        "vehicle.speed",
        TSDataType.FLOAT,
        TSEncoding.GORILLA,
        CompressionType.SNAPPY);
    assertEquals(
        "[vehicle.speed, x, y, GPS.x, vehicle.x, GPS.y, vehicle.y]",
        session.showMeasurementsInTemplate("treeTemplate").toString());

    session.deleteNodeInTemplate("treeTemplate", "vehicle");
    assertEquals(4, session.countMeasurementsInTemplate("treeTemplate"));

    session.addUnalignedMeasurementInTemplate(
        "treeTemplate",
        "vehicle.speed",
        TSDataType.FLOAT,
        TSEncoding.GORILLA,
        CompressionType.SNAPPY);
    assertEquals(
        "[vehicle.speed, x, y, GPS.x, GPS.y]",
        session.showMeasurementsInTemplate("treeTemplate").toString());
  }

  @Test
  public void treeStructuredSchemaTemplateTest()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    session = new Session("127.0.0.1", 6667, "root", "root", ZoneId.of("+05:00"));
    session.open();

    Template template = new Template("treeTemplate", true);
    TemplateNode iNodeGPS = new InternalNode("GPS", false);
    TemplateNode iNodeV = new InternalNode("vehicle", true);
    TemplateNode mNodeX =
        new MeasurementNode("x", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);

    iNodeGPS.addChild(mNodeX);
    iNodeV.addChild(mNodeX);
    template.addToTemplate(iNodeGPS);
    template.addToTemplate(iNodeV);
    template.addToTemplate(mNodeX);

    session.createSchemaTemplate(template);

    List<String> measurementPaths = new ArrayList<>();
    measurementPaths.add("GPS.X");
    measurementPaths.add("GPS.Y");
    measurementPaths.add("turbine.temperature");

    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.FLOAT);
    dataTypes.add(TSDataType.FLOAT);
    dataTypes.add(TSDataType.FLOAT);

    List<TSEncoding> encodings = new ArrayList<>();
    encodings.add(TSEncoding.GORILLA);
    encodings.add(TSEncoding.GORILLA);
    encodings.add(TSEncoding.GORILLA);

    List<CompressionType> compressionTypeList = new ArrayList<>();
    compressionTypeList.add(CompressionType.SNAPPY);
    compressionTypeList.add(CompressionType.SNAPPY);
    compressionTypeList.add(CompressionType.SNAPPY);

    try {
      session.addAlignedMeasurementsInTemplate(
          "treeTemplate", measurementPaths, dataTypes, encodings, compressionTypeList);
    } catch (Exception e) {
      assertEquals(
          "315: GPS is not a legal path, because path already exists but not aligned",
          e.getMessage());
    }

    session.addUnalignedMeasurementsInTemplate(
        "treeTemplate", measurementPaths, dataTypes, encodings, compressionTypeList);

    try {
      session.addUnalignedMeasurementInTemplate(
          "treeTemplate", "GPS.X", TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.SNAPPY);
    } catch (Exception e) {
      assertEquals("315: Path duplicated: GPS.X is not a legal path", e.getMessage());
    }

    session.deleteNodeInTemplate("treeTemplate", "GPS.X");
    session.addUnalignedMeasurementInTemplate(
        "treeTemplate", "GPS.X", TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.SNAPPY);

    assertEquals(6, session.countMeasurementsInTemplate("treeTemplate"));
    assertEquals(false, session.isMeasurementInTemplate("treeTemplate", "turbine"));
    assertEquals(true, session.isPathExistInTemplate("treeTemplate", "turbine"));
    assertEquals(
        "[turbine.temperature, x, GPS.x, vehicle.x, GPS.X, GPS.Y]",
        session.showMeasurementsInTemplate("treeTemplate").toString());
    assertEquals(
        "[GPS.Y, GPS.X, GPS.x]",
        session.showMeasurementsInTemplate("treeTemplate", "GPS").toString());

    session.deleteNodeInTemplate("treeTemplate", "GPS");
    session.addAlignedMeasurementInTemplate(
        "treeTemplate", "GPSX", TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.SNAPPY);
    assertEquals(
        "[turbine.temperature, x, vehicle.x, GPSX]",
        session.showMeasurementsInTemplate("treeTemplate").toString());
  }

  @Test
  public void createSchemaTemplate()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    session = new Session("127.0.0.1", 6667, "root", "root", ZoneId.of("+05:00"));
    session.open();

    InternalNode iNodeVector = new InternalNode("vector", true);

    for (int i = 0; i < 10; i++) {
      MeasurementNode mNodei =
          new MeasurementNode("s" + i, TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
      iNodeVector.addChild(mNodei);
    }

    MeasurementNode mNode11 =
        new MeasurementNode("s11", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);

    Template template = new Template("template1");

    template.addToTemplate(mNode11);
    template.addToTemplate(iNodeVector);

    session.createSchemaTemplate(template);
    session.setSchemaTemplate("template1", "root.sg.1");
  }

  @Test
  public void testCreateEmptyTemplateAndAppend()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    session = new Session("127.0.0.1", 6667, "root", "root", ZoneId.of("+05:00"));
    session.open();

    List<List<String>> measurements = new ArrayList<>();
    List<List<TSDataType>> dataTypes = new ArrayList<>();
    List<List<TSEncoding>> encodings = new ArrayList<>();
    List<List<CompressionType>> compressors = new ArrayList<>();
    Template template = new Template("emptyTemplate");
    session.createSchemaTemplate(template);

    session.addAlignedMeasurementInTemplate(
        "emptyTemplate", "speed", TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.SNAPPY);
  }

  @Test
  public void testBuilder() {
    session =
        new Session.Builder()
            .host("localhost")
            .port(1234)
            .fetchSize(1)
            .username("abc")
            .password("123456")
            .thriftDefaultBufferSize(2)
            .thriftMaxFrameSize(3)
            .enableCacheLeader(true)
            .zoneId(ZoneOffset.UTC)
            .build();

    assertEquals(1, session.fetchSize);
    assertEquals("abc", session.username);
    assertEquals("123456", session.password);
    assertEquals(2, session.thriftDefaultBufferSize);
    assertEquals(3, session.thriftMaxFrameSize);
    assertEquals(ZoneOffset.UTC, session.zoneId);
    assertTrue(session.enableCacheLeader);

    session = new Session.Builder().nodeUrls(Arrays.asList("aaa.com:12", "bbb.com:12")).build();
    assertEquals(Arrays.asList("aaa.com:12", "bbb.com:12"), session.nodeUrls);

    try {
      session =
          new Session.Builder()
              .nodeUrls(Arrays.asList("aaa.com:12", "bbb.com:12"))
              .port(1234)
              .build();
      fail("specifying both nodeUrls and (host + port) is not allowed");
    } catch (IllegalArgumentException e) {
      assertEquals(
          "You should specify either nodeUrls or (host + rpcPort), but not both", e.getMessage());
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testUnsetSchemaTemplate()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    session = new Session("127.0.0.1", 6667, "root", "root", ZoneId.of("+05:00"));
    session.open();

    Template template = new Template("template1", false);

    for (int i = 1; i <= 3; i++) {
      MeasurementNode mNodei =
          new MeasurementNode("s" + i, TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
      template.addToTemplate(mNodei);
    }

    session.createSchemaTemplate(template);

    // path does not exist test
    try {
      session.unsetSchemaTemplate("root.sg.1", "template1");
      fail("No exception thrown.");
    } catch (Exception e) {
      assertEquals("304: Path [root.sg.1] does not exist", e.getMessage());
    }

    session.setSchemaTemplate("template1", "root.sg.1");

    // template already exists test
    try {
      session.setSchemaTemplate("template1", "root.sg.1");
      fail("No exception thrown.");
    } catch (Exception e) {
      assertEquals("303: Template already exists on root.sg.1", e.getMessage());
    }

    // template unset test
    session.unsetSchemaTemplate("root.sg.1", "template1");

    session.setSchemaTemplate("template1", "root.sg.1");

    // no template on path test
    session.unsetSchemaTemplate("root.sg.1", "template1");
    try {
      session.unsetSchemaTemplate("root.sg.1", "template1");
      fail("No exception thrown.");
    } catch (Exception e) {
      assertEquals("324: NO template on root.sg.1", e.getMessage());
    }

    // template is in use test
    session.setSchemaTemplate("template1", "root.sg.1");

    String deviceId = "root.sg.1.cd";
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);

    for (long time = 0; time < 5; time++) {
      List<Object> values = new ArrayList<>();
      values.add(1L);
      values.add(2L);
      values.add(3L);
      session.insertRecord(deviceId, time, measurements, types, values);
    }

    try {
      session.unsetSchemaTemplate("root.sg.1", "template1");
      fail("No exception thrown.");
    } catch (Exception e) {
      assertEquals("326: Template is in use on root.sg.1.cd", e.getMessage());
    }
  }
}
