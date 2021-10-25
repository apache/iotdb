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

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
  public void createSchemaTemplate() throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root", ZoneId.of("+05:00"));
    session.open();

    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("s11"));
    List<String> measurements = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      measurements.add("s" + i);
    }
    measurementList.add(measurements);

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      dataTypes.add(TSDataType.INT64);
    }
    dataTypeList.add(dataTypes);

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.RLE));
    List<TSEncoding> encodings = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      encodings.add(TSEncoding.RLE);
    }
    encodingList.add(encodings);

    List<CompressionType> compressionTypes = new ArrayList<>();
    for (int i = 0; i < 11; i++) {
      compressionTypes.add(CompressionType.SNAPPY);
    }

    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("s11");
    schemaNames.add("test_vector");

    session.createSchemaTemplate(
        "template1", schemaNames, measurementList, dataTypeList, encodingList, compressionTypes);
    session.setSchemaTemplate("template1", "root.sg.1");
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
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root", ZoneId.of("+05:00"));
    session.open();

    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("s1"));
    measurementList.add(Collections.singletonList("s2"));
    measurementList.add(Collections.singletonList("s3"));

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.RLE));
    encodingList.add(Collections.singletonList(TSEncoding.RLE));
    encodingList.add(Collections.singletonList(TSEncoding.RLE));

    List<CompressionType> compressionTypes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      compressionTypes.add(CompressionType.SNAPPY);
    }
    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("s1");
    schemaNames.add("s2");
    schemaNames.add("s3");

    session.createSchemaTemplate(
        "template1", schemaNames, measurementList, dataTypeList, encodingList, compressionTypes);

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
