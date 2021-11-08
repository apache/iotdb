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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class IoTDBSessionVectorABDeviceIT {
  private static final String ROOT_SG1_D1_VECTOR1 = "root.sg1.d1.vector1";
  private static final String ROOT_SG1_D1 = "root.sg1.d1";
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static Session session;
  private static int originCompactionThreadNum;

  @BeforeClass
  public static void setUp() throws Exception {
    originCompactionThreadNum = CONFIG.getConcurrentCompactionThread();
    CONFIG.setConcurrentCompactionThread(0);
    EnvironmentUtils.envSetUp();
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    createAlignedTimeseries();
    prepareAlignedTimeSeriesData();
    prepareNonAlignedTimeSeriesData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    session.close();
    EnvironmentUtils.cleanEnv();
    CONFIG.setConcurrentCompactionThread(originCompactionThreadNum);
  }

  @Test
  public void subMeasurementAlignByDeviceTest() {
    try {
      SessionDataSet dataSet =
          session.executeQueryStatement(
              "select vector1.s1, vector1.s2 from root.sg1.d1 limit 1 align by device");
      assertEquals(4, dataSet.getColumnNames().size());
      assertEquals("Time", dataSet.getColumnNames().get(0));
      assertEquals("Device", dataSet.getColumnNames().get(1));
      assertEquals("vector1.s1", dataSet.getColumnNames().get(2));
      assertEquals("vector1.s2", dataSet.getColumnNames().get(3));
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(1, rowRecord.getTimestamp());
        assertEquals("root.sg1.d1", rowRecord.getFields().get(0).getBinaryV().toString());
        assertEquals(2, rowRecord.getFields().get(1).getLongV());
        assertEquals(3, rowRecord.getFields().get(2).getLongV());
        dataSet.next();
      }

      dataSet.closeOperationHandle();
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void vectorAlignByDeviceTest() {
    try {
      SessionDataSet dataSet =
          session.executeQueryStatement(
              "select vector1.* from root.sg1.d1 limit 1 align by device");
      assertEquals(4, dataSet.getColumnNames().size());
      assertEquals("Time", dataSet.getColumnNames().get(0));
      assertEquals("Device", dataSet.getColumnNames().get(1));
      assertEquals("vector1.s1", dataSet.getColumnNames().get(2));
      assertEquals("vector1.s2", dataSet.getColumnNames().get(3));
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(1, rowRecord.getTimestamp());
        assertEquals("root.sg1.d1", rowRecord.getFields().get(0).getBinaryV().toString());
        assertEquals(2, rowRecord.getFields().get(1).getLongV());
        assertEquals(3, rowRecord.getFields().get(2).getLongV());
        dataSet.next();
      }

      dataSet.closeOperationHandle();
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  /** Ignore until the tablet interface. */
  public void vectorAlignByDeviceWithWildcardTest() {
    try {
      SessionDataSet dataSet =
          session.executeQueryStatement("select ** from root.sg1.d1 limit 1 align by device");
      assertEquals(7, dataSet.getColumnNames().size());
      assertEquals("Time", dataSet.getColumnNames().get(0));
      assertEquals("Device", dataSet.getColumnNames().get(1));
      assertEquals("s3", dataSet.getColumnNames().get(2));
      assertEquals("s4", dataSet.getColumnNames().get(3));
      assertEquals("s5", dataSet.getColumnNames().get(4));
      assertEquals("vector1.s1", dataSet.getColumnNames().get(5));
      assertEquals("vector1.s2", dataSet.getColumnNames().get(6));

      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(1, rowRecord.getTimestamp());
        assertEquals("root.sg1.d1", rowRecord.getFields().get(0).getBinaryV().toString());
        assertEquals(2, rowRecord.getFields().get(4).getLongV());
        assertEquals(3, rowRecord.getFields().get(5).getLongV());
        assertEquals(4, rowRecord.getFields().get(1).getLongV());
        assertEquals(5, rowRecord.getFields().get(2).getLongV());
        assertEquals(6, rowRecord.getFields().get(3).getLongV());
        dataSet.next();
      }

      dataSet.closeOperationHandle();
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void vectorAggregationAlignByDeviceTest() {
    try {
      SessionDataSet dataSet =
          session.executeQueryStatement("select count(vector1.*) from root.sg1.d1 align by device");
      assertEquals(3, dataSet.getColumnNames().size());
      assertEquals("Device", dataSet.getColumnNames().get(0));
      assertEquals("count(vector1.s1)", dataSet.getColumnNames().get(1));
      assertEquals("count(vector1.s2)", dataSet.getColumnNames().get(2));
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals("root.sg1.d1", rowRecord.getFields().get(0).getBinaryV().toString());
        assertEquals(100, rowRecord.getFields().get(1).getLongV());
        assertEquals(100, rowRecord.getFields().get(2).getLongV());
        dataSet.next();
      }

      dataSet.closeOperationHandle();
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void createAlignedTimeseries()
      throws StatementExecutionException, IoTDBConnectionException {
    List<String> measurements = new ArrayList<>();
    for (int i = 1; i <= 2; i++) {
      measurements.add("s" + i);
    }
    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT64);
    dataTypes.add(TSDataType.INT64);
    List<TSEncoding> encodings = new ArrayList<>();
    for (int i = 1; i <= 2; i++) {
      encodings.add(TSEncoding.RLE);
    }
    session.createAlignedTimeseries(
        ROOT_SG1_D1_VECTOR1, measurements, dataTypes, encodings, CompressionType.SNAPPY, null);
  }

  private static void prepareAlignedTimeSeriesData()
      throws StatementExecutionException, IoTDBConnectionException {
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);

    for (long time = 1; time <= 100; time++) {
      List<Object> values = new ArrayList<>();
      values.add(time + 1);
      values.add(time + 2);
      session.insertAlignedRecord(ROOT_SG1_D1_VECTOR1, time, measurements, types, values);
    }
  }

  private static void prepareNonAlignedTimeSeriesData()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s3");
    measurements.add("s4");
    measurements.add("s5");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);

    for (long time = 1; time <= 100; time++) {
      List<Object> values = new ArrayList<>();
      values.add(time + 3L);
      values.add(time + 4L);
      values.add(time + 5L);
      session.insertRecord(ROOT_SG1_D1, time, measurements, types, values);
    }
  }
}
