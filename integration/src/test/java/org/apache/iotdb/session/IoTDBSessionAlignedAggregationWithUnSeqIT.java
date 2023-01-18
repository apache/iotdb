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
import org.apache.iotdb.isession.SessionDataSet;
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

public class IoTDBSessionAlignedAggregationWithUnSeqIT {

  private static final String ROOT_SG1_D1_VECTOR1 = "root.sg1.d1.vector1";
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static Session session;
  private static int originCompactionThreadNum;

  @BeforeClass
  public static void setUp() throws Exception {
    originCompactionThreadNum = CONFIG.getCompactionThreadCount();
    CONFIG.setCompactionThreadCount(0);
    EnvironmentUtils.envSetUp();
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    createAlignedTimeseries();
    prepareAlignedTimeseriesDataWithUnSeq();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    session.close();
    EnvironmentUtils.cleanEnv();
    CONFIG.setCompactionThreadCount(originCompactionThreadNum);
  }

  @Test
  public void vectorAggregationCountTest() {
    try {
      SessionDataSet dataSet =
          session.executeQueryStatement("select count(s1), count(s2) from root.sg1.d1.vector1");
      assertEquals(2, dataSet.getColumnNames().size());
      assertEquals("count(" + ROOT_SG1_D1_VECTOR1 + ".s1)", dataSet.getColumnNames().get(0));
      assertEquals("count(" + ROOT_SG1_D1_VECTOR1 + ".s2)", dataSet.getColumnNames().get(1));
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(200, rowRecord.getFields().get(0).getLongV());
        assertEquals(200, rowRecord.getFields().get(1).getLongV());
        dataSet.next();
      }

      dataSet.closeOperationHandle();
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void vectorAggregationMinMaxTimeTest() {
    try {
      SessionDataSet dataSet =
          session.executeQueryStatement(
              "select min_time(s1), max_time(s1), min_time(s2), max_time(s2) from root.sg1.d1.vector1");
      assertEquals(4, dataSet.getColumnNames().size());
      assertEquals("min_time(" + ROOT_SG1_D1_VECTOR1 + ".s1)", dataSet.getColumnNames().get(0));
      assertEquals("max_time(" + ROOT_SG1_D1_VECTOR1 + ".s1)", dataSet.getColumnNames().get(1));
      assertEquals("min_time(" + ROOT_SG1_D1_VECTOR1 + ".s2)", dataSet.getColumnNames().get(2));
      assertEquals("max_time(" + ROOT_SG1_D1_VECTOR1 + ".s2)", dataSet.getColumnNames().get(3));
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(1, rowRecord.getFields().get(0).getLongV());
        assertEquals(200, rowRecord.getFields().get(1).getLongV());
        assertEquals(1, rowRecord.getFields().get(2).getLongV());
        assertEquals(200, rowRecord.getFields().get(3).getLongV());
        dataSet.next();
      }

      dataSet.closeOperationHandle();
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void vectorAggregationMinMaxValueTest() {
    try {
      SessionDataSet dataSet =
          session.executeQueryStatement(
              "select min_value(s1), min_value(s2), max_value(s1), max_value(s2) from root.sg1.d1.vector1");
      assertEquals(4, dataSet.getColumnNames().size());
      assertEquals("min_value(" + ROOT_SG1_D1_VECTOR1 + ".s1)", dataSet.getColumnNames().get(0));
      assertEquals("min_value(" + ROOT_SG1_D1_VECTOR1 + ".s2)", dataSet.getColumnNames().get(1));
      assertEquals("max_value(" + ROOT_SG1_D1_VECTOR1 + ".s1)", dataSet.getColumnNames().get(2));
      assertEquals("max_value(" + ROOT_SG1_D1_VECTOR1 + ".s2)", dataSet.getColumnNames().get(3));
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(2, rowRecord.getFields().get(0).getLongV());
        assertEquals(3, rowRecord.getFields().get(1).getLongV());
        assertEquals(201, rowRecord.getFields().get(2).getLongV());
        assertEquals(202, rowRecord.getFields().get(3).getLongV());
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
    List<CompressionType> compressors = new ArrayList<>();
    for (int i = 1; i <= 2; i++) {
      encodings.add(TSEncoding.RLE);
      compressors.add(CompressionType.SNAPPY);
    }
    session.createAlignedTimeseries(
        ROOT_SG1_D1_VECTOR1, measurements, dataTypes, encodings, compressors, null, null, null);
  }

  private static void prepareAlignedTimeseriesDataWithUnSeq()
      throws StatementExecutionException, IoTDBConnectionException {
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);

    for (long time = 1; time <= 200; time++) {
      List<Object> values = new ArrayList<>();
      values.add(time + 1);
      values.add(time + 2);
      session.insertAlignedRecord(ROOT_SG1_D1_VECTOR1, time, measurements, types, values);
    }
    session.executeNonQueryStatement("flush");

    for (long time = 2; time <= 50; time += 2) {
      List<Object> values = new ArrayList<>();
      values.add(time + 1);
      values.add(time + 2);
      session.insertAlignedRecord(ROOT_SG1_D1_VECTOR1, time, measurements, types, values);
    }
    session.executeNonQueryStatement("flush");

    for (long time = 150; time <= 200; time += 10) {
      List<Object> values = new ArrayList<>();
      values.add(time + 1);
      values.add(time + 2);
      session.insertAlignedRecord(ROOT_SG1_D1_VECTOR1, time, measurements, types, values);
    }
    session.executeNonQueryStatement("flush");
  }
}
