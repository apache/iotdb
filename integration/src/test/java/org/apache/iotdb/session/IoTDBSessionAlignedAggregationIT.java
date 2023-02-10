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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class IoTDBSessionAlignedAggregationIT {

  private static final String ROOT_SG1_D1_VECTOR1 = "root.sg1.d1.vector1";
  private static final String ROOT_SG1_D1 = "root.sg1.d1";
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
    prepareAlignedTimeseriesData();
    prepareNonAlignedTimeSeriesData();
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
        assertEquals(100, rowRecord.getFields().get(0).getLongV());
        assertEquals(100, rowRecord.getFields().get(1).getLongV());
        dataSet.next();
      }

      dataSet.closeOperationHandle();
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void vectorAggregationSumAvgTest() {
    try {
      SessionDataSet dataSet =
          session.executeQueryStatement("select sum(s1), avg(s2) from root.sg1.d1.vector1");
      assertEquals(2, dataSet.getColumnNames().size());
      assertEquals("sum(" + ROOT_SG1_D1_VECTOR1 + ".s1)", dataSet.getColumnNames().get(0));
      assertEquals("avg(" + ROOT_SG1_D1_VECTOR1 + ".s2)", dataSet.getColumnNames().get(1));
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(5150, rowRecord.getFields().get(0).getDoubleV(), 0.01);
        assertEquals(52.5, rowRecord.getFields().get(1).getDoubleV(), 0.01);
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
              "select min_time(s1), min_time(s2), max_time(s1), max_time(s2) from root.sg1.d1.vector1");
      assertEquals(4, dataSet.getColumnNames().size());
      assertEquals("min_time(" + ROOT_SG1_D1_VECTOR1 + ".s1)", dataSet.getColumnNames().get(0));
      assertEquals("min_time(" + ROOT_SG1_D1_VECTOR1 + ".s2)", dataSet.getColumnNames().get(1));
      assertEquals("max_time(" + ROOT_SG1_D1_VECTOR1 + ".s1)", dataSet.getColumnNames().get(2));
      assertEquals("max_time(" + ROOT_SG1_D1_VECTOR1 + ".s2)", dataSet.getColumnNames().get(3));
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(1, rowRecord.getFields().get(0).getLongV());
        assertEquals(1, rowRecord.getFields().get(1).getLongV());
        assertEquals(100, rowRecord.getFields().get(2).getLongV());
        assertEquals(100, rowRecord.getFields().get(3).getLongV());
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
              "select min_value(s1), max_value(s2) from root.sg1.d1.vector1");
      assertEquals(2, dataSet.getColumnNames().size());
      assertEquals("min_value(" + ROOT_SG1_D1_VECTOR1 + ".s1)", dataSet.getColumnNames().get(0));
      assertEquals("max_value(" + ROOT_SG1_D1_VECTOR1 + ".s2)", dataSet.getColumnNames().get(1));
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(2, rowRecord.getFields().get(0).getLongV());
        assertEquals(102, rowRecord.getFields().get(1).getIntV());
        dataSet.next();
      }

      dataSet.closeOperationHandle();
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void vectorAggregationFirstLastValueTest() {
    try {
      SessionDataSet dataSet =
          session.executeQueryStatement(
              "select first_value(s1), last_value(s2) from root.sg1.d1.vector1");
      assertEquals(2, dataSet.getColumnNames().size());
      assertEquals("first_value(" + ROOT_SG1_D1_VECTOR1 + ".s1)", dataSet.getColumnNames().get(0));
      assertEquals("last_value(" + ROOT_SG1_D1_VECTOR1 + ".s2)", dataSet.getColumnNames().get(1));
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(2, rowRecord.getFields().get(0).getLongV());
        assertEquals(102, rowRecord.getFields().get(1).getIntV());
        dataSet.next();
      }

      dataSet.closeOperationHandle();
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /** Test query vector time series and non aligned time series togther. */
  @Test
  public void vectorComplexTest() {
    try {
      SessionDataSet dataSet =
          session.executeQueryStatement(
              "select count(vector1.s1), max_value(s3), count(vector1.s2), min_time(s4) from root.sg1.d1");
      assertEquals(4, dataSet.getColumnNames().size());
      assertEquals("count(" + ROOT_SG1_D1_VECTOR1 + ".s1)", dataSet.getColumnNames().get(0));
      assertEquals("max_value(" + ROOT_SG1_D1 + ".s3)", dataSet.getColumnNames().get(1));
      assertEquals("count(" + ROOT_SG1_D1_VECTOR1 + ".s2)", dataSet.getColumnNames().get(2));
      assertEquals("min_time(" + ROOT_SG1_D1 + ".s4)", dataSet.getColumnNames().get(3));
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(100, rowRecord.getFields().get(0).getLongV());
        assertEquals(103, rowRecord.getFields().get(1).getLongV());
        assertEquals(100, rowRecord.getFields().get(2).getLongV());
        assertEquals(1, rowRecord.getFields().get(3).getLongV());
        dataSet.next();
      }

      dataSet.closeOperationHandle();
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /** Method 1 for insert tablet with aligned timeseries */
  private static void prepareAlignedTimeseriesData()
      throws IoTDBConnectionException, StatementExecutionException {
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT32));

    Tablet tablet = new Tablet(ROOT_SG1_D1_VECTOR1, schemaList);

    for (long row = 1; row <= 100; row++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, row);
      tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, row + 1);
      tablet.addValue(schemaList.get(1).getMeasurementId(), rowIndex, (int) (row + 2));

      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertAlignedTablet(tablet, true);
        tablet.reset();
      }
    }

    if (tablet.rowSize != 0) {
      session.insertAlignedTablet(tablet);
      tablet.reset();
    }
    session.executeNonQueryStatement("flush");
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
