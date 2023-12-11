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

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSessionAlignedInsertIT {

  private static final String ROOT_SG1_D1_VECTOR1 = "root.sg_1.d1.vector";
  private static final String ROOT_SG1_D1 = "root.sg_1.d1";
  private static final String ROOT_SG1_D2 = "root.sg_1.d2";
  private static final double DELTA_DOUBLE = 1e-7d;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setMaxDegreeOfIndexNode(3);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void insertAlignedTabletTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      insertTabletWithAlignedTimeseriesMethod(session);
      session.executeNonQueryStatement("flush");
      SessionDataSet dataSet = session.executeQueryStatement("select * from root.sg_1.d1.vector");
      assertEquals(dataSet.getColumnNames().size(), 3);
      assertEquals(dataSet.getColumnNames().get(0), "Time");
      assertEquals(dataSet.getColumnNames().get(1), ROOT_SG1_D1_VECTOR1 + ".s1");
      assertEquals(dataSet.getColumnNames().get(2), ROOT_SG1_D1_VECTOR1 + ".s2");
      long time = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(time * 10 + 1, rowRecord.getFields().get(0).getLongV());
        assertEquals(time * 10 + 2, rowRecord.getFields().get(1).getIntV());
        time += 1;
      }
      assertEquals(100, time);

      dataSet.closeOperationHandle();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void insertAlignedRecord(ISession session, String deviceId)
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s2");
    measurements.add("s4");
    measurements.add("s5");
    measurements.add("s6");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);

    for (long time = 0; time < 100; time++) {
      List<Object> values = new ArrayList<>();
      values.add(time * 10 + 3L);
      values.add(time * 10 + 4L);
      values.add(time * 10 + 5L);
      values.add(time * 10 + 6L);
      session.insertAlignedRecord(deviceId, time, measurements, types, values);
    }
  }

  @Test
  public void insertAlignedRecordTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      insertAlignedRecord(session, ROOT_SG1_D1_VECTOR1);
      session.executeNonQueryStatement("flush");
      SessionDataSet dataSet = session.executeQueryStatement("select s2 from root.sg_1.d1.vector");
      assertEquals(2, dataSet.getColumnNames().size());
      assertEquals("Time", dataSet.getColumnNames().get(0));
      assertEquals(dataSet.getColumnNames().get(1), ROOT_SG1_D1_VECTOR1 + ".s2");
      long time = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(time * 10 + 3, rowRecord.getFields().get(0).getLongV());
        time += 1;
      }
      assertEquals(100, time);

      dataSet.closeOperationHandle();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void insertAlignedStringRecordTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      insertAlignedStringRecord(session, ROOT_SG1_D1_VECTOR1);
      session.executeNonQueryStatement("flush");
      SessionDataSet dataSet = session.executeQueryStatement("select s2 from root.sg_1.d1.vector");
      assertEquals(2, dataSet.getColumnNames().size());
      assertEquals("Time", dataSet.getColumnNames().get(0));
      assertEquals(ROOT_SG1_D1_VECTOR1 + ".s2", dataSet.getColumnNames().get(1));
      long time = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(time * 10 + 3, rowRecord.getFields().get(0).getFloatV(), DELTA_DOUBLE);
        time += 1;
      }
      assertEquals(100, time);

      dataSet.closeOperationHandle();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void insertAlignedStringRecordsTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      insertAlignedStringRecords(session, ROOT_SG1_D1_VECTOR1);
      session.executeNonQueryStatement("flush");
      SessionDataSet dataSet = session.executeQueryStatement("select s2 from root.sg_1.d1.vector");
      assertEquals(2, dataSet.getColumnNames().size());
      assertEquals("Time", dataSet.getColumnNames().get(0));
      assertEquals(dataSet.getColumnNames().get(1), ROOT_SG1_D1_VECTOR1 + ".s2");
      long time = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(time * 10 + 3, rowRecord.getFields().get(0).getFloatV(), DELTA_DOUBLE);
        time += 1;
      }
      assertEquals(100, time);

      dataSet.closeOperationHandle();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void insertAlignedRecordsTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      insertAlignedRecords(session, ROOT_SG1_D1_VECTOR1);
      session.executeNonQueryStatement("flush");
      SessionDataSet dataSet = session.executeQueryStatement("select s2 from root.sg_1.d1.vector");
      assertEquals(2, dataSet.getColumnNames().size());
      assertEquals("Time", dataSet.getColumnNames().get(0));
      assertEquals(dataSet.getColumnNames().get(1), ROOT_SG1_D1_VECTOR1 + ".s2");
      long time = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(time * 10 + 3, rowRecord.getFields().get(0).getLongV());
        time += 1;
      }
      assertEquals(100, time);

      dataSet.closeOperationHandle();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void insertAlignedRecordsOfOneDeviceTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      insertAlignedRecordsOfOneDevice(session, ROOT_SG1_D1_VECTOR1);
      session.executeNonQueryStatement("flush");
      SessionDataSet dataSet = session.executeQueryStatement("select s2 from root.sg_1.d1.vector");
      assertEquals(2, dataSet.getColumnNames().size());
      assertEquals("Time", dataSet.getColumnNames().get(0));
      assertEquals(dataSet.getColumnNames().get(1), ROOT_SG1_D1_VECTOR1 + ".s2");
      long time = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(time * 10 + 3, rowRecord.getFields().get(0).getLongV());
        time += 1;
      }
      assertEquals(100, time);

      dataSet.closeOperationHandle();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void nonAlignedSingleSelectTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      insertNonAlignedRecord(session, ROOT_SG1_D1);
      insertTabletWithAlignedTimeseriesMethod(session);
      insertNonAlignedRecord(session, ROOT_SG1_D2);
      session.executeNonQueryStatement("flush");
      SessionDataSet dataSet = session.executeQueryStatement("select s2 from root.sg_1.d1.vector");
      assertEquals(2, dataSet.getColumnNames().size());
      assertEquals("Time", dataSet.getColumnNames().get(0));
      assertEquals(dataSet.getColumnNames().get(1), ROOT_SG1_D1_VECTOR1 + ".s2");
      long time = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(time * 10 + 2, rowRecord.getFields().get(0).getIntV());
        time += 1;
      }
      assertEquals(100, time);

      dataSet.closeOperationHandle();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void nonAlignedVectorSelectTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      insertNonAlignedRecord(session, ROOT_SG1_D1);
      insertTabletWithAlignedTimeseriesMethod(session);
      insertNonAlignedRecord(session, ROOT_SG1_D2);
      session.executeNonQueryStatement("flush");
      SessionDataSet dataSet = session.executeQueryStatement("select * from root.sg_1.d1.vector");
      assertEquals(3, dataSet.getColumnNames().size());
      assertEquals("Time", dataSet.getColumnNames().get(0));
      assertEquals(ROOT_SG1_D1_VECTOR1 + ".s1", dataSet.getColumnNames().get(1));
      assertEquals(ROOT_SG1_D1_VECTOR1 + ".s2", dataSet.getColumnNames().get(2));
      long time = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(time * 10 + 1, rowRecord.getFields().get(0).getLongV());
        assertEquals(time * 10 + 2, rowRecord.getFields().get(1).getIntV());
        time += 1;
      }
      assertEquals(100, time);

      dataSet.closeOperationHandle();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /** Method 1 for insert tablet with aligned timeseries */
  private void insertTabletWithAlignedTimeseriesMethod(ISession session)
      throws IoTDBConnectionException, StatementExecutionException {
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT32));

    Tablet tablet = new Tablet(ROOT_SG1_D1_VECTOR1, schemaList);
    long timestamp = 0;

    for (long row = 0; row < 100; row++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, row * 10 + 1L);
      tablet.addValue(schemaList.get(1).getMeasurementId(), rowIndex, (int) (row * 10 + 2));

      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertAlignedTablet(tablet, true);
        tablet.reset();
      }
      timestamp++;
    }

    if (tablet.rowSize != 0) {
      session.insertAlignedTablet(tablet);
      tablet.reset();
    }
  }

  private void insertAlignedStringRecord(ISession session, String prefixPath)
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> subMeasurements = new ArrayList<>();
    subMeasurements.add("s2");
    subMeasurements.add("s4");
    subMeasurements.add("s5");
    subMeasurements.add("s6");

    for (long time = 0; time < 100; time++) {
      List<String> values = new ArrayList<>();
      values.add(String.valueOf(time * 10 + 3L));
      values.add(String.valueOf(time * 10 + 4L));
      values.add(String.valueOf(time * 10 + 5L));
      values.add(String.valueOf(time * 10 + 6L));
      session.insertAlignedRecord(prefixPath, time, subMeasurements, values);
    }
  }

  private void insertAlignedStringRecords(ISession session, String prefixPath)
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> prefixPaths = new ArrayList<>();
    List<List<String>> subMeasurementsList = new ArrayList<>();
    List<Long> times = new ArrayList<>();
    List<List<String>> valueList = new ArrayList<>();
    for (long time = 0; time < 100; time++) {
      prefixPaths.add(prefixPath);
      times.add(time);
      List<String> values = new ArrayList<>();
      List<String> measurements = new ArrayList<>();
      measurements.add("s2");
      measurements.add("s4");
      measurements.add("s5");
      measurements.add("s6");
      values.add(String.valueOf(time * 10 + 3L));
      values.add(String.valueOf(time * 10 + 4L));
      values.add(String.valueOf(time * 10 + 5L));
      values.add(String.valueOf(time * 10 + 6L));
      subMeasurementsList.add(measurements);
      valueList.add(values);
    }
    session.insertAlignedRecords(prefixPaths, times, subMeasurementsList, valueList);
  }

  private void insertAlignedRecords(ISession session, String prefixPath)
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> prefixPaths = new ArrayList<>();
    List<List<String>> subMeasurementsList = new ArrayList<>();
    List<Long> times = new ArrayList<>();
    List<List<TSDataType>> typeList = new ArrayList<>();
    List<List<Object>> valueList = new ArrayList<>();
    for (long time = 0; time < 100; time++) {
      prefixPaths.add(prefixPath);
      times.add(time);
      List<Object> values = new ArrayList<>();
      List<String> measurements = new ArrayList<>();
      List<TSDataType> types = new ArrayList<>();
      measurements.add("s2");
      measurements.add("s4");
      measurements.add("s5");
      measurements.add("s6");
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      values.add(time * 10 + 3L);
      values.add(time * 10 + 4L);
      values.add(time * 10 + 5L);
      values.add(time * 10 + 6L);
      subMeasurementsList.add(measurements);
      typeList.add(types);
      valueList.add(values);
    }
    session.insertAlignedRecords(prefixPaths, times, subMeasurementsList, typeList, valueList);
  }

  private void insertAlignedRecordsOfOneDevice(ISession session, String prefixPath)
      throws IoTDBConnectionException, StatementExecutionException {
    List<List<String>> subMeasurementsList = new ArrayList<>();
    List<Long> times = new ArrayList<>();
    List<List<TSDataType>> typeList = new ArrayList<>();
    List<List<Object>> valueList = new ArrayList<>();
    for (long time = 0; time < 100; time++) {
      times.add(time);
      List<Object> values = new ArrayList<>();
      List<String> measurements = new ArrayList<>();
      List<TSDataType> types = new ArrayList<>();
      measurements.add("s2");
      measurements.add("s4");
      measurements.add("s5");
      measurements.add("s6");
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      values.add(time * 10 + 3L);
      values.add(time * 10 + 4L);
      values.add(time * 10 + 5L);
      values.add(time * 10 + 6L);
      subMeasurementsList.add(measurements);
      typeList.add(types);
      valueList.add(values);
    }
    session.insertAlignedRecordsOfOneDevice(
        prefixPath, times, subMeasurementsList, typeList, valueList);
  }

  private void insertNonAlignedRecord(ISession session, String deviceId)
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s2");
    measurements.add("s4");
    measurements.add("s5");
    measurements.add("s6");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);

    for (long time = 0; time < 100; time++) {
      List<Object> values = new ArrayList<>();
      values.add(time * 10 + 3L);
      values.add(time * 10 + 4L);
      values.add(time * 10 + 5L);
      values.add(time * 10 + 6L);
      session.insertRecord(deviceId, time, measurements, types, values);
    }
  }
}
