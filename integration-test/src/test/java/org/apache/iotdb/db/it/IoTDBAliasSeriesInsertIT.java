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
package org.apache.iotdb.db.it;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.it.utils.TestUtils.assertNonQueryTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration test for alias series insert operations. This test covers: - Insert single row into
 * alias series (INSERT INTO) - Insert multiple rows into alias series (INSERT ROWS) - Insert tablet
 * into alias series - Insert into disabled series (should fail) - Mixed inserts (alias series and
 * physical series) - Verify data is written to physical path
 */
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAliasSeriesInsertIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void prepareData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // Create databases
      statement.execute("CREATE DATABASE root.sg1");
      statement.execute("CREATE DATABASE root.view");

      // Create physical time series
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.temperature WITH DATATYPE=FLOAT, ENCODING=RLE, COMPRESSION=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.pressure WITH DATATYPE=DOUBLE, ENCODING=GORILLA, COMPRESSION=LZ4");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d2.temperature WITH DATATYPE=FLOAT, ENCODING=RLE, COMPRESSION=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d2.humidity WITH DATATYPE=DOUBLE, ENCODING=GORILLA, COMPRESSION=LZ4");

      // Create alias series (RENAME operation)
      statement.execute(
          "ALTER TIMESERIES root.sg1.d1.temperature RENAME TO root.view.d1.temperature");
      statement.execute(
          "ALTER TIMESERIES root.sg1.d2.temperature RENAME TO root.view.d2.temperature");

      // Insert initial data into physical paths
      statement.execute(
          "INSERT INTO root.sg1.d1(timestamp, pressure) VALUES (1509465600012, 1013.25)");
      statement.execute(
          "INSERT INTO root.sg1.d2(timestamp, humidity) VALUES (1509465600012, 65.5)");

      // Create aligned time series
      statement.execute(
          "CREATE ALIGNED TIMESERIES root.sg1.d4(s1 FLOAT encoding=RLE compression=SNAPPY, s2 INT32 encoding=GORILLA compression=LZ4, s3 DOUBLE encoding=PLAIN compression=SNAPPY)");

      // Create alias series from aligned time series (RENAME operation)
      statement.execute("ALTER TIMESERIES root.sg1.d4.s1 RENAME TO root.view.d4.s1");
      statement.execute("ALTER TIMESERIES root.sg1.d4.s2 RENAME TO root.view.d4.s2");
      statement.execute("ALTER TIMESERIES root.sg1.d4.s3 RENAME TO root.view.d4.s3");
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // ============================================================================
  // Test: Insert single row into alias series
  // ============================================================================

  @Test
  public void testInsertSingleRowIntoAliasSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // Insert into alias series
      statement.execute(
          "INSERT INTO root.view.d1(timestamp, temperature) VALUES (1509465601000, 25.5)");

      // Verify data can be queried from alias series
      String expectedHeader = "Time,root.view.d1.temperature,";
      String[] retArray = new String[] {"1509465601000,25.5,"};

      resultSetEqualTest(
          "SELECT temperature FROM root.view.d1 WHERE time = 1509465601000",
          expectedHeader,
          retArray);
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  // ============================================================================
  // Test: Insert multiple rows into alias series
  // ============================================================================

  @Test
  public void testInsertMultipleRowsIntoAliasSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // Insert multiple rows into alias series
      statement.execute(
          "INSERT INTO root.view.d2(timestamp, temperature) VALUES (1509465601000, 22.5)");
      statement.execute(
          "INSERT INTO root.view.d2(timestamp, temperature) VALUES (1509465602000, 23.0)");
      statement.execute(
          "INSERT INTO root.view.d2(timestamp, temperature) VALUES (1509465603000, 23.5)");

      // Verify data can be queried
      String expectedHeader = "Time,root.view.d2.temperature,";
      String[] retArray =
          new String[] {"1509465601000,22.5,", "1509465602000,23.0,", "1509465603000,23.5,"};

      resultSetEqualTest("SELECT temperature FROM root.view.d2", expectedHeader, retArray);
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  // ============================================================================
  // Test: Insert into disabled series (should fail)
  // ============================================================================

  @Test
  public void testInsertIntoDisabledSeries() {
    // Attempting to insert into the original physical path (which is now disabled)
    // should fail
    assertNonQueryTestFail(
        "INSERT INTO root.sg1.d1(timestamp, temperature) VALUES (1509465603000, 27.0)",
        "Cannot insert data into invalid series: root.sg1.d1.temperature");
  }

  // ============================================================================
  // Test: Mixed inserts (alias series and physical series)
  // ============================================================================

  @Test
  public void testMixedInsertAliasAndPhysicalSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // Insert into alias series
      statement.execute(
          "INSERT INTO root.view.d1(timestamp, temperature) VALUES (1509465603000, 27.0)");
      // Insert into physical series (pressure was not renamed)
      statement.execute(
          "INSERT INTO root.sg1.d1(timestamp, pressure) VALUES (1509465603000, 1015.0)");

      // Verify both can be queried
      String expectedHeader = "Time,root.view.d1.temperature,root.sg1.d1.pressure,";
      String[] retArray = new String[] {"1509465603000,27.0,1015.0,"};

      resultSetEqualTest(
          "SELECT temperature, pressure FROM root.view.d1, root.sg1.d1 WHERE time = 1509465603000",
          expectedHeader,
          retArray);
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  // ============================================================================
  // Test: Insert with different data types
  // ============================================================================

  @Test
  public void testInsertDifferentDataTypes() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // Create time series with different data types
      statement.execute(
          "CREATE TIMESERIES root.sg1.d3.s1 WITH DATATYPE=INT32, ENCODING=RLE, COMPRESSION=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d3.s2 WITH DATATYPE=INT64, ENCODING=RLE, COMPRESSION=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d3.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN, COMPRESSION=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d3.s4 WITH DATATYPE=TEXT, ENCODING=PLAIN, COMPRESSION=SNAPPY");

      // Create alias series
      statement.execute("ALTER TIMESERIES root.sg1.d3.s1 RENAME TO root.view.d3.s1");
      statement.execute("ALTER TIMESERIES root.sg1.d3.s2 RENAME TO root.view.d3.s2");
      statement.execute("ALTER TIMESERIES root.sg1.d3.s3 RENAME TO root.view.d3.s3");
      statement.execute("ALTER TIMESERIES root.sg1.d3.s4 RENAME TO root.view.d3.s4");

      // Insert data into alias series
      statement.execute(
          "INSERT INTO root.view.d3(timestamp, s1, s2, s3, s4) VALUES (1509465600012, 100, 200, true, 'test')");

      // Verify data
      String expectedHeader =
          "Time,root.view.d3.s1,root.view.d3.s2,root.view.d3.s3,root.view.d3.s4,";
      String[] retArray = new String[] {"1509465600012,100,200,true,test,"};

      resultSetEqualTest("SELECT s1, s2, s3, s4 FROM root.view.d3", expectedHeader, retArray);
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  // ============================================================================
  // Test: Insert into non-existent alias series (should fail)
  // ============================================================================

  @Test
  public void testInsertIntoNonExistentAliasSeries() {
    assertNonQueryTestFail(
        "INSERT INTO root.sg1.d2(timestamp, temperature) VALUES (1509465600012, 25.0)",
        "Cannot insert data into invalid series");
  }

  // ============================================================================
  // Test: Insert using Session API (Client API)
  // ============================================================================

  @Test
  public void testInsertRecordUsingSession() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String deviceId = "root.view.d1";
      List<String> measurements = new ArrayList<>();
      measurements.add("temperature");
      List<TSDataType> dataTypes = new ArrayList<>();
      dataTypes.add(TSDataType.FLOAT);
      List<Object> values = new ArrayList<>();
      values.add(30.5f);

      // Insert record into alias series using Session API
      session.insertRecord(deviceId, 1509465605000L, measurements, dataTypes, values);

      // Verify data can be queried
      try (SessionDataSet dataSet =
          session.executeQueryStatement(
              "SELECT temperature FROM root.view.d1 WHERE time = 1509465605000")) {
        assertTrue(dataSet.hasNext());
        assertEquals(30.5f, dataSet.next().getFields().get(0).getFloatV(), 0.01f);
        assertFalse(dataSet.hasNext());
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testInsertRecordsUsingSession() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String deviceId = "root.view.d2";
      List<String> measurements = new ArrayList<>();
      measurements.add("temperature");
      List<TSDataType> dataTypes = new ArrayList<>();
      dataTypes.add(TSDataType.FLOAT);

      List<String> deviceIds = new ArrayList<>();
      List<Long> times = new ArrayList<>();
      List<List<String>> measurementsList = new ArrayList<>();
      List<List<TSDataType>> typesList = new ArrayList<>();
      List<List<Object>> valuesList = new ArrayList<>();

      for (int i = 0; i < 3; i++) {
        deviceIds.add(deviceId);
        times.add(1509465606000L + i * 1000);
        measurementsList.add(measurements);
        typesList.add(dataTypes);
        List<Object> values = new ArrayList<>();
        values.add(24.0f + i * 0.5f);
        valuesList.add(values);
      }

      // Insert multiple records into alias series using Session API
      session.insertRecords(deviceIds, times, measurementsList, typesList, valuesList);

      // Verify data can be queried
      try (SessionDataSet dataSet =
          session.executeQueryStatement(
              "SELECT temperature FROM root.view.d2 WHERE time >= 1509465606000")) {
        int count = 0;
        while (dataSet.hasNext()) {
          float value = dataSet.next().getFields().get(0).getFloatV();
          assertEquals(24.0f + count * 0.5f, value, 0.01f);
          count++;
        }
        assertEquals(3, count);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testInsertTabletUsingSession() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String deviceId = "root.view.d1";
      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("temperature", TSDataType.FLOAT));

      Tablet tablet = new Tablet(deviceId, schemaList, 10);
      long[] timestamps = tablet.getTimestamps();
      Object[] values = tablet.getValues();
      int rowIndex = 0;

      for (int i = 0; i < 3; i++) {
        rowIndex = tablet.getRowSize();
        timestamps[rowIndex] = 1509465607000L + i * 1000;
        ((float[]) values[0])[rowIndex] = 31.0f + i * 0.5f;
        tablet.setRowSize(tablet.getRowSize() + 1);
      }

      // Insert tablet into alias series using Session API
      session.insertTablet(tablet);

      // Verify data can be queried
      try (SessionDataSet dataSet =
          session.executeQueryStatement(
              "SELECT temperature FROM root.view.d1 WHERE time >= 1509465607000")) {
        int count = 0;
        while (dataSet.hasNext()) {
          float value = dataSet.next().getFields().get(0).getFloatV();
          assertEquals(31.0f + count * 0.5f, value, 0.01f);
          count++;
        }
        assertEquals(3, count);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testInsertRecordIntoDisabledSeriesUsingSession() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String deviceId = "root.sg1.d1";
      List<String> measurements = new ArrayList<>();
      measurements.add("temperature");
      List<TSDataType> dataTypes = new ArrayList<>();
      dataTypes.add(TSDataType.FLOAT);
      List<Object> values = new ArrayList<>();
      values.add(27.0f);

      // Attempting to insert into the original physical path (which is now disabled)
      // should fail
      try {
        session.insertRecord(deviceId, 1509465608000L, measurements, dataTypes, values);
        fail("Expected exception when inserting into disabled series");
      } catch (Exception e) {
        assertTrue(
            e.getMessage().contains("Cannot insert data into invalid series")
                || e.getMessage().contains("disabled"));
      }
    } catch (Exception e) {
      // Connection exception is acceptable
    }
  }

  // ============================================================================
  // Test: Insert into renamed aligned time series
  // ============================================================================

  @Test
  public void testInsertIntoRenamedAlignedSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // Get current count of data points before insertion
      long initialCount = 0;
      try (ResultSet countResult = statement.executeQuery("SELECT COUNT(s1) FROM root.view.d4")) {
        if (countResult.next()) {
          initialCount = countResult.getLong(1);
        }
      }

      // Insert into renamed aligned series using SQL
      statement.execute(
          "INSERT INTO root.view.d4(timestamp, s1, s2, s3) ALIGNED VALUES (1509465609000, 10.5, 100, 20.5)");
      statement.execute(
          "INSERT INTO root.view.d4(timestamp, s1, s2, s3) ALIGNED VALUES (1509465610000, 11.0, 200, 21.0)");

      // Verify total count after insertion
      try (ResultSet countResult = statement.executeQuery("SELECT COUNT(s1) FROM root.view.d4")) {
        assertTrue(countResult.next());
        long finalCount = countResult.getLong(1);
        assertEquals(initialCount + 2, finalCount);
      }

      // Verify the newly inserted data can be queried
      String expectedHeader = "Time,root.view.d4.s1,root.view.d4.s2,root.view.d4.s3,";
      String[] retArray =
          new String[] {
            "1509465609000,10.5,100,20.5,", "1509465610000,11.0,200,21.0,",
          };

      resultSetEqualTest(
          "SELECT s1, s2, s3 FROM root.view.d4 WHERE time = 1509465609000 OR time = 1509465610000",
          expectedHeader,
          retArray);
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testInsertIntoRenamedAlignedSeriesPartialMeasurements() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // Insert into renamed aligned series with partial measurements
      statement.execute(
          "INSERT INTO root.view.d4(timestamp, s1, s2) ALIGNED VALUES (1509465611000, 12.5, 300)");
      statement.execute(
          "INSERT INTO root.view.d4(timestamp, s1, s3) ALIGNED VALUES (1509465612000, 13.0, 22.5)");

      // Verify data can be queried
      String expectedHeader = "Time,root.view.d4.s1,root.view.d4.s2,root.view.d4.s3,";
      String[] retArray =
          new String[] {
            "1509465611000,12.5,300,null,", "1509465612000,13.0,null,22.5,",
          };

      resultSetEqualTest(
          "SELECT s1, s2, s3 FROM root.view.d4 WHERE time >= 1509465611000",
          expectedHeader,
          retArray);
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testQueryRenamedAlignedSeriesWithNonAlignedSeriesOnAliasDevice() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.renameAlignedMix");
      statement.execute("CREATE ALIGNED TIMESERIES root.renameAlignedMix.d1(s1 INT32, s2 TEXT)");
      statement.execute(
          "ALTER TIMESERIES root.renameAlignedMix.d1.s1 RENAME TO root.renameAlignedMix.d2.s1");
      statement.execute(
          "ALTER TIMESERIES root.renameAlignedMix.d1.s2 RENAME TO root.renameAlignedMix.d2.s2");
      statement.execute("CREATE TIMESERIES root.renameAlignedMix.d2.s3 FLOAT");
      statement.execute(
          "INSERT INTO root.renameAlignedMix.d2(timestamp, s1, s2, s3) VALUES (1, 1, \"a\", 1.1)");

      String expectedHeader =
          "Time,root.renameAlignedMix.d2.s3,root.renameAlignedMix.d2.s1,root.renameAlignedMix.d2.s2,";
      String[] retArray = new String[] {"1,1.1,1,a,"};

      resultSetEqualTest("SELECT * FROM root.renameAlignedMix.**", expectedHeader, retArray);
      resultSetEqualTest("SELECT d2.** FROM root.renameAlignedMix", expectedHeader, retArray);
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testInsertIntoRenamedAlignedSeriesUsingSession() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String deviceId = "root.view.d4";
      List<String> measurements = new ArrayList<>();
      measurements.add("s1");
      measurements.add("s2");
      measurements.add("s3");
      List<TSDataType> dataTypes = new ArrayList<>();
      dataTypes.add(TSDataType.FLOAT);
      dataTypes.add(TSDataType.INT32);
      dataTypes.add(TSDataType.DOUBLE);
      List<Object> values = new ArrayList<>();
      values.add(14.5f);
      values.add(400);
      values.add(23.5);

      // Insert record into renamed aligned series using Session API
      session.insertAlignedRecord(deviceId, 1509465613000L, measurements, dataTypes, values);

      // Verify data can be queried
      try (SessionDataSet dataSet =
          session.executeQueryStatement(
              "SELECT s1, s2, s3 FROM root.view.d4 WHERE time = 1509465613000")) {
        assertTrue(dataSet.hasNext());
        List<org.apache.tsfile.read.common.Field> fields = dataSet.next().getFields();
        assertEquals(14.5f, fields.get(0).getFloatV(), 0.01f);
        assertEquals(400, fields.get(1).getIntV());
        assertEquals(23.5, fields.get(2).getDoubleV(), 0.01);
        assertFalse(dataSet.hasNext());
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testInsertAlignedTabletIntoRenamedSeries() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String deviceId = "root.view.d4";
      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("s1", TSDataType.FLOAT));
      schemaList.add(new MeasurementSchema("s2", TSDataType.INT32));
      schemaList.add(new MeasurementSchema("s3", TSDataType.DOUBLE));

      Tablet tablet = new Tablet(deviceId, schemaList, 10);
      long[] timestamps = tablet.getTimestamps();
      Object[] values = tablet.getValues();

      for (int i = 0; i < 3; i++) {
        int rowIndex = tablet.getRowSize();
        timestamps[rowIndex] = 1509465614000L + i * 1000;
        ((float[]) values[0])[rowIndex] = 15.0f + i * 0.5f;
        ((int[]) values[1])[rowIndex] = 500 + i * 100;
        ((double[]) values[2])[rowIndex] = 24.0 + i * 0.5;
        tablet.setRowSize(tablet.getRowSize() + 1);
      }

      // Insert aligned tablet into renamed aligned series using Session API
      session.insertAlignedTablet(tablet);

      // Verify data can be queried
      try (SessionDataSet dataSet =
          session.executeQueryStatement(
              "SELECT s1, s2, s3 FROM root.view.d4 WHERE time >= 1509465614000")) {
        int count = 0;
        while (dataSet.hasNext()) {
          List<org.apache.tsfile.read.common.Field> fields = dataSet.next().getFields();
          assertEquals(15.0f + count * 0.5f, fields.get(0).getFloatV(), 0.01f);
          assertEquals(500 + count * 100, fields.get(1).getIntV());
          assertEquals(24.0 + count * 0.5, fields.get(2).getDoubleV(), 0.01);
          count++;
        }
        assertEquals(3, count);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testInsertIntoDisabledAlignedSeries() {
    // Attempting to insert into the original physical aligned path (which is now disabled)
    // should fail
    assertNonQueryTestFail(
        "INSERT INTO root.sg1.d4(timestamp, s1, s2, s3) VALUES (1509465615000, 16.0, 600, 25.0)",
        "Cannot insert data into invalid series: root.sg1.d4.s1");
  }
}
