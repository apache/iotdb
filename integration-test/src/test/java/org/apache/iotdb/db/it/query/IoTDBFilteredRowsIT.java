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

package org.apache.iotdb.db.it.query;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBFilteredRowsIT {

  private static final Logger log = LoggerFactory.getLogger(IoTDBFilteredRowsIT.class);

  private static final String DATABASE = "root.filter_info";
  private static final int MAX_NUMBER_OF_POINTS_IN_PAGE = 10;
  private final List<String> TARGET_KEYS =
      Arrays.asList(
          "timeSeriesIndexFilteredRows",
          "chunkIndexFilteredRows",
          "pageIndexFilteredRows",
          "rowScanFilteredRows");

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false)
        .setMaxNumberOfPointsInPage(MAX_NUMBER_OF_POINTS_IN_PAGE);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void prepareData(
      ISession session, String device, boolean isFlush, boolean isAligned)
      throws IoTDBConnectionException, StatementExecutionException {

    generateTimeRangeWithTimestamp(session, device, 1, 100, isFlush, isAligned);
    generateTimeRangeWithTimestamp(session, device, 101, 200, isFlush, isAligned);
    generateTimeRangeWithTimestamp(session, device, 201, 300, isFlush, isAligned);
    generateTimeRangeWithTimestamp(session, device, 301, 400, isFlush, isAligned);
  }

  private void verifyExplainMetrics(
      Connection connection,
      String condition,
      long expectedTimeSeries,
      long expectedChunk,
      long expectedPage,
      long expectedRowScan,
      String device)
      throws Exception {

    try (Statement statement = connection.createStatement()) {
      String sql = "explain analyze verbose select * from " + device + " where " + condition;
      log.info(sql);
      ResultSet resultSet = statement.executeQuery(sql);

      StringBuilder stringBuilder = new StringBuilder();
      while (resultSet.next()) {
        stringBuilder.append(resultSet.getString(1)).append(System.lineSeparator());
      }

      String[] allInfo = stringBuilder.toString().split(System.lineSeparator());
      List<Long> filteredRows =
          Arrays.stream(allInfo)
              .filter(line -> TARGET_KEYS.stream().anyMatch(line::contains))
              .map(line -> Long.parseLong(line.split(":")[1].trim()))
              .collect(Collectors.toList());

      assertEquals(
          "timeSeriesIndexFilteredRows mismatch " + condition,
          expectedTimeSeries,
          (long) filteredRows.get(0));
      assertEquals(
          "chunkIndexFilteredRows mismatch " + condition,
          expectedChunk,
          (long) filteredRows.get(1));
      assertEquals(
          "pageIndexFilteredRows mismatch " + condition, expectedPage, (long) filteredRows.get(2));
      assertEquals(
          "rowScanFilteredRows mismatch " + condition, expectedRowScan, (long) filteredRows.get(3));
    }
  }

  private static void generateTimeRangeWithTimestamp(
      ISession session, String device, long start, long end, boolean isFlush, boolean isAligned)
      throws IoTDBConnectionException, StatementExecutionException {

    // if the TSDataType is TEXT, the related filter (where col_text = 'text_001') would n
    List<IMeasurementSchema> measurementSchemas =
        Arrays.asList(
            new MeasurementSchema("s_int32", TSDataType.INT32),
            new MeasurementSchema("s_int64", TSDataType.INT64),
            new MeasurementSchema("s_boolean", TSDataType.BOOLEAN),
            new MeasurementSchema("s_float", TSDataType.FLOAT),
            new MeasurementSchema("s_double", TSDataType.DOUBLE),
            new MeasurementSchema("s_string", TSDataType.STRING));

    Tablet tablet = new Tablet(device, measurementSchemas);
    for (long currentTime = start; currentTime <= end; currentTime++) {
      int rowIndex = tablet.getRowSize();
      if (rowIndex == tablet.getMaxRowNumber()) {
        if (isAligned) {
          session.insertAlignedTablet(tablet);
        } else {
          session.insertTablet(tablet);
        }
        tablet.reset();
        rowIndex = 0;
      }
      tablet.addTimestamp(rowIndex, currentTime);
      tablet.addValue("s_int32", rowIndex, (int) currentTime);
      tablet.addValue("s_int64", rowIndex, (long) currentTime);
      tablet.addValue("s_boolean", rowIndex, currentTime % 2 == 0);
      tablet.addValue("s_float", rowIndex, (float) currentTime + 0.1f);
      tablet.addValue("s_double", rowIndex, (double) currentTime + 0.1d);
      tablet.addValue("s_string", rowIndex, String.format("string_%03d", currentTime));
    }
    if (tablet.getRowSize() > 0) {
      if (isAligned) {
        session.insertAlignedTablet(tablet);
      } else {
        session.insertTablet(tablet);
      }
    }
    if (isFlush) {
      session.executeNonQueryStatement("flush");
    }
  }

  @Test
  public void testUnAlignedReadDataFromDisk() throws Exception {
    try (ISession session = EnvFactory.getEnv().getSessionConnection();
        Connection connection = EnvFactory.getEnv().getConnection()) {
      String device_for_disk = DATABASE + ".device1";
      prepareData(session, device_for_disk, true, false);

      // 1. test INT32
      verifyExplainMetrics(connection, "s_int32 > 247", 200, 0, 40, 7, device_for_disk);

      // 2. test INT64
      verifyExplainMetrics(connection, "s_int64 > 123", 100, 0, 20, 3, device_for_disk);

      // 3. test FLOAT
      verifyExplainMetrics(connection, "s_float > 315.5", 300, 0, 10, 5, device_for_disk);

      // 4. test DOUBLE
      verifyExplainMetrics(connection, "s_double > 292.5", 200, 0, 90, 2, device_for_disk);

      // 5. test BOOLEAN
      verifyExplainMetrics(connection, "s_boolean = true", 0, 0, 0, 200, device_for_disk);

      // 6. test STRING
      verifyExplainMetrics(connection, "s_string = 'string_248'", 300, 0, 90, 9, device_for_disk);

      // 7. test TIME, the first tsfile (timeRange: 0-100), and second tsfile (timeRange : 100-200)
      // would be filtered and could not be counted
      // For non-aligned series, each measurement is filtered independently
      // before being merged into a row, so filtered row counts are accumulated per measurement.
      verifyExplainMetrics(connection, "time > 252", 0, 0, 50 * 6, 2 * 6, device_for_disk);

      // 8. AND condition
      verifyExplainMetrics(
          connection, "s_int32 > 105 and s_int64 <= 375", 100, 0, 20, 10, device_for_disk);

      // 9. AND condition, the 250th row will be filtered by rowScanFilteredRows
      verifyExplainMetrics(
          connection, "s_int32 > 150 AND s_string < 'string_250'", 200, 0, 100, 1, device_for_disk);

      // 10. or condition
      verifyExplainMetrics(
          connection, "time > 105 or s_double > 376.5", 100, 0, 0, 5, device_for_disk);
    }
  }

  @Test
  public void testUnAlignedReadDataFromMemory() throws Exception {
    try (ISession session = EnvFactory.getEnv().getSessionConnection();
        Connection connection = EnvFactory.getEnv().getConnection()) {
      String device_for_memory = DATABASE + ".device2";
      prepareData(session, device_for_memory, false, false);

      // 1. test INT32
      verifyExplainMetrics(connection, "s_int32 > 247", 0, 0, 0, 247, device_for_memory);

      // 2. test INT64
      verifyExplainMetrics(connection, "s_int64 > 123", 0, 0, 0, 123, device_for_memory);

      // 3. test FLOAT
      verifyExplainMetrics(connection, "s_float > 315.5", 0, 0, 0, 315, device_for_memory);

      // 4. test DOUBLE
      verifyExplainMetrics(connection, "s_double > 292.5", 0, 0, 0, 292, device_for_memory);

      // 5. test BOOLEAN
      verifyExplainMetrics(connection, "s_boolean = true", 0, 0, 0, 200, device_for_memory);

      // 6. test STRING
      verifyExplainMetrics(connection, "s_string = 'string_248'", 0, 0, 0, 399, device_for_memory);

      // 7. test TIME, all the rows will be counted
      verifyExplainMetrics(connection, "time > 252", 0, 0, 0, 252 * 6, device_for_memory);

      // 8. AND condition
      verifyExplainMetrics(
          connection, "s_int32 > 105 and s_int64 <= 375", 0, 0, 0, 130, device_for_memory);

      // 9. AND condition
      verifyExplainMetrics(
          connection, "s_int32 > 150 AND s_string < 'string_250'", 0, 0, 0, 301, device_for_memory);

      // 10. or condition
      verifyExplainMetrics(
          connection, "time > 105 or s_double > 376.5", 0, 0, 0, 105, device_for_memory);
    }
  }

  @Test
  public void testReadAlignedDataFromDisk() throws Exception {
    try (ISession session = EnvFactory.getEnv().getSessionConnection();
        Connection connection = EnvFactory.getEnv().getConnection()) {
      String aligned_device_for_disk = DATABASE + ".aligned_device1";
      prepareData(session, aligned_device_for_disk, true, true);

      // 1. test INT32
      verifyExplainMetrics(connection, "s_int32 > 247", 200, 0, 40, 7, aligned_device_for_disk);

      // 2. test INT64
      verifyExplainMetrics(connection, "s_int64 > 123", 100, 0, 20, 3, aligned_device_for_disk);

      // 3. test FLOAT
      verifyExplainMetrics(connection, "s_float > 315.5", 300, 0, 10, 5, aligned_device_for_disk);

      // 4. test DOUBLE
      verifyExplainMetrics(connection, "s_double > 292.5", 200, 0, 90, 2, aligned_device_for_disk);

      // 5. test BOOLEAN
      verifyExplainMetrics(connection, "s_boolean = true", 0, 0, 0, 200, aligned_device_for_disk);

      // 6. test STRING
      verifyExplainMetrics(
          connection, "s_string = 'string_248'", 300, 0, 90, 9, aligned_device_for_disk);

      // 7. test TIME
      verifyExplainMetrics(connection, "time > 252", 0, 0, 50, 2, aligned_device_for_disk);

      // 8. AND condition
      verifyExplainMetrics(
          connection, "s_int32 > 105 and s_int64 <= 375", 100, 0, 20, 10, aligned_device_for_disk);

      // 9. AND condition
      verifyExplainMetrics(
          connection,
          "s_int32 > 150 AND s_string < 'string_250'",
          200,
          0,
          100,
          1,
          aligned_device_for_disk);

      // 10. or condition
      verifyExplainMetrics(
          connection, "time > 105 or s_double > 376.5", 100, 0, 0, 5, aligned_device_for_disk);
    }
  }

  @Test
  public void testReadAlignedDataFromMemory() throws Exception {
    try (ISession session = EnvFactory.getEnv().getSessionConnection();
        Connection connection = EnvFactory.getEnv().getConnection()) {
      String aligned_device_for_memory = DATABASE + ".aligned_device2";
      prepareData(session, aligned_device_for_memory, false, true);

      // 1. test INT32
      verifyExplainMetrics(connection, "s_int32 > 247", 0, 0, 0, 247, aligned_device_for_memory);

      // 2. test INT64
      verifyExplainMetrics(connection, "s_int64 > 123", 0, 0, 0, 123, aligned_device_for_memory);

      // 3. test FLOAT
      verifyExplainMetrics(connection, "s_float > 315.5", 0, 0, 0, 315, aligned_device_for_memory);

      // 4. test DOUBLE
      verifyExplainMetrics(connection, "s_double > 292.5", 0, 0, 0, 292, aligned_device_for_memory);

      // 5. test BOOLEAN
      verifyExplainMetrics(connection, "s_boolean = true", 0, 0, 0, 200, aligned_device_for_memory);

      // 6. test STRING
      verifyExplainMetrics(
          connection, "s_string = 'string_248'", 0, 0, 0, 399, aligned_device_for_memory);

      // 7. test TIME
      verifyExplainMetrics(connection, "time > 252", 0, 0, 0, 252, aligned_device_for_memory);

      // 8. AND condition
      verifyExplainMetrics(
          connection, "s_int32 > 105 and s_int64 <= 375", 0, 0, 0, 130, aligned_device_for_memory);

      // 9. AND condition
      verifyExplainMetrics(
          connection,
          "s_int32 > 150 AND s_string < 'string_250'",
          0,
          0,
          0,
          301,
          aligned_device_for_memory);

      // 10. or condition
      verifyExplainMetrics(
          connection, "time > 105 or s_double > 376.5", 0, 0, 0, 105, aligned_device_for_memory);
    }
  }
}
