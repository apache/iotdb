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

package org.apache.iotdb.relational.it.query.recent;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class})
public class IoTDBTableFilteredRowsIT {

  private static final String TABLE_DATABASE = "filter_info";
  private static final int MAX_NUMBER_OF_POINTS_IN_PAGE = 10;
  private final List<String> targetKeys =
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
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("create database " + TABLE_DATABASE);
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void prepareData(ITableSession session, String device, boolean isFlush)
      throws IoTDBConnectionException, StatementExecutionException {

    // for device1
    session.executeNonQueryStatement("use " + TABLE_DATABASE);

    generateTimeRangeWithTimestamp(session, device, 1, 100, isFlush);

    generateTimeRangeWithTimestamp(session, device, 101, 200, isFlush);

    generateTimeRangeWithTimestamp(session, device, 201, 300, isFlush);

    generateTimeRangeWithTimestamp(session, device, 301, 400, isFlush);
  }

  private void verifyExplainMetrics(
      ITableSession session,
      String condition,
      long expectedTimeSeries,
      long expectedChunk,
      long expectedPage,
      long expectedRowScan,
      String device)
      throws IoTDBConnectionException, StatementExecutionException {

    SessionDataSet sessionDataSet =
        session.executeQueryStatement(
            "explain analyze verbose select * from " + device + " where " + condition);
    SessionDataSet.DataIterator iterator = sessionDataSet.iterator();

    StringBuilder stringBuilder = new StringBuilder();
    while (iterator.next()) {
      stringBuilder.append(iterator.getString(1)).append(System.lineSeparator());
    }

    String[] allInfo = stringBuilder.toString().split(System.lineSeparator());
    List<Long> filteredRows =
        Arrays.stream(allInfo)
            .filter(line -> targetKeys.stream().anyMatch(line::contains))
            .map(line -> Long.parseLong(line.split(":")[1].trim()))
            .collect(Collectors.toList());

    assertEquals(
        "timeSeriesIndexFilteredRows mismatch " + condition,
        expectedTimeSeries,
        (long) filteredRows.get(0));
    assertEquals(
        "chunkIndexFilteredRows mismatch " + condition, expectedChunk, (long) filteredRows.get(1));
    assertEquals(
        "pageIndexFilteredRows mismatch " + condition, expectedPage, (long) filteredRows.get(2));
    assertEquals(
        "rowScanFilteredRows mismatch " + condition, expectedRowScan, (long) filteredRows.get(3));
  }

  @Test
  public void testReadDataFromDisk() throws Exception {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("use " + TABLE_DATABASE);
      String device_for_disk = "device1";
      prepareData(session, device_for_disk, true);

      // 1. test INT32
      verifyExplainMetrics(session, "col_int32 > 247", 200, 0, 40, 7, device_for_disk);

      // 2. test INT64
      verifyExplainMetrics(session, "col_int64 > 123", 100, 0, 20, 3, device_for_disk);

      // 3. test FLOAT
      verifyExplainMetrics(session, "col_float > 315.5", 300, 0, 10, 5, device_for_disk);

      // 4. test DOUBLE
      verifyExplainMetrics(session, "col_double > 292.5", 200, 0, 90, 2, device_for_disk);

      // 5. test BOOLEAN
      verifyExplainMetrics(session, "col_boolean = true", 0, 0, 0, 200, device_for_disk);

      // 6. test STRING
      verifyExplainMetrics(session, "col_string = 'string_248'", 300, 0, 90, 9, device_for_disk);

      // 7. test TIME, the first tsfile (timeRange: 0-100), and second tsfile (timeRange : 100-200)
      // would be filtered and could not be counted
      verifyExplainMetrics(session, "time > 252", 0, 0, 50, 2, device_for_disk);

      // 8. AND condition
      verifyExplainMetrics(
          session, "col_int32 > 105 and col_int64 <= 375", 100, 0, 20, 10, device_for_disk);

      // 9. AND condition, the 250th row will be filtered by rowScanFilteredRows
      verifyExplainMetrics(
          session,
          "col_int32 > 150 AND col_string < 'string_250'",
          200,
          0,
          100,
          1,
          device_for_disk);

      // 10. or condition
      verifyExplainMetrics(
          session, "time > 105 or col_double > 376.5", 100, 0, 0, 5, device_for_disk);
    }
  }

  @Test
  public void testReadDataFromMemory() throws Exception {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("use " + TABLE_DATABASE);
      String device_for_memory = "device2";
      prepareData(session, device_for_memory, false);

      // 1. test INT32
      verifyExplainMetrics(session, "col_int32 > 247", 0, 0, 0, 247, device_for_memory);

      // 2. test INT64
      verifyExplainMetrics(session, "col_int64 > 123", 0, 0, 0, 123, device_for_memory);

      // 3. test FLOAT
      verifyExplainMetrics(session, "col_float > 315.5", 0, 0, 0, 315, device_for_memory);

      // 4. test DOUBLE
      verifyExplainMetrics(session, "col_double > 292.5", 0, 0, 0, 292, device_for_memory);

      // 5. test BOOLEAN
      verifyExplainMetrics(session, "col_boolean = true", 0, 0, 0, 200, device_for_memory);

      // 6. test STRING
      verifyExplainMetrics(session, "col_string = 'string_248'", 0, 0, 0, 399, device_for_memory);

      // 7. test TIME, all the rows will be counted
      verifyExplainMetrics(session, "time > 252", 0, 0, 0, 252, device_for_memory);

      // 8. AND condition
      verifyExplainMetrics(
          session, "col_int32 > 105 and col_int64 <= 375", 0, 0, 0, 130, device_for_memory);

      // 9. AND condition
      verifyExplainMetrics(
          session,
          "col_int32 > 150 AND col_string < 'string_250'",
          0,
          0,
          0,
          301,
          device_for_memory);

      // 10. or condition
      verifyExplainMetrics(
          session, "time > 105 or col_double > 376.5", 0, 0, 0, 105, device_for_memory);
    }
  }

  private static void generateTimeRangeWithTimestamp(
      ITableSession session, String device, long start, long end, boolean isFlush)
      throws IoTDBConnectionException, StatementExecutionException {
    List<IMeasurementSchema> measurementSchemas =
        Arrays.asList(
            new MeasurementSchema("col_int32", TSDataType.INT32),
            new MeasurementSchema("col_int64", TSDataType.INT64),
            new MeasurementSchema("col_boolean", TSDataType.BOOLEAN),
            new MeasurementSchema("col_float", TSDataType.FLOAT),
            new MeasurementSchema("col_double", TSDataType.DOUBLE),
            new MeasurementSchema("col_string", TSDataType.STRING));

    Tablet tablet = new Tablet(device, measurementSchemas);
    for (long currentTime = start; currentTime <= end; currentTime++) {
      int rowIndex = tablet.getRowSize();
      if (rowIndex == tablet.getMaxRowNumber()) {
        session.insert(tablet);
        tablet.reset();
        rowIndex = 0;
      }
      tablet.addTimestamp(rowIndex, currentTime);

      // INT32
      tablet.addValue(rowIndex, 0, (int) currentTime);
      // INT64
      tablet.addValue(rowIndex, 1, (long) currentTime);
      // BOOLEAN
      tablet.addValue(rowIndex, 2, currentTime % 2 == 0);
      // FLOAT
      tablet.addValue(rowIndex, 3, (float) currentTime + 0.1f);
      // DOUBLE
      tablet.addValue(rowIndex, 4, (double) currentTime + 0.1d);
      // STRING, pad with zeros (e.g: "string_001") to ensure consistent char lengths.
      tablet.addValue(rowIndex, 5, String.format("string_%03d", currentTime));
    }
    if (tablet.getRowSize() > 0) {
      session.insert(tablet);
    }
    if (isFlush) {
      session.executeNonQueryStatement("flush");
    }
  }
}
