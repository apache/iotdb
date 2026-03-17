package org.apache.iotdb.relational.it.query.recent;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
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
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBTableFilteredRowsIT {

  private static final String TABLE_DATABASE = "filter_info";
  private static final int max_number_of_points_in_page = 10;
  private static final String DEVICE1 = "device1";
  private static final List<String> targetKeys =
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
        .setMaxNumberOfPointsInPage(max_number_of_points_in_page);
    EnvFactory.getEnv().initClusterEnvironment();
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("create database " + TABLE_DATABASE);
      prepareData(session);
    }
  }

  private static void prepareData(ITableSession session)
      throws IoTDBConnectionException, StatementExecutionException {

    // for device1
    session.executeNonQueryStatement("use " + TABLE_DATABASE);

    generateTimeRangeWithTimestamp(session, DEVICE1, 1, 100, true);

    generateTimeRangeWithTimestamp(session, DEVICE1, 101, 200, true);

    generateTimeRangeWithTimestamp(session, DEVICE1, 201, 300, true);

    generateTimeRangeWithTimestamp(session, DEVICE1, 301, 400, true);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private void verifyExplainMetrics(
      ITableSession session,
      String condition,
      long expectedTimeSeries,
      long expectedChunk,
      long expectedPage,
      long expectedRowScan)
      throws IoTDBConnectionException, StatementExecutionException {

    SessionDataSet sessionDataSet =
        session.executeQueryStatement(
            "explain analyze verbose select * from " + DEVICE1 + " where " + condition);
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
        "timeSeriesIndexFilteredRows wrong for " + condition,
        expectedTimeSeries,
        (long) filteredRows.get(0));
    assertEquals(
        "chunkIndexFilteredRows wrong for " + condition, expectedChunk, (long) filteredRows.get(1));
    assertEquals(
        "pageIndexFilteredRows wrong for " + condition, expectedPage, (long) filteredRows.get(2));
    assertEquals(
        "rowScanFilteredRows wrong for " + condition, expectedRowScan, (long) filteredRows.get(3));
  }

  @Test
  public void testReadDataFromDiskVariousConditions() throws Exception {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("use " + TABLE_DATABASE);

      // 1. test INT32
      verifyExplainMetrics(session, "col_int32 > 247", 200, 0, 40, 7);

      // 2. test INT64
      verifyExplainMetrics(session, "col_int64 > 123", 100, 0, 20, 3);

      // 3. test FLOAT
      verifyExplainMetrics(session, "col_float > 315.5", 300, 0, 10, 5);

      // 4. test DOUBLE
      verifyExplainMetrics(session, "col_double > 90.5", 0, 0, 90, 0);

      // 5. test BOOLEAN
      verifyExplainMetrics(session, "col_boolean = true", 0, 0, 0, 200);

      // 6. test STRING
      verifyExplainMetrics(session, "col_string = 'string_248'", 300, 0, 90, 9);

      // 7. test TIME
      verifyExplainMetrics(session, "time > 252", 0, 0, 50, 2);
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
