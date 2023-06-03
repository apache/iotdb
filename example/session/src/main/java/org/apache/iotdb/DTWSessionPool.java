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

package org.apache.iotdb;

import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.Collections;
import java.util.List;
import java.util.Random;

public class DTWSessionPool {

  private static final String DATABASE = "root.database";
  private static final String S = "root.database.device.s";
  private static final String P = "root.database.device.p";

  private static final int BATCH_SIZE = 32768;
  private static final long TIMESTAMP_STRIDE = 1000L;
  private static final long DATA_START_TIME = 954475200000L;

  private static final int PATTERN_COUNT = 100;
  private static final int PATTERN_LENGTH = 20;
  private static final double PATTERN_RANGE = 100.0;

  private static final int SERIES_LENGTH = 10000;
  private static final double SERIES_RANGE = 1000.0;

  private static final String SQL =
      String.format(
          "select top_k_dtw_sliding_window(s, p, 'k'='%d') from root.database.device;",
          PATTERN_COUNT);

  private static SessionPool sessionPool;

  private static void constructSessionPool() {
    sessionPool =
        new SessionPool.Builder()
            .host("127.0.0.1")
            .port(6667)
            .user("root")
            .password("root")
            .maxSize(3)
            .build();
  }

  private static void registerSchema() {
    try {
      sessionPool.deleteDatabase(DATABASE);
      System.out.println("Delete database successfully");
    } catch (Exception e) {
      // ignore
    }
    try {
      sessionPool.createDatabase(DATABASE);
      sessionPool.createTimeseries(
          S, TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
      sessionPool.createTimeseries(
          P, TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
      System.out.println("Create timeseries successfully");
    } catch (Exception e) {
      // ignore
    }
  }

  private static void insertRecordByTablet()
      throws IoTDBConnectionException, StatementExecutionException {
    Random random = new Random();
    List<MeasurementSchema> sList =
        Collections.singletonList(new MeasurementSchema("s", TSDataType.DOUBLE));
    List<MeasurementSchema> pList =
        Collections.singletonList(new MeasurementSchema("p", TSDataType.DOUBLE));

    double[] pattern = new double[PATTERN_LENGTH];
    Tablet tablet = new Tablet("root.database.device", pList, BATCH_SIZE);
    for (int i = 0; i < PATTERN_LENGTH; i++) {
      // each point is a random double in [-100, 100]
      pattern[i] = PATTERN_RANGE * (2.0 * random.nextDouble() - 1.0);
      tablet.addTimestamp(i, i * TIMESTAMP_STRIDE);
      tablet.addValue("p", i, pattern[i]);
    }
    tablet.rowSize = PATTERN_LENGTH;
    sessionPool.insertTablet(tablet);
    System.out.println("Insert pattern successfully");

    long currentTime = DATA_START_TIME;
    for (int i = 0; i < PATTERN_COUNT; i++) {
      int rowCount = 0;
      tablet = new Tablet("root.database.device", sList, BATCH_SIZE);
      // each point is a random double in [-1000, 1000]
      for (int j = 0; j < SERIES_LENGTH; j++) {
        tablet.addTimestamp(rowCount, currentTime);
        tablet.addValue("s", rowCount, SERIES_RANGE * (2.0 * random.nextDouble() - 1.0));
        currentTime += TIMESTAMP_STRIDE;
        rowCount++;
      }

      for (int j = 0; j < PATTERN_LENGTH; j++) {
        // Construct a similar pattern
        int repeatTime = (int) Math.abs(random.nextGaussian()) + 1;
        if (j == 0 || j == PATTERN_LENGTH - 1) {
          // Make sure the first and last points are the same
          repeatTime = 1;
        }
        for (int k = 0; k < repeatTime; k++) {
          tablet.addTimestamp(rowCount, currentTime);
          tablet.addValue("s", rowCount, pattern[j]);
          currentTime += TIMESTAMP_STRIDE;
          rowCount++;
        }
      }

      tablet.rowSize = rowCount;
      sessionPool.insertTablet(tablet);
    }
    System.out.println("Insert series successfully");
  }

  private static void executeQuery() throws IoTDBConnectionException, StatementExecutionException {
    long startTime = System.currentTimeMillis();
    try (SessionDataSetWrapper wrapper = sessionPool.executeQueryStatement(SQL, 10 * 60 * 1000)) {
      while (wrapper.hasNext()) {
        RowRecord record = wrapper.next();
        System.out.println(record);
      }
    }
    long endTime = System.currentTimeMillis();

    System.out.printf(
        "Query series length: %d pattern length: %d cost: %fs%n",
        SERIES_LENGTH * PATTERN_COUNT, PATTERN_LENGTH, (double) (endTime - startTime) / 1000.0);
  }

  public static void main(String[] args)
      throws StatementExecutionException, IoTDBConnectionException {

    constructSessionPool();
    registerSchema();
    insertRecordByTablet();
    executeQuery();
    sessionPool.close();
  }
}
