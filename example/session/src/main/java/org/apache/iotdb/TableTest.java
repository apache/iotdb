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

import org.apache.iotdb.isession.IPooledSession;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TableTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableTest.class);

  private static final String TABLE_SQL_DIALECT = "table";

  private static final AtomicInteger deviceIdGenerator = new AtomicInteger(0);

  private static final List<IMeasurementSchema> TABLE_SCHEMA_LIST = new ArrayList<>();
  private static final List<Tablet.ColumnType> TABLE_COLUMN_TYPES =
      Arrays.asList(
          Tablet.ColumnType.ID,
          Tablet.ColumnType.ID,
          Tablet.ColumnType.ID,
          //          Tablet.ColumnType.ATTRIBUTE,
          Tablet.ColumnType.MEASUREMENT,
          Tablet.ColumnType.MEASUREMENT,
          Tablet.ColumnType.MEASUREMENT);

  private static final String COLUMN_NAME_1 = "city";
  private static final String COLUMN_NAME_2 = "region";
  private static final String COLUMN_NAME_3 = "device_id";
  private static final String COLUMN_NAME_4 = "color";
  private static final String COLUMN_NAME_5 = "s1";
  private static final String COLUMN_NAME_6 = "s2";
  private static final String COLUMN_NAME_7 = "s3";

  private static final List<IMeasurementSchema> TREE_SCHEMA_LIST = new ArrayList<>();

  static {
    TABLE_SCHEMA_LIST.add(new MeasurementSchema(COLUMN_NAME_1, TSDataType.STRING));
    TABLE_SCHEMA_LIST.add(new MeasurementSchema(COLUMN_NAME_2, TSDataType.STRING));
    TABLE_SCHEMA_LIST.add(new MeasurementSchema(COLUMN_NAME_3, TSDataType.STRING));
    //    TABLE_SCHEMA_LIST.add(new MeasurementSchema(COLUMN_NAME_4, TSDataType.STRING));
    TABLE_SCHEMA_LIST.add(new MeasurementSchema(COLUMN_NAME_5, TSDataType.DOUBLE));
    TABLE_SCHEMA_LIST.add(new MeasurementSchema(COLUMN_NAME_6, TSDataType.DOUBLE));
    TABLE_SCHEMA_LIST.add(new MeasurementSchema(COLUMN_NAME_7, TSDataType.DOUBLE));

    TREE_SCHEMA_LIST.add(new MeasurementSchema(COLUMN_NAME_5, TSDataType.DOUBLE));
    TREE_SCHEMA_LIST.add(new MeasurementSchema(COLUMN_NAME_6, TSDataType.DOUBLE));
    TREE_SCHEMA_LIST.add(new MeasurementSchema(COLUMN_NAME_7, TSDataType.DOUBLE));
  }

  // 2024-10-01T00:00:00+08:00
  private static final long START_TIME = 1727712000000L;

  public static void main(String[] args) {

    // table
    String sqlDialect = args[0];
    // 127.0.0.1
    String ip = args[1];
    // 10
    int maxSize = Integer.parseInt(args[2]);

    String database = args[3];

    final int deviceNum = Integer.parseInt(args[4]);

    SessionPool sessionPool =
        new SessionPool.Builder()
            .host(ip)
            .port(6667)
            .user("root")
            .password("root")
            .maxSize(maxSize)
            .sqlDialect(sqlDialect)
            .database(database)
            .build();

    long startTime = System.nanoTime();
    List<Thread> subThreads = new ArrayList<>(maxSize);
    if (TABLE_SQL_DIALECT.equalsIgnoreCase(sqlDialect)) {
      // CREATE TABLE table1(city STRING ID, region STRING ID, device_id STRING ID, color STRING
      // ATTRIBUTE, s1 DOUBLE MEASUREMENT, s2 DOUBLE MEASUREMENT, s3 DOUBLE MEASUREMENT)
      for (int i = 0; i < maxSize; i++) {
        Thread t = new Thread(() -> writeTable(sessionPool, deviceNum));
        subThreads.add(t);
        t.start();
      }
    } else {
      for (int i = 0; i < maxSize; i++) {
        Thread t = new Thread(() -> writeTree(sessionPool, deviceNum, database));
        subThreads.add(t);
        t.start();
      }
    }
    subThreads.forEach(
        t -> {
          try {
            t.join();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        });
    LOGGER.info(
        "{} writing {} devices costs {}ms", sqlDialect, deviceNum, System.nanoTime() - startTime);
  }

  private static void writeTable(final SessionPool sessionPool, final int deviceNum) {

    try (IPooledSession session = sessionPool.getPooledSession()) {
      while (true) {
        long startTime = System.nanoTime();
        int device = deviceIdGenerator.getAndIncrement();
        if (device >= deviceNum) {
          break;
        }
        int city = device % 10;
        int region = device % 100;
        int color = device % 5;

        Tablet tablet = new Tablet("table1", TABLE_SCHEMA_LIST, TABLE_COLUMN_TYPES, 10_000);
        String cityId = "city_" + city;
        String regionId = "region_" + region;
        String deviceId = "d_" + device;
        String colorId = "color_" + color;
        long roundStartTime = System.nanoTime();
        for (int i = 0; i < 6 * 60 * 24 * 30; i++) {
          int rowIndex = tablet.rowSize++;
          tablet.addTimestamp(rowIndex, START_TIME + i * 10_000L);
          tablet.addValue(COLUMN_NAME_1, rowIndex, cityId);
          tablet.addValue(COLUMN_NAME_2, rowIndex, regionId);
          tablet.addValue(COLUMN_NAME_3, rowIndex, deviceId);
          //          tablet.addValue(COLUMN_NAME_4, rowIndex, colorId);
          tablet.addValue(COLUMN_NAME_5, rowIndex, i * 1.0d);
          tablet.addValue(COLUMN_NAME_6, rowIndex, i * 1.0d);
          tablet.addValue(COLUMN_NAME_7, rowIndex, i * 1.0d);

          if (tablet.rowSize == tablet.getMaxRowNumber()) {
            session.insertTablet(tablet);
            tablet.reset();
            long writtenRows = i + 1L;
            if (writtenRows % 100_000 == 0) {
              LOGGER.info(
                  "Device {} has written {} rows, time cost is {}ms",
                  deviceId,
                  writtenRows,
                  (System.nanoTime() - roundStartTime) / 1_000_000);
              roundStartTime = System.nanoTime();
            }
          }
        }

        if (tablet.rowSize != 0) {
          session.insertTablet(tablet);
          tablet.reset();
        }
        session.executeNonQueryStatement(
            String.format(
                "UPDATE table1 SET %s='%s' WHERE %s='%s' AND %s='%s' AND %s='%s'",
                COLUMN_NAME_4,
                colorId,
                COLUMN_NAME_1,
                cityId,
                COLUMN_NAME_2,
                regionId,
                COLUMN_NAME_3,
                deviceId));
        LOGGER.info(
            "Device {} finished, total time cost is {}ms",
            deviceId,
            (System.nanoTime() - startTime) / 1_000_000);
      }

    } catch (IoTDBConnectionException e) {
      LOGGER.error("Connection error", e);
      throw new RuntimeException(e);
    } catch (StatementExecutionException e) {
      LOGGER.error("Execute error", e);
      throw new RuntimeException(e);
    }
  }

  private static void writeTree(
      final SessionPool sessionPool, final int deviceNum, final String database) {

    try {
      while (true) {
        long startTime = System.nanoTime();
        int device = deviceIdGenerator.getAndIncrement();
        if (device >= deviceNum) {
          break;
        }
        int city = device % 10;
        int region = device % 100;
        //        int color = device % 5;

        String deviceId =
            String.format(
                "%s.%s.%s.%s", database, "city_" + city, "region_" + region, "d_" + device);
        Tablet tablet = new Tablet(deviceId, TREE_SCHEMA_LIST, 10000);
        long roundStartTime = System.nanoTime();
        for (int i = 0; i < 6 * 60 * 24 * 30; i++) {
          int rowIndex = tablet.rowSize++;
          tablet.addTimestamp(rowIndex, START_TIME + i * 10_000L);
          tablet.addValue(COLUMN_NAME_5, rowIndex, i * 1.0d);
          tablet.addValue(COLUMN_NAME_6, rowIndex, i * 1.0d);
          tablet.addValue(COLUMN_NAME_7, rowIndex, i * 1.0d);
          if (tablet.rowSize == tablet.getMaxRowNumber()) {
            sessionPool.insertTablet(tablet);
            tablet.reset();
            long writtenRows = i + 1L;
            if (writtenRows % 100_000 == 0) {
              LOGGER.info(
                  "Device {} has written {} rows, time cost is {}ms",
                  deviceId,
                  writtenRows,
                  (System.nanoTime() - roundStartTime) / 1_000_000);
              roundStartTime = System.nanoTime();
            }
          }
        }

        if (tablet.rowSize != 0) {
          sessionPool.insertTablet(tablet);
          tablet.reset();
        }

        LOGGER.info(
            "Device {} finished, total time cost is {}ms",
            deviceId,
            (System.nanoTime() - startTime) / 1_000_000);
      }

    } catch (IoTDBConnectionException e) {
      LOGGER.error("Connection error", e);
      throw new RuntimeException(e);
    } catch (StatementExecutionException e) {
      LOGGER.error("Execute error", e);
      throw new RuntimeException(e);
    }
  }
}
