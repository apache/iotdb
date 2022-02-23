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

package org.apache.iotdb.library.anomaly;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.integration.env.ConfigFactory;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.fail;

public class AnomalyTests {
  private static final float oldUdfCollectorMemoryBudgetInMB =
      IoTDBDescriptor.getInstance().getConfig().getUdfCollectorMemoryBudgetInMB();
  private static final float oldUdfTransformerMemoryBudgetInMB =
      IoTDBDescriptor.getInstance().getConfig().getUdfTransformerMemoryBudgetInMB();
  private static final float oldUdfReaderMemoryBudgetInMB =
      IoTDBDescriptor.getInstance().getConfig().getUdfReaderMemoryBudgetInMB();

  @BeforeClass
  public static void setUp() throws Exception {
    ConfigFactory.getConfig()
        .setUdfCollectorMemoryBudgetInMB(5)
        .setUdfTransformerMemoryBudgetInMB(5)
        .setUdfReaderMemoryBudgetInMB(5);
    EnvFactory.getEnv().initBeforeClass();
    createTimeSeries();
    generateData();
    registerUDF();
  }

  private static void createTimeSeries() throws MetadataException {
    IoTDB.metaManager.setStorageGroup(new PartialPath("root.vehicle"));
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d1.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d1.s2"),
        TSDataType.INT64,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d1.s3"),
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d1.s4"),
        TSDataType.DOUBLE,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d2.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d2.s2"),
        TSDataType.INT64,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d2.s3"),
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d2.s4"),
        TSDataType.DOUBLE,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d3.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d3.s2"),
        TSDataType.INT64,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d3.s3"),
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d3.s4"),
        TSDataType.DOUBLE,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d3.s5"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d3.s6"),
        TSDataType.INT64,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d3.s7"),
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d3.s8"),
        TSDataType.DOUBLE,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d4.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d4.s2"),
        TSDataType.INT64,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d4.s3"),
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d4.s4"),
        TSDataType.DOUBLE,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d6.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d6.s2"),
        TSDataType.INT64,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d6.s3"),
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d6.s4"),
        TSDataType.DOUBLE,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
  }

  private static void generateData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              100, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              200, 2, 2, 2, 2));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              300, -2, -2, -2, -2));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              400, -1, -1, -1, -1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              500, 10, 10, 10, 10));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              600, 1, 1, 1, 1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              700, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              800, 2, 2, 2, 2));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              900, -1, -1, -1, -1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1000, 1, 1, 1, 1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              100, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              200, 50, 50, 50, 50));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              300, 100, 100, 100, 100));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              400, 150, 150, 150, 150));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              500, 200, 200, 200, 200));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              600, 200, 200, 200, 200));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              700, 200, 200, 200, 200));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              800, 200, 200, 200, 200));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              900, 200, 200, 200, 200));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1000, 200, 200, 200, 200));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1100, 150, 150, 150, 150));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1200, 100, 100, 100, 100));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1300, 50, 50, 50, 50));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1400, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d3(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d))",
              100, 0, 0, 0, 0, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d3(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d))",
              200, 0, 0, 0, 0, 1, 1, 1, 1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d3(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d))",
              300, 1, 1, 1, 1, 1, 1, 1, 1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d3(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d))",
              400, 1, 1, 1, 1, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d3(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d))",
              500, 0, 0, 0, 0, -1, -1, -1, -1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d3(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d))",
              600, -1, -1, -1, -1, -1, -1, -1, -1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d3(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d))",
              700, -1, -1, -1, -1, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d3(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d))",
              800, 2, 2, 2, 2, 2, 2, 2, 2));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              100, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              200, 1, 1, 1, 1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              300, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              400, 1, 1, 1, 1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              500, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              600, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              700, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              800, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              900, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1000, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1100, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1200, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1300, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1400, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1500, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1600, 1, 1, 1, 1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1700, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1800, 1, 1, 1, 1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1900, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              2000, 1, 1, 1, 1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              100, 2002, 2002, 2002, 2002));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              200, 1946, 1946, 1946, 1946));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              300, 1958, 1958, 1958, 1958));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              400, 2012, 2012, 2012, 2012));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              500, 2051, 2051, 2051, 2051));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              600, 1898, 1898, 1898, 1898));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              700, 2014, 2014, 2014, 2014));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              800, 2052, 2052, 2052, 2052));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              900, 1935, 1935, 1935, 1935));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1000, 1901, 1901, 1901, 1901));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1100, 1972, 1972, 1972, 1972));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1200, 1969, 1969, 1969, 1969));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1300, 1984, 1984, 1984, 1984));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1400, 2018, 2018, 2018, 2018));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1500, 1484, 1484, 1484, 1484));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1600, 1055, 1055, 1055, 1055));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1700, 1050, 1050, 1050, 1050));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1800, 1023, 1023, 1023, 1023));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1900, 1056, 1056, 1056, 1056));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              2000, 978, 978, 978, 978));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              2100, 1050, 1050, 1050, 1050));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              2200, 1123, 1123, 1123, 1123));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              2300, 1150, 1150, 1150, 1150));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              2400, 1034, 1034, 1034, 1034));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              2500, 950, 950, 950, 950));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              2600, 1059, 1059, 1059, 1059));
      statement.executeBatch();
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create function iqr as 'org.apache.iotdb.library.anomaly.UDTFIQR'");
      statement.execute("create function ksigma as 'org.apache.iotdb.library.anomaly.UDTFKSigma'");
      statement.execute(
          "create function missdetect as 'org.apache.iotdb.library.anomaly.UDTFMissDetect'");
      statement.execute("create function lof as 'org.apache.iotdb.library.anomaly.UDTFLOF'");
      statement.execute("create function range as 'org.apache.iotdb.library.anomaly.UDTFRange'");
      statement.execute(
          "create function TwoSidedFilter as 'org.apache.iotdb.library.anomaly.UDTFTwoSidedFilter'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
    ConfigFactory.getConfig()
        .setUdfCollectorMemoryBudgetInMB(oldUdfCollectorMemoryBudgetInMB)
        .setUdfTransformerMemoryBudgetInMB(oldUdfTransformerMemoryBudgetInMB)
        .setUdfReaderMemoryBudgetInMB(oldUdfReaderMemoryBudgetInMB);
  }

  @Test
  public void testIRQR1() {
    String sqlStr = "select iqr(d1.s1) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      String result1 = resultSet.getString(1);
      Assert.assertEquals("10.0", result1);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testIQR2() {
    String sqlStr = "select iqr(d1.s2) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      String result1 = resultSet.getString(1);
      Assert.assertEquals("10.0", result1);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testIQR3() {
    String sqlStr = "select iqr(d1.s3) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      String result1 = resultSet.getString(1);
      Assert.assertEquals("10.0", result1);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testIQR4() {
    String sqlStr = "select iqr(d1.s4) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      String result1 = resultSet.getString(1);
      Assert.assertEquals("10.0", result1);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testKSigma1() {
    String sqlStr = "select ksigma(d2.s1,\"k\"=\"1.0\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      String result1 = resultSet.getString(1);
      resultSet.next();
      String result2 = resultSet.getString(1);
      resultSet.next();
      String result3 = resultSet.getString(1);
      resultSet.next();
      String result4 = resultSet.getString(1);
      Assert.assertEquals("0.0", result1);
      Assert.assertEquals("50.0", result2);
      Assert.assertEquals("50.0", result3);
      Assert.assertEquals("0.0", result4);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testKSigma2() {
    String sqlStr = "select ksigma(d2.s2,\"k\"=\"1.0\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      String result1 = resultSet.getString(1);
      resultSet.next();
      String result2 = resultSet.getString(1);
      resultSet.next();
      String result3 = resultSet.getString(1);
      resultSet.next();
      String result4 = resultSet.getString(1);
      Assert.assertEquals("0.0", result1);
      Assert.assertEquals("50.0", result2);
      Assert.assertEquals("50.0", result3);
      Assert.assertEquals("0.0", result4);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testKSigma3() {
    String sqlStr = "select ksigma(d2.s3,\"k\"=\"1.0\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      String result1 = resultSet.getString(1);
      resultSet.next();
      String result2 = resultSet.getString(1);
      resultSet.next();
      String result3 = resultSet.getString(1);
      resultSet.next();
      String result4 = resultSet.getString(1);
      Assert.assertEquals("0.0", result1);
      Assert.assertEquals("50.0", result2);
      Assert.assertEquals("50.0", result3);
      Assert.assertEquals("0.0", result4);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testKSigma4() {
    String sqlStr = "select ksigma(d2.s4,\"k\"=\"1.0\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      String result1 = resultSet.getString(1);
      resultSet.next();
      String result2 = resultSet.getString(1);
      resultSet.next();
      String result3 = resultSet.getString(1);
      resultSet.next();
      String result4 = resultSet.getString(1);
      Assert.assertEquals("0.0", result1);
      Assert.assertEquals("50.0", result2);
      Assert.assertEquals("50.0", result3);
      Assert.assertEquals("0.0", result4);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testMissDetect1() {
    String sqlStr = "select missdetect(d4.s1,'minlen'='10') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      String result1 = resultSet.getString(1);
      resultSet.next();
      String result2 = resultSet.getString(1);
      resultSet.next();
      String result3 = resultSet.getString(1);
      resultSet.next();
      String result4 = resultSet.getString(1);
      resultSet.next();
      String result5 = resultSet.getString(1);
      resultSet.next();
      String result6 = resultSet.getString(1);
      resultSet.next();
      String result7 = resultSet.getString(1);
      resultSet.next();
      String result8 = resultSet.getString(1);
      resultSet.next();
      String result9 = resultSet.getString(1);
      resultSet.next();
      String result10 = resultSet.getString(1);
      resultSet.next();
      String result11 = resultSet.getString(1);
      resultSet.next();
      String result12 = resultSet.getString(1);
      resultSet.next();
      String result13 = resultSet.getString(1);
      resultSet.next();
      String result14 = resultSet.getString(1);
      resultSet.next();
      String result15 = resultSet.getString(1);
      resultSet.next();
      String result16 = resultSet.getString(1);
      resultSet.next();
      String result17 = resultSet.getString(1);
      resultSet.next();
      String result18 = resultSet.getString(1);
      resultSet.next();
      String result19 = resultSet.getString(1);
      resultSet.next();
      String result20 = resultSet.getString(1);
      Assert.assertEquals("false", result1);
      Assert.assertEquals("false", result2);
      Assert.assertEquals("false", result3);
      Assert.assertEquals("false", result4);
      Assert.assertEquals("true", result5);
      Assert.assertEquals("true", result6);
      Assert.assertEquals("true", result7);
      Assert.assertEquals("true", result8);
      Assert.assertEquals("true", result9);
      Assert.assertEquals("true", result10);
      Assert.assertEquals("true", result11);
      Assert.assertEquals("true", result12);
      Assert.assertEquals("true", result13);
      Assert.assertEquals("true", result14);
      Assert.assertEquals("true", result15);
      Assert.assertEquals("false", result16);
      Assert.assertEquals("false", result17);
      Assert.assertEquals("false", result18);
      Assert.assertEquals("false", result19);
      Assert.assertEquals("false", result20);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testMissDetect2() {
    String sqlStr = "select missdetect(d4.s2,'minlen'='10') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      String result1 = resultSet.getString(1);
      resultSet.next();
      String result2 = resultSet.getString(1);
      resultSet.next();
      String result3 = resultSet.getString(1);
      resultSet.next();
      String result4 = resultSet.getString(1);
      resultSet.next();
      String result5 = resultSet.getString(1);
      resultSet.next();
      String result6 = resultSet.getString(1);
      resultSet.next();
      String result7 = resultSet.getString(1);
      resultSet.next();
      String result8 = resultSet.getString(1);
      resultSet.next();
      String result9 = resultSet.getString(1);
      resultSet.next();
      String result10 = resultSet.getString(1);
      resultSet.next();
      String result11 = resultSet.getString(1);
      resultSet.next();
      String result12 = resultSet.getString(1);
      resultSet.next();
      String result13 = resultSet.getString(1);
      resultSet.next();
      String result14 = resultSet.getString(1);
      resultSet.next();
      String result15 = resultSet.getString(1);
      resultSet.next();
      String result16 = resultSet.getString(1);
      resultSet.next();
      String result17 = resultSet.getString(1);
      resultSet.next();
      String result18 = resultSet.getString(1);
      resultSet.next();
      String result19 = resultSet.getString(1);
      resultSet.next();
      String result20 = resultSet.getString(1);
      Assert.assertEquals("false", result1);
      Assert.assertEquals("false", result2);
      Assert.assertEquals("false", result3);
      Assert.assertEquals("false", result4);
      Assert.assertEquals("true", result5);
      Assert.assertEquals("true", result6);
      Assert.assertEquals("true", result7);
      Assert.assertEquals("true", result8);
      Assert.assertEquals("true", result9);
      Assert.assertEquals("true", result10);
      Assert.assertEquals("true", result11);
      Assert.assertEquals("true", result12);
      Assert.assertEquals("true", result13);
      Assert.assertEquals("true", result14);
      Assert.assertEquals("true", result15);
      Assert.assertEquals("false", result16);
      Assert.assertEquals("false", result17);
      Assert.assertEquals("false", result18);
      Assert.assertEquals("false", result19);
      Assert.assertEquals("false", result20);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testMissDetect3() {
    String sqlStr = "select missdetect(d4.s3,'minlen'='10') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      String result1 = resultSet.getString(1);
      resultSet.next();
      String result2 = resultSet.getString(1);
      resultSet.next();
      String result3 = resultSet.getString(1);
      resultSet.next();
      String result4 = resultSet.getString(1);
      resultSet.next();
      String result5 = resultSet.getString(1);
      resultSet.next();
      String result6 = resultSet.getString(1);
      resultSet.next();
      String result7 = resultSet.getString(1);
      resultSet.next();
      String result8 = resultSet.getString(1);
      resultSet.next();
      String result9 = resultSet.getString(1);
      resultSet.next();
      String result10 = resultSet.getString(1);
      resultSet.next();
      String result11 = resultSet.getString(1);
      resultSet.next();
      String result12 = resultSet.getString(1);
      resultSet.next();
      String result13 = resultSet.getString(1);
      resultSet.next();
      String result14 = resultSet.getString(1);
      resultSet.next();
      String result15 = resultSet.getString(1);
      resultSet.next();
      String result16 = resultSet.getString(1);
      resultSet.next();
      String result17 = resultSet.getString(1);
      resultSet.next();
      String result18 = resultSet.getString(1);
      resultSet.next();
      String result19 = resultSet.getString(1);
      resultSet.next();
      String result20 = resultSet.getString(1);
      Assert.assertEquals("false", result1);
      Assert.assertEquals("false", result2);
      Assert.assertEquals("false", result3);
      Assert.assertEquals("false", result4);
      Assert.assertEquals("true", result5);
      Assert.assertEquals("true", result6);
      Assert.assertEquals("true", result7);
      Assert.assertEquals("true", result8);
      Assert.assertEquals("true", result9);
      Assert.assertEquals("true", result10);
      Assert.assertEquals("true", result11);
      Assert.assertEquals("true", result12);
      Assert.assertEquals("true", result13);
      Assert.assertEquals("true", result14);
      Assert.assertEquals("true", result15);
      Assert.assertEquals("false", result16);
      Assert.assertEquals("false", result17);
      Assert.assertEquals("false", result18);
      Assert.assertEquals("false", result19);
      Assert.assertEquals("false", result20);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testMissDetect4() {
    String sqlStr = "select missdetect(d4.s4,'minlen'='10') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      String result1 = resultSet.getString(1);
      resultSet.next();
      String result2 = resultSet.getString(1);
      resultSet.next();
      String result3 = resultSet.getString(1);
      resultSet.next();
      String result4 = resultSet.getString(1);
      resultSet.next();
      String result5 = resultSet.getString(1);
      resultSet.next();
      String result6 = resultSet.getString(1);
      resultSet.next();
      String result7 = resultSet.getString(1);
      resultSet.next();
      String result8 = resultSet.getString(1);
      resultSet.next();
      String result9 = resultSet.getString(1);
      resultSet.next();
      String result10 = resultSet.getString(1);
      resultSet.next();
      String result11 = resultSet.getString(1);
      resultSet.next();
      String result12 = resultSet.getString(1);
      resultSet.next();
      String result13 = resultSet.getString(1);
      resultSet.next();
      String result14 = resultSet.getString(1);
      resultSet.next();
      String result15 = resultSet.getString(1);
      resultSet.next();
      String result16 = resultSet.getString(1);
      resultSet.next();
      String result17 = resultSet.getString(1);
      resultSet.next();
      String result18 = resultSet.getString(1);
      resultSet.next();
      String result19 = resultSet.getString(1);
      resultSet.next();
      String result20 = resultSet.getString(1);
      Assert.assertEquals("false", result1);
      Assert.assertEquals("false", result2);
      Assert.assertEquals("false", result3);
      Assert.assertEquals("false", result4);
      Assert.assertEquals("true", result5);
      Assert.assertEquals("true", result6);
      Assert.assertEquals("true", result7);
      Assert.assertEquals("true", result8);
      Assert.assertEquals("true", result9);
      Assert.assertEquals("true", result10);
      Assert.assertEquals("true", result11);
      Assert.assertEquals("true", result12);
      Assert.assertEquals("true", result13);
      Assert.assertEquals("true", result14);
      Assert.assertEquals("true", result15);
      Assert.assertEquals("false", result16);
      Assert.assertEquals("false", result17);
      Assert.assertEquals("false", result18);
      Assert.assertEquals("false", result19);
      Assert.assertEquals("false", result20);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testLOF1() {
    String sqlStr = "select lof(d3.s1,d3.s5) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      String result1 = resultSet.getString(1);
      resultSet.next();
      String result2 = resultSet.getString(1);
      resultSet.next();
      String result3 = resultSet.getString(1);
      resultSet.next();
      String result4 = resultSet.getString(1);
      resultSet.next();
      String result5 = resultSet.getString(1);
      resultSet.next();
      String result6 = resultSet.getString(1);
      resultSet.next();
      String result7 = resultSet.getString(1);
      resultSet.next();
      String result8 = resultSet.getString(1);
      Assert.assertEquals("3.8274824267668244", result1);
      Assert.assertEquals("3.0117631741126156", result2);
      Assert.assertEquals("2.838155437762879", result3);
      Assert.assertEquals("3.0117631741126156", result4);
      Assert.assertEquals("2.73518261244453", result5);
      Assert.assertEquals("2.371440975708148", result6);
      Assert.assertEquals("2.73518261244453", result7);
      Assert.assertEquals("1.7561416374270742", result8);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testLOF2() {
    String sqlStr = "select lof(d3.s2,d3.s6) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      String result1 = resultSet.getString(1);
      resultSet.next();
      String result2 = resultSet.getString(1);
      resultSet.next();
      String result3 = resultSet.getString(1);
      resultSet.next();
      String result4 = resultSet.getString(1);
      resultSet.next();
      String result5 = resultSet.getString(1);
      resultSet.next();
      String result6 = resultSet.getString(1);
      resultSet.next();
      String result7 = resultSet.getString(1);
      resultSet.next();
      String result8 = resultSet.getString(1);
      Assert.assertEquals("3.8274824267668244", result1);
      Assert.assertEquals("3.0117631741126156", result2);
      Assert.assertEquals("2.838155437762879", result3);
      Assert.assertEquals("3.0117631741126156", result4);
      Assert.assertEquals("2.73518261244453", result5);
      Assert.assertEquals("2.371440975708148", result6);
      Assert.assertEquals("2.73518261244453", result7);
      Assert.assertEquals("1.7561416374270742", result8);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testLOF3() {
    String sqlStr = "select lof(d3.s3,d3.s7) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      String result1 = resultSet.getString(1);
      resultSet.next();
      String result2 = resultSet.getString(1);
      resultSet.next();
      String result3 = resultSet.getString(1);
      resultSet.next();
      String result4 = resultSet.getString(1);
      resultSet.next();
      String result5 = resultSet.getString(1);
      resultSet.next();
      String result6 = resultSet.getString(1);
      resultSet.next();
      String result7 = resultSet.getString(1);
      resultSet.next();
      String result8 = resultSet.getString(1);
      Assert.assertEquals("3.8274824267668244", result1);
      Assert.assertEquals("3.0117631741126156", result2);
      Assert.assertEquals("2.838155437762879", result3);
      Assert.assertEquals("3.0117631741126156", result4);
      Assert.assertEquals("2.73518261244453", result5);
      Assert.assertEquals("2.371440975708148", result6);
      Assert.assertEquals("2.73518261244453", result7);
      Assert.assertEquals("1.7561416374270742", result8);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testLOF4() {
    String sqlStr = "select lof(d3.s4,d3.s8) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      String result1 = resultSet.getString(1);
      resultSet.next();
      String result2 = resultSet.getString(1);
      resultSet.next();
      String result3 = resultSet.getString(1);
      resultSet.next();
      String result4 = resultSet.getString(1);
      resultSet.next();
      String result5 = resultSet.getString(1);
      resultSet.next();
      String result6 = resultSet.getString(1);
      resultSet.next();
      String result7 = resultSet.getString(1);
      resultSet.next();
      String result8 = resultSet.getString(1);
      Assert.assertEquals("3.8274824267668244", result1);
      Assert.assertEquals("3.0117631741126156", result2);
      Assert.assertEquals("2.838155437762879", result3);
      Assert.assertEquals("3.0117631741126156", result4);
      Assert.assertEquals("2.73518261244453", result5);
      Assert.assertEquals("2.371440975708148", result6);
      Assert.assertEquals("2.73518261244453", result7);
      Assert.assertEquals("1.7561416374270742", result8);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRange1() {
    String sqlStr =
        "select range(d1.s1,\"lower_bound\"=\"-5.0\",\"upper_bound\"=\"5.0\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      String result1 = resultSet.getString(1);
      Assert.assertEquals("10.0", result1);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRange2() {
    String sqlStr =
        "select range(d1.s2,\"lower_bound\"=\"-5.0\",\"upper_bound\"=\"5.0\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      String result1 = resultSet.getString(1);
      Assert.assertEquals("10.0", result1);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRange3() {
    String sqlStr =
        "select range(d1.s3,\"lower_bound\"=\"-5.0\",\"upper_bound\"=\"5.0\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      String result1 = resultSet.getString(1);
      Assert.assertEquals("10.0", result1);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRange4() {
    String sqlStr =
        "select range(d1.s4,\"lower_bound\"=\"-5.0\",\"upper_bound\"=\"5.0\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      String result1 = resultSet.getString(1);
      Assert.assertEquals("10.0", result1);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testTwoSidedFileter1() {
    String sqlStr = "select TwoSidedFilter(d6.s1, 'len'='3', 'threshold'='0.3') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      String result1 = resultSet.getString(1);
      resultSet.next();
      String result2 = resultSet.getString(1);
      resultSet.next();
      String result3 = resultSet.getString(1);
      resultSet.next();
      String result4 = resultSet.getString(1);
      resultSet.next();
      String result5 = resultSet.getString(1);
      resultSet.next();
      String result6 = resultSet.getString(1);
      resultSet.next();
      String result7 = resultSet.getString(1);
      resultSet.next();
      String result8 = resultSet.getString(1);
      resultSet.next();
      String result9 = resultSet.getString(1);
      resultSet.next();
      String result10 = resultSet.getString(1);
      resultSet.next();
      String result11 = resultSet.getString(1);
      resultSet.next();
      String result12 = resultSet.getString(1);
      resultSet.next();
      String result13 = resultSet.getString(1);
      resultSet.next();
      String result14 = resultSet.getString(1);
      resultSet.next();
      String result15 = resultSet.getString(1);
      resultSet.next();
      String result16 = resultSet.getString(1);
      resultSet.next();
      String result17 = resultSet.getString(1);
      resultSet.next();
      String result18 = resultSet.getString(1);
      resultSet.next();
      String result19 = resultSet.getString(1);
      resultSet.next();
      String result20 = resultSet.getString(1);
      resultSet.next();
      String result21 = resultSet.getString(1);
      resultSet.next();
      String result22 = resultSet.getString(1);
      resultSet.next();
      String result23 = resultSet.getString(1);
      Assert.assertEquals("2002.0", result1);
      Assert.assertEquals("1946.0", result2);
      Assert.assertEquals("1958.0", result3);
      Assert.assertEquals("2012.0", result4);
      Assert.assertEquals("2051.0", result5);
      Assert.assertEquals("1898.0", result6);
      Assert.assertEquals("2014.0", result7);
      Assert.assertEquals("2052.0", result8);
      Assert.assertEquals("1935.0", result9);
      Assert.assertEquals("1901.0", result10);
      Assert.assertEquals("1972.0", result11);
      Assert.assertEquals("1969.0", result12);
      Assert.assertEquals("1984.0", result13);
      Assert.assertEquals("2018.0", result14);
      Assert.assertEquals("1023.0", result15);
      Assert.assertEquals("1056.0", result16);
      Assert.assertEquals("978.0", result17);
      Assert.assertEquals("1050.0", result18);
      Assert.assertEquals("1123.0", result19);
      Assert.assertEquals("1150.0", result20);
      Assert.assertEquals("1134.0", result21);
      Assert.assertEquals("950.0", result22);
      Assert.assertEquals("1059.0", result23);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testTwoSidedFileter2() {
    String sqlStr = "select TwoSidedFilter(d6.s2, 'len'='3', 'threshold'='0.3') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      String result1 = resultSet.getString(1);
      resultSet.next();
      String result2 = resultSet.getString(1);
      resultSet.next();
      String result3 = resultSet.getString(1);
      resultSet.next();
      String result4 = resultSet.getString(1);
      resultSet.next();
      String result5 = resultSet.getString(1);
      resultSet.next();
      String result6 = resultSet.getString(1);
      resultSet.next();
      String result7 = resultSet.getString(1);
      resultSet.next();
      String result8 = resultSet.getString(1);
      resultSet.next();
      String result9 = resultSet.getString(1);
      resultSet.next();
      String result10 = resultSet.getString(1);
      resultSet.next();
      String result11 = resultSet.getString(1);
      resultSet.next();
      String result12 = resultSet.getString(1);
      resultSet.next();
      String result13 = resultSet.getString(1);
      resultSet.next();
      String result14 = resultSet.getString(1);
      resultSet.next();
      String result15 = resultSet.getString(1);
      resultSet.next();
      String result16 = resultSet.getString(1);
      resultSet.next();
      String result17 = resultSet.getString(1);
      resultSet.next();
      String result18 = resultSet.getString(1);
      resultSet.next();
      String result19 = resultSet.getString(1);
      resultSet.next();
      String result20 = resultSet.getString(1);
      resultSet.next();
      String result21 = resultSet.getString(1);
      resultSet.next();
      String result22 = resultSet.getString(1);
      resultSet.next();
      String result23 = resultSet.getString(1);
      Assert.assertEquals("2002.0", result1);
      Assert.assertEquals("1946.0", result2);
      Assert.assertEquals("1958.0", result3);
      Assert.assertEquals("2012.0", result4);
      Assert.assertEquals("2051.0", result5);
      Assert.assertEquals("1898.0", result6);
      Assert.assertEquals("2014.0", result7);
      Assert.assertEquals("2052.0", result8);
      Assert.assertEquals("1935.0", result9);
      Assert.assertEquals("1901.0", result10);
      Assert.assertEquals("1972.0", result11);
      Assert.assertEquals("1969.0", result12);
      Assert.assertEquals("1984.0", result13);
      Assert.assertEquals("2018.0", result14);
      Assert.assertEquals("1023.0", result15);
      Assert.assertEquals("1056.0", result16);
      Assert.assertEquals("978.0", result17);
      Assert.assertEquals("1050.0", result18);
      Assert.assertEquals("1123.0", result19);
      Assert.assertEquals("1150.0", result20);
      Assert.assertEquals("1134.0", result21);
      Assert.assertEquals("950.0", result22);
      Assert.assertEquals("1059.0", result23);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testTwoSidedFileter3() {
    String sqlStr = "select TwoSidedFilter(d6.s3, 'len'='3', 'threshold'='0.3') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      String result1 = resultSet.getString(1);
      resultSet.next();
      String result2 = resultSet.getString(1);
      resultSet.next();
      String result3 = resultSet.getString(1);
      resultSet.next();
      String result4 = resultSet.getString(1);
      resultSet.next();
      String result5 = resultSet.getString(1);
      resultSet.next();
      String result6 = resultSet.getString(1);
      resultSet.next();
      String result7 = resultSet.getString(1);
      resultSet.next();
      String result8 = resultSet.getString(1);
      resultSet.next();
      String result9 = resultSet.getString(1);
      resultSet.next();
      String result10 = resultSet.getString(1);
      resultSet.next();
      String result11 = resultSet.getString(1);
      resultSet.next();
      String result12 = resultSet.getString(1);
      resultSet.next();
      String result13 = resultSet.getString(1);
      resultSet.next();
      String result14 = resultSet.getString(1);
      resultSet.next();
      String result15 = resultSet.getString(1);
      resultSet.next();
      String result16 = resultSet.getString(1);
      resultSet.next();
      String result17 = resultSet.getString(1);
      resultSet.next();
      String result18 = resultSet.getString(1);
      resultSet.next();
      String result19 = resultSet.getString(1);
      resultSet.next();
      String result20 = resultSet.getString(1);
      resultSet.next();
      String result21 = resultSet.getString(1);
      resultSet.next();
      String result22 = resultSet.getString(1);
      resultSet.next();
      String result23 = resultSet.getString(1);
      Assert.assertEquals("2002.0", result1);
      Assert.assertEquals("1946.0", result2);
      Assert.assertEquals("1958.0", result3);
      Assert.assertEquals("2012.0", result4);
      Assert.assertEquals("2051.0", result5);
      Assert.assertEquals("1898.0", result6);
      Assert.assertEquals("2014.0", result7);
      Assert.assertEquals("2052.0", result8);
      Assert.assertEquals("1935.0", result9);
      Assert.assertEquals("1901.0", result10);
      Assert.assertEquals("1972.0", result11);
      Assert.assertEquals("1969.0", result12);
      Assert.assertEquals("1984.0", result13);
      Assert.assertEquals("2018.0", result14);
      Assert.assertEquals("1023.0", result15);
      Assert.assertEquals("1056.0", result16);
      Assert.assertEquals("978.0", result17);
      Assert.assertEquals("1050.0", result18);
      Assert.assertEquals("1123.0", result19);
      Assert.assertEquals("1150.0", result20);
      Assert.assertEquals("1134.0", result21);
      Assert.assertEquals("950.0", result22);
      Assert.assertEquals("1059.0", result23);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testTwoSidedFileter4() {
    String sqlStr = "select TwoSidedFilter(d6.s4, 'len'='3', 'threshold'='0.3') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      String result1 = resultSet.getString(1);
      resultSet.next();
      String result2 = resultSet.getString(1);
      resultSet.next();
      String result3 = resultSet.getString(1);
      resultSet.next();
      String result4 = resultSet.getString(1);
      resultSet.next();
      String result5 = resultSet.getString(1);
      resultSet.next();
      String result6 = resultSet.getString(1);
      resultSet.next();
      String result7 = resultSet.getString(1);
      resultSet.next();
      String result8 = resultSet.getString(1);
      resultSet.next();
      String result9 = resultSet.getString(1);
      resultSet.next();
      String result10 = resultSet.getString(1);
      resultSet.next();
      String result11 = resultSet.getString(1);
      resultSet.next();
      String result12 = resultSet.getString(1);
      resultSet.next();
      String result13 = resultSet.getString(1);
      resultSet.next();
      String result14 = resultSet.getString(1);
      resultSet.next();
      String result15 = resultSet.getString(1);
      resultSet.next();
      String result16 = resultSet.getString(1);
      resultSet.next();
      String result17 = resultSet.getString(1);
      resultSet.next();
      String result18 = resultSet.getString(1);
      resultSet.next();
      String result19 = resultSet.getString(1);
      resultSet.next();
      String result20 = resultSet.getString(1);
      resultSet.next();
      String result21 = resultSet.getString(1);
      resultSet.next();
      String result22 = resultSet.getString(1);
      resultSet.next();
      String result23 = resultSet.getString(1);
      Assert.assertEquals("2002.0", result1);
      Assert.assertEquals("1946.0", result2);
      Assert.assertEquals("1958.0", result3);
      Assert.assertEquals("2012.0", result4);
      Assert.assertEquals("2051.0", result5);
      Assert.assertEquals("1898.0", result6);
      Assert.assertEquals("2014.0", result7);
      Assert.assertEquals("2052.0", result8);
      Assert.assertEquals("1935.0", result9);
      Assert.assertEquals("1901.0", result10);
      Assert.assertEquals("1972.0", result11);
      Assert.assertEquals("1969.0", result12);
      Assert.assertEquals("1984.0", result13);
      Assert.assertEquals("2018.0", result14);
      Assert.assertEquals("1023.0", result15);
      Assert.assertEquals("1056.0", result16);
      Assert.assertEquals("978.0", result17);
      Assert.assertEquals("1050.0", result18);
      Assert.assertEquals("1123.0", result19);
      Assert.assertEquals("1150.0", result20);
      Assert.assertEquals("1134.0", result21);
      Assert.assertEquals("950.0", result22);
      Assert.assertEquals("1059.0", result23);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
