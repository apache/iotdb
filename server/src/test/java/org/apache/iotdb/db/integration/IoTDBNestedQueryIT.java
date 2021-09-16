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

package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class IoTDBNestedQueryIT {

  protected static final int ITERATION_TIMES = 10_000;

  @BeforeClass
  public static void setUp() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setUdfCollectorMemoryBudgetInMB(3);
    IoTDBDescriptor.getInstance().getConfig().setUdfTransformerMemoryBudgetInMB(3);
    IoTDBDescriptor.getInstance().getConfig().setUdfReaderMemoryBudgetInMB(3);
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    createTimeSeries();
    generateData();
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
        new PartialPath("root.vehicle.d2.s1"),
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d2.s2"),
        TSDataType.DOUBLE,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
  }

  private static void generateData() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (int i = 1; i <= ITERATION_TIMES; ++i) {
        statement.execute(
            String.format(
                "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)", i, i, i));
        statement.execute(
            (String.format(
                "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)", i, i, i)));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setUdfCollectorMemoryBudgetInMB(100);
    IoTDBDescriptor.getInstance().getConfig().setUdfTransformerMemoryBudgetInMB(100);
    IoTDBDescriptor.getInstance().getConfig().setUdfReaderMemoryBudgetInMB(100);
  }

  @Test
  public void testNestedArithmeticExpressions() {
    String sqlStr =
        "select d1.s1, d2.s2, d1.s1 + d1.s2 - (d2.s1 + d2.s2), d1.s2 * (d2.s1 / d1.s1), d1.s2 + d1.s2 * d2.s1 - d2.s1, d1.s1 - (d1.s1 - (-d1.s1)), (-d2.s1) * (-d2.s2) / (-d1.s2) from root.vehicle";

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);

      assertEquals(1 + 7, resultSet.getMetaData().getColumnCount());

      int count = 0;
      while (resultSet.next()) {
        ++count;

        assertEquals(count, Integer.parseInt(resultSet.getString(1)));
        assertEquals(count, Double.parseDouble(resultSet.getString(2)), 0);
        assertEquals(count, Double.parseDouble(resultSet.getString(3)), 0);
        assertEquals(0.0, Double.parseDouble(resultSet.getString(4)), 0);
        assertEquals(count, Double.parseDouble(resultSet.getString(5)), 0);
        assertEquals(count * count, Double.parseDouble(resultSet.getString(6)), 0);
        assertEquals(-count, Double.parseDouble(resultSet.getString(7)), 0);
        assertEquals(-count, Double.parseDouble(resultSet.getString(8)), 0);
      }
      assertEquals(ITERATION_TIMES, count);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testNestedRowByRowUDFExpressions() {
    String sqlStr =
        "select s1, s2, sin(sin(s1) * sin(s2) + cos(s1) * cos(s1)) + sin(sin(s1 - s1 + s2) * sin(s2) + cos(s1) * cos(s1)), asin(sin(asin(sin(s1 - s2 / (-s1))))) from root.vehicle.d2";

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);

      assertEquals(1 + 4, resultSet.getMetaData().getColumnCount());

      int count = 0;
      while (resultSet.next()) {
        ++count;
        assertEquals(count, Integer.parseInt(resultSet.getString(1)));
        assertEquals(count, Double.parseDouble(resultSet.getString(2)), 0);
        assertEquals(count, Double.parseDouble(resultSet.getString(3)), 0);
        assertEquals(2 * Math.sin(1.0), Double.parseDouble(resultSet.getString(4)), 1e-5);
        assertEquals(
            Math.asin(Math.sin(Math.asin(Math.sin(count - count / (-count))))),
            Double.parseDouble(resultSet.getString(5)),
            1e-5);
      }
      assertEquals(ITERATION_TIMES, count);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
