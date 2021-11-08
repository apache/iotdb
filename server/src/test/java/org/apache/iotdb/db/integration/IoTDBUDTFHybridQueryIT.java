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
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IoTDBUDTFHybridQueryIT {

  public static final String ACCESS_STRATEGY_KEY = "access";
  public static final String ACCESS_STRATEGY_ROW_BY_ROW = "row-by-row";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
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
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
  }

  private static void generateData() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < 10; ++i) {
        statement.execute(
            (String.format(
                "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)", i, i, i)));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create function counter as 'org.apache.iotdb.db.query.udf.example.Counter'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testUserDefinedBuiltInHybridAggregationQuery() {
    String sql =
        String.format(
            "select count(*), counter(s1, '%s'='%s') from root.vehicle.d1",
            ACCESS_STRATEGY_KEY, ACCESS_STRATEGY_ROW_BY_ROW);

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.executeQuery(sql);
      fail();
    } catch (SQLException throwable) {
      assertTrue(
          throwable
              .getMessage()
              .contains("User-defined and built-in hybrid aggregation is not supported together."));
    }
  }

  @Test
  public void testUserDefinedFunctionFillFunctionHybridQuery() {
    String sql =
        String.format(
            "select temperature, counter(temperature, '%s'='%s') from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(float [linear, 1m, 1m])",
            ACCESS_STRATEGY_KEY, ACCESS_STRATEGY_ROW_BY_ROW);

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.executeQuery(sql);
      fail();
    } catch (SQLException throwable) {
      assertTrue(
          throwable.getMessage().contains("Fill functions are not supported in UDF queries."));
    }
  }

  @Test
  public void testLastUserDefinedFunctionQuery() {
    String sql =
        String.format(
            "select last counter(temperature, '%s'='%s') from root.sgcc.wf03.wt01",
            ACCESS_STRATEGY_KEY, ACCESS_STRATEGY_ROW_BY_ROW);

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.executeQuery(sql);
      fail();
    } catch (SQLException throwable) {
      assertTrue(
          throwable.getMessage().contains("Last queries can only be applied on raw time series."));
    }
  }

  @Test
  public void testUserDefinedFunctionAlignByDeviceQuery() {
    String sql =
        String.format(
            "select adder(temperature), counter(temperature, '%s'='%s') from root.sgcc.wf03.wt01 align by device",
            ACCESS_STRATEGY_KEY, ACCESS_STRATEGY_ROW_BY_ROW);

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.executeQuery(sql);
      fail();
    } catch (SQLException throwable) {
      assertTrue(
          throwable
              .getMessage()
              .contains("ALIGN BY DEVICE clause is not supported in UDF queries."));
    }
  }
}
