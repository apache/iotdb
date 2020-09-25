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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.query.udf.example.Counter;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class IoTDBUDFWindowQueryIT {

  protected final static int ITERATION_TIMES = 1_000;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    createTimeSeries();
    generateData();
    registerUDF();
  }

  private static void createTimeSeries() throws MetadataException {
    IoTDB.metaManager.setStorageGroup("root.vehicle");
    IoTDB.metaManager.createTimeseries("root.vehicle.d1.s1", TSDataType.INT32, TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED, null);
    IoTDB.metaManager.createTimeseries("root.vehicle.d1.s2", TSDataType.INT32, TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED, null);
  }

  private static void generateData() {
    try (Connection connection = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < ITERATION_TIMES; ++i) {
        statement.execute((String
            .format("insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)", i, i, i)));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("create function udf as \"org.apache.iotdb.db.query.udf.example.Counter\"");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testRowByRow() {
    String sql = String.format("select udf(s1, \"%s\"=\"%s\") from root.vehicle.d1",
        Counter.ACCESS_STRATEGY_KEY, Counter.ACCESS_STRATEGY_ONE_BY_ONE);

    try (Statement statement = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/",
            "root", "root").createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      int count = 0;
      assertEquals(2, resultSet.getMetaData().getColumnCount());
      while (resultSet.next()) {
        assertEquals(count++, (int) (Double.parseDouble(resultSet.getString(1))));
      }
      assertEquals(ITERATION_TIMES, count);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testTumblingWindow1() {
    final int WINDOW_SIZE = (int) (0.1 * ITERATION_TIMES);
    String sql = String.format("select udf(s1, \"%s\"=\"%s\", \"%s\"=\"%s\") from root.vehicle.d1",
        Counter.ACCESS_STRATEGY_KEY, Counter.ACCESS_STRATEGY_TUMBLING,
        Counter.WINDOW_SIZE, WINDOW_SIZE);

    try (Statement statement = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/",
            "root", "root").createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      int count = 0;
      assertEquals(2, resultSet.getMetaData().getColumnCount());
      while (resultSet.next()) {
        assertEquals(count < ITERATION_TIMES / WINDOW_SIZE
                ? WINDOW_SIZE
                : ITERATION_TIMES - (ITERATION_TIMES / WINDOW_SIZE) * WINDOW_SIZE,
            (int) (Double.parseDouble(resultSet.getString(2))));
        ++count;
      }
      assertEquals(ITERATION_TIMES / WINDOW_SIZE + (ITERATION_TIMES % WINDOW_SIZE == 0 ? 0 : 1),
          count);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testTumblingWindow2() {
    final int WINDOW_SIZE = (int) (0.333 * ITERATION_TIMES);
    String sql = String.format("select udf(s1, \"%s\"=\"%s\", \"%s\"=\"%s\") from root.vehicle.d1",
        Counter.ACCESS_STRATEGY_KEY, Counter.ACCESS_STRATEGY_TUMBLING,
        Counter.WINDOW_SIZE, WINDOW_SIZE);

    try (Statement statement = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/",
            "root", "root").createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      int count = 0;
      assertEquals(2, resultSet.getMetaData().getColumnCount());
      while (resultSet.next()) {
        assertEquals(count < ITERATION_TIMES / WINDOW_SIZE
                ? WINDOW_SIZE
                : ITERATION_TIMES - (ITERATION_TIMES / WINDOW_SIZE) * WINDOW_SIZE,
            (int) (Double.parseDouble(resultSet.getString(2))));
        ++count;
      }
      assertEquals(ITERATION_TIMES / WINDOW_SIZE + (ITERATION_TIMES % WINDOW_SIZE == 0 ? 0 : 1),
          count);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
