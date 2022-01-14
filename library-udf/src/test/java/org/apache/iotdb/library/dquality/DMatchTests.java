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

package org.apache.iotdb.library.dquality;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.integration.env.ConfigFactory;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.fail;

public class DMatchTests {
  protected static final int ITERATION_TIMES = 10_000;

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
    double x = -100d, y = 100d; // borders of random value
    long a = 0, b = 1000000000;
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (int i = 1; i <= ITERATION_TIMES; ++i) {
        statement.execute(
            String.format(
                "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",
                (int) Math.floor(a + Math.random() * b % (b - a + 1)),
                (int) Math.floor(x + Math.random() * y % (y - x + 1)),
                (int) Math.floor(x + Math.random() * y % (y - x + 1))));
        statement.execute(
            (String.format(
                "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%f,%f)",
                (int) Math.floor(a + Math.random() * b % (b - a + 1)),
                x + Math.random() * y % (y - x + 1),
                x + Math.random() * y % (y - x + 1))));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create function completeness as 'org.apache.iotdb.library.dmatch.UDAFCov'");
      statement.execute(
          "create function timeliness as 'org.apache.iotdb.library.dmatch.UDAFDtw'");
      statement.execute(
          "create function consistency as 'org.apache.iotdb.library.dmatch.UDAFPearson'");
      statement.execute(
              "create function validity as 'org.apache.iotdb.library.dmatch.UDTFPtnSym'");
      statement.execute(
          "create function validity as 'org.apache.iotdb.library.dmatch.UDTFXcorr'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
    ConfigFactory.getConfig()
        .setUdfCollectorMemoryBudgetInMB(100)
        .setUdfTransformerMemoryBudgetInMB(100)
        .setUdfReaderMemoryBudgetInMB(100);
  }

  @Test
  public void testCov1() {
    String sqlStr = "select cov(d1.s1,d1.s2) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= -0.0D && result <= 1.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testCov2() {
    String sqlStr = "select cov(d1.s1,d1.s2) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= -0.0D && result <= 1.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testCov3() {
    String sqlStr = "select cov(d1.s1,d1.s2) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= -0.0D && result <= 1.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testCov4() {
    String sqlStr = "select cov(d1.s1,d1.s2) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= -0.0D && result <= 1.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testDtw1() {
    String sqlStr = "select dtw(d1.s1,d1.s2) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= -0.0D && result <= 1.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testDtw2() {
    String sqlStr = "select dtw(d1.s1,d1.s2) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= -0.0D && result <= 1.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testDtw3() {
    String sqlStr = "select dtw(d1.s1,d1.s2) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= -0.0D && result <= 1.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testDtw4() {
    String sqlStr = "select dtw(d1.s1,d1.s2) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= -0.0D && result <= 1.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testPearson1() {
    String sqlStr = "select pearson(d1.s1,d1.s2) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= -0.0D && result <= 1.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testPearson2() {
    String sqlStr = "select pearson(d1.s1,d1.s2) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= -0.0D && result <= 1.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testPearson3() {
    String sqlStr = "select pearson(d1.s1,d1.s2) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= -0.0D && result <= 1.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testPearson4() {
    String sqlStr = "select pearson(d1.s1,d1.s2) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= -0.0D && result <= 1.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testPthSym1() {
    String sqlStr = "select ptnsym(d2.s1, 'window'='5', 'threshold'='0') from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= -0.0D && result <= 1.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testPtnSym2() {
    String sqlStr = "select ptnsym(d1.s2, 'window'='5', 'threshold'='0') from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= -0.0D && result <= 1.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testPtnSym3() {
    String sqlStr = "select ptnsym(d2.s1, 'window'='5', 'threshold'='0') from";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= -0.0D && result <= 1.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testPtnSym4() {
    String sqlStr = "select ptnsym(d2.s2, 'window'='5', 'threshold'='0') from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= -0.0D && result <= 1.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testXCorr1() {
    String sqlStr = "select xcorr(d1.s1,d1.s2) from root.vehicle";
    try (Connection connection =
                 DriverManager.getConnection(
                         Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= -0.0D && result <= 1.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testXCorr2() {
    String sqlStr = "select xcorr(d1.s1, d1.s2) from root.vehicle";
    try (Connection connection =
                 DriverManager.getConnection(
                         Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= -0.0D && result <= 1.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testXCorr3() {
    String sqlStr = "select xcorr(d1.s1, d1.s2) from root.vehicle";
    try (Connection connection =
                 DriverManager.getConnection(
                         Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= -0.0D && result <= 1.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testXCorr4() {
    String sqlStr = "select xcorr(d1.s1,d1.s2) from root.vehicle";
    try (Connection connection =
                 DriverManager.getConnection(
                         Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= -0.0D && result <= 1.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
