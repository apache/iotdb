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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.fail;

public class DMatchTests {
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
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d1.s3"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d1.s4"),
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
  }

  private static void generateData() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s3) values(%d,%d,%d)", 100, 100, 101));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s3) values(%d,%d,%d)", 200, 102, 101));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s3) values(%d,%d,%d)", 300, 104, 102));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s3) values(%d,%d,%d)", 400, 126, 102));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s3) values(%d,%d,%d)", 500, 108, 103));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s3) values(%d,%d,%d)", 600, 112, 104));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s3) values(%d,%d,%d)", 700, 114, 104));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s3) values(%d,%d,%d)", 800, 112, 104));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s3) values(%d,%d,%d)", 900, 118, 105));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s3) values(%d,%d,%d)", 1000, 100, 106));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s3) values(%d,%d,%d)", 100, 100, 101));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s3) values(%d,%d,%d)", 200, 102, 101));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s3) values(%d,%d,%d)", 300, 104, 102));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s3) values(%d,%d,%d)", 400, 126, 102));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s3) values(%d,%d,%d)", 500, 108, 103));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s3) values(%d,%d,%d)", 600, 112, 104));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s3) values(%d,%d,%d)", 700, 114, 104));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s3) values(%d,%d,%d)", 800, 112, 104));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s3) values(%d,%d,%d)", 900, 118, 105));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s3) values(%d,%d,%d)", 1000, 100, 106));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s2,s4) values(%d,%d,%d)", 100, 100, 101));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s2,s4) values(%d,%d,%d)", 200, 102, 101));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s2,s4) values(%d,%d,%d)", 300, 104, 102));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s2,s4) values(%d,%d,%d)", 400, 126, 102));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s2,s4) values(%d,%d,%d)", 500, 108, 103));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s2,s4) values(%d,%d,%d)", 600, 112, 104));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s2,s4) values(%d,%d,%d)", 700, 114, 104));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s2,s4) values(%d,%d,%d)", 800, 112, 104));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s2,s4) values(%d,%d,%d)", 900, 118, 105));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s2,s4) values(%d,%d,%d)", 1000, 100, 106));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s2,s4) values(%d,%d,%d)", 100, 100, 101));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s2,s4) values(%d,%d,%d)", 200, 102, 101));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s2,s4) values(%d,%d,%d)", 300, 104, 102));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s2,s4) values(%d,%d,%d)", 400, 126, 102));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s2,s4) values(%d,%d,%d)", 500, 108, 103));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s2,s4) values(%d,%d,%d)", 600, 112, 104));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s2,s4) values(%d,%d,%d)", 700, 114, 104));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s2,s4) values(%d,%d,%d)", 800, 112, 104));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s2,s4) values(%d,%d,%d)", 900, 118, 105));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s2,s4) values(%d,%d,%d)", 1000, 100, 106));
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create function completeness as 'org.apache.iotdb.library.dmatch.UDAFCov'");
      statement.execute("create function timeliness as 'org.apache.iotdb.library.dmatch.UDAFDtw'");
      statement.execute(
          "create function consistency as 'org.apache.iotdb.library.dmatch.UDAFPearson'");
      statement.execute("create function validity as 'org.apache.iotdb.library.dmatch.UDTFPtnSym'");
      statement.execute("create function validity as 'org.apache.iotdb.library.dmatch.UDTFXcorr'");
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
    String sqlStr = "select cov(d1.s1,d1.s3) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength = resultSet.getRow();
      assert resultSetLength == 1;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testCov2() {
    String sqlStr = "select cov(d1.s2,d1.s4) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength = resultSet.getRow();
      assert resultSetLength == 1;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testCov3() {
    String sqlStr = "select cov(d2.s1,d2.s3) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength = resultSet.getRow();
      assert resultSetLength == 1;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testCov4() {
    String sqlStr = "select cov(d2.s2,d2.s4) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength = resultSet.getRow();
      assert resultSetLength == 1;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testDtw1() {
    String sqlStr = "select dtw(d1.s1,d1.s3) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength = resultSet.getRow();
      assert resultSetLength == 1;
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= 0.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testDtw2() {
    String sqlStr = "select dtw(d1.s2,d1.s4) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength = resultSet.getRow();
      assert resultSetLength == 1;
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= 0.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testDtw3() {
    String sqlStr = "select dtw(d2.s1,d2.s3) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength = resultSet.getRow();
      assert resultSetLength == 1;
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= 0.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testDtw4() {
    String sqlStr = "select dtw(d2.s2,d2.s4) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength = resultSet.getRow();
      assert resultSetLength == 1;
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= 0.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testPearson1() {
    String sqlStr = "select pearson(d1.s1,d1.s3) from root.vehicle";
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
    String sqlStr = "select pearson(d1.s2,d1.s4) from root.vehicle";
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
    String sqlStr = "select pearson(d2.s1,d2.s3) from root.vehicle";
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
    String sqlStr = "select pearson(d2.s2,d2.s4) from root.vehicle";
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
    String sqlStr = "select ptnsym(d1.s1, 'window'='3', 'threshold'='0') from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength = resultSet.getRow();
      assert resultSetLength == 1;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testPtnSym2() {
    String sqlStr = "select ptnsym(d1.s2, 'window'='3', 'threshold'='0') from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength = resultSet.getRow();
      assert resultSetLength == 1;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testPtnSym3() {
    String sqlStr = "select ptnsym(d2.s1, 'window'='3', 'threshold'='0') from";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength = resultSet.getRow();
      assert resultSetLength == 1;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testPtnSym4() {
    String sqlStr = "select ptnsym(d2.s2, 'window'='3', 'threshold'='0') from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength = resultSet.getRow();
      assert resultSetLength == 1;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testXCorr1() {
    String sqlStr = "select xcorr(d1.s1,d1.s3) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength = resultSet.getRow();
      assert resultSetLength == 19;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testXCorr2() {
    String sqlStr = "select xcorr(d1.s2, d1.s4) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength = resultSet.getRow();
      assert resultSetLength == 19;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testXCorr3() {
    String sqlStr = "select xcorr(d2.s1, d2.s3) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength = resultSet.getRow();
      assert resultSetLength == 19;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testXCorr4() {
    String sqlStr = "select xcorr(d2.s2,d2.s4) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength = resultSet.getRow();
      assert resultSetLength == 19;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
