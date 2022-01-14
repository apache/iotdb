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

package org.apache.iotdb.library.string;

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

public class StringTests {
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
      statement.execute(String.format("insert into root.vehicle.d1(timestamp,s1) values(%d,%d,%d)",100,100,101));
      statement.execute(String.format("insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",200,102,101));
      statement.execute(String.format("insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",300,104,102));
      statement.execute(String.format("insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",400,126,102));
      statement.execute(String.format("insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",500,108,103));
      statement.execute(String.format("insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",600,112,104));
      statement.execute(String.format("insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",700,114,104));
      statement.execute(String.format("insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",800,112,104));
      statement.execute(String.format("insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",900,118,105));
      statement.execute(String.format("insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",1000,100,106));
      statement.execute(String.format("insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",100,100,101));
      statement.execute(String.format("insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",200,102,101));
      statement.execute(String.format("insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",300,104,102));
      statement.execute(String.format("insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",400,126,102));
      statement.execute(String.format("insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",500,108,103));
      statement.execute(String.format("insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",600,112,104));
      statement.execute(String.format("insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",700,114,104));
      statement.execute(String.format("insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",800,112,104));
      statement.execute(String.format("insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",900,118,105));
      statement.execute(String.format("insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",1000,100,106));
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create function regexmatch as 'org.apache.iotdb.library.string.UDTFRegexMatch'");
      statement.execute(
          "create function regexreplace as 'org.apache.iotdb.library.string.UDTFRegexReplace'");
      statement.execute(
          "create function regexsplit as 'org.apache.iotdb.library.string.UDTFRegexSplit'");
      statement.execute(
          "create function strreplace as 'org.apache.iotdb.library.string.UDTFStrReplace'");
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
  public void testRegexMatch1() {
    String sqlStr = "select regexmatch(d1.s1,\"regex\"=\"\\d+\\.\\d+\\.\\d+\\.\\d+\", \"group\"=\"0\") from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength=resultSet.getRow();
      assert resultSetLength==1;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRegexMatch2() {
    String sqlStr = "select regexmatch(d1.s1,\"regex\"=\"\\d+\\.\\d+\\.\\d+\\.\\d+\", \"group\"=\"0\") from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength=resultSet.getRow();
      assert resultSetLength==1;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRegexMatch3() {
    String sqlStr = "select regexmatch(d1.s1,\"regex\"=\"\\d+\\.\\d+\\.\\d+\\.\\d+\", \"group\"=\"0\") from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength=resultSet.getRow();
      assert resultSetLength==1;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRegexMatch4() {
    String sqlStr = "select regexmatch(d1.s1,\"regex\"=\"\\d+\\.\\d+\\.\\d+\\.\\d+\", \"group\"=\"0\") from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength=resultSet.getRow();
      assert resultSetLength==1;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRegexReplace1() {
    String sqlStr = "select regexreplace(d1.s1,\"regex\"=\"192\\.168\\.0\\.(\\d+)\", \"replace\"=\"cluster-$1\") from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength=resultSet.getRow();
      assert resultSetLength==1;
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= 0.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRegexReplace2() {
    String sqlStr = "select regexreplace(d1.s1,\"regex\"=\"192\\.168\\.0\\.(\\d+)\", \"replace\"=\"cluster-$1\") from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength=resultSet.getRow();
      assert resultSetLength==1;
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= 0.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRegexReplace3() {
    String sqlStr = "select regexreplace(d1.s1,\"regex\"=\"192\\.168\\.0\\.(\\d+)\", \"replace\"=\"cluster-$1\") from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength=resultSet.getRow();
      assert resultSetLength==1;
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= 0.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRegexReplace4() {
    String sqlStr = "select regexreplace(d1.s1,\"regex\"=\"192\\.168\\.0\\.(\\d+)\", \"replace\"=\"cluster-$1\") from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength=resultSet.getRow();
      assert resultSetLength==1;
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= 0.0D;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRegexSplit1() {
    String sqlStr = "select regexsplit(d1.s1, \"regex\"=\",\") from root.vehicle";
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
  public void testRegexSplit2() {
    String sqlStr = "select regexsplit(d1.s1, \"regex\"=\",\") from root.vehicle";
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
  public void testRegexSplit3() {
    String sqlStr = "select regexsplit(d1.s1, \"regex\"=\",\") from root.vehicle";
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
  public void testRegexSplit4() {
    String sqlStr = "select regexsplit(d1.s1, \"regex\"=\",\") from root.vehicle";
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
  public void testStrReplace1() {
    String sqlStr = "select strreplace(d2.s1,\"target\"=\",\", \"replace\"=\"_\") from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength=resultSet.getRow();
      assert resultSetLength==1;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testStrReplace2() {
    String sqlStr = "select strreplace(d1.s2,\"target\"=\",\", \"replace\"=\"_\") from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength=resultSet.getRow();
      assert resultSetLength==1;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testStrReplace3() {
    String sqlStr = "select strreplace(d2.s1,\"target\"=\",\", \"replace\"=\"_\") from";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength=resultSet.getRow();
      assert resultSetLength==1;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testStrReplace4() {
    String sqlStr = "select strreplace(d2.s2,\"target\"=\",\", \"replace\"=\"_\") from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength=resultSet.getRow();
      assert resultSetLength==1;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
