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

public class AnomalyTests {
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
      statement.execute(String.format("insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",100,0,0));
      statement.execute(String.format("insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",100,2,2));
      statement.execute(String.format("insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",100,-2,-2));
      statement.execute(String.format("insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",100,-1,-1));
      statement.execute(String.format("insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",100,10,10));
      statement.execute(String.format("insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",100,1,1));
      statement.execute(String.format("insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",100,0,0));
      statement.execute(String.format("insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",100,2,2));
      statement.execute(String.format("insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",100,-1,-1));
      statement.execute(String.format("insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",100,1,1));
      statement.execute(String.format("insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",100,0,0));
      statement.execute(String.format("insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",100,2,2));
      statement.execute(String.format("insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",100,-2,-2));
      statement.execute(String.format("insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",100,-1,-1));
      statement.execute(String.format("insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",100,10,10));
      statement.execute(String.format("insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",100,1,1));
      statement.execute(String.format("insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",100,0,0));
      statement.execute(String.format("insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",100,2,2));
      statement.execute(String.format("insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",100,-1,-1));
      statement.execute(String.format("insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%d,%d)",100,1,1));
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
        .setUdfCollectorMemoryBudgetInMB(100)
        .setUdfTransformerMemoryBudgetInMB(100)
        .setUdfReaderMemoryBudgetInMB(100);
  }

  @Test
  public void testIRQR1() {
    String sqlStr = "select iqr(d1.s1) from root.vehicle";
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
  public void testIQR2() {
    String sqlStr = "select iqr(d1.s2) from root.vehicle";
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
  public void testIQR3() {
    String sqlStr = "select iqr(d2.s1) from root.vehicle";
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
  public void testIQR4() {
    String sqlStr = "select iqr(d2.s2) from root.vehicle";
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
  public void testKSigma1() {
    String sqlStr = "select ksigma(d1.s1,\"k\"=\"1.0\") from root.vehicle";
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
  public void testKSigma2() {
    String sqlStr = "select ksigma(d1.s2,\"k\"=\"1.0\") from root.vehicle";
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
  public void testKSigma3() {
    String sqlStr = "select ksigma(d2.s1,\"k\"=\"1.0\") from root.vehicle";
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
  public void testKSigma4() {
    String sqlStr = "select ksigma(d2.s2,\"k\"=\"1.0\") from root.vehicle";
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
  public void testMissDetect1() {
    String sqlStr = "select missdetect(d1.s1,'minlen'='10') from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result == 1 || result == 0;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testMissDetect2() {
    String sqlStr = "select missdetect(d1.s2,'minlen'='10') from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result == 1 || result == 0;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testMissDetect3() {
    String sqlStr = "select missdetect(d2.s1,'minlen'='10') from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result == 1 || result == 0;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testMissDetect4() {
    String sqlStr = "select missdetect(d2.s2,'minlen'='10') from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result == 1 || result == 0;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testLOF1() {
    String sqlStr = "select lof(d1.s1, \"method\"=\"series\") from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= 0;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testLOF2() {
    String sqlStr = "select lof(d1.s2, \"method\"=\"series\") from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= 0;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testLOF3() {
    String sqlStr = "select lof(d2.s1, \"method\"=\"series\") from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= 0;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testLOF4() {
    String sqlStr = "select lof(d2.s2, \"method\"=\"series\") from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
      assert result >= 0;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRange1() {
    String sqlStr =
        "select range(d1.s1,\"lower_bound\"=\"-5.0\",\"upper_bound\"=\"5.0\") from root.vehicle";
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
  public void testRange2() {
    String sqlStr =
        "select range(d1.s2,\"lower_bound\"=\"-5.0\",\"upper_bound\"=\"5.0\") from root.vehicle";
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
  public void testRange3() {
    String sqlStr =
        "select range(d2.s1,\"lower_bound\"=\"-5.0\",\"upper_bound\"=\"5.0\") from root.vehicle";
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
  public void testRange4() {
    String sqlStr =
        "select range(d2.s2,\"lower_bound\"=\"-5.0\",\"upper_bound\"=\"5.0\") from root.vehicle";
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
  public void testTwoSidedFileter1() {
    String sqlStr = "select TwoSidedFilter(d1.s1, 'len'='3', 'threshold'='0.3') from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength=resultSet.getRow();
      assert resultSetLength<=10;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testTwoSidedFileter2() {
    String sqlStr = "select TwoSidedFilter(d1.s2, 'len'='3', 'threshold'='0.3') from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength=resultSet.getRow();
      assert resultSetLength<=10;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testTwoSidedFileter3() {
    String sqlStr = "select TwoSidedFilter(d2.s1, 'len'='3', 'threshold'='0.3') from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength=resultSet.getRow();
      assert resultSetLength<=10;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testTwoSidedFileter4() {
    String sqlStr = "select TwoSidedFilter(d2.s2, 'len'='3', 'threshold'='0.3') from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength=resultSet.getRow();
      assert resultSetLength<=10;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
