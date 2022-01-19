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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.fail;

public class StringTests {
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
        TSDataType.TEXT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d2.s1"),
        TSDataType.TEXT,
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
              "insert into root.vehicle.d1(timestamp,s1) values(%d,%s)",
              100, "[192.168.0.1] [SUCCESS]"));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1) values(%d,%s)",
              200, "[192.168.0.24] [SUCCESS]"));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1) values(%d,%s)",
              300, "[192.168.0.2] [FAIL]"));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1) values(%d,%s)",
              400, "[192.168.0.5] [SUCCESS]"));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1) values(%d,%s)",
              500, "[192.168.0.124] [SUCCESS]"));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1) values(%d,%s)", 100, "A,B,A+,B-"));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1) values(%d,%s)", 200, "A,A+,A,B+"));
      statement.execute(
          String.format("insert into root.vehicle.d2(timestamp,s1) values(%d,%s)", 300, "B+,B,B"));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1) values(%d,%s)", 400, "A+,A,A+,A"));
      statement.execute(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1) values(%d,%s)", 500, "A,B-,B,B"));
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
        .setUdfCollectorMemoryBudgetInMB(oldUdfCollectorMemoryBudgetInMB)
        .setUdfTransformerMemoryBudgetInMB(oldUdfTransformerMemoryBudgetInMB)
        .setUdfReaderMemoryBudgetInMB(oldUdfReaderMemoryBudgetInMB);
  }

  @Test
  public void testRegexMatch1() {
    String sqlStr =
        "select regexmatch(d1.s1,\"regex\"=\"\\d+\\.\\d+\\.\\d+\\.\\d+\", \"group\"=\"0\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength = resultSet.getRow();
      Assert.assertEquals(resultSetLength, 5);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRegexReplace1() {
    String sqlStr =
        "select regexreplace(d2.s1,\"regex\"=\"192\\.168\\.0\\.(\\d+)\", \"replace\"=\"cluster-$1\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength = resultSet.getRow();
      Assert.assertEquals(resultSetLength, 5);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRegexSplit1() {
    String sqlStr = "select regexsplit(d2.s1, \"regex\"=\",\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength = resultSet.getRow();
      Assert.assertEquals(resultSetLength, 5);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testStrReplace1() {
    String sqlStr =
        "select strreplace(d2.s1,\"target\"=\",\", \"replace\"=\"_\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.last();
      int resultSetLength = resultSet.getRow();
      Assert.assertEquals(resultSetLength, 5);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
