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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

// TODO: check null values
public class IoTDBSelectIntoIT {

  private static final String[] INSERTION_SQLS = {
    "insert into root.sg.d1(time, s2, s3, s4, s5, s6) values (0, 0, 0, 0, true, '0')",
    "insert into root.sg.d1(time, s1, s3, s4, s5, s6) values (1, 1, 1, 1, false, '1')",
    "insert into root.sg.d1(time, s1, s2, s4, s5, s6) values (2, 2, 2, 2, true, '2')",
    "insert into root.sg.d1(time, s1, s2, s3, s5, s6) values (3, 3, 3, 3, false, '3')",
    "insert into root.sg.d1(time, s1, s2, s3, s4, s5, s6) values (4, 4, 4, 4, 4, true, '4')",
  };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    createTimeSeries();
    generateData();
  }

  private static void createTimeSeries() throws MetadataException {
    IoTDB.metaManager.setStorageGroup(new PartialPath("root.sg"));

    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.sg.d1.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.sg.d1.s2"),
        TSDataType.INT64,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.sg.d1.s3"),
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.sg.d1.s4"),
        TSDataType.DOUBLE,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.sg.d1.s5"),
        TSDataType.BOOLEAN,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.sg.d1.s6"),
        TSDataType.TEXT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);

    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.sg.d1.empty"),
        TSDataType.TEXT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);

    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.sg.d2.s1"),
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
      for (String dataGenerationSql : INSERTION_SQLS) {
        statement.execute(dataGenerationSql);
      }

      statement.execute("insert into root.sg.d2(time, s1) values (0, 0)");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void selectIntoSameDevice() {
    try (Statement statement =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root")
            .createStatement()) {
      statement.execute(
          "select s1, s2, s3, s4, s5, s6 into s7, s8, s9, s10, s11, s12 from root.sg.d1");

      ResultSet resultSet =
          statement.executeQuery("select s7, s8, s9, s10, s11, s12 from root.sg.d1");

      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 6, columnCount);

      for (int i = 0; i < INSERTION_SQLS.length; ++i) {
        assertTrue(resultSet.next());
        StringBuilder stringBuilder = new StringBuilder();
        for (int j = 0; j < 6 + 1; ++j) {
          stringBuilder.append(resultSet.getString(j + 1)).append(',');
        }
        System.out.println(stringBuilder.toString());
      }

      assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void selectIntoDifferentDevices() {
    try (Statement statement =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root")
            .createStatement()) {
      statement.execute(
          "select s1, s2, s3, s4, s5, s6 into pre_${2}_suf.s1, pre_${2}_suf.s2, pre_${2}_suf.s3, pre_${2}_suf.s4, pre_${2}_suf.s5, pre_${2}_suf.s6 from root.sg.d1");

      ResultSet resultSet =
          statement.executeQuery(
              "select pre_d1_suf.s1, pre_d1_suf.s2, pre_d1_suf.s3, pre_d1_suf.s4, pre_d1_suf.s5, pre_d1_suf.s6 from root.sg.d1");

      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 6, columnCount);

      for (int i = 0; i < INSERTION_SQLS.length; ++i) {
        assertTrue(resultSet.next());
        StringBuilder stringBuilder = new StringBuilder();
        for (int j = 0; j < 6 + 1; ++j) {
          stringBuilder.append(resultSet.getString(j + 1)).append(',');
        }
        System.out.println(stringBuilder.toString());
      }

      assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      throwable.printStackTrace();
      fail(throwable.getMessage());
    }
  }

  @Test
  public void selectFromEmptySourcePath() {
    try (Statement statement =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root")
            .createStatement()) {
      statement.execute("select empty into target from root.sg.d1");

      ResultSet resultSet = statement.executeQuery("select target from root.sg.d1");

      assertEquals(1, resultSet.getMetaData().getColumnCount());
      assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      throwable.printStackTrace();
      fail(throwable.getMessage());
    }
  }

  @Test
  public void selectIntoFullTargetPath() {
    try (Statement statement =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root")
            .createStatement()) {
      statement.execute("select s1 into root.${2}.${1}.s1 from root.sg.d1 where time>0");

      ResultSet resultSet = statement.executeQuery("select sg.d1.s1, d1.sg.s1 from root");

      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 2, columnCount);

      for (int i = 0; i < INSERTION_SQLS.length - 1; ++i) {
        assertTrue(resultSet.next());
        assertEquals(resultSet.getString(1), String.valueOf(i + 1));
        assertEquals(resultSet.getString(2), resultSet.getString(3));
      }

      assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testDifferentNumbersOfSourcePathsAndTargetPaths() {
    try (Statement statement =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root")
            .createStatement()) {
      statement.execute("select s1, s2 into target from root.sg.d1");
      fail();
    } catch (SQLException throwable) {
      assertTrue(
          throwable
              .getMessage()
              .contains(
                  "the number of source paths and the number of target paths should be the same"));
    }

    try (Statement statement =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root")
            .createStatement()) {
      statement.execute("select s1 into target from root.sg.*");
      fail();
    } catch (SQLException throwable) {
      assertTrue(
          throwable
              .getMessage()
              .contains(
                  "the number of source paths and the number of target paths should be the same"));
    }

    try (Statement statement =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root")
            .createStatement()) {
      statement.execute("select * into target from root.sg.d1");
      fail();
    } catch (SQLException throwable) {
      assertTrue(
          throwable
              .getMessage()
              .contains(
                  "the number of source paths and the number of target paths should be the same"));
    }
  }

  @Test
  public void testMultiPrefixPathsInFromClause() {
    try (Statement statement =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root")
            .createStatement()) {
      statement.execute("select s1 into target from root.sg.d1, root.sg.d2");
      fail();
    } catch (SQLException throwable) {
      assertTrue(
          throwable
              .getMessage()
              .contains("the number of prefix paths in the from clause should be 1"));
    }
  }

  @Test
  public void testLeveledPathNodePatternLimit() {
    try (Statement statement =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root")
            .createStatement()) {
      statement.execute("select s1 into root.${100}.s1 from root.sg.d1");
      ResultSet resultSet = statement.executeQuery("select sg.d1.s1, d1.sg.s1 from root");
      assertEquals(1, resultSet.getMetaData().getColumnCount());
      assertFalse(resultSet.next());
      fail();
    } catch (SQLException throwable) {
      assertTrue(
          throwable
              .getMessage()
              .contains(
                  "the x of ${x} should be greater than 0 and equal to or less than <level> or the length of queried path prefix."));
    }

    try (Statement statement =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root")
            .createStatement()) {
      statement.execute("select s1 into root.${0}.s1 from root.sg.d1");
      ResultSet resultSet = statement.executeQuery("select sg.d1.s1, d1.sg.s1 from root");
      assertEquals(1, resultSet.getMetaData().getColumnCount());
      assertFalse(resultSet.next());
      fail();
    } catch (SQLException throwable) {
      assertTrue(
          throwable
              .getMessage()
              .contains(
                  "the x of ${x} should be greater than 0 and equal to or less than <level> or the length of queried path prefix."));
    }

    try (Statement statement =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root")
            .createStatement()) {
      statement.execute("select s1 into root.${wrong}.s1 from root.sg.d1");
      ResultSet resultSet = statement.executeQuery("select sg.d1.s1, d1.sg.s1 from root");
      assertEquals(1, resultSet.getMetaData().getColumnCount());
      assertFalse(resultSet.next());
      fail();
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("the x of ${x} should be an integer."));
    }
  }
}
