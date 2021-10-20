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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

// TODO: check null values
public class IoTDBSelectIntoIT {

  private static final int ROW_LIMIT =
      IoTDBDescriptor.getInstance().getConfig().getSelectIntoInsertTabletPlanRowLimit();

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

      final int size = ROW_LIMIT + 1;
      for (int i = 0; i < size; ++i) {
        statement.execute(String.format("insert into root.sg.d3(time, s1) values (%d, %d)", i, i));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test // TODO: check values
  public void selectIntoSameDevice() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "select s1, s2, s3, s4, s5, s6 into s7, s8, s9, s10, s11, s12 from root.sg.d1");

      try (ResultSet resultSet =
          statement.executeQuery("select s7, s8, s9, s10, s11, s12 from root.sg.d1")) {
        assertEquals(1 + 6, resultSet.getMetaData().getColumnCount());

        for (int i = 0; i < INSERTION_SQLS.length; ++i) {
          assertTrue(resultSet.next());
          StringBuilder stringBuilder = new StringBuilder();
          for (int j = 0; j < 6 + 1; ++j) {
            stringBuilder.append(resultSet.getString(j + 1)).append(',');
          }
          System.out.println(stringBuilder.toString());
        }

        assertFalse(resultSet.next());
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test // TODO: check values
  public void selectIntoDifferentDevices() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "select s1, s2, s3, s4, s5, s6 into pre_${2}_suf.s1, pre_${2}_suf.s2, pre_${2}_suf.s3, pre_${2}_suf.s4, pre_${2}_suf.s5, pre_${2}_suf.s6 from root.sg.d1");

      try (ResultSet resultSet =
          statement.executeQuery(
              "select pre_d1_suf.s1, pre_d1_suf.s2, pre_d1_suf.s3, pre_d1_suf.s4, pre_d1_suf.s5, pre_d1_suf.s6 from root.sg.d1")) {
        assertEquals(1 + 6, resultSet.getMetaData().getColumnCount());

        for (int i = 0; i < INSERTION_SQLS.length; ++i) {
          assertTrue(resultSet.next());
          StringBuilder stringBuilder = new StringBuilder();
          for (int j = 0; j < 6 + 1; ++j) {
            stringBuilder.append(resultSet.getString(j + 1)).append(',');
          }
          System.out.println(stringBuilder.toString());
        }

        assertFalse(resultSet.next());
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void selectFromEmptySourcePath() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select empty into target from root.sg.d1");

      try (ResultSet resultSet = statement.executeQuery("select target from root.sg.d1")) {
        assertEquals(1, resultSet.getMetaData().getColumnCount());
        assertFalse(resultSet.next());
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void selectIntoFullTargetPath() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select s1 into root.${2}.${1}.s1 from root.sg.d1 where time>0");

      try (ResultSet resultSet = statement.executeQuery("select sg.d1.s1, d1.sg.s1 from root")) {
        assertEquals(1 + 2, resultSet.getMetaData().getColumnCount());

        for (int i = 0; i < INSERTION_SQLS.length - 1; ++i) {
          assertTrue(resultSet.next());
          assertEquals(resultSet.getString(1), String.valueOf(i + 1));
          assertEquals(resultSet.getString(2), resultSet.getString(3));
        }

        assertFalse(resultSet.next());
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void selectSameTimeSeries() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select s1, s1 into s1s2, s1s3 from root.sg.d1");

      try (ResultSet resultSet = statement.executeQuery("select s1s2, s1s3 from root.sg.d1")) {
        assertEquals(1 + 2, resultSet.getMetaData().getColumnCount());

        for (int i = 1; i < INSERTION_SQLS.length; ++i) {
          assertTrue(resultSet.next());
          for (int j = 0; j < 2 + 1; ++j) {
            assertEquals(resultSet.getString(2), resultSet.getString(3));
          }
        }

        assertFalse(resultSet.next());
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testLargeData() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select s1 into large_s1 from root.sg.d3");

      try (ResultSet resultSet = statement.executeQuery("select large_s1 from root.sg.d3")) {
        assertEquals(1 + 1, resultSet.getMetaData().getColumnCount());

        final int size = ROW_LIMIT + 1;
        for (int i = 0; i < size; ++i) {
          assertTrue(resultSet.next());
          assertEquals(
              Double.parseDouble(resultSet.getString(1)),
              Double.parseDouble(resultSet.getString(2)),
              0);
        }
        assertFalse(resultSet.next());
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testUDFQuery() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "select s1, sin(s1), s1 + s1 into ${2}.s2, ${2}.s3, ${2}.s4 from root.sg.d1");

      try (ResultSet resultSet = statement.executeQuery("select s2, s3, s4 from root.sg.d1.d1")) {
        assertEquals(1 + 3, resultSet.getMetaData().getColumnCount());

        for (int i = 1; i < INSERTION_SQLS.length; ++i) {
          assertTrue(resultSet.next());
          for (int j = 0; j < 2 + 1; ++j) {
            double s2 = Double.parseDouble(resultSet.getString(2));
            double s3 = Double.parseDouble(resultSet.getString(3));
            double s4 = Double.parseDouble(resultSet.getString(4));
            assertEquals(i, s2, 0);
            assertEquals(Math.sin(i), s3, 0);
            assertEquals((double) i + (double) i, s4, 0);
          }
        }

        assertFalse(resultSet.next());
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testNestedQuery() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "select s1 * sin(s1) + cos(s1), sin(s1) / s1 + s1, s1 into ${2}.n2, ${2}.n3, ${2}.n4 from root.sg.d1");

      try (ResultSet resultSet = statement.executeQuery("select n2, n3, n4 from root.sg.d1.d1")) {
        assertEquals(1 + 3, resultSet.getMetaData().getColumnCount());

        for (int i = 1; i < INSERTION_SQLS.length; ++i) {
          assertTrue(resultSet.next());
          for (int j = 0; j < 2 + 1; ++j) {
            double s2 = Double.parseDouble(resultSet.getString(2));
            double s3 = Double.parseDouble(resultSet.getString(3));
            double s4 = Double.parseDouble(resultSet.getString(4));
            assertEquals(i * Math.sin(i) + Math.cos(i), s2, 0);
            assertEquals(Math.sin(i) / i + i, s3, 0);
            assertEquals(i, s4, 0);
          }
        }

        assertFalse(resultSet.next());
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testGroupByQuery() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select count(s1) into count_s1 from root.sg.d1 group by ([1, 5),1ms);");

      try (ResultSet resultSet = statement.executeQuery("select count_s1 from root.sg.d1")) {
        assertEquals(1 + 1, resultSet.getMetaData().getColumnCount());

        for (int i = 1; i < INSERTION_SQLS.length; ++i) {
          assertTrue(resultSet.next());
          for (int j = 0; j < 1 + 1; ++j) {
            assertEquals(String.valueOf(i), resultSet.getString(1));
            assertEquals("1", resultSet.getString(2));
          }
        }

        assertFalse(resultSet.next());
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testGroupByFillQuery() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "select last_value(s1) into gbf_s1 from root.sg.d1 group by ([1, 10),1ms) fill (float[PREVIOUS]);");

      try (ResultSet resultSet = statement.executeQuery("select gbf_s1 from root.sg.d1")) {
        assertEquals(1 + 1, resultSet.getMetaData().getColumnCount());

        for (int i = 1; i < 5; ++i) {
          assertTrue(resultSet.next());
          for (int j = 0; j < 1 + 1; ++j) {
            assertEquals(String.valueOf(i), resultSet.getString(1));
            assertEquals(i < 5 ? String.valueOf(i) : "0", resultSet.getString(2));
          }
        }

        assertFalse(resultSet.next());
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testFillQuery() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "select s1 into fill_s1 from root.sg.d1 where time = 10 fill(float [linear, 1ms, 1ms])");

      try (ResultSet resultSet = statement.executeQuery("select fill_s1 from root.sg.d1")) {
        assertEquals(1 + 1, resultSet.getMetaData().getColumnCount());

        assertTrue(resultSet.next());
        assertEquals("10", resultSet.getString(1));
        assertEquals("4", resultSet.getString(2));

        assertFalse(resultSet.next());
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testDifferentNumbersOfSourcePathsAndTargetPaths() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select s1, s2 into target from root.sg.d1");
      fail();
    } catch (SQLException throwable) {
      assertTrue(
          throwable
              .getMessage()
              .contains(
                  "the number of source paths and the number of target paths should be the same"));
    }

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select s1 into target from root.sg.*");
      fail();
    } catch (SQLException throwable) {
      assertTrue(
          throwable
              .getMessage()
              .contains(
                  "the number of source paths and the number of target paths should be the same"));
    }

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
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
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
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
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select s1 into root.${100}.s1 from root.sg.d1");
      fail();
    } catch (SQLException throwable) {
      assertTrue(
          throwable
              .getMessage()
              .contains(
                  "the x of ${x} should be greater than 0 and equal to or less than <level> or the length of queried path prefix."));
    }

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select s1 into root.${0}.s1 from root.sg.d1");
      fail();
    } catch (SQLException throwable) {
      assertTrue(
          throwable
              .getMessage()
              .contains(
                  "the x of ${x} should be greater than 0 and equal to or less than <level> or the length of queried path prefix."));
    }

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select s1 into root.${wrong}.s1 from root.sg.d1");
      fail();
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("the x of ${x} should be an integer."));
    }
  }

  @Test
  public void testAlignByDevice() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select s1 into root.${1}.s1 from root.sg.d1 align by device");
      fail();
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("align by device clauses are not supported."));
    }
  }

  @Test
  public void testDisableDevice() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select s1 into root.${1}.s1 from root.sg.d1 disable align");
      fail();
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("disable align clauses are not supported."));
    }
  }

  @Test
  public void testLastQuery() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select last s1 into root.${1}.s1 from root.sg.d1");
      fail();
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("last clauses are not supported."));
    }
  }

  @Test
  public void testSlimit() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select s1, s2 into ${1}.s1, ${2}.s1 from root.sg.d1 slimit 1");
      fail();
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("slimit clauses are not supported."));
    }
  }

  @Test
  public void testDescending() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select s1, s2 into ${1}.s1, ${2}.s1 from root.sg.d1 order by time desc");
      fail();
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("desc clauses are not supported."));
    }
  }

  @Test
  public void testSameTargetPaths() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select s1, s2 into ${1}.s1, ${1}.s1 from root.sg.d1");
      fail();
    } catch (SQLException throwable) {
      assertTrue(
          throwable.getMessage().contains("target paths in into clause should be different."));
    }
  }

  @Test
  public void testContainerCase() throws SQLException {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (int i = 0; i < 10; i++) {
        statement.execute(
            String.format("INSERT INTO root.sg.device%s(timestamp,s) VALUES(1,1)", i));
      }

      statement.execute(
          "SELECT device0.s, device1.s, device2.s, device3.s, device4.s, device5.s, device6.s, device7.s, device8.s, device9.s "
              + "INTO device0.t, device1.t, device2.t, device3.t, device4.t, device5.t, device6.t, device7.t, device8.t, device9.t "
              + "FROM root.sg;");

      for (int i = 0; i < 10; i++) {
        statement.execute(
            String.format("INSERT INTO root.sg.device%s(timestamp,s) VALUES(2,2)", i));
        statement.execute(String.format("SELECT device%s.s into device%s.t from root.sg;", i, i));
      }

      for (int i = 0; i < 10; ++i) {
        try (ResultSet resultSet =
            statement.executeQuery(String.format("SELECT s, t FROM root.sg.device%s", i))) {
          assertTrue(resultSet.next());
          assertEquals(1, Double.parseDouble(resultSet.getString(1)), 0);
          assertEquals(
              Double.parseDouble(resultSet.getString(1)),
              Double.parseDouble(resultSet.getString(2)),
              0);
          assertEquals(
              Double.parseDouble(resultSet.getString(2)),
              Double.parseDouble(resultSet.getString(3)),
              0);

          assertTrue(resultSet.next());
          assertEquals(2, Double.parseDouble(resultSet.getString(1)), 0);
          assertEquals(
              Double.parseDouble(resultSet.getString(1)),
              Double.parseDouble(resultSet.getString(2)),
              0);
          assertEquals(
              Double.parseDouble(resultSet.getString(2)),
              Double.parseDouble(resultSet.getString(3)),
              0);

          assertFalse(resultSet.next());
        }
      }
    }
  }
}
