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
package org.apache.iotdb.db.integration.versionadaption;

import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.jdbc.Constant;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({LocalStandaloneTest.class})
public class IoTDBDDLVersionAdaptionIT {
  private static final Logger logger = LoggerFactory.getLogger(IoTDBDDLVersionAdaptionIT.class);

  private static void insertSQL() {
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_0_12);
        Statement statement = connection.createStatement()) {

      String[] insertSqls =
          new String[] {
            "SET STORAGE GROUP TO root.ln.wf01.wt01",
            "SET STORAGE GROUP TO root.ln.wf01.wt02",
            "SET STORAGE GROUP TO root.ln1.wf01.wt01",
            "SET STORAGE GROUP TO root.ln2.wf01.wt01",
            "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE = BOOLEAN, ENCODING = PLAIN",
            "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE = FLOAT, ENCODING = RLE, "
                + "compressor = SNAPPY, MAX_POINT_NUMBER = 3",
            "CREATE ALIGNED TIMESERIES root.ln.wf01.wt02(s1 INT32, s2 DOUBLE)",
            "CREATE TIMESERIES root.ln1.wf01.wt01.status WITH DATATYPE = BOOLEAN, ENCODING = PLAIN",
            "CREATE TIMESERIES root.ln1.wf01.wt01.temperature WITH DATATYPE = FLOAT, ENCODING = RLE, "
                + "compressor = SNAPPY, MAX_POINT_NUMBER = 3",
            "CREATE TIMESERIES root.ln2.wf01.wt01.status WITH DATATYPE = BOOLEAN, ENCODING = PLAIN",
            "CREATE TIMESERIES root.ln2.wf01.wt01.temperature WITH DATATYPE = FLOAT, ENCODING = RLE, "
                + "compressor = SNAPPY, MAX_POINT_NUMBER = 3"
          };

      for (String sql : insertSqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      logger.error("insertSQL() failed", e);
      fail(e.getMessage());
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeTest();

    insertSQL();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterTest();
  }

  @Test
  public void showTimeseriesTest() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_0_12);
        Statement statement = connection.createStatement()) {

      String[] sqls =
          new String[] {
            "show timeseries root.ln.wf01.wt01.status", // full seriesPath
            "show timeseries root.ln.*", // prefix seriesPath
            "show timeseries root.ln.*.wt01", // seriesPath with stars
            "show timeseries", // the same as root
            "show timeseries root.a.b", // nonexistent timeseries, thus returning ""
          };
      Set<String>[] standards =
          new Set[] {
            new HashSet<>(
                Collections.singletonList(
                    "root.ln.wf01.wt01.status,null,root.ln.wf01.wt01,BOOLEAN,PLAIN,SNAPPY,null,null,")),
            new HashSet<>(
                Arrays.asList(
                    "root.ln.wf01.wt01.status,null,root.ln.wf01.wt01,BOOLEAN,PLAIN,SNAPPY,null,null,",
                    "root.ln.wf01.wt01.temperature,null,root.ln.wf01.wt01,FLOAT,RLE,SNAPPY,null,null,",
                    "root.ln.wf01.wt02.s1,null,root.ln.wf01.wt02,INT32,RLE,SNAPPY,null,null,",
                    "root.ln.wf01.wt02.s2,null,root.ln.wf01.wt02,DOUBLE,GORILLA,SNAPPY,null,null,")),
            new HashSet<>(
                Arrays.asList(
                    "root.ln.wf01.wt01.status,null,root.ln.wf01.wt01,BOOLEAN,PLAIN,SNAPPY,null,null,",
                    "root.ln.wf01.wt01.temperature,null,root.ln.wf01.wt01,FLOAT,RLE,SNAPPY,null,null,")),
            new HashSet<>(
                Arrays.asList(
                    "root.ln.wf01.wt01.status,null,root.ln.wf01.wt01,BOOLEAN,PLAIN,SNAPPY,null,null,",
                    "root.ln.wf01.wt01.temperature,null,root.ln.wf01.wt01,FLOAT,RLE,SNAPPY,null,null,",
                    "root.ln.wf01.wt02.s1,null,root.ln.wf01.wt02,INT32,RLE,SNAPPY,null,null,",
                    "root.ln.wf01.wt02.s2,null,root.ln.wf01.wt02,DOUBLE,GORILLA,SNAPPY,null,null,",
                    "root.ln1.wf01.wt01.status,null,root.ln1.wf01.wt01,BOOLEAN,PLAIN,SNAPPY,null,null,",
                    "root.ln1.wf01.wt01.temperature,null,root.ln1.wf01.wt01,FLOAT,RLE,SNAPPY,null,null,",
                    "root.ln2.wf01.wt01.status,null,root.ln2.wf01.wt01,BOOLEAN,PLAIN,SNAPPY,null,null,",
                    "root.ln2.wf01.wt01.temperature,null,root.ln2.wf01.wt01,FLOAT,RLE,SNAPPY,null,null,")),
            new HashSet<>()
          };
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        Set<String> standard = standards[n];
        try {
          boolean hasResultSet = statement.execute(sql);
          if (hasResultSet) {
            try (ResultSet resultSet = statement.getResultSet()) {
              ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
              while (resultSet.next()) {
                StringBuilder builder = new StringBuilder();
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                  builder.append(resultSet.getString(i)).append(",");
                }
                String string = builder.toString();
                Assert.assertTrue(standard.contains(string));
                standard.remove(string);
              }
              assertEquals(0, standard.size());
            }
          }
        } catch (SQLException e) {
          logger.error("showTimeseriesTest() failed", e);
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showStorageGroupTest() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_0_12);
        Statement statement = connection.createStatement()) {
      String[] sqls =
          new String[] {
            "show storage group",
            "show storage group root.ln.wf01",
            "show storage group root.ln.wf01.wt01.status"
          };
      Set<String>[] standards =
          new Set[] {
            new HashSet<>(
                Arrays.asList(
                    "root.ln.wf01.wt01,",
                    "root.ln.wf01.wt02,",
                    "root.ln1.wf01.wt01,",
                    "root.ln2.wf01.wt01,")),
            new HashSet<>(Arrays.asList("root.ln.wf01.wt01,", "root.ln.wf01.wt02,")),
            new HashSet<>()
          };

      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        Set<String> standard = standards[n];
        try {
          boolean hasResultSet = statement.execute(sql);
          if (hasResultSet) {
            try (ResultSet resultSet = statement.getResultSet()) {
              ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
              while (resultSet.next()) {
                StringBuilder builder = new StringBuilder();
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                  builder.append(resultSet.getString(i)).append(",");
                }
                Assert.assertTrue(standard.contains(builder.toString()));
                String string = builder.toString();
                Assert.assertTrue(standard.contains(string));
                standard.remove(string);
              }
              assertEquals(0, standard.size());
            }
          }
        } catch (SQLException e) {
          logger.error("showStorageGroupTest() failed", e);
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showDevicesWithSgTest() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_0_12);
        Statement statement = connection.createStatement()) {
      String[] sqls =
          new String[] {
            "show devices root.ln with storage group", "show devices root.ln.wf01.wt01.temperature"
          };
      Set<String>[] standards =
          new Set[] {
            new HashSet<>(
                Arrays.asList(
                    "root.ln.wf01.wt01,root.ln.wf01.wt01,false,",
                    "root.ln.wf01.wt02,root.ln.wf01.wt02,true,")),
            new HashSet<>()
          };

      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        Set<String> standard = standards[n];
        try {
          boolean hasResultSet = statement.execute(sql);
          if (hasResultSet) {
            try (ResultSet resultSet = statement.getResultSet()) {
              ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
              while (resultSet.next()) {
                StringBuilder builder = new StringBuilder();
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                  builder.append(resultSet.getString(i)).append(",");
                }
                String string = builder.toString();
                System.out.println(string);
                Assert.assertTrue(standard.contains(string));
                standard.remove(string);
              }
              assertEquals(0, standard.size());
            }
          }
        } catch (SQLException e) {
          logger.error("showDevicesTest() failed", e);
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showDevicesTest() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_0_12);
        Statement statement = connection.createStatement()) {
      String[] sqls =
          new String[] {"show devices root.ln.*", "show devices root.ln.wf01.wt01.temperature"};
      Set<String>[] standards =
          new Set[] {
            new HashSet<>(Arrays.asList("root.ln.wf01.wt01,false,", "root.ln.wf01.wt02,true,")),
            new HashSet<>()
          };

      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        Set<String> standard = standards[n];
        try {
          boolean hasResultSet = statement.execute(sql);
          if (hasResultSet) {
            try (ResultSet resultSet = statement.getResultSet()) {
              ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
              while (resultSet.next()) {
                StringBuilder builder = new StringBuilder();
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                  builder.append(resultSet.getString(i)).append(",");
                }
                String string = builder.toString();
                Assert.assertTrue(standard.contains(string));
                standard.remove(string);
              }
              assertEquals(0, standard.size());
            }
          }
        } catch (SQLException e) {
          logger.error("showDevicesTest() failed", e);
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showChildPaths() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_0_12);
        Statement statement = connection.createStatement()) {
      String[] sqls = new String[] {"show child paths root.ln"};
      String[] standards = new String[] {"root.ln.wf01,\n"};
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        String standard = standards[n];
        StringBuilder builder = new StringBuilder();
        try {
          boolean hasResultSet = statement.execute(sql);
          if (hasResultSet) {
            try (ResultSet resultSet = statement.getResultSet()) {
              ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
              while (resultSet.next()) {
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                  builder.append(resultSet.getString(i)).append(",");
                }
                builder.append("\n");
              }
            }
          }
          Assert.assertEquals(standard, builder.toString());
        } catch (SQLException e) {
          logger.error("showChildPaths() failed", e);
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showChildNodes() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_0_12);
        Statement statement = connection.createStatement()) {
      String[] sqls = new String[] {"show child nodes root.ln"};
      String[] standards = new String[] {"wf01,\n"};
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        String standard = standards[n];
        StringBuilder builder = new StringBuilder();
        try {
          boolean hasResultSet = statement.execute(sql);
          if (hasResultSet) {
            try (ResultSet resultSet = statement.getResultSet()) {
              ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
              while (resultSet.next()) {
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                  builder.append(resultSet.getString(i)).append(",");
                }
                builder.append("\n");
              }
            }
          }
          Assert.assertEquals(standard, builder.toString());
        } catch (SQLException e) {
          logger.error("showChildNodes() failed", e);
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showCountTimeSeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_0_12);
        Statement statement = connection.createStatement()) {
      String[] sqls = new String[] {"COUNT TIMESERIES root.ln.*", "COUNT TIMESERIES"};
      String[] standards = new String[] {"4,\n", "8,\n"};
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        String standard = standards[n];
        StringBuilder builder = new StringBuilder();
        try {
          boolean hasResultSet = statement.execute(sql);
          if (hasResultSet) {
            try (ResultSet resultSet = statement.getResultSet()) {
              ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
              while (resultSet.next()) {
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                  builder.append(resultSet.getString(i)).append(",");
                }
                builder.append("\n");
              }
            }
          }
          Assert.assertEquals(standard, builder.toString());
        } catch (SQLException e) {
          logger.error("showCountTimeSeries() failed", e);
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showCountDevices() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_0_12);
        Statement statement = connection.createStatement()) {
      String[] sqls =
          new String[] {
            "COUNT DEVICES root.ln", "COUNT DEVICES", "COUNT DEVICES root.ln.wf01.wt01.temperature"
          };
      String[] standards = new String[] {"2,\n", "4,\n", "0,\n"};
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        String standard = standards[n];
        StringBuilder builder = new StringBuilder();
        try {
          boolean hasResultSet = statement.execute(sql);
          if (hasResultSet) {
            try (ResultSet resultSet = statement.getResultSet()) {
              ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
              while (resultSet.next()) {
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                  builder.append(resultSet.getString(i)).append(",");
                }
                builder.append("\n");
              }
            }
          }
          Assert.assertEquals(standard, builder.toString());
        } catch (SQLException e) {
          logger.error("showCountDevices() failed", e);
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showCountStorageGroup() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_0_12);
        Statement statement = connection.createStatement()) {
      String[] sqls =
          new String[] {
            "count storage group root.ln",
            "count storage group",
            "count storage group root.ln.wf01.wt01.status"
          };
      String[] standards = new String[] {"2,\n", "4,\n", "0,\n"};
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        String standard = standards[n];
        StringBuilder builder = new StringBuilder();
        try {
          boolean hasResultSet = statement.execute(sql);
          if (hasResultSet) {
            try (ResultSet resultSet = statement.getResultSet()) {
              ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
              while (resultSet.next()) {
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                  builder.append(resultSet.getString(i)).append(",");
                }
                builder.append("\n");
              }
            }
          }
          Assert.assertEquals(standard, builder.toString());
        } catch (SQLException e) {
          logger.error("showCountStorageGroup() failed", e);
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showCountTimeSeriesGroupBy() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_0_12);
        Statement statement = connection.createStatement()) {
      String[] sqls =
          new String[] {
            "COUNT TIMESERIES root group by level=1",
            "COUNT TIMESERIES root group by level=3",
            "COUNT TIMESERIES root.*.wf01 group by level=2"
          };
      Set<String>[] standards =
          new Set[] {
            new HashSet<>(Arrays.asList("root.ln,4,", "root.ln1,2,", "root.ln2,2,")),
            new HashSet<>(
                Arrays.asList(
                    "root.ln.wf01.wt01,2,",
                    "root.ln.wf01.wt02,2,",
                    "root.ln1.wf01.wt01,2,",
                    "root.ln2.wf01.wt01,2,")),
            new HashSet<>(Arrays.asList("root.ln.wf01,4,", "root.ln1.wf01,2,", "root.ln2.wf01,2,")),
          };
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        Set<String> standard = standards[n];
        try {
          boolean hasResultSet = statement.execute(sql);
          if (hasResultSet) {
            try (ResultSet resultSet = statement.getResultSet()) {
              while (resultSet.next()) {
                String string = resultSet.getString(1) + "," + resultSet.getInt(2) + ",";
                Assert.assertTrue(standard.contains(string));
                standard.remove(string);
              }
              assertEquals(0, standard.size());
            }
          }
        } catch (SQLException e) {
          logger.error("showCountTimeSeriesGroupBy() failed", e);
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showCountNodes() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_0_12);
        Statement statement = connection.createStatement()) {
      String[] sqls =
          new String[] {
            "COUNT NODES root level=1",
            "COUNT NODES root.ln level=1",
            "COUNT NODES root.ln level=4",
            "COUNT NODES root.ln.wf01 level=1",
            "COUNT NODES root.ln.wf01 level=2",
            "COUNT NODES root.ln.wf01 level=3",
            "COUNT NODES root.ln.wf01 level=4"
          };
      String[] standards = new String[] {"3,\n", "1,\n", "4,\n", "0,\n", "1,\n", "2,\n", "4,\n"};
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        String standard = standards[n];
        StringBuilder builder = new StringBuilder();
        try {
          boolean hasResultSet = statement.execute(sql);
          if (hasResultSet) {
            try (ResultSet resultSet = statement.getResultSet()) {
              ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
              while (resultSet.next()) {
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                  builder.append(resultSet.getString(i)).append(",");
                }
                builder.append("\n");
              }
            }
          }
          Assert.assertEquals(standard, builder.toString());
        } catch (SQLException e) {
          logger.error("showCountNodes() failed", e);
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void testDeleteStorageGroup() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_0_12);
        Statement statement = connection.createStatement()) {
      statement.execute("DELETE STORAGE GROUP root.ln.*");
      boolean hasResult = statement.execute("SHOW STORAGE GROUP");
      assertTrue(hasResult);
      String[] expected = new String[] {"root.ln1.wf01.wt01", "root.ln2.wf01.wt01"};
      List<String> expectedList = new ArrayList<>();
      Collections.addAll(expectedList, expected);
      List<String> result = new ArrayList<>();
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          result.add(resultSet.getString(1));
        }
      }
      assertEquals(expected.length, result.size());
      assertTrue(expectedList.containsAll(result));

      statement.execute("DELETE STORAGE GROUP root.ln1");
      hasResult = statement.execute("SHOW STORAGE GROUP");
      assertTrue(hasResult);
      expected = new String[] {"root.ln2.wf01.wt01"};
      expectedList = new ArrayList<>();
      Collections.addAll(expectedList, expected);
      result = new ArrayList<>();
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          result.add(resultSet.getString(1));
        }
      }
      assertEquals(expected.length, result.size());
      assertTrue(expectedList.containsAll(result));
    }
  }

  @Test
  public void testDeleteTimeseries() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_0_12);
        Statement statement = connection.createStatement()) {
      statement.execute("DELETE TIMESERIES root.ln.*");
      boolean hasResult = statement.execute("SHOW TIMESERIES");
      assertTrue(hasResult);
      String[] expected =
          new String[] {
            "root.ln1.wf01.wt01.status",
            "root.ln1.wf01.wt01.temperature",
            "root.ln2.wf01.wt01.status",
            "root.ln2.wf01.wt01.temperature"
          };
      List<String> expectedList = new ArrayList<>();
      Collections.addAll(expectedList, expected);
      List<String> result = new ArrayList<>();
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          result.add(resultSet.getString(1));
        }
      }
      assertEquals(expected.length, result.size());
      assertTrue(expectedList.containsAll(result));

      statement.execute("DELETE TIMESERIES root.ln1");
      hasResult = statement.execute("SHOW TIMESERIES");
      assertTrue(hasResult);
      expected = new String[] {"root.ln2.wf01.wt01.status", "root.ln2.wf01.wt01.temperature"};
      expectedList = new ArrayList<>();
      Collections.addAll(expectedList, expected);
      result = new ArrayList<>();
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          result.add(resultSet.getString(1));
        }
      }
      assertEquals(expected.length, result.size());
      assertTrue(expectedList.containsAll(result));
    }
  }
}
