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
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.itbase.category.RemoteTest;
import org.apache.iotdb.jdbc.Constant;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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

@Category({LocalStandaloneTest.class, ClusterTest.class, RemoteTest.class})
public class IoTDBDDLVersionAdaptionIT {
  private static final Logger logger = LoggerFactory.getLogger(IoTDBDDLVersionAdaptionIT.class);

  private static void insertSQL() {
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_1_0);
        Statement statement = connection.createStatement()) {

      String[] insertSqls =
          new String[] {
            "CREATE DATABASE root.ln.wf01.wt01",
            "CREATE DATABASE root.ln.wf01.wt02",
            "CREATE DATABASE root.ln1.wf01.wt01",
            "CREATE DATABASE root.ln2.wf01.wt01",
            "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE = BOOLEAN, ENCODING = PLAIN",
            "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE = FLOAT, ENCODING = RLE, "
                + "compressor = SNAPPY, 'MAX_POINT_NUMBER' = '3'",
            "CREATE ALIGNED TIMESERIES root.ln.wf01.wt02(s1 INT32, s2 DOUBLE)",
            "CREATE TIMESERIES root.ln1.wf01.wt01.status WITH DATATYPE = BOOLEAN, ENCODING = PLAIN",
            "CREATE TIMESERIES root.ln1.wf01.wt01.temperature WITH DATATYPE = FLOAT, ENCODING = RLE, "
                + "compressor = SNAPPY, 'MAX_POINT_NUMBER' = '3'",
            "CREATE TIMESERIES root.ln2.wf01.wt01.status WITH DATATYPE = BOOLEAN, ENCODING = PLAIN",
            "CREATE TIMESERIES root.ln2.wf01.wt01.temperature WITH DATATYPE = FLOAT, ENCODING = RLE, "
                + "compressor = SNAPPY, 'MAX_POINT_NUMBER' = '3'"
          };

      for (String sql : insertSqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      logger.error("insertSQL() failed", e);
      fail(e.getMessage());
    }
  }

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeTest();

    insertSQL();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterTest();
  }

  @Test
  public void showTimeseriesTest() throws SQLException {
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
                  "root.ln.wf01.wt01.status,null,root.ln.wf01.wt01,BOOLEAN,PLAIN,SNAPPY,null,null,null,null,")),
          new HashSet<>(
              Arrays.asList(
                  "root.ln.wf01.wt01.status,null,root.ln.wf01.wt01,BOOLEAN,PLAIN,SNAPPY,null,null,null,null,",
                  "root.ln.wf01.wt01.temperature,null,root.ln.wf01.wt01,FLOAT,RLE,SNAPPY,null,null,null,null,",
                  "root.ln.wf01.wt02.s1,null,root.ln.wf01.wt02,INT32,RLE,SNAPPY,null,null,null,null,",
                  "root.ln.wf01.wt02.s2,null,root.ln.wf01.wt02,DOUBLE,GORILLA,SNAPPY,null,null,null,null,")),
          new HashSet<>(
              Arrays.asList(
                  "root.ln.wf01.wt01.status,null,root.ln.wf01.wt01,BOOLEAN,PLAIN,SNAPPY,null,null,null,null,",
                  "root.ln.wf01.wt01.temperature,null,root.ln.wf01.wt01,FLOAT,RLE,SNAPPY,null,null,null,null,")),
          new HashSet<>(
              Arrays.asList(
                  "root.ln.wf01.wt01.status,null,root.ln.wf01.wt01,BOOLEAN,PLAIN,SNAPPY,null,null,null,null,",
                  "root.ln.wf01.wt01.temperature,null,root.ln.wf01.wt01,FLOAT,RLE,SNAPPY,null,null,null,null,",
                  "root.ln.wf01.wt02.s1,null,root.ln.wf01.wt02,INT32,RLE,SNAPPY,null,null,null,null,",
                  "root.ln.wf01.wt02.s2,null,root.ln.wf01.wt02,DOUBLE,GORILLA,SNAPPY,null,null,null,null,",
                  "root.ln1.wf01.wt01.status,null,root.ln1.wf01.wt01,BOOLEAN,PLAIN,SNAPPY,null,null,null,null,",
                  "root.ln1.wf01.wt01.temperature,null,root.ln1.wf01.wt01,FLOAT,RLE,SNAPPY,null,null,null,null,",
                  "root.ln2.wf01.wt01.status,null,root.ln2.wf01.wt01,BOOLEAN,PLAIN,SNAPPY,null,null,null,null,",
                  "root.ln2.wf01.wt01.temperature,null,root.ln2.wf01.wt01,FLOAT,RLE,SNAPPY,null,null,null,null,")),
          new HashSet<>()
        };
    executeAndCheckResult(sqls, standards);
  }

  @Test
  public void showStorageGroupTest() throws SQLException {
    String[] sqls =
        new String[] {
          "show databases", "show databases root.ln.wf01", "show databases root.ln.wf01.wt01.status"
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
    executeAndCheckResult(sqls, standards);
  }

  @Test
  public void showDevicesWithSgTest() throws SQLException {
    String[] sqls =
        new String[] {
          "show devices root.ln with database", "show devices root.ln.wf01.wt01.temperature"
        };
    Set<String>[] standards =
        new Set[] {
          new HashSet<>(
              Arrays.asList(
                  "root.ln.wf01.wt01,root.ln.wf01.wt01,false,",
                  "root.ln.wf01.wt02,root.ln.wf01.wt02,true,")),
          new HashSet<>()
        };
    executeAndCheckResult(sqls, standards);
  }

  @Test
  public void showDevicesTest() throws SQLException {
    String[] sqls =
        new String[] {"show devices root.ln.*", "show devices root.ln.wf01.wt01.temperature"};
    Set<String>[] standards =
        new Set[] {
          new HashSet<>(Arrays.asList("root.ln.wf01.wt01,false,", "root.ln.wf01.wt02,true,")),
          new HashSet<>()
        };
    executeAndCheckResult(sqls, standards);
  }

  @Test
  public void showChildPaths() throws SQLException {
    String[] sqls = new String[] {"show child paths root.ln"};
    Set<String>[] standards =
        new Set[] {new HashSet<>(Collections.singletonList("root.ln.wf01,SG INTERNAL,"))};
    executeAndCheckResult(sqls, standards);
  }

  @Test
  public void showChildNodes() throws SQLException {
    String[] sqls = new String[] {"show child nodes root.ln"};
    Set<String>[] standards = new Set[] {new HashSet<>(Collections.singletonList("wf01,"))};
    executeAndCheckResult(sqls, standards);
  }

  @Test
  public void showCountTimeSeries() throws SQLException {
    String[] sqls = new String[] {"COUNT TIMESERIES root.ln.*", "COUNT TIMESERIES"};
    Set<String>[] standards =
        new Set[] {
          new HashSet<>(Collections.singletonList("4,")),
          new HashSet<>(Collections.singletonList("8,"))
        };
    executeAndCheckResult(sqls, standards);
  }

  @Test
  public void showCountDevices() throws SQLException {
    String[] sqls =
        new String[] {
          "COUNT DEVICES root.ln", "COUNT DEVICES", "COUNT DEVICES root.ln.wf01.wt01.temperature"
        };
    Set<String>[] standards =
        new Set[] {
          new HashSet<>(Collections.singletonList("2,")),
          new HashSet<>(Collections.singletonList("4,")),
          new HashSet<>(Collections.singletonList("0,"))
        };
    executeAndCheckResult(sqls, standards);
  }

  @Test
  public void showCountStorageGroup() throws SQLException {
    String[] sqls =
        new String[] {
          "count databases root.ln", "count databases", "count databases root.ln.wf01.wt01.status"
        };
    Set<String>[] standards =
        new Set[] {
          new HashSet<>(Collections.singletonList("2,")),
          new HashSet<>(Collections.singletonList("4,")),
          new HashSet<>(Collections.singletonList("0,"))
        };
    executeAndCheckResult(sqls, standards);
  }

  @Test
  public void showCountTimeSeriesGroupBy() throws SQLException {
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
    executeAndCheckResult(sqls, standards);
  }

  @Test
  public void showCountNodes() throws SQLException {
    String[] sqls =
        new String[] {
          "COUNT NODES root level=1",
          "COUNT NODES root.ln level=1",
          "COUNT NODES root.ln level=4",
          "COUNT NODES root.ln.wf01 level=1",
          "COUNT NODES root.ln.wf01 level=2",
          "COUNT NODES root.ln.wf01 level=3",
          "COUNT NODES root.ln.wf01.wt01 level=4"
        };
    Set<String>[] standards =
        new Set[] {
          new HashSet<>(Collections.singletonList("3,")),
          new HashSet<>(Collections.singletonList("1,")),
          new HashSet<>(Collections.singletonList("4,")),
          new HashSet<>(Collections.singletonList("1,")),
          new HashSet<>(Collections.singletonList("1,")),
          new HashSet<>(Collections.singletonList("2,")),
          new HashSet<>(Collections.singletonList("2,"))
        };
    executeAndCheckResult(sqls, standards);
  }

  private void executeAndCheckResult(String[] sqls, Set<String>[] standards) throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_1_0);
        Statement statement = connection.createStatement()) {
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        Set<String> standard = standards[n];
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
      }
    }
  }

  @Test
  public void testDeleteStorageGroup() throws Exception {
    executeDeleteAndCheckResult(
        "DELETE DATABASE root.ln.*",
        "SHOW DATABASES",
        new String[] {"root.ln1.wf01.wt01", "root.ln2.wf01.wt01"});
    executeDeleteAndCheckResult(
        "DELETE DATABASE root.ln1", "SHOW DATABASES", new String[] {"root.ln2.wf01.wt01"});
  }

  @Test
  public void testDeleteTimeseries() throws Exception {
    executeDeleteAndCheckResult(
        "DELETE TIMESERIES root.ln.*",
        "SHOW TIMESERIES",
        new String[] {
          "root.ln1.wf01.wt01.status",
          "root.ln1.wf01.wt01.temperature",
          "root.ln2.wf01.wt01.status",
          "root.ln2.wf01.wt01.temperature"
        });
    executeDeleteAndCheckResult(
        "DELETE TIMESERIES root.ln1",
        "SHOW TIMESERIES",
        new String[] {"root.ln2.wf01.wt01.status", "root.ln2.wf01.wt01.temperature"});
  }

  private void executeDeleteAndCheckResult(String deleteSql, String showSql, String[] expected)
      throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_1_0);
        Statement statement = connection.createStatement()) {
      statement.execute(deleteSql);
      boolean hasResult = statement.execute(showSql);
      assertTrue(hasResult);
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
    }
  }
}
