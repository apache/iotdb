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
package org.apache.iotdb.db.it.schema;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBMetadataFetchIT extends AbstractSchemaIT {

  public IoTDBMetadataFetchIT(SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
  }

  private static void insertSQL() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      String[] insertSqls =
          new String[] {
            "CREATE DATABASE root.ln.wf01.wt01",
            "CREATE DATABASE root.ln.wf01.wt02",
            "CREATE DATABASE root.ln1.wf01.wt01",
            "CREATE DATABASE root.ln2.wf01.wt01",
            "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE = BOOLEAN, ENCODING = PLAIN",
            "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE = FLOAT, ENCODING = RLE, "
                + "compressor = SNAPPY, 'MAX_POINT_NUMBER' = '3' ",
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
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    EnvFactory.getEnv().initClusterEnvironment();

    insertSQL();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    super.tearDown();
  }

  @Test
  public void showTimeseriesTest() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] sqls =
          new String[] {
            "show timeseries root.ln.wf01.wt01.status", // full seriesPath
            "show timeseries root.ln.**", // prefix seriesPath
            "show timeseries root.ln.*.wt01.*", // seriesPath with stars
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
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        Set<String> standard = standards[n];
        try (ResultSet resultSet = statement.executeQuery(sql)) {
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
        } catch (SQLException e) {
          e.printStackTrace();
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showStorageGroupTest() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] sqls =
          new String[] {
            "show databases",
            "show databases root.ln.wf01.**",
            "show databases root.ln.wf01.wt01.status"
          };
      Set<String>[] standards =
          new Set[] {
            new HashSet<>(
                Arrays.asList(
                    "root.ln.wf01.wt01",
                    "root.ln.wf01.wt02",
                    "root.ln1.wf01.wt01",
                    "root.ln2.wf01.wt01")),
            new HashSet<>(Arrays.asList("root.ln.wf01.wt01", "root.ln.wf01.wt02")),
            new HashSet<>()
          };

      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        Set<String> standard = standards[n];
        try (ResultSet resultSet = statement.executeQuery(sql)) {
          while (resultSet.next()) {
            Assert.assertTrue(standard.contains(resultSet.getString(1)));
            standard.remove(resultSet.getString(1));
          }
          assertEquals(0, standard.size());
        } catch (SQLException e) {
          e.printStackTrace();
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showDevicesWithSgTest() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] sqls =
          new String[] {
            "show devices root.ln.** with database", "show devices root.ln.wf01.wt01.temperature"
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
        try (ResultSet resultSet = statement.executeQuery(sql)) {
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
        } catch (SQLException e) {
          e.printStackTrace();
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showDevicesTest() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] sqls =
          new String[] {"show devices root.ln.**", "show devices root.ln.wf01.wt01.temperature"};
      Set<String>[] standards =
          new Set[] {
            new HashSet<>(Arrays.asList("root.ln.wf01.wt01,false,", "root.ln.wf01.wt02,true,")),
            new HashSet<>()
          };

      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        Set<String> standard = standards[n];
        try (ResultSet resultSet = statement.executeQuery(sql)) {
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
        } catch (SQLException e) {
          e.printStackTrace();
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showChildPaths() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] sqls = new String[] {"show child paths root.ln"};
      String[] standards = new String[] {"root.ln.wf01,SG INTERNAL,\n"};
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        String standard = standards[n];
        StringBuilder builder = new StringBuilder();
        try (ResultSet resultSet = statement.executeQuery(sql)) {
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          while (resultSet.next()) {
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
              builder.append(resultSet.getString(i)).append(",");
            }
            builder.append("\n");
          }
          Assert.assertEquals(standard, builder.toString());
        } catch (SQLException e) {
          e.printStackTrace();
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showChildNodes() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] sqls = new String[] {"show child nodes root.ln"};
      String[] standards = new String[] {"wf01,\n"};
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        String standard = standards[n];
        StringBuilder builder = new StringBuilder();
        try (ResultSet resultSet = statement.executeQuery(sql)) {
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          while (resultSet.next()) {
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
              builder.append(resultSet.getString(i)).append(",");
            }
            builder.append("\n");
          }
          Assert.assertEquals(standard, builder.toString());
        } catch (SQLException e) {
          e.printStackTrace();
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showCountTimeSeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] sqls = new String[] {"COUNT TIMESERIES root.ln.**", "COUNT TIMESERIES"};
      String[] standards = new String[] {"4,\n", "8,\n"};
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        String standard = standards[n];
        StringBuilder builder = new StringBuilder();
        try (ResultSet resultSet = statement.executeQuery(sql)) {
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          while (resultSet.next()) {
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
              builder.append(resultSet.getString(i)).append(",");
            }
            builder.append("\n");
          }
          Assert.assertEquals(standard, builder.toString());
        } catch (SQLException e) {
          e.printStackTrace();
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showCountTimeSeriesWithTag() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("ALTER timeseries root.ln1.wf01.wt01.status ADD TAGS tag1=v1, tag2=v2");
      statement.execute("ALTER timeseries root.ln2.wf01.wt01.status ADD TAGS tag1=v1");
      String[] sqls =
          new String[] {
            "COUNT TIMESERIES root.ln1.** where tag1 = v1",
            "COUNT TIMESERIES where tag1 = v1",
            "COUNT TIMESERIES where tag3 = v3"
          };
      String[] standards = new String[] {"1,\n", "2,\n", "0,\n"};
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        String standard = standards[n];
        StringBuilder builder = new StringBuilder();
        try (ResultSet resultSet = statement.executeQuery(sql)) {
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          while (resultSet.next()) {
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
              builder.append(resultSet.getString(i)).append(",");
            }
            builder.append("\n");
          }
          Assert.assertEquals(standard, builder.toString());
        } catch (SQLException e) {
          e.printStackTrace();
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showCountDevices() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] sqls =
          new String[] {
            "COUNT DEVICES root.ln.**",
            "COUNT DEVICES",
            "COUNT DEVICES root.ln.wf01.wt01.temperature"
          };
      String[] standards = new String[] {"2,\n", "4,\n", "0,\n"};
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        String standard = standards[n];
        StringBuilder builder = new StringBuilder();
        try (ResultSet resultSet = statement.executeQuery(sql)) {
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          while (resultSet.next()) {
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
              builder.append(resultSet.getString(i)).append(",");
            }
            builder.append("\n");
          }
          Assert.assertEquals(standard, builder.toString());
        } catch (SQLException e) {
          e.printStackTrace();
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showCountStorageGroup() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] sqls =
          new String[] {
            "count databases root.ln.**",
            "count databases",
            "count databases root.ln.wf01.wt01.status"
          };
      String[] standards = new String[] {"2,\n", "4,\n", "0,\n"};
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        String standard = standards[n];
        StringBuilder builder = new StringBuilder();
        try (ResultSet resultSet = statement.executeQuery(sql)) {
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          while (resultSet.next()) {
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
              builder.append(resultSet.getString(i)).append(",");
            }
            builder.append("\n");
          }
          Assert.assertEquals(standard, builder.toString());
        } catch (SQLException e) {
          e.printStackTrace();
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showCountTimeSeriesGroupBy() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] sqls =
          new String[] {
            "COUNT TIMESERIES root.** group by level=1",
            "COUNT TIMESERIES root.** group by level=3",
            "COUNT TIMESERIES root.**.status group by level=2"
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
            new HashSet<>(Arrays.asList("root.ln.wf01,1,", "root.ln1.wf01,1,", "root.ln2.wf01,1,")),
          };
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        Set<String> standard = standards[n];
        try (ResultSet resultSet = statement.executeQuery(sql)) {
          while (resultSet.next()) {
            String string = resultSet.getString(1) + "," + resultSet.getLong(2) + ",";
            Assert.assertTrue(standard.contains(string));
            standard.remove(string);
          }
          assertEquals(0, standard.size());
        } catch (SQLException e) {
          e.printStackTrace();
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showCountTimeSeriesGroupByWithTag() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("ALTER timeseries root.ln1.wf01.wt01.status ADD TAGS tag1=v1, tag2=v2");
      statement.execute("ALTER timeseries root.ln2.wf01.wt01.status ADD TAGS tag1=v1");
      String[] sqls =
          new String[] {
            "COUNT TIMESERIES root.** where tag1 = v1 group by level=1",
            "COUNT TIMESERIES root.** where tag2 = v2 group by level=3",
            "COUNT TIMESERIES root.**.status where tag1 = v1 group by level=2",
            "COUNT TIMESERIES root.** where tag3 = v3 group by level=2"
          };
      Set<String>[] standards =
          new Set[] {
            new HashSet<>(Arrays.asList("root.ln1,1,", "root.ln2,1,")),
            new HashSet<>(Collections.singletonList("root.ln1.wf01.wt01,1,")),
            new HashSet<>(Arrays.asList("root.ln1.wf01,1,", "root.ln2.wf01,1,")),
            Collections.emptySet(),
          };
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        Set<String> standard = standards[n];
        try (ResultSet resultSet = statement.executeQuery(sql)) {
          while (resultSet.next()) {
            String string = resultSet.getString(1) + "," + resultSet.getInt(2) + ",";
            Assert.assertTrue(standard.contains(string));
            standard.remove(string);
          }
          assertEquals(0, standard.size());
        } catch (SQLException e) {
          e.printStackTrace();
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showCountNodes() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] sqls =
          new String[] {
            "COUNT NODES root.** level=1",
            "COUNT NODES root.ln level=1",
            "COUNT NODES root.ln.wf01.** level=1",
            "COUNT NODES root.ln.wf01.* level=2",
            "COUNT NODES root.ln.wf01.* level=3",
            "COUNT NODES root.ln.wf01.* level=4"
          };
      String[] standards = new String[] {"3,\n", "1,\n", "1,\n", "1,\n", "2,\n", "0,\n"};
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        String standard = standards[n];
        StringBuilder builder = new StringBuilder();
        try (ResultSet resultSet = statement.executeQuery(sql)) {
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          while (resultSet.next()) {
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
              builder.append(resultSet.getString(i)).append(",");
            }
            builder.append("\n");
          }
          Assert.assertEquals(standard, builder.toString());
        } catch (SQLException e) {
          e.printStackTrace();
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showAlignedTimeseriesWithAliasAndTags() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create aligned timeseries root.sg.d(s1(alias1) int32 tags('tag1'='v1', 'tag2'='v2'), s2 double attributes('attr3'='v3'))");
      String[] expected =
          new String[] {
            "root.sg.d.s1,alias1,root.sg,INT32,RLE,SNAPPY,{\"tag1\":\"v1\",\"tag2\":\"v2\"},null,null,null,",
            "root.sg.d.s2,null,root.sg,DOUBLE,GORILLA,SNAPPY,null,{\"attr3\":\"v3\"},null,null,"
          };

      int num = 0;
      try (ResultSet resultSet = statement.executeQuery("show timeseries root.sg.d.*")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          Assert.assertEquals(expected[num++], builder.toString());
        }
      }
      Assert.assertEquals(2, num);
    }
  }

  @Test
  public void showLatestTimeseriesTest() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("insert into root.ln.wf01.wt01(time, status) values(1, 1)");
      statement.execute("insert into root.ln.wf01.wt01(time, temperature) values(2, 1)");
      String sql = "show latest timeseries root.ln.wf01.wt01.*";
      Set<String> standard =
          new HashSet<>(
              Arrays.asList(
                  "root.ln.wf01.wt01.temperature,null,root.ln.wf01.wt01,FLOAT,RLE,SNAPPY,null,null,null,null,",
                  "root.ln.wf01.wt01.status,null,root.ln.wf01.wt01,BOOLEAN,PLAIN,SNAPPY,null,null,null,null,"));
      try (ResultSet resultSet = statement.executeQuery(sql)) {
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
      } catch (SQLException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
    }
  }

  @Test
  public void showDeadbandInfo() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      String[] sqls =
          new String[] {
            "CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=INT32",
            "CREATE TIMESERIES root.sg1.d0.s1 WITH DATATYPE=INT32,ENCODING=PLAIN,LOSS=SDT,COMPDEV=2",
            "CREATE TIMESERIES root.sg1.d0.s2 WITH DATATYPE=INT32,ENCODING=PLAIN,LOSS=SDT, COMPDEV=0.01, COMPMINTIME=2, COMPMAXTIME=15"
          };
      for (String sql : sqls) {
        statement.execute(sql);
      }
      Set<String> standard =
          new HashSet<>(
              Arrays.asList(
                  "root.sg1.d0.s0,null,root.sg1,INT32,RLE,SNAPPY,null,null,null,null,\n",
                  "root.sg1.d0.s1,null,root.sg1,INT32,PLAIN,SNAPPY,null,null,SDT,{compdev=2},\n",
                  "root.sg1.d0.s2,null,root.sg1,INT32,PLAIN,SNAPPY,null,null,SDT,{compdev=0.01, compmintime=2, compmaxtime=15},\n"));
      try (ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES root.sg1.d0.*")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          builder.append("\n");
          Assert.assertTrue(standard.contains(builder.toString()));
        }
      } catch (SQLException e) {
        fail(e.getMessage());
      } finally {
        statement.execute("delete timeseries root.sg1.d0.*");
      }
    }
  }
}
