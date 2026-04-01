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
import org.apache.iotdb.util.AbstractSchemaIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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

  @Parameterized.BeforeParam
  public static void before() throws Exception {
    setUpEnvironment();
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @Parameterized.AfterParam
  public static void after() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    tearDownEnvironment();
  }

  @Before
  public void setUp() throws Exception {
    insertSQL();
  }

  @After
  public void tearDown() throws Exception {
    clearSchema();
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
            "show timeseries root.ln*.**", // the same as root
            "show timeseries root.a.b", // nonexistent timeseries, thus returning ""
            "show timeseries root.ln*.** where timeseries contains 'tat'",
            "show timeseries root.ln.** where timeseries contains 'wf01.wt01'",
            "show timeseries root.ln.** where dataType=BOOLEAN"
          };
      Set<String>[] standards =
          new Set[] {
            new HashSet<>(
                Collections.singletonList(
                    "root.ln.wf01.wt01.status,null,root.ln.wf01.wt01,BOOLEAN,PLAIN,LZ4,null,null,null,null,BASE,")),
            new HashSet<>(
                Arrays.asList(
                    "root.ln.wf01.wt01.status,null,root.ln.wf01.wt01,BOOLEAN,PLAIN,LZ4,null,null,null,null,BASE,",
                    "root.ln.wf01.wt01.temperature,null,root.ln.wf01.wt01,FLOAT,RLE,SNAPPY,null,null,null,null,BASE,",
                    "root.ln.wf01.wt02.s1,null,root.ln.wf01.wt02,INT32,TS_2DIFF,LZ4,null,null,null,null,BASE,",
                    "root.ln.wf01.wt02.s2,null,root.ln.wf01.wt02,DOUBLE,GORILLA,LZ4,null,null,null,null,BASE,")),
            new HashSet<>(
                Arrays.asList(
                    "root.ln.wf01.wt01.status,null,root.ln.wf01.wt01,BOOLEAN,PLAIN,LZ4,null,null,null,null,BASE,",
                    "root.ln.wf01.wt01.temperature,null,root.ln.wf01.wt01,FLOAT,RLE,SNAPPY,null,null,null,null,BASE,")),
            new HashSet<>(
                Arrays.asList(
                    "root.ln.wf01.wt01.status,null,root.ln.wf01.wt01,BOOLEAN,PLAIN,LZ4,null,null,null,null,BASE,",
                    "root.ln.wf01.wt01.temperature,null,root.ln.wf01.wt01,FLOAT,RLE,SNAPPY,null,null,null,null,BASE,",
                    "root.ln.wf01.wt02.s1,null,root.ln.wf01.wt02,INT32,TS_2DIFF,LZ4,null,null,null,null,BASE,",
                    "root.ln.wf01.wt02.s2,null,root.ln.wf01.wt02,DOUBLE,GORILLA,LZ4,null,null,null,null,BASE,",
                    "root.ln1.wf01.wt01.status,null,root.ln1.wf01.wt01,BOOLEAN,PLAIN,LZ4,null,null,null,null,BASE,",
                    "root.ln1.wf01.wt01.temperature,null,root.ln1.wf01.wt01,FLOAT,RLE,SNAPPY,null,null,null,null,BASE,",
                    "root.ln2.wf01.wt01.status,null,root.ln2.wf01.wt01,BOOLEAN,PLAIN,LZ4,null,null,null,null,BASE,",
                    "root.ln2.wf01.wt01.temperature,null,root.ln2.wf01.wt01,FLOAT,RLE,SNAPPY,null,null,null,null,BASE,")),
            new HashSet<>(),
            new HashSet<>(
                Arrays.asList(
                    "root.ln.wf01.wt01.status,null,root.ln.wf01.wt01,BOOLEAN,PLAIN,LZ4,null,null,null,null,BASE,",
                    "root.ln1.wf01.wt01.status,null,root.ln1.wf01.wt01,BOOLEAN,PLAIN,LZ4,null,null,null,null,BASE,",
                    "root.ln2.wf01.wt01.status,null,root.ln2.wf01.wt01,BOOLEAN,PLAIN,LZ4,null,null,null,null,BASE,")),
            new HashSet<>(
                Arrays.asList(
                    "root.ln.wf01.wt01.status,null,root.ln.wf01.wt01,BOOLEAN,PLAIN,LZ4,null,null,null,null,BASE,",
                    "root.ln.wf01.wt01.temperature,null,root.ln.wf01.wt01,FLOAT,RLE,SNAPPY,null,null,null,null,BASE,")),
            new HashSet<>(
                Collections.singletonList(
                    "root.ln.wf01.wt01.status,null,root.ln.wf01.wt01,BOOLEAN,PLAIN,LZ4,null,null,null,null,BASE,"))
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
  public void showDatabasesTest() throws SQLException {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      final String[] sqls =
          new String[] {
            "show databases root.ln*.**",
            "show databases root.ln.wf01.**",
            "show databases root.ln.wf01.wt01.status"
          };
      final List<String>[] standards =
          new List[] {
            Arrays.asList(
                "root.ln.wf01.wt01",
                "root.ln.wf01.wt02",
                "root.ln1.wf01.wt01",
                "root.ln2.wf01.wt01"),
            Arrays.asList("root.ln.wf01.wt01", "root.ln.wf01.wt02"),
            Collections.emptyList()
          };

      for (int n = 0; n < sqls.length; n++) {
        final String sql = sqls[n];
        final List<String> standard = standards[n];
        int i = 0;
        try (final ResultSet resultSet = statement.executeQuery(sql)) {
          while (resultSet.next()) {
            assertEquals(standard.get(i++), resultSet.getString(1));
          }
          assertEquals(i, standard.size());
        } catch (final SQLException e) {
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
      statement.execute("set ttl to root.ln.wf01.wt02 8888");
      String[] sqls =
          new String[] {
            "show devices root.ln.** with database", "show devices root.ln.wf01.wt01.temperature",
          };
      Set<String>[] standards =
          new Set[] {
            new HashSet<>(
                Arrays.asList(
                    "root.ln.wf01.wt01,root.ln.wf01.wt01,false,null,INF,",
                    "root.ln.wf01.wt02,root.ln.wf01.wt02,true,null,8888,")),
            new HashSet<>(),
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
  public void showDevicesWithTemplate() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.sg1");
      statement.execute("CREATE DATABASE root.sg2");
      statement.execute("CREATE DEVICE TEMPLATE t1 (s1 INT64, s2 DOUBLE)");
      statement.execute("CREATE DEVICE TEMPLATE t2 aligned (s1 INT64, s2 DOUBLE)");
      statement.execute("SET DEVICE TEMPLATE t1 TO root.sg1.d1");
      statement.execute("SET DEVICE TEMPLATE t2 TO root.sg1.d2");
      statement.execute("SET DEVICE TEMPLATE t1 TO root.sg2.d1");
      statement.execute("SET DEVICE TEMPLATE t2 TO root.sg2.d2");
      statement.execute("CREATE TIMESERIES OF DEVICE TEMPLATE ON root.sg1.d2");
      statement.execute("CREATE TIMESERIES OF DEVICE TEMPLATE ON root.sg2.d1");
      statement.execute("CREATE TIMESERIES OF DEVICE TEMPLATE ON root.sg2.d2");

      String[] sqls =
          new String[] {
            "show devices root.** where template is not null",
            "show devices root.sg2.** with database where template = 't2'",
          };
      Set<String>[] standards =
          new Set[] {
            new HashSet<>(
                Arrays.asList(
                    "root.sg1.d2,true,t2,INF,",
                    "root.sg2.d1,false,t1,INF,",
                    "root.sg2.d2,true,t2,INF,")),
            new HashSet<>(Arrays.asList("root.sg2.d2,root.sg2,true,t2,INF,")),
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
  public void showDevicesTest() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("set ttl to root.ln.wf01.wt02 8888");
      String[] sqls =
          new String[] {
            "show devices root.ln.**",
            "show devices root.ln.wf01.wt01.temperature",
            "show devices root.** where device contains 'wt02'",
          };
      Set<String>[] standards =
          new Set[] {
            new HashSet<>(
                Arrays.asList(
                    "root.ln.wf01.wt01,false,null,INF,", "root.ln.wf01.wt02,true,null,8888,")),
            new HashSet<>(),
            new HashSet<>(Arrays.asList("root.ln.wf01.wt02,true,null,8888,")),
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
  public void showDevicesWithWildcardTest() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("set ttl to root.ln.wf01.wt02 8888");
      String[] sqls =
          new String[] {
            "show devices root.l*.wf01.w*",
            "show devices root.ln.*f01.*",
            "show devices root.l*.*f*.*1",
          };
      Set<String>[] standards =
          new Set[] {
            new HashSet<>(
                Arrays.asList(
                    "root.ln.wf01.wt01,false,null,INF,",
                    "root.ln.wf01.wt02,true,null,8888,",
                    "root.ln1.wf01.wt01,false,null,INF,",
                    "root.ln2.wf01.wt01,false,null,INF,")),
            new HashSet<>(
                Arrays.asList(
                    "root.ln.wf01.wt01,false,null,INF,", "root.ln.wf01.wt02,true,null,8888,")),
            new HashSet<>(
                Arrays.asList(
                    "root.ln.wf01.wt01,false,null,INF,",
                    "root.ln1.wf01.wt01,false,null,INF,",
                    "root.ln2.wf01.wt01,false,null,INF,"))
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
      String[] sqls = new String[] {"COUNT TIMESERIES root.ln.**", "COUNT TIMESERIES root.ln*.**"};
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
            "COUNT TIMESERIES root.ln1.** where TAGS(tag1) = v1",
            "COUNT TIMESERIES where TAGS(tag1) = v1",
            "COUNT TIMESERIES where TAGS(tag3) = v3"
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
  public void showCountTimeSeriesWithPathContains() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] sqls =
          new String[] {
            "COUNT TIMESERIES root.** where TIMESERIES contains 'wf01.wt01'",
            "COUNT TIMESERIES root.ln.** where TIMESERIES contains 's'",
          };
      String[] standards = new String[] {"6,\n", "3,\n"};
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
            "COUNT DEVICES root.ln*.**",
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
  public void showCountDatabase() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] sqls =
          new String[] {
            "count databases root.ln.**",
            "count databases root.ln*.**",
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
            "COUNT TIMESERIES root.ln*.** group by level=1",
            "COUNT TIMESERIES root.ln*.** group by level=3",
            "COUNT TIMESERIES root.ln*.**.status group by level=2",
            "COUNT TIMESERIES root.ln*.** group by level=5"
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
            Collections.emptySet()
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
            "COUNT TIMESERIES root.** where TAGS(tag1) = v1 group by level=1",
            "COUNT TIMESERIES root.** where TAGS(tag2) = v2 group by level=3",
            "COUNT TIMESERIES root.**.status where TAGS(tag1) = v1 group by level=2",
            "COUNT TIMESERIES root.** where TAGS(tag3) = v3 group by level=2"
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
  public void showCountTimeSeriesGroupByWithPathContains() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] sqls =
          new String[] {
            "COUNT TIMESERIES root.** where TIMESERIES contains 'wf01.wt01' group by level=1",
            "COUNT TIMESERIES root.ln.** where TIMESERIES contains 's' group by level=3"
          };
      Set<String>[] standards =
          new Set[] {
            new HashSet<>(Arrays.asList("root.ln,2,", "root.ln1,2,", "root.ln2,2,")),
            new HashSet<>(Arrays.asList("root.ln.wf01.wt01,1,", "root.ln.wf01.wt02,2,"))
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
            "COUNT NODES root.ln*.** level=1",
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
            "root.sg.d.s1,alias1,root.sg,INT32,TS_2DIFF,LZ4,{\"tag1\":\"v1\",\"tag2\":\"v2\"},null,null,null,BASE,",
            "root.sg.d.s2,null,root.sg,DOUBLE,GORILLA,LZ4,null,{\"attr3\":\"v3\"},null,null,BASE,"
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
                  "root.ln.wf01.wt01.temperature,null,root.ln.wf01.wt01,FLOAT,RLE,SNAPPY,null,null,null,null,BASE,",
                  "root.ln.wf01.wt01.status,null,root.ln.wf01.wt01,BOOLEAN,PLAIN,LZ4,null,null,null,null,BASE,"));
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
                  "root.sg1.d0.s0,null,root.sg1,INT32,TS_2DIFF,LZ4,null,null,null,null,BASE,\n",
                  "root.sg1.d0.s1,null,root.sg1,INT32,PLAIN,LZ4,null,null,SDT,{compdev=2},BASE,\n",
                  "root.sg1.d0.s2,null,root.sg1,INT32,PLAIN,LZ4,null,null,SDT,{compdev=0.01, compmintime=2, compmaxtime=15},BASE,\n"));
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
