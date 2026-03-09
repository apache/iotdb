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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBExtendTemplateIT extends AbstractSchemaIT {

  public IoTDBExtendTemplateIT(SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
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

  @After
  public void tearDown() throws Exception {
    clearSchema();
  }

  @Test
  public void testManualExtendTemplate() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // create database
      statement.execute("CREATE DATABASE root.db");

      // create device template
      statement.execute(
          "CREATE DEVICE TEMPLATE t1 (s1 INT64 ENCODING=PLAIN, s2 DOUBLE ENCODING=RLE)");

      statement.execute("SET DEVICE TEMPLATE t1 to root.db");

      statement.execute("CREATE TIMESERIES USING DEVICE TEMPLATE on root.db.d1");

      statement.execute(
          "ALTER DEVICE TEMPLATE t1 ADD(s3 INT64 ENCODING=RLE, s4 DOUBLE ENCODING=GORILLA)");

      try {
        statement.execute(
            "ALTER DEVICE TEMPLATE t1 ADD(s5 INT64 ENCODING=RLE, s5 DOUBLE ENCODING=GORILLA)");
      } catch (SQLException e) {
        Assert.assertTrue(
            e.getMessage()
                .contains("Duplicated measurement [s5] in device template alter request"));
      }

      String[] sqls =
          new String[] {
            "show timeseries root.db.**",
          };
      Set<String>[] standards =
          new Set[] {
            new HashSet<>(
                Arrays.asList(
                    "root.db.d1.s1,null,root.db,INT64,PLAIN,LZ4,null,null,null,null,BASE,",
                    "root.db.d1.s2,null,root.db,DOUBLE,RLE,LZ4,null,null,null,null,BASE,",
                    "root.db.d1.s3,null,root.db,INT64,RLE,LZ4,null,null,null,null,BASE,",
                    "root.db.d1.s4,null,root.db,DOUBLE,GORILLA,LZ4,null,null,null,null,BASE,"))
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
  public void testAutoExtendTemplate() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // create database
      statement.execute("CREATE DATABASE root.db");

      // create device template
      statement.execute(
          "CREATE DEVICE TEMPLATE t1 (s1 INT64 ENCODING=PLAIN, s2 DOUBLE ENCODING=RLE)");

      statement.execute("SET DEVICE TEMPLATE t1 to root.db");

      // single-row insertion
      statement.execute("INSERT INTO root.db.d1(time, s1, s3) values(1, 1, 1)");
      statement.execute("INSERT INTO root.db.d2(time, s4, s5) values(1, 1, 1)");
      statement.execute("INSERT INTO root.db1.d1(time, s2, s3) values(1, 1, 1)");

      // multi-row insertion with null
      statement.execute(
          "INSERT INTO root.db.d1(time, s1, s6) values(1, 1, 1), (2, 2, null), (3, 3, 3)");

      String[] sqls =
          new String[] {
            "show timeseries root.db*.**",
          };
      Set<String>[] standards =
          new Set[] {
            new HashSet<>(
                Arrays.asList(
                    "root.db.d1.s1,null,root.db,INT64,PLAIN,LZ4,null,null,null,null,BASE,",
                    "root.db.d1.s2,null,root.db,DOUBLE,RLE,LZ4,null,null,null,null,BASE,",
                    "root.db.d1.s3,null,root.db,DOUBLE,GORILLA,LZ4,null,null,null,null,BASE,",
                    "root.db.d1.s4,null,root.db,DOUBLE,GORILLA,LZ4,null,null,null,null,BASE,",
                    "root.db.d1.s5,null,root.db,DOUBLE,GORILLA,LZ4,null,null,null,null,BASE,",
                    "root.db.d1.s6,null,root.db,DOUBLE,GORILLA,LZ4,null,null,null,null,BASE,",
                    "root.db.d2.s1,null,root.db,INT64,PLAIN,LZ4,null,null,null,null,BASE,",
                    "root.db.d2.s2,null,root.db,DOUBLE,RLE,LZ4,null,null,null,null,BASE,",
                    "root.db.d2.s3,null,root.db,DOUBLE,GORILLA,LZ4,null,null,null,null,BASE,",
                    "root.db.d2.s4,null,root.db,DOUBLE,GORILLA,LZ4,null,null,null,null,BASE,",
                    "root.db.d2.s5,null,root.db,DOUBLE,GORILLA,LZ4,null,null,null,null,BASE,",
                    "root.db.d2.s6,null,root.db,DOUBLE,GORILLA,LZ4,null,null,null,null,BASE,",
                    "root.db1.d1.s2,null,root.db1,DOUBLE,GORILLA,LZ4,null,null,null,null,BASE,",
                    "root.db1.d1.s3,null,root.db1,DOUBLE,GORILLA,LZ4,null,null,null,null,BASE,"))
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
  public void testSelectInto() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // create database
      statement.execute("CREATE DATABASE root.db");

      // create device template
      statement.execute(
          "CREATE DEVICE TEMPLATE t1 (s1 INT64 ENCODING=PLAIN, s2 DOUBLE ENCODING=RLE)");

      statement.execute("SET DEVICE TEMPLATE t1 to root.db");

      statement.execute("INSERT INTO root.db.d1(time, s1, s2) values(1, 1, 1)");

      statement.execute("SELECT s1, s2 into root.::(t1, t2) from root.db.d1");

      String[] sqls =
          new String[] {
            "show timeseries root.db.**",
          };
      Set<String>[] standards =
          new Set[] {
            new HashSet<>(
                Arrays.asList("root.db.d1.s1", "root.db.d1.s2", "root.db.d1.t1", "root.db.d1.t2"))
          };
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        Set<String> standard = standards[n];
        try (ResultSet resultSet = statement.executeQuery(sql)) {
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          while (resultSet.next()) {
            String string = resultSet.getString(1);
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
}
