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
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBExtendTemplateIT extends AbstractSchemaIT {

  public IoTDBExtendTemplateIT(SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    super.tearDown();
  }

  @Test
  public void testManualExtendTemplate() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // create database
      statement.execute("CREATE DATABASE root.db");

      // create schema template
      statement.execute(
          "CREATE SCHEMA TEMPLATE t1 (s1 INT64 ENCODING=PLAIN, s2 DOUBLE ENCODING=RLE)");

      statement.execute("SET SCHEMA TEMPLATE t1 to root.db");

      statement.execute("CREATE TIMESERIES USING SCHEMA TEMPLATE on root.db.d1");

      statement.execute(
          "ALTER SCHEMA TEMPLATE t1 ADD(s3 INT64 ENCODING=RLE, s4 DOUBLE ENCODING=GORILLA)");

      String[] sqls =
          new String[] {
            "show timeseries",
          };
      Set<String>[] standards =
          new Set[] {
            new HashSet<>(
                Arrays.asList(
                    "root.db.d1.s1,null,root.db,INT64,PLAIN,SNAPPY,null,null,null,null,",
                    "root.db.d1.s2,null,root.db,DOUBLE,RLE,SNAPPY,null,null,null,null,",
                    "root.db.d1.s3,null,root.db,INT64,RLE,SNAPPY,null,null,null,null,",
                    "root.db.d1.s4,null,root.db,DOUBLE,GORILLA,SNAPPY,null,null,null,null,"))
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

      // create schema template
      statement.execute(
          "CREATE SCHEMA TEMPLATE t1 (s1 INT64 ENCODING=PLAIN, s2 DOUBLE ENCODING=RLE)");

      statement.execute("SET SCHEMA TEMPLATE t1 to root.db");

      statement.execute("INSERT INTO root.db.d1(time, s1, s3) values(1, 1, 1)");
      statement.execute("INSERT INTO root.db.d2(time, s4, s5) values(1, 1, 1)");
      statement.execute("INSERT INTO root.db1.d1(time, s2, s3) values(1, 1, 1)");

      String[] sqls =
          new String[] {
            "show timeseries",
          };
      Set<String>[] standards =
          new Set[] {
            new HashSet<>(
                Arrays.asList(
                    "root.db.d1.s1,null,root.db,INT64,PLAIN,SNAPPY,null,null,null,null,",
                    "root.db.d1.s2,null,root.db,DOUBLE,RLE,SNAPPY,null,null,null,null,",
                    "root.db.d1.s3,null,root.db,FLOAT,GORILLA,SNAPPY,null,null,null,null,",
                    "root.db.d1.s4,null,root.db,FLOAT,GORILLA,SNAPPY,null,null,null,null,",
                    "root.db.d1.s5,null,root.db,FLOAT,GORILLA,SNAPPY,null,null,null,null,",
                    "root.db.d2.s1,null,root.db,INT64,PLAIN,SNAPPY,null,null,null,null,",
                    "root.db.d2.s2,null,root.db,DOUBLE,RLE,SNAPPY,null,null,null,null,",
                    "root.db.d2.s3,null,root.db,FLOAT,GORILLA,SNAPPY,null,null,null,null,",
                    "root.db.d2.s4,null,root.db,FLOAT,GORILLA,SNAPPY,null,null,null,null,",
                    "root.db.d2.s5,null,root.db,FLOAT,GORILLA,SNAPPY,null,null,null,null,",
                    "root.db1.d1.s2,null,root.db1,FLOAT,GORILLA,SNAPPY,null,null,null,null,",
                    "root.db1.d1.s3,null,root.db1,FLOAT,GORILLA,SNAPPY,null,null,null,null,"))
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
}
