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

package org.apache.iotdb.db.it.schema.view;

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
import java.sql.SQLException;
import java.sql.Statement;

@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAliasSeriesIT extends AbstractSchemaIT {

  public IoTDBAliasSeriesIT(SchemaTestMode schemaTestMode) {
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
  public void testSelectIntoAliasSeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("CREATE DATABASE root.db");
      statement.execute("CREATE DATABASE root.view");

      statement.execute("create timeseries root.db.device.s01 with datatype=INT32");
      statement.execute("create timeseries root.db.device.s02 with datatype=INT32");
      statement.execute("CREATE VIEW root.view.device.status AS SELECT s01 FROM root.db.device");
      statement.execute("insert into root.db.device(time,s02) values(1,1)");

      try (ResultSet resultSet =
          statement.executeQuery("select s02 into root.view.device(status) from root.db.device")) {
        StringBuilder stringBuilder = new StringBuilder();
        if (resultSet.next()) {
          for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
            stringBuilder.append(resultSet.getString(i)).append(",");
          }
          Assert.assertEquals(
              "root.db.device.s02,root.view.device.status,1,", stringBuilder.toString());
        }
        Assert.assertFalse(resultSet.next());
      }
      try (ResultSet resultSet = statement.executeQuery("select status from root.view.device")) {
        StringBuilder stringBuilder = new StringBuilder();
        if (resultSet.next()) {
          for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
            stringBuilder.append(resultSet.getString(i)).append(",");
          }
          Assert.assertEquals("1,1,", stringBuilder.toString());
        }
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testInsertIntoViewSourceNotExist() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.db");
      statement.execute("CREATE TIMESERIES root.db.device.s01 with datatype=BOOLEAN");
      statement.execute("CREATE VIEW root.db.device.v_s01 AS root.db.device.s01");
      statement.execute("DELETE timeseries root.db.device.s01;");
      try {
        statement.execute("insert into root.db.device(time,s01,v_s01) values(1,true,true)");
        Assert.fail("expect exception");
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage()
                .contains(
                    "Insertion is illegal because measurement [s01] under device [root.db.device] is duplicate"));
      }
      try {
        statement.execute("insert into root.db.device(time,v_s01) values(1,true)");
        Assert.fail("expect exception");
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage()
                .contains(
                    "The source path [root.db.device.s01] of view [root.db.device.v_s01] does not exist"));
      }
    }
  }

  @Test
  public void testInsertIntoViewWithTypeMismatch() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.db");
      statement.execute("CREATE DATABASE root.view");
      statement.execute("CREATE ALIGNED TIMESERIES root.db.d1(s01 INT32, s02 INT64)");
      statement.execute("CREATE TIMESERIES root.db.d2.s01 WITH DATATYPE=INT32");
      statement.execute("CREATE TIMESERIES root.db.d2.s02 WITH DATATYPE=TEXT");
      statement.execute("CREATE VIEW root.view.v1(col1, col2) AS root.db.d1.s01, root.db.d2.s01");

      statement.execute("INSERT INTO root.db.d1(time, s01, s02) ALIGNED VALUES(100, 200, 300)");
      statement.execute("INSERT INTO root.db.d1(time, s01, s02) ALIGNED VALUES(200, 300, 400)");
      statement.execute("INSERT INTO root.db.d2(time, s01, s02) VALUES(300, 300, 400)");
      statement.execute("INSERT INTO root.db.d2(time, s01, s02) VALUES(400, 300, 400)");
      try {
        statement.execute("INSERT INTO root.view.v1(time, col1) ALIGNED VALUES(300, \"hello\")");
        Assert.fail("expect exception");
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage(), e.getMessage().contains("Fail to insert measurements [col1]"));
        Assert.assertFalse(
            e.getMessage(),
            e.getMessage()
                .contains(
                    "Database not exists and failed to create automatically because enable_auto_create_schema is FALSE."));
      }
    }
  }
}
