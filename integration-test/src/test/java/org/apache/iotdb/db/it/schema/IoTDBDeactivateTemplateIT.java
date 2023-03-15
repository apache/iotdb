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
import org.apache.iotdb.rpc.TSStatusCode;

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

@Category({ClusterIT.class})
public class IoTDBDeactivateTemplateIT extends AbstractSchemaIT {

  public IoTDBDeactivateTemplateIT(SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    EnvFactory.getEnv().initClusterEnvironment();

    prepareTemplate();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    super.tearDown();
  }

  private void prepareTemplate() throws SQLException {
    // create database
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("CREATE DATABASE root.sg1");
      statement.execute("CREATE DATABASE root.sg2");
      statement.execute("CREATE DATABASE root.sg3");
      statement.execute("CREATE DATABASE root.sg4");

      // create schema template
      statement.execute("CREATE SCHEMA TEMPLATE t1 (s1 INT64, s2 DOUBLE)");
      statement.execute("CREATE SCHEMA TEMPLATE t2 (s1 INT64, s2 DOUBLE)");

      // set schema template
      statement.execute("SET SCHEMA TEMPLATE t1 TO root.sg1");
      statement.execute("SET SCHEMA TEMPLATE t1 TO root.sg2");
      statement.execute("SET SCHEMA TEMPLATE t2 TO root.sg3");
      statement.execute("SET SCHEMA TEMPLATE t2 TO root.sg4");

      String insertSql = "insert into root.sg%d.d1(time, s1, s2) values(%d, %d, %d)";
      for (int i = 1; i <= 4; i++) {
        for (int j = 1; j <= 4; j++) {
          statement.execute(String.format(insertSql, j, i, i, i));
        }
      }
    }
  }

  @Test
  public void deactivateTemplateAndReactivateTest() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("DEACTIVATE SCHEMA TEMPLATE t1 FROM root.sg1.d1");
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.sg1.*")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Assert.assertEquals(1, resultSetMetaData.getColumnCount());
        Assert.assertFalse(resultSet.next());
      }

      statement.execute("CREATE TIMESERIES OF SCHEMA TEMPLATE ON root.sg1.d1");

      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.sg1.*")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Assert.assertEquals(3, resultSetMetaData.getColumnCount());
        Assert.assertFalse(resultSet.next());
      }

      statement.execute("insert into root.sg1.d1(time, s1, s2) values(1, 1, 1)");

      String[] retArray = new String[] {"1,1,1.0,"};
      int cnt = 0;
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.sg1.*")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Assert.assertEquals(3, resultSetMetaData.getColumnCount());
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          Assert.assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    }
  }

  @Test
  public void deactivateTemplateAndAutoDeleteDeviceTest() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("DEACTIVATE SCHEMA TEMPLATE t1 FROM root.sg1.d1");
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.sg1.*")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Assert.assertEquals(1, resultSetMetaData.getColumnCount());
        Assert.assertFalse(resultSet.next());
      }
      try (ResultSet resultSet = statement.executeQuery("SHOW DEVICES root.sg1.*")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void deactivateTemplateCrossSchemaRegionTest() throws Exception {
    String insertSql = "insert into root.sg1.d%d(time, s1, s2) values(%d, %d, %d)";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (int i = 1; i <= 4; i++) {
        for (int j = 1; j <= 4; j++) {
          statement.execute(String.format(insertSql, j, i, i, i));
        }
      }

      statement.execute("DEACTIVATE SCHEMA TEMPLATE FROM root.sg1.*");
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.sg1.**")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Assert.assertEquals(1, resultSetMetaData.getColumnCount());
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void deactivateTemplateCrossStorageGroupTest() throws Exception {
    String insertSql = "insert into root.sg%d.d2(time, s1, s2) values(%d, %d, %d)";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (int i = 1; i <= 4; i++) {
        for (int j = 1; j <= 2; j++) {
          statement.execute(String.format(insertSql, j, i, i, i));
        }
      }

      statement.execute("DEACTIVATE SCHEMA TEMPLATE FROM root.*.d1");
      String[] retArray =
          new String[] {"1,1,1.0,1,1.0,", "2,2,2.0,2,2.0,", "3,3,3.0,3,3.0,", "4,4,4.0,4,4.0,"};
      int cnt = 0;
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.**")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Assert.assertEquals(5, resultSetMetaData.getColumnCount());
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          Assert.assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      statement.execute("DEACTIVATE SCHEMA TEMPLATE FROM root.**, root.sg1.*");
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.**")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Assert.assertEquals(1, resultSetMetaData.getColumnCount());
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void deactivateTemplateWithMultiPatternTest() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("DEACTIVATE SCHEMA TEMPLATE t1 FROM root.sg1.d1, root.sg2.*");
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.sg1.*, root.sg2.*")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Assert.assertEquals(1, resultSetMetaData.getColumnCount());
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void deactivateNoneUsageTemplateTest() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("DEACTIVATE SCHEMA TEMPLATE t1 FROM root.sg5.d1");
        Assert.fail();
      } catch (SQLException e) {
        Assert.assertEquals(
            TSStatusCode.TEMPLATE_NOT_SET.getStatusCode()
                + ": Schema Template t1 is not set on any prefix path of [root.sg5.d1]",
            e.getMessage());
      }

      statement.execute("CREATE DATABASE root.sg5");
      statement.execute("SET SCHEMA TEMPLATE t1 TO root.sg5 ");
      try {
        statement.execute("DEACTIVATE SCHEMA TEMPLATE t1 FROM root.sg5.d1");
        Assert.fail();
      } catch (SQLException e) {
        Assert.assertEquals(
            TSStatusCode.TEMPLATE_NOT_ACTIVATED.getStatusCode()
                + ": Target schema Template is not activated on any path matched by given path pattern",
            e.getMessage());
      }
    }
  }

  @Test
  public void multiSyntaxTest() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("DELETE TIMESERIES OF SCHEMA TEMPLATE t1 FROM root.sg1.d1");
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.sg1.*")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Assert.assertEquals(1, resultSetMetaData.getColumnCount());
        Assert.assertFalse(resultSet.next());
      }

      statement.execute("DEACTIVATE SCHEMA TEMPLATE t2 FROM root.sg3.d1");
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.sg3.*")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Assert.assertEquals(1, resultSetMetaData.getColumnCount());
        Assert.assertFalse(resultSet.next());
      }

      statement.execute("DEACTIVATE SCHEMA TEMPLATE FROM root.**");
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.**")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Assert.assertEquals(1, resultSetMetaData.getColumnCount());
        Assert.assertFalse(resultSet.next());
      }
    }
  }
}
