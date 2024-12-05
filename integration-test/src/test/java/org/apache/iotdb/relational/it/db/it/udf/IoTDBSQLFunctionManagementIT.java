/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.relational.it.db.it.udf;

import org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinAggregationFunction;
import org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinScalarFunction;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_USER_DEFINED_SCALAR_FUNC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSQLFunctionManagementIT {

  private static final int BUILTIN_SCALAR_FUNCTIONS_COUNT =
      TableBuiltinScalarFunction.getBuiltInScalarFunctionName().size();
  private static final int BUILTIN_AGGREGATE_FUNCTIONS_COUNT =
      TableBuiltinAggregationFunction.values().length;

  private static final String UDF_LIB_PREFIX =
      System.getProperty("user.dir")
          + File.separator
          + "target"
          + File.separator
          + "test-classes"
          + File.separator;

  private static final String UDF_JAR_PREFIX = new File(UDF_LIB_PREFIX).toURI().toString();

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testCreateShowDropScalarFunction() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create function udsf as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull'");

      try (ResultSet resultSet = statement.executeQuery("show functions")) {
        assertEquals(4, resultSet.getMetaData().getColumnCount());
        int count = 0;
        while (resultSet.next()) {
          StringBuilder stringBuilder = new StringBuilder();
          for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); ++i) {
            stringBuilder.append(resultSet.getString(i)).append(",");
          }
          String result = stringBuilder.toString();
          if (result.contains("FUNCTION_TYPE_USER_DEFINED_SCALAR_FUNC")) {
            Assert.assertEquals(
                String.format(
                    "udsf,%s,org.apache.iotdb.db.query.udf.example.relational.ContainNull,AVAILABLE,",
                    FUNCTION_TYPE_USER_DEFINED_SCALAR_FUNC),
                result);
          }
          ++count;
        }
        Assert.assertEquals(
            1 + BUILTIN_AGGREGATE_FUNCTIONS_COUNT + BUILTIN_SCALAR_FUNCTIONS_COUNT, count);
      }
      statement.execute("drop function udsf");
      try (ResultSet resultSet = statement.executeQuery("show functions")) {
        assertEquals(4, resultSet.getMetaData().getColumnCount());
        int count = 0;
        while (resultSet.next()) {
          StringBuilder stringBuilder = new StringBuilder();
          for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); ++i) {
            stringBuilder.append(resultSet.getString(i)).append(",");
          }
          String result = stringBuilder.toString();
          if (result.contains("FUNCTION_TYPE_USER_DEFINED_SCALAR_FUNC")) {
            Assert.fail();
          }
          ++count;
        }
        Assert.assertEquals(
            BUILTIN_AGGREGATE_FUNCTIONS_COUNT + BUILTIN_SCALAR_FUNCTIONS_COUNT, count);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testCreateFunctionWithBuiltinFunctionName() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            "create function COS as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull'");
        fail();
      } catch (SQLException throwable) {
        assertTrue(
            throwable
                .getMessage()
                .contains("the given function name conflicts with the built-in function name"));
      }
      try {
        statement.execute(
            "create function aVg as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull'");
        fail();
      } catch (SQLException throwable) {
        assertTrue(
            throwable
                .getMessage()
                .contains("the given function name conflicts with the built-in function name"));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testCreateFunctionTwice() throws SQLException { // create function twice
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create function udsf as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull'");
      try {
        statement.execute(
            "create function udsf as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull'");
        fail();
      } catch (SQLException throwable) {
        assertTrue(throwable.getMessage().contains("the same name UDF has been created"));
      }
    }
  }

  @Test
  public void testCreateInvalidFunction() throws SQLException { // create function twice
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            "create function udsf as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull123'");
        fail();
      } catch (SQLException throwable) {
        assertTrue(throwable.getMessage().contains("invalid"));
      }
    }
  }

  @Test
  public void testCreateFunctionWithURI() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create function udsf as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull' using URI '%s'",
              UDF_JAR_PREFIX + "udf-example.jar"));
      statement.execute(
          String.format(
              "create function udsf2 as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull' using URI '%s'",
              UDF_JAR_PREFIX + "udf-example.jar"));

      try (ResultSet resultSet = statement.executeQuery("show functions")) {
        int count = 0;
        while (resultSet.next()) {
          ++count;
        }
        Assert.assertEquals(
            2 + BUILTIN_AGGREGATE_FUNCTIONS_COUNT + BUILTIN_SCALAR_FUNCTIONS_COUNT, count);
        assertEquals(4, resultSet.getMetaData().getColumnCount());
      } catch (Exception e) {
        fail();
      }
    }
  }

  @Test
  public void testCreateFunctionWithInvalidURI() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            String.format(
                "create function udsf as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull' using URI '%s'",
                ""));
        fail();
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("URI"));
      }

      try {
        statement.execute(
            String.format(
                "create function udsf as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull' using URI '%s'",
                "file:///data/udf/upload-test.jar"));
        fail();
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("URI"));
      }
    } catch (SQLException throwable) {
      fail();
    }
  }

  @Test
  public void testDropFunctionTwice() throws SQLException { // create + drop twice
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create function udsf as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull'");
      statement.execute("drop function udsf");

      try {
        // drop UDF that does not exist will not throw exception now.
        statement.execute("drop function udsf");
      } catch (SQLException throwable) {
        assertTrue(throwable.getMessage().contains("this UDF has not been created"));
      }
    }
  }

  @Test
  public void testDropNotExistFunction() { // drop
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      // drop UDF that does not exist will not throw exception now.
      statement.execute("drop function udsf");
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("this UDF has not been created"));
    }
  }

  @Test
  public void testDropBuiltInFunction() throws SQLException { // drop
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("drop function abs");
        fail();
      } catch (SQLException throwable) {
        assertTrue(
            throwable.getMessage().contains("Built-in function ABS can not be deregistered"));
      }
      // ensure that abs is not dropped
      statement.execute("CREATE DATABASE db");
      statement.execute("USE db");
      statement.execute("CREATE TABLE table0 (device string id, s1 INT32)");
      statement.execute("INSERT INTO table0 (time, device, s1) VALUES (1, 'd1', -10)");
      try (ResultSet rs = statement.executeQuery("SELECT time, ABS(s1) FROM table0")) {
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getLong(1));
        Assert.assertEquals(10, rs.getInt(2));
        Assert.assertFalse(rs.next());
      }
    }
  }
}
