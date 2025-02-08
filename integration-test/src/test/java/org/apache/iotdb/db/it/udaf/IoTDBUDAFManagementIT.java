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

package org.apache.iotdb.db.it.udaf;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.constant.BuiltinAggregationFunctionEnum;
import org.apache.iotdb.itbase.constant.BuiltinScalarFunctionEnum;
import org.apache.iotdb.itbase.constant.BuiltinTimeSeriesGeneratingFunctionEnum;

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

import static org.junit.Assert.*;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBUDAFManagementIT {
  private static final int FUNCTIONS_COUNT =
      BuiltinAggregationFunctionEnum.getNativeFunctionNames().size()
          + BuiltinTimeSeriesGeneratingFunctionEnum.values().length
          + BuiltinScalarFunctionEnum.values().length;

  private static final String FUNCTION_TYPE_EXTERNAL_UDAF = "external UDAF";

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
  public void createReflectShowDropUDAFTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE FUNCTION udaf AS 'org.apache.iotdb.db.query.udf.example.UDAFCount'");
      statement.executeQuery("SELECT udaf(*) FROM root.vehicle");

      try (ResultSet resultSet = statement.executeQuery("SHOW FUNCTIONS")) {
        assertEquals(4, resultSet.getMetaData().getColumnCount());
        int count = 0;
        while (resultSet.next()) {
          ++count;
        }
        Assert.assertEquals(1 + FUNCTIONS_COUNT, count);
        statement.execute("DROP FUNCTION udaf");
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void createAndDropUDAFSeveralTimesTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE FUNCTION udaf AS 'org.apache.iotdb.db.query.udf.example.UDAFCount'");
      statement.executeQuery("SELECT udaf(*) FROM root.vehicle");

      try (ResultSet resultSet = statement.executeQuery("SHOW FUNCTIONS")) {
        int count = 0;
        while (resultSet.next()) {
          ++count;
        }
        Assert.assertEquals(1 + FUNCTIONS_COUNT, count);
        assertEquals(4, resultSet.getMetaData().getColumnCount());
        statement.execute("DROP FUNCTION udaf");

        statement.execute(
            "CREATE FUNCTION udaf AS 'org.apache.iotdb.db.query.udf.example.UDAFCount'");
        statement.executeQuery("SELECT udaf(*) FROM root.vehicle");
      }

      try (ResultSet resultSet = statement.executeQuery("SHOW FUNCTIONS")) {
        int count = 0;
        while (resultSet.next()) {
          ++count;
        }
        Assert.assertEquals(1 + FUNCTIONS_COUNT, count);
        assertEquals(4, resultSet.getMetaData().getColumnCount());
        statement.execute("DROP FUNCTION udaf");
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void reflectBeforeCreateTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery("SELECT udaf(*) FROM root.vehicle");
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("Failed to reflect UDF instance"));
    }
  }

  @Test
  public void reflectAfterDropTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE FUNCTION udaf AS 'org.apache.iotdb.db.query.udf.example.UDAFCount'");
      statement.execute("DROP FUNCTION udaf");
      statement.executeQuery("SELECT udaf(*) FROM root.vehicle");
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("Failed to reflect UDF instance"));
    }
  }

  @Test
  public void createFunctionWithBuiltinFunctionNameTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE FUNCTION count AS 'org.apache.iotdb.db.query.udf.example.UDAFCount'");
      fail();
    } catch (SQLException throwable) {
      assertTrue(
          throwable
              .getMessage()
              .contains("the given function name conflicts with the built-in function name"));
    }
  }

  @Test
  public void createFunctionTwiceTest() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE FUNCTION udaf AS 'org.apache.iotdb.db.query.udf.example.UDAFCount'");

      try {
        statement.execute(
            "CREATE FUNCTION udaf AS 'org.apache.iotdb.db.query.udf.example.UDAFCount'");
        fail();
      } catch (SQLException throwable) {
        assertTrue(throwable.getMessage().contains("Failed to create"));
      }
    }
  }

  @Test
  public void createFunctionTwiceTest2() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE FUNCTION udaf AS 'org.apache.iotdb.db.query.udf.example.UDAFCount'");

      try {
        statement.execute(
            "CREATE FUNCTION udaf AS 'org.apache.iotdb.db.query.udf.example.UDAFCount'");
        fail();
      } catch (SQLException throwable) {
        assertTrue(throwable.getMessage().contains("the same name UDF has been created"));
      }
    }
  }

  @Test
  public void createFunctionWithURITest() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "CREATE FUNCTION udaf1 AS 'org.apache.iotdb.db.query.udf.example.UDAFCount' USING URI '%s'",
              UDF_JAR_PREFIX + "udf-example.jar"));

      statement.execute(
          String.format(
              "CREATE FUNCTION udaf2 AS 'org.apache.iotdb.db.query.udf.example.UDAFCount' USING URI '%s'",
              UDF_JAR_PREFIX + "udf-example.jar"));

      try (ResultSet resultSet = statement.executeQuery("SHOW FUNCTIONS")) {
        int count = 0;
        while (resultSet.next()) {
          ++count;
        }
        Assert.assertEquals(2 + FUNCTIONS_COUNT, count);
        assertEquals(4, resultSet.getMetaData().getColumnCount());
        statement.execute("DROP FUNCTION udaf1");
        statement.execute("DROP FUNCTION udaf2");
      } catch (Exception e) {
        fail();
      }
    }
  }

  @Test
  public void createFunctionWithInvalidURITest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            String.format(
                "CREATE FUNCTION udaf AS 'org.apache.iotdb.db.query.udf.example.UDAFCount' USING URI '%s'",
                ""));
        fail();
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("URI"));
      }

      try {
        statement.execute(
            String.format(
                "CREATE FUNCTION udaf AS 'org.apache.iotdb.db.query.udf.example.UDAFCount' USING URI '%s'",
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
  public void createAndDropTwiceFunctionTest() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE FUNCTION udaf AS 'org.apache.iotdb.db.query.udf.example.UDAFCount'");
      statement.execute("DROP FUNCTION udaf");

      try {
        // drop UDF that does not exist will not throw exception now.
        statement.execute("DROP FUNCTION udaf");
      } catch (SQLException throwable) {
        assertTrue(throwable.getMessage().contains("this UDF has not been created"));
      }
    }
  }

  @Test
  public void dropWithoutCreateFunction() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // drop UDF that does not exist will not throw exception now.
      statement.execute("DROP FUNCTION udaf");
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("this UDF has not been created"));
    }
  }

  @Test
  public void testCreateBuiltinFunction() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE FUNCTION avg AS 'org.apache.iotdb.db.query.udf.example.UDAFCount'");
      fail();
    } catch (SQLException throwable) {
      assertTrue(
          throwable
              .getMessage()
              .contains("the given function name conflicts with the built-in function name"));
    }
  }

  @Test
  public void testShowBuiltinFunction() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE FUNCTION udaf AS 'org.apache.iotdb.db.query.udf.example.UDAFCount'");

      try (ResultSet resultSet = statement.executeQuery("SHOW FUNCTIONS")) {
        assertEquals(4, resultSet.getMetaData().getColumnCount());
        int count = 0;
        while (resultSet.next()) {
          StringBuilder stringBuilder = new StringBuilder();
          for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); ++i) {
            stringBuilder.append(resultSet.getString(i)).append(",");
          }
          String result = stringBuilder.toString();
          if (result.contains(FUNCTION_TYPE_EXTERNAL_UDAF)) {
            Assert.assertEquals(
                String.format(
                    "UDAF,%s,org.apache.iotdb.db.query.udf.example.UDAFCount,AVAILABLE,",
                    FUNCTION_TYPE_EXTERNAL_UDAF),
                result);
          }
          ++count;
        }
        Assert.assertEquals(1 + FUNCTIONS_COUNT, count);
        statement.execute("DROP FUNCTION udaf");
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
