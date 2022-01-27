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
package org.apache.iotdb.db.integration;

import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.itbase.category.RemoteTest;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.fail;

@Category({LocalStandaloneTest.class, ClusterTest.class, RemoteTest.class})
public class IoTDBSyntaxConventionIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeTest();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterTest();
  }

  @Test
  public void testStringLiteral() {
    String[] insertData = {
      "'string'",
      "'\"string\"'",
      "'\"\"string\"\"'",
      "'str''ing'",
      "'\\'string'",
      "\"string\"",
      "\"'string'\"",
      "\"''string''\"",
      "\"str\"\"ing\"",
      "\"\\\"string\""
    };
    String[] resultData = {
      "string",
      "\"string\"",
      "\"\"string\"\"",
      "str'ing",
      "'string",
      "string",
      "'string'",
      "''string''",
      "str\"ing",
      "\"string"
    };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg1.d1.s1 TEXT");
      for (int i = 0; i < insertData.length; i++) {
        String insertSql =
            String.format("INSERT INTO root.sg1.d1(time, s1) values (%d, %s)", i, insertData[i]);
        System.out.println("INSERT STATEMENT: " + insertSql);
        statement.execute(insertSql);
      }

      boolean hasResult = statement.execute("SELECT s1 FROM root.sg1.d1");
      Assert.assertTrue(hasResult);

      int cnt = 0;
      ResultSet resultSet = statement.getResultSet();
      while (resultSet.next()) {
        Assert.assertEquals(resultData[cnt], resultSet.getString("root.sg1.d1.s1"));
        cnt++;
      }
      Assert.assertEquals(10, cnt);

      for (int i = 0; i < insertData.length; i++) {
        String querySql = String.format("SELECT s1 FROM root.sg1.d1 WHERE s1 = %s", insertData[i]);
        System.out.println("QUERY STATEMENT: " + querySql);
        hasResult = statement.execute(querySql);
        Assert.assertTrue(hasResult);

        resultSet = statement.getResultSet();
        Assert.assertTrue(resultSet.next());
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testIllegalStringLiteral1() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg1.d1.s1 TEXT");
      statement.execute("INSERT INTO root.sg1.d1(time, s1) values (1, string)");
      fail();
    } catch (SQLException ignored) {
    }
  }

  @Test
  public void testIllegalStringLiteral2() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg1.d1.s1 TEXT");
      statement.execute("INSERT INTO root.sg1.d1(time, s1) values (1, `string`)");
      fail();
    } catch (SQLException ignored) {
    }
  }

  @Test
  public void testExpression1() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg1.d1.`1` INT32");
      boolean hasResult = statement.execute("SELECT `1` FROM root.sg1.d1");
      Assert.assertTrue(hasResult);

      ResultSet resultSet = statement.getResultSet();
      Assert.assertFalse(resultSet.next());
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testIllegalExpression1() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg1.d1.`1` INT32");
      statement.execute("SELECT 1 FROM root.sg1.d1");
      fail();
    } catch (SQLException ignored) {
    }
  }

  @Test
  public void testExpression2() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg1.d1.`1` INT32");
      boolean hasResult = statement.execute("SELECT `1` + 1 FROM root.sg1.d1");
      Assert.assertTrue(hasResult);

      ResultSet resultSet = statement.getResultSet();
      Assert.assertFalse(resultSet.next());
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testIllegalExpression2() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg1.d1.`1` INT32");
      statement.execute("SELECT 1 + 1 FROM root.sg1.d1");
      fail();
    } catch (SQLException ignored) {
    }
  }

  @Test
  public void testExpression3() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg1.d1.`1` INT64");
      boolean hasResult = statement.execute("SELECT sin(`1`) FROM root.sg1.d1");
      Assert.assertTrue(hasResult);

      ResultSet resultSet = statement.getResultSet();
      Assert.assertFalse(resultSet.next());
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testIllegalExpression3() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg1.d1.`1` INT64");
      statement.execute("SELECT sin(1) FROM root.sg1.d1");
      fail();
    } catch (SQLException ignored) {
    }
  }

  @Test
  public void testIllegalExpression4() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg1.d1.'a' INT64");
      try {
        statement.execute("SELECT 'a' FROM root.sg1.d1");
        fail();
      } catch (SQLException e) {
        // ignored
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testNodeName() {
    String[] createNodeNames = {
      "`select`",
      "'select'",
      "\"select\"",
      "`a+b`",
      "'a+b'",
      "\"a+b\"",
      "'a.b'",
      "\"a.b\"",
      "\"a'.'b\""
    };
    String[] selectNodeNames = {
      "`select`",
      "`'select'`",
      "`\"select\"`",
      "`a+b`",
      "`'a+b'`",
      "`\"a+b\"`",
      "`'a.b'`",
      "`\"a.b\"`",
      "`\"a'.'b\"`"
    };
    String[] resultNodeNames = {
      "select", "'select'", "\"select\"", "a+b", "'a+b'", "\"a+b\"", "'a.b'", "\"a.b\"", "\"a'.'b\""
    };
    String[] resultTimeseries = {
      "root.sg1.d1.select",
      "root.sg1.d1.'select'",
      "root.sg1.d1.\"select\"",
      "root.sg1.d1.a+b",
      "root.sg1.d1.'a+b'",
      "root.sg1.d1.\"a+b\"",
      "root.sg1.d1.'a.b'",
      "root.sg1.d1.\"a.b\"",
      "root.sg1.d1.\"a'.'b\""
    };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < createNodeNames.length; i++) {
        String createSql =
            String.format("CREATE TIMESERIES root.sg1.d1.%s INT32", createNodeNames[i]);
        String insertSql =
            String.format("INSERT INTO root.sg1.d1(time, %s) VALUES(1, 1)", createNodeNames[i]);
        System.out.println("CREATE TIMESERIES: " + createSql);
        statement.execute(createSql);
        statement.execute(insertSql);
      }

      boolean hasResult = statement.execute("SHOW TIMESERIES");
      Assert.assertTrue(hasResult);
      Set<String> expectedResult = new HashSet<>(Arrays.asList(resultTimeseries));

      ResultSet resultSet = statement.getResultSet();
      while (resultSet.next()) {
        Assert.assertTrue(expectedResult.contains(resultSet.getString("timeseries")));
        expectedResult.remove(resultSet.getString("timeseries"));
      }
      Assert.assertEquals(0, expectedResult.size());

      for (int i = 0; i < selectNodeNames.length; i++) {
        String selectSql =
            String.format("SELECT %s FROM root.sg1.d1 WHERE time = 1", selectNodeNames[i]);
        System.out.println("SELECT STATEMENT: " + selectSql);
        hasResult = statement.execute(selectSql);
        Assert.assertTrue(hasResult);

        resultSet = statement.getResultSet();
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(1, resultSet.getInt("root.sg1.d1." + resultNodeNames[i]));
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testIllegalNodeName1() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg1.d1.`a.b` TEXT");
      fail();
    } catch (SQLException ignored) {
    }
  }

  @Test
  public void testIllegalNodeName2() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg1.d1.\"a\"\".\"\"b\" TEXT");
      fail();
    } catch (SQLException e) {
      Assert.assertTrue(e.getMessage().contains("\"a\".\"b\" is not a legal node name"));
    }
  }

  @Test
  public void testIllegalNodeName3() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg1.d1.'a\\'.\\'b' TEXT");
      fail();
    } catch (SQLException e) {
      Assert.assertTrue(e.getMessage().contains("'a'.'b' is not a legal node name"));
    }
  }
}
