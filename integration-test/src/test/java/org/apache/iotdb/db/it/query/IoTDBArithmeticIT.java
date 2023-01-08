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

package org.apache.iotdb.db.it.query;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBArithmeticIT {

  private static final double E = 0.0001;

  private static final String[] INSERTION_SQLS = {
    "CREATE DATABASE root.test",
    "CREATE TIMESERIES root.sg.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
    "CREATE TIMESERIES root.sg.d1.s2 WITH DATATYPE=INT64, ENCODING=PLAIN",
    "CREATE TIMESERIES root.sg.d1.s3 WITH DATATYPE=FLOAT, ENCODING=PLAIN",
    "CREATE TIMESERIES root.sg.d1.s4 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
    "CREATE TIMESERIES root.sg.d1.s5 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
    "CREATE TIMESERIES root.sg.d1.s6 WITH DATATYPE=TEXT, ENCODING=PLAIN",
    "CREATE TIMESERIES root.sg.d1.s7 WITH DATATYPE=INT32, ENCODING=PLAIN",
    "CREATE TIMESERIES root.sg.d1.s8 WITH DATATYPE=INT32, ENCODING=PLAIN",
    "insert into root.sg.d1(time, s1, s2, s3, s4, s5, s6, s7) values (1, 1, 1, 1, 1, false, '1', 1)",
    "insert into root.sg.d1(time, s1, s2, s3, s4, s5, s6, s8) values (2, 2, 2, 2, 2, false, '2', 2)",
    "insert into root.sg.d1(time, s1, s2, s3, s4, s5, s6, s7) values (3, 3, 3, 3, 3, true, '3', 3)",
    "insert into root.sg.d1(time, s1, s2, s3, s4, s5, s6, s8) values (4, 4, 4, 4, 4, true, '4', 4)",
    "insert into root.sg.d1(time, s1, s2, s3, s4, s5, s6, s7, s8) values (5, 5, 5, 5, 5, true, '5', 5, 5)",
  };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(INSERTION_SQLS);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testArithmeticBinary() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      String[] operands = new String[] {"s1", "s2", "s3", "s4"};
      for (String operator : new String[] {" + ", " - ", " * ", " / ", " % "}) {
        List<String> expressions = new ArrayList<>();
        for (String leftOperand : operands) {
          for (String rightOperand : operands) {
            expressions.add(leftOperand + operator + rightOperand);
          }
        }
        String sql = String.format("select %s from root.sg.d1", String.join(",", expressions));

        ResultSet resultSet = statement.executeQuery(sql);

        assertEquals(1 + expressions.size(), resultSet.getMetaData().getColumnCount());

        for (int i = 1; i < 6; ++i) {
          resultSet.next();
          for (int j = 0; j < expressions.size(); ++j) {
            double expected = 0;
            switch (operator) {
              case " + ":
                expected = i + i;
                break;
              case " - ":
                expected = i - i;
                break;
              case " * ":
                expected = i * i;
                break;
              case " / ":
                expected = i / i;
                break;
              case " % ":
                expected = i % i;
                break;
            }
            double actual = Double.parseDouble(resultSet.getString(2 + j));
            assertEquals(expected, actual, E);
          }
        }
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testArithmeticUnary() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] expressions = new String[] {"- s1", "- s2", "- s3", "- s4"};
      String sql = String.format("select %s from root.sg.d1", String.join(",", expressions));
      ResultSet resultSet = statement.executeQuery(sql);

      assertEquals(1 + expressions.length, resultSet.getMetaData().getColumnCount());

      for (int i = 1; i < 6; ++i) {
        resultSet.next();
        for (int j = 0; j < expressions.length; ++j) {
          double expected = -i;
          double actual = Double.parseDouble(resultSet.getString(2 + j));
          assertEquals(expected, actual, E);
        }
      }
      resultSet.close();
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testHybridQuery() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] expressions = new String[] {"s1", "s1 + s2", "sin(s1)"};
      String sql = String.format("select %s from root.sg.d1", String.join(",", expressions));
      ResultSet resultSet = statement.executeQuery(sql);

      assertEquals(1 + expressions.length, resultSet.getMetaData().getColumnCount());

      for (int i = 1; i < 6; ++i) {
        resultSet.next();
        assertEquals(i, Double.parseDouble(resultSet.getString(2)), E);
        assertEquals(i + i, Double.parseDouble(resultSet.getString(3)), E);
        assertEquals(Math.sin(i), Double.parseDouble(resultSet.getString(4)), E);
      }
      resultSet.close();
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testNonAlign() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("select s7 + s8 from root.sg.d1");
      assertEquals(1 + 1, resultSet.getMetaData().getColumnCount());
      assertTrue(resultSet.next());
      String curr = null;
      while (curr == null) {
        curr = resultSet.getString(2);
        resultSet.next();
      }
      assertEquals(10, Double.parseDouble(resultSet.getString(2)), E);
      assertFalse(resultSet.next());

      resultSet = statement.executeQuery("select s7 + s8 from root.sg.d1 where time < 5");
      assertEquals(1 + 1, resultSet.getMetaData().getColumnCount());
      curr = null;
      while (curr == null) {
        if (!resultSet.next()) {
          break;
        }
        curr = resultSet.getString(2);
      }
      assertEquals(null, curr);
      resultSet.close();
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testWrongTypeBoolean() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery("select s1 + s5 from root.sg.d1");
    } catch (Exception throwable) {
      assertTrue(throwable.getMessage().contains("Invalid input expression data type."));
    }
  }

  @Test
  public void testWrongTypeText() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery("select s1 + s6 from root.sg.d1");
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("Invalid input expression data type."));
    }
  }
}
