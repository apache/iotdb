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
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSelectCompareExpressionIT {

  private static String[] INSERTION_SQLS;
  private static List<Long> time = new ArrayList<>(0);
  private static List<Integer> intValue = new ArrayList<>(0);
  private static List<Long> longValue = new ArrayList<>(0);
  private static List<Float> floatValue = new ArrayList<>(0);
  private static List<Double> doubleValue = new ArrayList<>(0);
  private static List<Boolean> boolValue = new ArrayList<>(0);

  private static void generateInsertionSQLS() {
    INSERTION_SQLS = new String[50];
    Random random = new Random();
    for (int j = 0; j < 50; ++j) {
      intValue.add(random.nextInt(10));
      longValue.add((long) random.nextInt(10));
      floatValue.add((float) (random.nextInt(100) / 10.0));
      doubleValue.add(random.nextInt(100) / 10.0);
      boolValue.add(random.nextBoolean());
      INSERTION_SQLS[j] =
          generateInsertionSQL(
              (long) j,
              intValue.get(intValue.size() - 1),
              longValue.get(longValue.size() - 1),
              floatValue.get(floatValue.size() - 1),
              doubleValue.get(doubleValue.size() - 1),
              boolValue.get(boolValue.size() - 1),
              "\"magic_words\"");
    }
  }

  private static String generateInsertionSQL(
      long time,
      int intValue32,
      long intValue64,
      float floatValue,
      double doubleValue,
      boolean boolValue,
      String _text) {
    return String.format(
        "insert into root.sg.d1(time, s1, s2, s3, s4, s5, s6) values (%d, %d, %d, %f, %f, %s, %s);",
        time, intValue32, intValue64, floatValue, doubleValue, boolValue ? "true" : "false", _text);
  }

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    createTimeSeries();
    generateData();
  }

  private static void generateData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      generateInsertionSQLS();
      for (String dataGenerationSql : INSERTION_SQLS) {
        statement.execute(dataGenerationSql);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void createTimeSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.sg");
      statement.execute("CREATE TIMESERIES root.sg.d1.s1 with datatype=INT32,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d1.s2 with datatype=INT64,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d1.s3 with datatype=FLOAT,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d1.s4 with datatype=DOUBLE,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d1.s5 with datatype=BOOLEAN,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d1.s6 with datatype=TEXT,encoding=PLAIN");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  /*
   * Test compare expressions between different TSDataType
   * */
  @Test
  public void testCompareWithConstant() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery("select s1>=5, s1<=5, s1>5, s1<5, s1==5, s1!=5 from root.sg.d1");
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 6, columnCount);

      boolean bool;
      for (int i = 0; i < time.size(); ++i) {
        resultSet.next();

        bool = Boolean.parseBoolean(resultSet.getString(2));
        assertEquals(intValue.get(i) >= 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(3));
        assertEquals(intValue.get(i) <= 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(4));
        assertEquals(intValue.get(i) > 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(5));
        assertEquals(intValue.get(i) < 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(6));
        assertEquals(intValue.get(i) == 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(7));
        assertEquals(intValue.get(i) != 5, bool);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery("select s2>=5, s2<=5, s2>5, s2<5, s2==5, s2!=5 from root.sg.d1");
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 6, columnCount);

      boolean bool;
      for (int i = 0; i < time.size(); ++i) {
        resultSet.next();

        bool = Boolean.parseBoolean(resultSet.getString(2));
        assertEquals(longValue.get(i) >= 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(3));
        assertEquals(longValue.get(i) <= 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(4));
        assertEquals(longValue.get(i) > 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(5));
        assertEquals(longValue.get(i) < 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(6));
        assertEquals(longValue.get(i) == 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(7));
        assertEquals(longValue.get(i) != 5, bool);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery("select s3>=5, s3<=5, s3>5, s3<5, s3==5, s3!=5 from root.sg.d1");
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 6, columnCount);

      boolean bool;
      for (int i = 0; i < time.size(); ++i) {
        resultSet.next();

        bool = Boolean.parseBoolean(resultSet.getString(2));
        assertEquals(floatValue.get(i) >= 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(3));
        assertEquals(floatValue.get(i) <= 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(4));
        assertEquals(floatValue.get(i) > 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(5));
        assertEquals(floatValue.get(i) < 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(6));
        assertEquals(floatValue.get(i) == 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(7));
        assertEquals(floatValue.get(i) != 5, bool);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery("select s4>=5, s4<=5, s4>5, s4<5, s4==5, s4!=5 from root.sg.d1");
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 6, columnCount);

      boolean bool;
      for (int i = 0; i < time.size(); ++i) {
        resultSet.next();

        bool = Boolean.parseBoolean(resultSet.getString(2));
        assertEquals(doubleValue.get(i) >= 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(3));
        assertEquals(doubleValue.get(i) <= 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(4));
        assertEquals(doubleValue.get(i) > 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(5));
        assertEquals(doubleValue.get(i) < 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(6));
        assertEquals(doubleValue.get(i) == 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(7));
        assertEquals(doubleValue.get(i) != 5, bool);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery("select s5==true, s5!=true, s5==false, s5!=false from root.sg.d1");
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 4, columnCount);

      boolean bool;
      for (int i = 0; i < time.size(); ++i) {
        resultSet.next();

        bool = Boolean.parseBoolean(resultSet.getString(2));
        assertEquals(boolValue.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(3));
        assertEquals(!boolValue.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(4));
        assertEquals(!boolValue.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(5));
        assertEquals(boolValue.get(i), bool);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testCompareDifferentType() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery(
              "select s1>=s2, s1<=s2, s1>s3, s1<s3, s1==s4, s1!=s4 from root.sg.d1");
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 6, columnCount);

      boolean bool;
      for (int i = 0; i < time.size(); ++i) {
        resultSet.next();

        bool = Boolean.parseBoolean(resultSet.getString(2));
        assertEquals(intValue.get(i) >= longValue.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(3));
        assertEquals(intValue.get(i) <= longValue.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(4));
        assertEquals(intValue.get(i) > floatValue.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(5));
        assertEquals(intValue.get(i) < floatValue.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(6));
        assertEquals((double) intValue.get(i) == doubleValue.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(7));
        assertEquals((double) intValue.get(i) != doubleValue.get(i), bool);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery(
              "select s2>=s3, s2<=s3, s2>s4, s2<s4, s2==s1, s2!=s1 from root.sg.d1");
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 6, columnCount);

      boolean bool;
      for (int i = 0; i < time.size(); ++i) {
        resultSet.next();

        bool = Boolean.parseBoolean(resultSet.getString(2));
        assertEquals(longValue.get(i) >= floatValue.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(3));
        assertEquals(longValue.get(i) <= floatValue.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(4));
        assertEquals(longValue.get(i) > doubleValue.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(5));
        assertEquals(longValue.get(i) < doubleValue.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(6));
        assertEquals(longValue.get(i) == (long) intValue.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(7));
        assertEquals(longValue.get(i) != (long) intValue.get(i), bool);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery(
              "select s3>=s4, s3<=s4, s3>s1, s3<s1, s3==s2, s3!=s2 from root.sg.d1");
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 6, columnCount);

      boolean bool;
      for (int i = 0; i < time.size(); ++i) {
        resultSet.next();

        bool = Boolean.parseBoolean(resultSet.getString(2));
        assertEquals(floatValue.get(i) >= doubleValue.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(3));
        assertEquals(floatValue.get(i) <= doubleValue.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(4));
        assertEquals(floatValue.get(i) > intValue.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(5));
        assertEquals(floatValue.get(i) < intValue.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(6));
        assertEquals(floatValue.get(i) == (float) longValue.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(7));
        assertEquals(floatValue.get(i) != (float) longValue.get(i), bool);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery(
              "select s4>=s1, s4<=s1, s4>s2, s4<s2, s4==s3, s4!=s3 from root.sg.d1");
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 6, columnCount);

      boolean bool;
      for (int i = 0; i < time.size(); ++i) {
        resultSet.next();

        bool = Boolean.parseBoolean(resultSet.getString(2));
        assertEquals(doubleValue.get(i) >= intValue.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(3));
        assertEquals(doubleValue.get(i) <= intValue.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(4));
        assertEquals(doubleValue.get(i) > longValue.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(5));
        assertEquals(doubleValue.get(i) < longValue.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(6));
        assertEquals(doubleValue.get(i) == (double) floatValue.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(7));
        assertEquals(doubleValue.get(i) != (double) floatValue.get(i), bool);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testLogicOrAndNot() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery(
              "select s1>=1 && s1<3, !(s1 < 2 || s1> 8), !(s2>3) from root.sg.d1");
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 3, columnCount);

      boolean bool;
      for (int i = 0; i < time.size(); ++i) {
        resultSet.next();

        bool = Boolean.parseBoolean(resultSet.getString(2));
        assertEquals(intValue.get(i) >= 1 && intValue.get(i) < 3, bool);

        bool = Boolean.parseBoolean(resultSet.getString(3));
        assertEquals(!(intValue.get(i) < 2 || intValue.get(i) > 8), bool);

        bool = Boolean.parseBoolean(resultSet.getString(4));
        assertEquals(!(longValue.get(i) > 3), bool);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testComplexExpression() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery(
              "select ( s1 + 1 ) * 2 - 4 < ( s3 * 3 - 6) / 2 && ( s1 + 5 ) * 2 > s2 * 3 + 4 from root.sg.d1");
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 1, columnCount);

      boolean bool;
      for (int i = 0; i < time.size(); ++i) {
        resultSet.next();

        bool = Boolean.parseBoolean(resultSet.getString(2));
        assertEquals(
            (intValue.get(i) + 1) * 2 - 4 < (floatValue.get(i) * 3 - 6) / 2
                && (intValue.get(i) + 5) * 2 > longValue.get(i) * 3 + 4,
            bool);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
