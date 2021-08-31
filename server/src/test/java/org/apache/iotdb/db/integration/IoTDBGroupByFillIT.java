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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.qp.logical.crud.AggregationQueryOperator;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBSQLException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.apache.iotdb.db.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.db.constant.TestConstant.last_value;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IoTDBGroupByFillIT {

  private static String[] dataSet1 =
      new String[] {
        "SET STORAGE GROUP TO root.ln.wf01.wt01",
        "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.hardware WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(10, 21, false, 11.1)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(12, 23, true, 22.3)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(14, 25, false, 33.5)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(29, 26, false, 33.2)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(36, 29, false, 44.7)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(37, 30, false, 55.8)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(39, 40, false, 33.0)",
        "flush"
      };

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(1000);
    Class.forName(Config.JDBC_DRIVER_NAME);
    prepareData();
  }

  @After
  public void tearDown() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(86400);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void previousTest1() {
    String[] retArray =
        new String[] {
          "17,25", "22,25", "27,26", "32,29", "37,40", "42,40", "47,40",
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(temperature) from root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 48), 5ms) FILL(int32[previous])");

      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"));
          assertEquals(retArray[cnt], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select last_value(temperature) from root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 48), 5ms) FILL(int32[previous]) order by time desc");

      assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"));
          assertEquals(retArray[retArray.length - cnt - 1], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void previousTest2() {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "select count(temperature) from root.ln.wf01.wt01 "
              + "GROUP BY ([17, 48), 5ms) FILL(int32[previous])");
    } catch (IoTDBSQLException e) {
      assertTrue(e.getMessage().contains("Group By Fill only support last_value function"));
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "select count(temperature) from root.ln.wf01.wt01 "
              + "GROUP BY ([17, 48), 5ms) FILL(int32[previous]) order by time desc");
    } catch (IoTDBSQLException e) {
      assertTrue(e.getMessage().contains("Group By Fill only support last_value function"));
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void previousTest3() {
    String[] retArray =
        new String[] {
          "2,null", "7,21", "12,25", "17,25", "22,25", "27,26", "32,29", "37,40", "42,40", "47,40",
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(temperature) from root.ln.wf01.wt01 "
                  + "GROUP BY ([2, 48), 5ms) FILL(int32[previous])");

      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"));
          assertEquals(retArray[cnt], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select last_value(temperature) from root.ln.wf01.wt01 "
                  + "GROUP BY ([2, 48), 5ms) FILL(int32[previous]) order by time desc");

      assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"));
          assertEquals(retArray[retArray.length - cnt - 1], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void previousTest4() {
    String[] retArray =
        new String[] {
          "2,null,null",
          "7,21,11.1",
          "12,25,33.5",
          "17,25,33.5",
          "22,25,33.5",
          "27,26,33.2",
          "32,29,44.7",
          "37,40,33.0",
          "42,40,33.0",
          "47,40,33.0",
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(temperature), last_value(hardware) from root.ln.wf01.wt01 "
                  + "GROUP BY ([2, 48), 5ms) FILL(int32[previous], double[previous])");

      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.hardware"));
          assertEquals(retArray[cnt], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select last_value(temperature), last_value(hardware) from root.ln.wf01.wt01 "
                  + "GROUP BY ([2, 48), 5ms) FILL(int32[previous], double[previous]) order by time desc");

      assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.hardware"));
          assertEquals(retArray[retArray.length - cnt - 1], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void leftORightCPreviousTest() {
    String[] retArray =
        new String[] {
          "10,21", "15,25", "20,25", "25,25", "30,26", "35,26", "40,40",
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(temperature) from root.ln.wf01.wt01 "
                  + "GROUP BY ((5, 40], 5ms) FILL(int32[previous])");

      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"));
          assertEquals(retArray[cnt], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select last_value(temperature) from root.ln.wf01.wt01 "
                  + "GROUP BY ((5, 40], 5ms) FILL(int32[previous]) order by time desc");

      assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"));
          assertEquals(retArray[retArray.length - cnt - 1], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void previousAllTest() {
    String[] retArray =
        new String[] {
          "2,null,null",
          "7,21,11.1",
          "12,25,33.5",
          "17,25,33.5",
          "22,25,33.5",
          "27,26,33.2",
          "32,29,44.7",
          "37,40,33.0",
          "42,40,33.0",
          "47,40,33.0",
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(temperature), last_value(hardware) from root.ln.wf01.wt01 "
                  + "GROUP BY ([2, 48), 5ms) FILL(ALL[previous])");

      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.hardware"));
          assertEquals(retArray[cnt], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select last_value(temperature), last_value(hardware) from root.ln.wf01.wt01 "
                  + "GROUP BY ([2, 48), 5ms) FILL(ALL[previous]) order by time desc");

      assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.hardware"));
          assertEquals(retArray[retArray.length - cnt - 1], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void previousUntilLastTest1() {
    String[] retArray =
        new String[] {
          "17,25", "22,25", "27,26", "32,29", "37,40", "42,null", "47,null",
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(temperature) from root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 48), 5ms) FILL(int32[previousUntilLast])");

      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"));
          assertEquals(retArray[cnt], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
      hasResultSet =
          statement.execute(
              "select last_value(temperature) from root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 48), 5ms) FILL(int32[previousUntilLast]) order by time desc");

      assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"));
          assertEquals(retArray[retArray.length - cnt - 1], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void previousUntilLastTest2() {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "select count(temperature) from root.ln.wf01.wt01 "
              + "GROUP BY ([17, 48), 5ms) FILL(int32[previousUntilLast])");
    } catch (IoTDBSQLException e) {
      System.out.println(e.getMessage());
      assertTrue(e.getMessage().contains("Group By Fill only support last_value function"));
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "select count(temperature) from root.ln.wf01.wt01 "
              + "GROUP BY ([17, 48), 5ms) FILL(int32[previousUntilLast]) order by time desc");
    } catch (IoTDBSQLException e) {
      System.out.println(e.getMessage());
      assertTrue(e.getMessage().contains("Group By Fill only support last_value function"));
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void previousUntilLastTest3() {
    String[] retArray =
        new String[] {
          "2,null", "7,21", "12,25", "17,25", "22,25", "27,26", "32,29", "37,40", "42,null",
          "47,null",
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(temperature) from root.ln.wf01.wt01 "
                  + "GROUP BY ([2, 48), 5ms) FILL(int32[previousUntilLast])");

      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"));
          assertEquals(retArray[cnt], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select last_value(temperature) from root.ln.wf01.wt01 "
                  + "GROUP BY ([2, 48), 5ms) FILL(int32[previousUntilLast]) order by time desc");

      assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"));
          assertEquals(retArray[retArray.length - cnt - 1], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void previousUntilLastTest4() {
    String[] retArray =
        new String[] {
          "2,null,null",
          "7,21,11.1",
          "12,25,33.5",
          "17,25,33.5",
          "22,25,33.5",
          "27,26,33.2",
          "32,29,44.7",
          "37,40,33.0",
          "42,null,null",
          "47,null,null",
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(temperature), last_value(hardware) from root.ln.wf01.wt01 "
                  + "GROUP BY ([2, 48), 5ms) FILL(int32[previousUntilLast], double[previousUntilLast])");

      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.hardware"));
          assertEquals(retArray[cnt], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select last_value(temperature), last_value(hardware) from root.ln.wf01.wt01 "
                  + "GROUP BY ([2, 48), 5ms) FILL(int32[previousUntilLast], double[previousUntilLast]) order by time desc");

      assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.hardware"));
          assertEquals(retArray[retArray.length - cnt - 1], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void previousUntilLastTest5() {
    String[] retArray =
        new String[] {
          "17,25", "22,25", "27,26", "32,29", "37,40", "42,null", "47,null",
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(temperature) from root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 48), 5ms) FILL(float[previousUntilLast])");

      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"));
          assertEquals(retArray[cnt], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select last_value(temperature) from root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 48), 5ms) FILL(float[previousUntilLast]) order by time desc");

      assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"));
          assertEquals(retArray[retArray.length - cnt - 1], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void leftORightCPreviousUntilLastTest() {
    String[] retArray =
        new String[] {"9,null", "14,25", "19,25", "24,25", "29,26", "34,26", "39,40", "44,null"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(temperature) from root.ln.wf01.wt01 "
                  + "GROUP BY ((4, 44], 5ms) FILL(int32[previousUntilLast])");

      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"));
          assertEquals(retArray[cnt], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void previousUntilLastAllTest() {
    String[] retArray =
        new String[] {
          "2,null,null",
          "7,21,11.1",
          "12,25,33.5",
          "17,25,33.5",
          "22,25,33.5",
          "27,26,33.2",
          "32,29,44.7",
          "37,40,33.0",
          "42,null,null",
          "47,null,null",
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(temperature), last_value(hardware) from root.ln.wf01.wt01 "
                  + "GROUP BY ([2, 48), 5ms) FILL(ALL[previousUntilLast])");

      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.hardware"));
          assertEquals(retArray[cnt], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void usingLimit() {

    String[] retArray =
        new String[] {
          "27,26", "32,29", "37,40", "42,40", "47,40",
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(temperature) from root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 48), 5ms) FILL(int32[previous]) "
                  + "limit 5 offset 2");

      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"));
          assertEquals(retArray[cnt], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /**
   * Test group by fill without aggregation function used in select clause. The expected situation
   * is throwing an exception.
   */
  @Test
  public void TestGroupByFillWithoutAggregationFunc() {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.execute(
          "select temperature from root.ln.wf01.wt01 "
              + "group by ([0, 100), 5ms) FILL(int32[previous])");

      fail("No expected exception thrown");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains(AggregationQueryOperator.ERROR_MESSAGE1));
    }
  }

  private void prepareData() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : dataSet1) {
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
