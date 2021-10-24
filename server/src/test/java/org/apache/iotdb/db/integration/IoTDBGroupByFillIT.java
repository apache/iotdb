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
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.apache.iotdb.jdbc.IoTDBSQLException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.constant.TestConstant.*;
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
              "CREATE TIMESERIES root.ln.wf01.wt01.text WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(10, 21, false, 11.1)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(12, 23, true, 22.3)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(14, 25, false, 33.5)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(23, 28, true, 34.9)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(25, 23, false, 31.7)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(33, 29, false, 44.6)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(36, 24, true, 44.8)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(48, 28, false, 54.6)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(50, 30, true, 55.8)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(66, 40, false, 33.0)",
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
  public void previousLastValueTest() {
    String[] retArray =
        new String[] {"17,null", "22,23", "27,23", "32,24", "37,24", "42,24", "47,30", "52,30"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(temperature) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 55), 5ms) FILL(int32[previous])");

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

      //      hasResultSet =
      //          statement.execute(
      //              "select last_value(temperature) from "
      //                  + "root.ln.wf01.wt01 "
      //                  + "GROUP BY ([17, 48), 5ms) FILL(int32[previous]) order by time desc");
      //
      //      assertTrue(hasResultSet);
      //      try (ResultSet resultSet = statement.getResultSet()) {
      //        cnt = 0;
      //        while (resultSet.next()) {
      //          String ans =
      //              resultSet.getString(TIMESTAMP_STR)
      //                  + ","
      //                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"));
      //          assertEquals(retArray[retArray.length - cnt - 1], ans);
      //          cnt++;
      //        }
      //        assertEquals(retArray.length, cnt);
      //      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void previousFirstValueTest() {
    String[] retArray =
        new String[] {
          "17,null", "22,34.9", "27,34.9", "32,44.6", "37,44.6", "42,44.6", "47,54.6", "52,54.6"
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select first_value(hardware) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 55), 5ms) FILL(double[previous])");

      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(first_value("root.ln.wf01.wt01.hardware"));
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
  public void previousAvgTest() {
    String[] retArray1 =
        new String[] {
          "17,null", "22,33.3", "27,33.3", "32,44.7", "37,44.7", "42,44.7", "47,55.2", "52,55.2"
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select avg(hardware) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 55), 5ms) FILL(double[previous])");
      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(avg("root.ln.wf01.wt01.hardware"));
          assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void previousCountTest() {
    // TODO: fix count error
    String[] retArray1 =
        new String[] {"17,0", "22,2", "27,2", "32,2", "37,2", "42,2", "47,2", "52,2"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(status) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 55), 5ms) FILL(int32[previous])");
      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.ln.wf01.wt01.status"));
          assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void previousMaxTimeTest() {
    String[] retArray1 =
        new String[] {"17,null", "22,25", "27,25", "32,36", "37,36", "42,36", "47,50", "52,50"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select max_time(hardware) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 55), 5ms) FILL(double[previous])");
      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(max_time("root.ln.wf01.wt01.hardware"));
          assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void previousMaxValueTest() {
    String[] retArray1 =
        new String[] {"17,null", "22,28", "27,28", "32,29", "37,29", "42,29", "47,30", "52,30"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select max_value(temperature) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 55), 5ms) FILL(int32[previous])");
      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(max_value("root.ln.wf01.wt01.temperature"));
          assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void previousMinTimeTest() {
    String[] retArray1 =
        new String[] {"17,null", "22,23", "27,23", "32,33", "37,33", "42,33", "47,48", "52,48"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select min_time(hardware) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 55), 5ms) FILL(double[previous])");
      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(min_time("root.ln.wf01.wt01.hardware"));
          assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void previousMinValueTest() {
    String[] retArray1 =
        new String[] {"17,null", "22,23", "27,23", "32,24", "37,24", "42,24", "47,28", "52,28"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select min_value(temperature) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 55), 5ms) FILL(int32[previous])");
      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(min_value("root.ln.wf01.wt01.temperature"));
          assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void previousSumTest() {
    // TODO: fix sum error
    String[] retArray1 =
        new String[] {
          "17,0.0", "22,66.6", "27,66.6", "32,89.4", "37,89.4", "42,89.4", "47,110.4", "52,110.4"
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select sum(hardware) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 55), 5ms) FILL(double[previous])");
      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(sum("root.ln.wf01.wt01.hardware"));
          assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void previousUntilLastLastValueTest() {
    String[] retArray =
        new String[] {
          "17,null", "22,false", "27,false", "32,true", "37,true", "42,true", "47,true", "52,null"
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(status) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 55), 5ms) FILL(boolean[previousUntilLast])");

      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.status"));
          assertEquals(retArray[cnt], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

      //      hasResultSet =
      //          statement.execute(
      //              "select last_value(temperature) from "
      //                  + "root.ln.wf01.wt01 "
      //                  + "GROUP BY ([17, 48), 5ms) FILL(int32[previous]) order by time desc");
      //
      //      assertTrue(hasResultSet);
      //      try (ResultSet resultSet = statement.getResultSet()) {
      //        cnt = 0;
      //        while (resultSet.next()) {
      //          String ans =
      //              resultSet.getString(TIMESTAMP_STR)
      //                  + ","
      //                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"));
      //          assertEquals(retArray[retArray.length - cnt - 1], ans);
      //          cnt++;
      //        }
      //        assertEquals(retArray.length, cnt);
      //      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void previousUntilLastFirstValueTest() {
    String[] retArray =
        new String[] {
          "17,null", "22,34.9", "27,34.9", "32,44.6", "37,44.6", "42,44.6", "47,54.6", "52,null"
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select first_value(hardware) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 55), 5ms) FILL(double[previousUntilLast])");

      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(first_value("root.ln.wf01.wt01.hardware"));
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
  public void previousUntilLastAvgTest() {
    String[] retArray1 =
        new String[] {
          "17,null", "22,33.3", "27,33.3", "32,44.7", "37,44.7", "42,44.7", "47,55.2", "52,null"
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select avg(hardware) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 55), 5ms) FILL(double[previousUntilLast])");
      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(avg("root.ln.wf01.wt01.hardware"));
          assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void previousUntilLastCountTest() {
    // TODO: fix count error
    String[] retArray1 =
        new String[] {"17,0", "22,2", "27,2", "32,2", "37,2", "42,2", "47,2", "52,2"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(hardware) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 55), 5ms) FILL(int32[previousUntilLast])");
      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.ln.wf01.wt01.hardware"));
          assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void previousUntilLastMaxTimeTest() {
    String[] retArray1 =
        new String[] {"17,null", "22,25", "27,25", "32,36", "37,36", "42,36", "47,50", "52,null"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select max_time(hardware) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 55), 5ms) FILL(double[previousUntilLast])");
      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(max_time("root.ln.wf01.wt01.hardware"));
          assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void previousUntilLastMaxValueTest() {
    String[] retArray1 =
        new String[] {"17,null", "22,28", "27,28", "32,29", "37,29", "42,29", "47,30", "52,null"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select max_value(temperature) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 55), 5ms) FILL(int32[previousUntilLast])");
      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(max_value("root.ln.wf01.wt01.temperature"));
          assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void previousUntilLastMinTimeTest() {
    String[] retArray1 =
        new String[] {"17,null", "22,23", "27,23", "32,33", "37,33", "42,33", "47,48", "52,null"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select min_time(hardware) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 55), 5ms) FILL(double[previousUntilLast])");
      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(min_time("root.ln.wf01.wt01.hardware"));
          assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void previousUntilLastMinValueTest() {
    String[] retArray1 =
        new String[] {"17,null", "22,23", "27,23", "32,24", "37,24", "42,24", "47,28", "52,null"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select min_value(temperature) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 55), 5ms) FILL(int32[previousUntilLast])");
      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(min_value("root.ln.wf01.wt01.temperature"));
          assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void previousUntilLastSumTest() {
    // TODO: fix sum error
    String[] retArray1 =
        new String[] {
          "17,0.0", "22,66.6", "27,66.6", "32,89.4", "37,89.4", "42,89.4", "47,110.4", "52,110.4"
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select sum(hardware) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 55), 5ms) FILL(double[previousUntilLast])");
      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(sum("root.ln.wf01.wt01.hardware"));
          assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        assertEquals(retArray1.length, cnt);
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
              "select last_value(temperature) from "
                  + "root.ln.wf01.wt01 "
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

  @Test
  public void valueTest() {
    // TODO: add value logic
    String[] retArray1 =
        new String[] {"17,2.0", "22,2.0", "27,33.2", "32,44.7", "37,44.4", "42,2.0", "47,2.0"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select avg(hardware) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 48), 5ms) FILL(float [2.0])");
      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.ln.wf01.wt01.hardware"));
          assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    //    String[] retArray2 = new String[] {
    //            "47,0", "42,0", "37,2", "32,1", "27,1", "22,0", "17,0"
    //    };
    //    try (Connection connection =
    //                 DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
    //         Statement statement = connection.createStatement()) {
    //      boolean hasResultSet = statement.execute(
    //              "select count(temperature) from "
    //                      + "root.ln.wf01.wt01 "
    //                      + "GROUP BY ([17, 48), 5ms) FILL(int32[previous]) order by time desc");
    //      assertTrue(hasResultSet);
    //      int cnt;
    //      try (ResultSet resultSet = statement.getResultSet()) {
    //        cnt = 0;
    //        while (resultSet.next()) {
    //          String ans =
    //                  resultSet.getString(TIMESTAMP_STR)
    //                          + ","
    //                          + resultSet.getString(count("root.ln.wf01.wt01.temperature"));
    //          assertEquals(retArray2[cnt], ans);
    //          cnt++;
    //        }
    //        assertEquals(retArray2.length, cnt);
    //      }
    //    } catch (Exception e) {
    //      e.printStackTrace();
    //      fail(e.getMessage());
    //    }
  }

  @Test
  public void linearLastValueTest() {
    String[] retArray =
        new String[] {"17,null", "22,23", "27,23", "32,24", "37,26", "42,28", "47,30", "52,null"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(temperature) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 55), 5ms) FILL(int32[linear])");

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

      //      hasResultSet =
      //          statement.execute(
      //              "select last_value(temperature) from "
      //                  + "root.ln.wf01.wt01 "
      //                  + "GROUP BY ([17, 48), 5ms) FILL(int32[previous]) order by time desc");
      //
      //      assertTrue(hasResultSet);
      //      try (ResultSet resultSet = statement.getResultSet()) {
      //        cnt = 0;
      //        while (resultSet.next()) {
      //          String ans =
      //              resultSet.getString(TIMESTAMP_STR)
      //                  + ","
      //                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"));
      //          assertEquals(retArray[retArray.length - cnt - 1], ans);
      //          cnt++;
      //        }
      //        assertEquals(retArray.length, cnt);
      //      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void linearFirstValueTest() {
    String[] retArray =
        new String[] {
          "17,null",
          "22,34.9",
          "27,39.75",
          "32,44.6",
          "37,47.93333333333334",
          "42,51.266666666666666",
          "47,54.6",
          "52,null"
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select first_value(hardware) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 55), 5ms) FILL(double[linear])");

      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(first_value("root.ln.wf01.wt01.hardware"));
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
  public void linearAvgTest() {
    String[] retArray1 =
        new String[] {
          "17,null", "22,33.3", "27,39.0", "32,44.7", "37,48.2", "42,51.7", "47,55.2", "52,null"
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select avg(hardware) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 55), 5ms) FILL(double[linear])");
      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(avg("root.ln.wf01.wt01.hardware"));
          assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void linearCountTest() {
    // TODO: fix count error
    String[] retArray1 =
        new String[] {"17,0", "22,2", "27,?", "32,2", "37,2", "42,?", "47,?", "52,2"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(status) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 55), 5ms) FILL(int32[linear])");
      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.ln.wf01.wt01.status"));
          assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void linearMaxTimeTest() {
    String[] retArray1 =
        new String[] {"17,null", "22,25", "27,30", "32,36", "37,40", "42,45", "47,50", "52,null"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select max_time(hardware) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 55), 5ms) FILL(double[linear])");
      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(max_time("root.ln.wf01.wt01.hardware"));
          assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void linearMaxValueTest() {
    String[] retArray1 =
        new String[] {"17,null", "22,28", "27,28", "32,29", "37,29", "42,29", "47,30", "52,null"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select max_value(temperature) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 55), 5ms) FILL(int32[linear])");
      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(max_value("root.ln.wf01.wt01.temperature"));
          assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void linearMinTimeTest() {
    String[] retArray1 =
        new String[] {"17,null", "22,23", "27,28", "32,33", "37,38", "42,43", "47,48", "52,null"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select min_time(hardware) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 55), 5ms) FILL(double[linear])");
      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(min_time("root.ln.wf01.wt01.hardware"));
          assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void linearMinValueTest() {
    String[] retArray1 =
        new String[] {"17,null", "22,23", "27,23", "32,24", "37,25", "42,26", "47,28", "52,null"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select min_value(temperature) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 55), 5ms) FILL(int32[linear])");
      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(min_value("root.ln.wf01.wt01.temperature"));
          assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void linearSumTest() {
    // TODO: fix sum error
    String[] retArray1 =
        new String[] {
          "17,0.0", "22,66.6", "27,?", "32,89.4", "37,?", "42,?", "47,110.4", "52,null"
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select sum(hardware) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 55), 5ms) FILL(double[linear])");
      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(sum("root.ln.wf01.wt01.hardware"));
          assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test(expected = IoTDBSQLException.class)
  public void linearFailTest1() throws SQLException {
    try (Connection connection =
                 DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {
      statement.execute(
              "select last_value(status) from "
                      + "root.ln.wf01.wt01 "
                      + "GROUP BY ([17, 55), 5ms) FILL(boolean[linear])");
    }
  }

  @Test(expected = IoTDBSQLException.class)
  public void linearFailTest2() throws SQLException {
    try (Connection connection =
                 DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {
      statement.execute(
              "select first_value(text) from "
                      + "root.ln.wf01.wt01 "
                      + "GROUP BY ([17, 55), 5ms) FILL(text[linear])");
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
