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

import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class IoTDBAsIT {

  private static String[] sqls =
      new String[] {
        "SET STORAGE GROUP TO root.sg",
        "CREATE TIMESERIES root.sg.d1.s1 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d1.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d2.s1 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d2.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d2.s3 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "INSERT INTO root.sg.d1(timestamp,s1,s2) values(100, 10.1, 20.7)",
        "INSERT INTO root.sg.d1(timestamp,s1,s2) values(200, 15.2, 22.9)",
        "INSERT INTO root.sg.d1(timestamp,s1,s2) values(300, 30.3, 25.1)",
        "INSERT INTO root.sg.d1(timestamp,s1,s2) values(400, 50.4, 28.3)",
        "INSERT INTO root.sg.d2(timestamp,s1,s2,s3) values(100, 11.1, 20.2, 80.0)",
        "INSERT INTO root.sg.d2(timestamp,s1,s2,s3) values(200, 20.2, 21.8, 81.0)",
        "INSERT INTO root.sg.d2(timestamp,s1,s2,s3) values(300, 45.3, 23.4, 82.0)",
        "INSERT INTO root.sg.d2(timestamp,s1,s2,s3) values(400, 73.4, 26.3, 83.0)"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();

    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  private static void insertData() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void selectWithAsTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {"100,10.1,20.7,", "200,15.2,22.9,", "300,30.3,25.1,", "400,50.4,28.3,"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute("select s1 as speed, s2 as temperature from root.sg.d1");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("Time,speed,temperature,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /** Test some time series use alias while others stay unchanged. */
  @Test
  public void selectWithAsMixedTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {"100,10.1,20.7,", "200,15.2,22.9,", "300,30.3,25.1,", "400,50.4,28.3,"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute("select s1 as speed, s2 from root.sg.d1");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("Time,speed,root.sg.d1.s2,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
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
   * When one alias is used but wildcard is corresponding to multi time series, it should throw one
   * exception.
   */
  @Test
  public void selectWithAsFailTest() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      // root.sg.*.s1 matches root.sg.d1.s1 and root.sg.d2.s1 both
      statement.execute("select s1 as speed from root.sg.*");
      fail();
    } catch (Exception e) {
      Assert.assertTrue(
          e.getMessage().contains("alias 'speed' can only be matched with one time series"));
    }
  }

  /** When wild is exactly corresponding to one time series, the result will be correct. */
  @Test
  public void selectWithAsSingleTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"100,80.0,", "200,81.0,", "300,82.0,", "400,83.0,"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      // root.sg.*.s3 matches root.sg.d2.s3 exactly
      boolean hasResultSet = statement.execute("select s3 as power from root.sg.*");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("Time,power,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
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
  public void aggregationWithAsTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "4,28.3,",
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute("select count(s1) as s1_num, max_value(s2) as s2_max from root.sg.d1");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("s1_num,s2_max,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
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
  public void aggregationWithAsFailTest() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      // root.sg.*.s1 matches root.sg.d1.s1 and root.sg.d2.s1 both
      statement.execute("select count(s1) as s1_num from root.sg.*");
      fail();
    } catch (Exception e) {
      Assert.assertTrue(
          e.getMessage().contains("alias 's1_num' can only be matched with one time series"));
    }
  }

  @Test
  public void groupByWithAsTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "100,1,", "180,1,", "260,1,", "340,1,", "420,0,",
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(s1) as s1_num from root.sg.d1 group by ([100,500), 80ms)");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("Time,s1_num,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
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
  public void alignByDeviceWithAsTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "100,root.sg.d1,10.1,20.7,",
          "200,root.sg.d1,15.2,22.9,",
          "300,root.sg.d1,30.3,25.1,",
          "400,root.sg.d1,50.4,28.3,"
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select s1 as speed, s2 as temperature from root.sg.d1 align by device");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("Time,Device,speed,temperature,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
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
  public void alignByDeviceWithAsMixedTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "100,root.sg.d1,10.1,20.7,",
          "200,root.sg.d1,15.2,22.9,",
          "300,root.sg.d1,30.3,25.1,",
          "400,root.sg.d1,50.4,28.3,",
          "100,root.sg.d2,11.1,20.2,",
          "200,root.sg.d2,20.2,21.8,",
          "300,root.sg.d2,45.3,23.4,",
          "400,root.sg.d2,73.4,26.3,"
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute("select s1 as speed, s2 from root.sg.* align by device");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("Time,Device,speed,s2,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
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
  public void alignByDeviceWithAsFailTest() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      // root.sg.*.s1 matches root.sg.d1.s1 and root.sg.d2.s1 both
      statement.execute("select * as speed from root.sg.d1 align by device");
      fail();
    } catch (Exception e) {
      Assert.assertTrue(
          e.getMessage().contains("alias speed can only be matched with one time series"));
    }
  }

  @Test
  public void alignByDeviceWithAsDuplicatedTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "100,root.sg.d1,10.1,10.1,",
          "200,root.sg.d1,15.2,15.2,",
          "300,root.sg.d1,30.3,30.3,",
          "400,root.sg.d1,50.4,50.4,"
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute("select s1 as speed, s1 from root.sg.d1 align by device");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("Time,Device,speed,speed,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
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
  public void alignByDeviceWithAsAggregationTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "root.sg.d2,4,4,4,",
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(s1) as s1_num, count(s2), count(s3) as s3_num from root.sg.d2 align by device");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("Device,s1_num,count(s2),s3_num,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
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
  public void lastWithAsTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"400,speed,50.4,FLOAT,", "400,root.sg.d1.s2,28.3,FLOAT,"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute("select last s1 as speed, s2 from root.sg.d1");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("Time,timeseries,value,dataType,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
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
  public void lastWithAsDuplicatedTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "400,speed,50.4,FLOAT,", "400,root.sg.d1.s1,50.4,FLOAT,", "400,temperature,28.3,FLOAT,"
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute("select last s1 as speed, s1, s2 as temperature from root.sg.d1");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("Time,timeseries,value,dataType,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
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
  public void lastWithAsFailTest() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      // root.sg.*.s1 matches root.sg.d1.s1 and root.sg.d2.s1 both
      statement.execute("select last s1 as speed from root.sg.*");
      fail();
    } catch (Exception e) {
      Assert.assertTrue(
          e.getMessage().contains("alias 'speed' can only be matched with one time series"));
    }
  }
}
