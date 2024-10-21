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

package org.apache.iotdb.relational.it.query.old.alignbydevice;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.RpcUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.db.it.utils.TestUtils.DEFAULT_ZONE_ID;
import static org.apache.iotdb.db.it.utils.TestUtils.TIME_PRECISION_IN_MS;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.apache.iotdb.rpc.RpcUtils.DEFAULT_TIME_FORMAT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBOrderByWithAlignByDeviceTableIT {
  protected static final String DATABASE_NAME = "db";
  protected static final String[] places =
      new String[] {
        "London",
        "Edinburgh",
        "Belfast",
        "Birmingham",
        "Liverpool",
        "Derby",
        "Durham",
        "Hereford",
        "Manchester",
        "Oxford"
      };
  protected static final long startPrecipitation = 200;
  protected static final double startTemperature = 20.0;
  protected static final long startTime = 1668960000000L;
  protected static final int numOfPointsInDevice = 20;
  protected static final long timeGap = 100L;
  protected static final Map<String, Long> deviceToStartTimestamp = new HashMap<>();
  public static final Map<String, double[]> deviceToMaxTemperature = new HashMap<>();
  public static final Map<String, double[]> deviceToAvgTemperature = new HashMap<>();
  public static final Map<String, long[]> deviceToMaxPrecipitation = new HashMap<>();
  public static final Map<String, double[]> deviceToAvgPrecipitation = new HashMap<>();

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
    insertData2();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  /**
   * This method generate test data with crossing time.
   *
   * <p>The data can be viewed in online doc:
   *
   * <p>https://docs.google.com/spreadsheets/d/18XlOIi27ZIIdRnar2WNXVMxkZwjgwlPZmzJLVpZRpAA/edit#gid=0
   */
  protected static void insertData() {
    try (Connection iotDBConnection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = iotDBConnection.createStatement()) {
      statement.execute("create database " + DATABASE_NAME);
      statement.execute("use " + DATABASE_NAME);

      statement.execute(
          "create table weather(city STRING ID, precipitation INT64 MEASUREMENT, temperature DOUBLE MEASUREMENT)");

      // insert data
      long start = startTime;
      double[][] temperatures = new double[places.length][29];
      long[][] precipitations = new long[places.length][29];
      for (int index = 0; index < places.length; index++) {
        String place = places[index];

        for (int i = 0; i < numOfPointsInDevice; i++) {
          long precipitation = startPrecipitation + place.hashCode() + (start + i * timeGap);
          double temperature = startTemperature + place.hashCode() + (start + i * timeGap);
          precipitations[index][(int) ((start - startTime) / timeGap) + i] = precipitation;
          temperatures[index][(int) ((start - startTime) / timeGap) + i] = temperature;
          String insertUniqueTime =
              "INSERT INTO weather"
                  + "(Time,city,precipitation,temperature) VALUES("
                  + (start + i * timeGap)
                  + ","
                  + String.format("'%s'", place)
                  + ","
                  + precipitation
                  + ","
                  + temperature
                  + ")";
          statement.addBatch(insertUniqueTime);
          if (i == 0) {
            deviceToStartTimestamp.put(place, start);
          }
        }
        statement.executeBatch();
        statement.clearBatch();
        start += timeGap;
      }

      for (int i = 0; i < places.length; i++) {
        double[] aT = new double[3];
        double[] aP = new double[3];
        double[] mT = new double[3];
        long[] mP = new long[3];
        double totalTemperature = 0;
        long totalPrecipitation = 0;
        double maxTemperature = -1;
        long maxPrecipitation = -1;
        int cnt = 0;
        for (int j = 0; j < precipitations[i].length; j++) {
          totalTemperature += temperatures[i][j];
          totalPrecipitation += precipitations[i][j];
          maxPrecipitation = Math.max(maxPrecipitation, precipitations[i][j]);
          maxTemperature = Math.max(maxTemperature, temperatures[i][j]);
          if ((j + 1) % 10 == 0 || j == precipitations[i].length - 1) {
            aT[cnt] = totalTemperature / 10;
            aP[cnt] = (double) totalPrecipitation / 10;
            mP[cnt] = maxPrecipitation;
            mT[cnt] = maxTemperature;
            cnt++;
            totalTemperature = 0;
            totalPrecipitation = 0;
            maxTemperature = -1;
            maxPrecipitation = -1;
          }
        }
        deviceToMaxTemperature.put(places[i], mT);
        deviceToMaxPrecipitation.put(places[i], mP);
        deviceToAvgTemperature.put(places[i], aT);
        deviceToAvgPrecipitation.put(places[i], aP);
      }

      for (String sql : optimizedSQL) {
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  // use to test if the compare result of time will overflow.
  protected static void insertData2() {
    try (Connection iotDBConnection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = iotDBConnection.createStatement()) {
      statement.execute("use " + DATABASE_NAME);
      statement.execute("create table overflow(device_id STRING ID, value INT32 MEASUREMENT)");
      long startTime = 1;
      for (int i = 0; i < 20; i++) {
        String insertTime =
            "INSERT INTO "
                + "overflow"
                + "(Time,device_id,value) VALUES("
                + (startTime + 2147483648L)
                + ","
                + "'virtual_device'"
                + ","
                + i
                + ")";
        statement.addBatch(insertTime);
        startTime += 2147483648L;
      }
      statement.executeBatch();
      statement.clearBatch();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void overFlowTest() {

    String[] expectedHeader = new String[] {"time", "value"};
    String[] retArray = new String[20];

    long startTime = 1;
    for (int i = 0; i < 20; i++) {
      long time = startTime + 2147483648L;
      retArray[i] =
          String.format(
              "%s,%d,",
              RpcUtils.formatDatetime(
                  DEFAULT_TIME_FORMAT, TIME_PRECISION_IN_MS, time, DEFAULT_ZONE_ID),
              i);
      startTime += 2147483648L;
    }

    tableResultSetEqualTest(
        "SELECT time, value FROM overflow", expectedHeader, retArray, DATABASE_NAME);
  }

  // ORDER BY DEVICE
  @Test
  public void orderByDeviceTest1() {

    String[] expectedHeader = new String[] {"time", "city", "precipitation", "temperature"};

    String[] expectedDevice = Arrays.stream(places.clone()).sorted().toArray(String[]::new);

    int index = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM weather order by city")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          assertEquals(expectedHeader[i - 1], resultSetMetaData.getColumnName(i));
        }
        assertEquals(expectedHeader.length, resultSetMetaData.getColumnCount());

        int cnt = 0;
        while (resultSet.next()) {
          long actualTimeStamp = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertEquals(expectedDevice[index], actualDevice);
          assertEquals(deviceToStartTimestamp.get(actualDevice) + cnt * timeGap, actualTimeStamp);

          long actualPrecipitation = resultSet.getLong(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(
              startPrecipitation + actualDevice.hashCode() + actualTimeStamp, actualPrecipitation);
          assertTrue(
              startTemperature + actualDevice.hashCode() + actualTimeStamp - actualTemperature
                  < 0.00001);

          cnt++;
          if (cnt % numOfPointsInDevice == 0) {
            index++;
            cnt = 0;
          }
        }
        assertEquals(10, index);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void orderByDeviceTest2() {

    String[] expectedHeader = new String[] {"time", "city", "precipitation", "temperature"};

    String[] expectedDevice = Arrays.stream(places.clone()).sorted().toArray(String[]::new);

    int index = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);

      try (ResultSet resultSet =
          statement.executeQuery("SELECT * FROM weather order by city asc")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          assertEquals(expectedHeader[i - 1], resultSetMetaData.getColumnName(i));
        }
        assertEquals(expectedHeader.length, resultSetMetaData.getColumnCount());

        int cnt = 0;
        while (resultSet.next()) {
          long actualTimeStamp = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertEquals(expectedDevice[index], actualDevice);
          assertEquals(deviceToStartTimestamp.get(actualDevice) + cnt * timeGap, actualTimeStamp);

          long actualPrecipitation = resultSet.getLong(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(
              startPrecipitation + actualDevice.hashCode() + actualTimeStamp, actualPrecipitation);
          assertTrue(
              startTemperature + actualDevice.hashCode() + actualTimeStamp - actualTemperature
                  < 0.00001);

          cnt++;
          if (cnt % numOfPointsInDevice == 0) {
            index++;
            cnt = 0;
          }
        }
        assertEquals(10, index);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void orderByDeviceTest3() {

    String[] expectedHeader = new String[] {"time", "city", "precipitation", "temperature"};

    String[] expectedDevice =
        Arrays.stream(places.clone()).sorted(Comparator.reverseOrder()).toArray(String[]::new);

    int index = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);

      try (ResultSet resultSet =
          statement.executeQuery("SELECT * FROM weather order by city desc")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          assertEquals(expectedHeader[i - 1], resultSetMetaData.getColumnName(i));
        }
        assertEquals(expectedHeader.length, resultSetMetaData.getColumnCount());

        int cnt = 0;
        while (resultSet.next()) {
          long actualTimeStamp = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertEquals(expectedDevice[index], actualDevice);
          assertEquals(deviceToStartTimestamp.get(actualDevice) + cnt * timeGap, actualTimeStamp);

          long actualPrecipitation = resultSet.getLong(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(
              startPrecipitation + actualDevice.hashCode() + actualTimeStamp, actualPrecipitation);
          assertTrue(
              startTemperature + actualDevice.hashCode() + actualTimeStamp - actualTemperature
                  < 0.00001);

          cnt++;
          if (cnt % numOfPointsInDevice == 0) {
            index++;
            cnt = 0;
          }
        }
        assertEquals(10, index);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // ORDER BY TIME

  @Test
  public void orderByTimeTest1() {
    String[] expectedHeader = new String[] {"time", "city", "precipitation", "temperature"};

    orderByTimeTest1(
        "SELECT * FROM weather ORDER BY time, city",
        numOfPointsInDevice * places.length,
        expectedHeader);

    orderByTimeTest1("SELECT * FROM weather ORDER BY time, city LIMIT 100", 100, expectedHeader);
  }

  @Test
  public void orderByTimeTest2() {
    String[] expectedHeader = new String[] {"time", "city", "precipitation", "temperature"};
    orderByTimeTest1(
        "SELECT * FROM weather ORDER BY TIME ASC, city",
        numOfPointsInDevice * places.length,
        expectedHeader);

    orderByTimeTest1(
        "SELECT * FROM weather ORDER BY time ASC, city ASC LIMIT 100", 100, expectedHeader);
  }

  @Test
  public void orderByTimeTest3() {
    String[] expectedHeader = new String[] {"time", "city", "precipitation", "temperature"};
    orderByTimeTest3(
        "SELECT * FROM weather ORDER BY time DESC, city ASC",
        numOfPointsInDevice * places.length,
        expectedHeader);

    orderByTimeTest3(
        "SELECT * FROM weather ORDER BY time DESC, city LIMIT 100", 100, expectedHeader);
  }

  @Test
  public void orderByTimeExpressionTest1() {
    String[] expectedHeader = new String[] {"time", "city", "precipitation", "temperature"};

    orderByTimeExpressionTest1(
        "SELECT * FROM weather ORDER BY time DESC, precipitation DESC, city",
        numOfPointsInDevice * places.length,
        expectedHeader);

    orderByTimeExpressionTest1(
        "SELECT * FROM weather ORDER BY time DESC, precipitation DESC, city asc LIMIT 100",
        100,
        expectedHeader);
  }

  @Test
  public void orderExpressionTest1() {
    String[] expectedHeader = new String[] {"time", "city", "precipitation", "temperature"};

    orderByExpressionTest1(
        "SELECT * FROM weather ORDER BY precipitation DESC, time DESC, city asc",
        numOfPointsInDevice * places.length,
        expectedHeader);

    orderByExpressionTest1(
        "SELECT * FROM weather ORDER BY precipitation DESC, time DESC, city limit 100",
        100,
        expectedHeader);
  }

  @Test
  public void orderByDeviceTimeTest1() {
    String[] expectedHeader = new String[] {"time", "city", "precipitation", "temperature"};

    orderByDeviceTimeTest1("SELECT * FROM weather ORDER BY city ASC,time DESC", 10, expectedHeader);

    orderByDeviceTimeTest1(
        "SELECT * FROM weather ORDER BY city ASC,time DESC LIMIT 100", 5, expectedHeader);
  }

  @Test
  public void orderByDeviceTimeTest2() {
    String[] expectedHeader = new String[] {"time", "city", "precipitation", "temperature"};

    orderByDeviceTimeTest2("SELECT * FROM weather ORDER BY city ASC,time ASC", 10, expectedHeader);

    orderByDeviceTimeTest2(
        "SELECT * FROM weather ORDER BY city ASC,Time ASC LIMIT 100", 5, expectedHeader);
  }

  @Test
  public void orderByDeviceTimeTest3() {
    String[] expectedHeader = new String[] {"time", "city", "precipitation", "temperature"};

    orderByDeviceTimeTest3(
        "SELECT * FROM weather ORDER BY city DESC,time DESC", 10, expectedHeader);

    orderByDeviceTimeTest3(
        "SELECT * FROM weather ORDER BY city DESC,time DESC LIMIT 100", 5, expectedHeader);
  }

  @Test
  public void orderByDeviceTimeTest4() {
    String[] expectedHeader = new String[] {"time", "city", "precipitation", "temperature"};

    orderByDeviceTimeTest4("SELECT * FROM weather ORDER BY city DESC,time ASC", 10, expectedHeader);

    orderByDeviceTimeTest4(
        "SELECT * FROM weather ORDER BY city DESC,time ASC LIMIT 100", 5, expectedHeader);
  }

  @Test
  public void orderByTimeDeviceTest1() {
    String[] expectedHeader = new String[] {"time", "city", "precipitation", "temperature"};

    orderByTimeDeviceTest1(
        "SELECT * FROM weather ORDER BY time ASC,city DESC",
        numOfPointsInDevice * places.length,
        expectedHeader);

    orderByTimeDeviceTest1(
        "SELECT * FROM weather ORDER BY time ASC,city DESC LIMIT 100", 100, expectedHeader);
  }

  @Test
  public void orderByTimeDeviceTest2() {
    String[] expectedHeader = new String[] {"time", "city", "precipitation", "temperature"};

    orderByTimeDeviceTest2(
        "SELECT * FROM weather ORDER BY time ASC,city ASC",
        numOfPointsInDevice * places.length,
        expectedHeader);

    orderByTimeDeviceTest2(
        "SELECT * FROM weather ORDER BY time ASC,city ASC LIMIT 100", 100, expectedHeader);
  }

  @Test
  public void orderByTimeDeviceTest3() {
    String[] expectedHeader = new String[] {"time", "city", "precipitation", "temperature"};

    orderByTimeDeviceTest3(
        "SELECT * FROM weather ORDER BY time DESC,city DESC",
        numOfPointsInDevice * places.length,
        expectedHeader);

    orderByTimeDeviceTest3(
        "SELECT * FROM weather ORDER BY time DESC,city DESC LIMIT 100", 100, expectedHeader);
  }

  @Test
  public void orderByTimeDeviceTest4() {
    String[] expectedHeader = new String[] {"time", "city", "precipitation", "temperature"};

    orderByTimeDeviceTest4(
        "SELECT * FROM weather ORDER BY time DESC,city ASC",
        numOfPointsInDevice * places.length,
        expectedHeader);

    orderByTimeDeviceTest4(
        "SELECT * FROM weather ORDER BY time DESC,city ASC LIMIT 100", 100, expectedHeader);
  }

  public static void orderByTimeTest1(String sql, int count, String[] expectedHeader) {
    int total = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use " + DATABASE_NAME);
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          assertEquals(expectedHeader[i - 1], resultSetMetaData.getColumnName(i));
        }
        assertEquals(expectedHeader.length, resultSetMetaData.getColumnCount());

        long lastTimeStamp = -1;
        String lastDevice = "";
        while (resultSet.next()) {
          long actualTimeStamp = resultSet.getLong(1);
          assertTrue(actualTimeStamp >= lastTimeStamp);
          String actualDevice = resultSet.getString(2);
          if (!lastDevice.isEmpty() && actualTimeStamp == lastTimeStamp) {
            assertTrue(actualDevice.compareTo(lastDevice) >= 0);
          }
          lastDevice = actualDevice;
          lastTimeStamp = actualTimeStamp;
          long actualPrecipitation = resultSet.getLong(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(
              startPrecipitation + actualDevice.hashCode() + actualTimeStamp, actualPrecipitation);
          assertTrue(
              startTemperature + actualDevice.hashCode() + actualTimeStamp - actualTemperature
                  < 0.00001);
          total++;
        }
        assertEquals(count, total);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void orderByTimeTest3(String sql, int count, String[] expectedHeader) {

    int total = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use " + DATABASE_NAME);
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          assertEquals(expectedHeader[i - 1], resultSetMetaData.getColumnName(i));
        }
        assertEquals(expectedHeader.length, resultSetMetaData.getColumnCount());

        long lastTimeStamp = Long.MAX_VALUE;
        String lastDevice = "";
        while (resultSet.next()) {
          long actualTimeStamp = resultSet.getLong(1);
          assertTrue(actualTimeStamp <= lastTimeStamp);
          String actualDevice = resultSet.getString(2);
          if (!lastDevice.equals("") && actualTimeStamp == lastTimeStamp) {
            assertTrue(actualDevice.compareTo(lastDevice) >= 0);
          }
          lastDevice = actualDevice;
          lastTimeStamp = actualTimeStamp;
          long actualPrecipitation = resultSet.getLong(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(
              startPrecipitation + actualDevice.hashCode() + actualTimeStamp, actualPrecipitation);
          assertTrue(
              startTemperature + actualDevice.hashCode() + actualTimeStamp - actualTemperature
                  < 0.00001);
          total++;
        }
        assertEquals(count, total);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void orderByTimeExpressionTest1(String sql, int count, String[] expectedHeader) {
    int total = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use " + DATABASE_NAME);
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          assertEquals(expectedHeader[i - 1], resultSetMetaData.getColumnName(i));
        }
        assertEquals(expectedHeader.length, resultSetMetaData.getColumnCount());

        long lastTimeStamp = Long.MAX_VALUE;
        long lastPrecipitation = Long.MAX_VALUE;
        String lastDevice = "";
        while (resultSet.next()) {
          long actualTimeStamp = resultSet.getLong(1);
          assertTrue(actualTimeStamp <= lastTimeStamp);
          String actualDevice = resultSet.getString(2);

          long actualPrecipitation = resultSet.getLong(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(
              startPrecipitation + actualDevice.hashCode() + actualTimeStamp, actualPrecipitation);
          assertTrue(
              startTemperature + actualDevice.hashCode() + actualTimeStamp - actualTemperature
                  < 0.00001);
          if (actualDevice.equals(lastDevice) && actualTimeStamp == lastTimeStamp) {
            assertTrue(actualPrecipitation <= lastPrecipitation);
            lastPrecipitation = actualPrecipitation;
          }
          lastDevice = actualDevice;
          lastTimeStamp = actualTimeStamp;
          total++;
        }
        assertEquals(count, total);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void orderByExpressionTest1(String sql, int count, String[] expectedHeader) {

    int total = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use " + DATABASE_NAME);
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          assertEquals(expectedHeader[i - 1], resultSetMetaData.getColumnName(i));
        }
        assertEquals(expectedHeader.length, resultSetMetaData.getColumnCount());

        long lastTimeStamp = Long.MAX_VALUE;
        long lastPrecipitation = Long.MAX_VALUE;
        while (resultSet.next()) {
          long actualTimeStamp = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          long actualPrecipitation = resultSet.getLong(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(
              startPrecipitation + actualDevice.hashCode() + actualTimeStamp, actualPrecipitation);
          assertTrue(
              startTemperature + actualDevice.hashCode() + actualTimeStamp - actualTemperature
                  < 0.00001);
          assertTrue(actualPrecipitation <= lastPrecipitation);
          if (actualPrecipitation == lastPrecipitation) {
            assertTrue(actualTimeStamp <= lastTimeStamp);
          }
          lastPrecipitation = actualPrecipitation;
          lastTimeStamp = actualTimeStamp;
          total++;
        }
        assertEquals(count, total);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // ORDER BY DEVICE,TIME
  public static void orderByDeviceTimeTest1(String sql, int count, String[] expectedHeader) {
    String[] expectedDevice = Arrays.stream(places.clone()).sorted().toArray(String[]::new);
    int index = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use " + DATABASE_NAME);
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          assertEquals(expectedHeader[i - 1], resultSetMetaData.getColumnName(i));
        }
        assertEquals(expectedHeader.length, resultSetMetaData.getColumnCount());

        int cnt = 0;
        while (resultSet.next()) {
          long actualTimeStamp = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertEquals(expectedDevice[index], actualDevice);
          assertEquals(
              deviceToStartTimestamp.get(actualDevice) + timeGap * (numOfPointsInDevice - cnt - 1),
              actualTimeStamp);

          long actualPrecipitation = resultSet.getLong(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(
              startPrecipitation + actualDevice.hashCode() + actualTimeStamp, actualPrecipitation);
          assertTrue(
              (startTemperature + actualDevice.hashCode() + actualTimeStamp) - actualTemperature
                  < 0.00001);

          cnt++;
          if (cnt % numOfPointsInDevice == 0) {
            index++;
            cnt = 0;
          }
        }
        assertEquals(count, index);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void orderByDeviceTimeTest2(String sql, int count, String[] expectedHeader) {
    String[] expectedDevice = Arrays.stream(places.clone()).sorted().toArray(String[]::new);
    int index = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use " + DATABASE_NAME);
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          assertEquals(expectedHeader[i - 1], resultSetMetaData.getColumnName(i));
        }
        assertEquals(expectedHeader.length, resultSetMetaData.getColumnCount());

        int cnt = 0;
        while (resultSet.next()) {
          long actualTimeStamp = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertEquals(expectedDevice[index], actualDevice);
          assertEquals(deviceToStartTimestamp.get(actualDevice) + cnt * timeGap, actualTimeStamp);

          long actualPrecipitation = resultSet.getLong(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(
              startPrecipitation + actualDevice.hashCode() + actualTimeStamp, actualPrecipitation);
          assertTrue(
              (startTemperature + actualDevice.hashCode() + actualTimeStamp) - actualTemperature
                  < 0.00001);

          cnt++;
          if (cnt % numOfPointsInDevice == 0) {
            index++;
            cnt = 0;
          }
        }
        assertEquals(count, index);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void orderByDeviceTimeTest3(String sql, int count, String[] expectedHeader) {
    String[] expectedDevice =
        Arrays.stream(places.clone()).sorted(Comparator.reverseOrder()).toArray(String[]::new);
    int index = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use " + DATABASE_NAME);
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          assertEquals(expectedHeader[i - 1], resultSetMetaData.getColumnName(i));
        }
        assertEquals(expectedHeader.length, resultSetMetaData.getColumnCount());

        int cnt = 0;
        while (resultSet.next()) {
          long actualTimeStamp = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertEquals(expectedDevice[index], actualDevice);
          assertEquals(
              deviceToStartTimestamp.get(actualDevice) + timeGap * (numOfPointsInDevice - cnt - 1),
              actualTimeStamp);

          long actualPrecipitation = resultSet.getLong(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(
              startPrecipitation + actualDevice.hashCode() + actualTimeStamp, actualPrecipitation);
          assertTrue(
              (startTemperature + actualDevice.hashCode() + actualTimeStamp) - actualTemperature
                  < 0.00001);

          cnt++;
          if (cnt % numOfPointsInDevice == 0) {
            index++;
            cnt = 0;
          }
        }
        assertEquals(count, index);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void orderByDeviceTimeTest4(String sql, int count, String[] expectedHeader) {
    String[] expectedDevice =
        Arrays.stream(places.clone()).sorted(Comparator.reverseOrder()).toArray(String[]::new);
    int index = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use " + DATABASE_NAME);
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          assertEquals(expectedHeader[i - 1], resultSetMetaData.getColumnName(i));
        }
        assertEquals(expectedHeader.length, resultSetMetaData.getColumnCount());

        int cnt = 0;
        while (resultSet.next()) {
          long actualTimeStamp = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertEquals(expectedDevice[index], actualDevice);
          assertEquals(deviceToStartTimestamp.get(actualDevice) + cnt * timeGap, actualTimeStamp);

          long actualPrecipitation = resultSet.getLong(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(
              startPrecipitation + actualDevice.hashCode() + actualTimeStamp, actualPrecipitation);
          assertTrue(
              (startTemperature + actualDevice.hashCode() + actualTimeStamp) - actualTemperature
                  < 0.00001);

          cnt++;
          if (cnt % numOfPointsInDevice == 0) {
            index++;
            cnt = 0;
          }
        }
        assertEquals(count, index);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // ORDER BY TIME,DEVICE
  public static void orderByTimeDeviceTest1(String sql, int count, String[] expectedHeader) {
    int total = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use " + DATABASE_NAME);
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          assertEquals(expectedHeader[i - 1], resultSetMetaData.getColumnName(i));
        }
        assertEquals(expectedHeader.length, resultSetMetaData.getColumnCount());

        long lastTimeStamp = -1;
        String lastDevice = "";
        while (resultSet.next()) {
          long actualTimeStamp = resultSet.getLong(1);
          assertTrue(actualTimeStamp >= lastTimeStamp);
          String actualDevice = resultSet.getString(2);
          if (!lastDevice.equals("") && actualTimeStamp == lastTimeStamp) {
            assertTrue(actualDevice.compareTo(lastDevice) <= 0);
          }
          lastDevice = actualDevice;
          lastTimeStamp = actualTimeStamp;
          long actualPrecipitation = resultSet.getLong(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(
              startPrecipitation + actualDevice.hashCode() + actualTimeStamp, actualPrecipitation);
          assertTrue(
              startTemperature + actualDevice.hashCode() + actualTimeStamp - actualTemperature
                  < 0.00001);
          total++;
        }
        assertEquals(count, total);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void orderByTimeDeviceTest2(String sql, int count, String[] expectedHeader) {
    int total = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use " + DATABASE_NAME);
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          assertEquals(expectedHeader[i - 1], resultSetMetaData.getColumnName(i));
        }
        assertEquals(expectedHeader.length, resultSetMetaData.getColumnCount());
        long lastTimeStamp = -1;
        String lastDevice = "";
        while (resultSet.next()) {
          long actualTimeStamp = resultSet.getLong(1);
          assertTrue(actualTimeStamp >= lastTimeStamp);
          String actualDevice = resultSet.getString(2);
          if (!lastDevice.equals("") && actualTimeStamp == lastTimeStamp) {
            assertTrue(actualDevice.compareTo(lastDevice) >= 0);
          }
          lastDevice = actualDevice;
          lastTimeStamp = actualTimeStamp;
          long actualPrecipitation = resultSet.getLong(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(
              startPrecipitation + actualDevice.hashCode() + actualTimeStamp, actualPrecipitation);
          assertTrue(
              startTemperature + actualDevice.hashCode() + actualTimeStamp - actualTemperature
                  < 0.00001);
          total++;
        }
        assertEquals(count, total);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void orderByTimeDeviceTest3(String sql, int count, String[] expectedHeader) {
    int total = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use " + DATABASE_NAME);
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          assertEquals(expectedHeader[i - 1], resultSetMetaData.getColumnName(i));
        }
        assertEquals(expectedHeader.length, resultSetMetaData.getColumnCount());
        long lastTimeStamp = Long.MAX_VALUE;
        String lastDevice = "";
        while (resultSet.next()) {
          long actualTimeStamp = resultSet.getLong(1);
          assertTrue(actualTimeStamp <= lastTimeStamp);
          String actualDevice = resultSet.getString(2);
          if (!lastDevice.equals("") && actualTimeStamp == lastTimeStamp) {
            assertTrue(actualDevice.compareTo(lastDevice) <= 0);
          }
          lastDevice = actualDevice;
          lastTimeStamp = actualTimeStamp;
          long actualPrecipitation = resultSet.getLong(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(
              startPrecipitation + actualDevice.hashCode() + actualTimeStamp, actualPrecipitation);
          assertTrue(
              startTemperature + actualDevice.hashCode() + actualTimeStamp - actualTemperature
                  < 0.00001);
          total++;
        }
        assertEquals(count, total);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void orderByTimeDeviceTest4(String sql, int count, String[] expectedHeader) {
    int total = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use " + DATABASE_NAME);
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          assertEquals(expectedHeader[i - 1], resultSetMetaData.getColumnName(i));
        }
        assertEquals(expectedHeader.length, resultSetMetaData.getColumnCount());
        long lastTimeStamp = Long.MAX_VALUE;
        String lastDevice = "";
        while (resultSet.next()) {
          long actualTimeStamp = resultSet.getLong(1);
          assertTrue(actualTimeStamp <= lastTimeStamp);
          String actualDevice = resultSet.getString(2);
          if (!lastDevice.equals("") && actualTimeStamp == lastTimeStamp) {
            assertTrue(actualDevice.compareTo(lastDevice) >= 0);
          }
          lastDevice = actualDevice;
          lastTimeStamp = actualTimeStamp;
          long actualPrecipitation = resultSet.getLong(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(
              startPrecipitation + actualDevice.hashCode() + actualTimeStamp, actualPrecipitation);
          assertTrue(
              startTemperature + actualDevice.hashCode() + actualTimeStamp - actualTemperature
                  < 0.00001);
          total++;
        }
        assertEquals(count, total);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  //
  //  // aggregation query
  //  private static final int[][] countIn1000MSFiledWith100MSTimeGap =
  //      new int[][] {
  //          {10, 10, 0},
  //          {9, 10, 1},
  //          {8, 10, 2},
  //          {7, 10, 3},
  //          {6, 10, 4},
  //          {5, 10, 5},
  //          {4, 10, 6},
  //          {3, 10, 7},
  //          {2, 10, 8},
  //          {1, 10, 9}
  //      };
  //
  //  private static int getCountNum(String device, int cnt) {
  //    int index = 0;
  //    for (int i = 0; i < places.length; i++) {
  //      if (places[i].equals(device)) {
  //        index = i;
  //        break;
  //      }
  //    }
  //
  //    return countIn1000MSFiledWith100MSTimeGap[index][cnt];
  //  }
  //
  //  @Test
  //  public void groupByTimeOrderByDeviceTest1() {
  //    String sql =
  //        "SELECT AVG(*),COUNT(*),MAX_VALUE(*) FROM weather GROUP
  // BY([2022-11-21T00:00:00.000+08:00,2022-11-21T00:00:02.801+08:00),1000msC";
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //      long lastTime = -1;
  //      String lastDevice = "";
  //      long[] expectedTime = new long[] {startTime, startTime + 1000, startTime + 1000 * 2};
  //      try (ResultSet resultSet = statement.executeQuery(sql)) {
  //        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
  //        checkHeader(
  //            resultSetMetaData,
  //
  // "Time,Device,AVG(precipitation),AVG(temperature),COUNT(precipitation),COUNT(temperature),MAX_VALUE(precipitation),MAX_VALUE(temperature)");
  //        int cnt = 0;
  //        while (resultSet.next()) {
  //
  //          long actualTime = resultSet.getLong(1);
  //          String actualDevice = resultSet.getString(2);
  //          assertEquals(expectedTime[cnt], actualTime);
  //          if (!Objects.equals(lastDevice, "") && Objects.equals(actualDevice, lastDevice)) {
  //            assertTrue(actualTime >= lastTime);
  //          }
  //          if (!Objects.equals(lastDevice, "")) {
  //            assertTrue(actualDevice.compareTo(lastDevice) >= 0);
  //          }
  //          lastTime = actualTime;
  //          lastDevice = actualDevice;
  //
  //          double avgPrecipitation = resultSet.getDouble(3);
  //          assertTrue(deviceToAvgPrecipitation.get(actualDevice)[cnt] - avgPrecipitation <
  // 0.00001);
  //
  //          double avgTemperature = resultSet.getDouble(4);
  //          assertTrue(deviceToAvgTemperature.get(actualDevice)[cnt] - avgTemperature < 0.00001);
  //
  //          int countPrecipitation = resultSet.getInt(5);
  //          assertEquals(getCountNum(actualDevice, cnt), countPrecipitation);
  //          int countTemperature = resultSet.getInt(6);
  //          assertEquals(getCountNum(actualDevice, cnt), countTemperature);
  //
  //          long maxPrecipitation = resultSet.getLong(7);
  //          assertEquals(deviceToMaxPrecipitation.get(actualDevice)[cnt], maxPrecipitation);
  //
  //          double maxTemperature = resultSet.getDouble(8);
  //          assertTrue(deviceToMaxTemperature.get(actualDevice)[cnt] - maxTemperature < 0.00001);
  //          cnt++;
  //          if (cnt % 3 == 0) {
  //            cnt = 0;
  //          }
  //        }
  //      }
  //    } catch (Exception e) {
  //      e.printStackTrace();
  //      fail(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  public void groupByTimeOrderByDeviceTest2() {
  //    String sql =
  //        "SELECT AVG(*),COUNT(*),MAX_VALUE(*) FROM weather GROUP
  // BY([2022-11-21T00:00:00.000+08:00,2022-11-21T00:00:02.801+08:00),1000ms) ORDER BY DEVICE DESC";
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //      long lastTime = -1;
  //      String lastDevice = "";
  //      long[] expectedTime = new long[] {startTime, startTime + 1000, startTime + 1000 * 2};
  //      try (ResultSet resultSet = statement.executeQuery(sql)) {
  //        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
  //        checkHeader(
  //            resultSetMetaData,
  //
  // "Time,Device,AVG(precipitation),AVG(temperature),COUNT(precipitation),COUNT(temperature),MAX_VALUE(precipitation),MAX_VALUE(temperature)");
  //        int cnt = 0;
  //        while (resultSet.next()) {
  //
  //          long actualTime = resultSet.getLong(1);
  //          String actualDevice = resultSet.getString(2);
  //          assertEquals(expectedTime[cnt], actualTime);
  //          if (!Objects.equals(lastDevice, "") && Objects.equals(actualDevice, lastDevice)) {
  //            assertTrue(actualTime >= lastTime);
  //          }
  //          if (!Objects.equals(lastDevice, "")) {
  //            assertTrue(actualDevice.compareTo(lastDevice) <= 0);
  //          }
  //          lastTime = actualTime;
  //          lastDevice = actualDevice;
  //
  //          double avgPrecipitation = resultSet.getDouble(3);
  //          assertTrue(deviceToAvgPrecipitation.get(actualDevice)[cnt] - avgPrecipitation <
  // 0.00001);
  //
  //          double avgTemperature = resultSet.getDouble(4);
  //          assertTrue(deviceToAvgTemperature.get(actualDevice)[cnt] - avgTemperature < 0.00001);
  //
  //          int countPrecipitation = resultSet.getInt(5);
  //          assertEquals(getCountNum(actualDevice, cnt), countPrecipitation);
  //          int countTemperature = resultSet.getInt(6);
  //          assertEquals(getCountNum(actualDevice, cnt), countTemperature);
  //
  //          long maxPrecipitation = resultSet.getLong(7);
  //          assertEquals(deviceToMaxPrecipitation.get(actualDevice)[cnt], maxPrecipitation);
  //
  //          double maxTemperature = resultSet.getDouble(8);
  //          assertTrue(deviceToMaxTemperature.get(actualDevice)[cnt] - maxTemperature < 0.00001);
  //          cnt++;
  //          if (cnt % 3 == 0) {
  //            cnt = 0;
  //          }
  //        }
  //      }
  //    } catch (Exception e) {
  //      e.printStackTrace();
  //      fail(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  public void groupByTimeOrderByDeviceTest3() {
  //    String sql =
  //        "SELECT AVG(*),COUNT(*),MAX_VALUE(*) FROM weather GROUP
  // BY([2022-11-21T00:00:00.000+08:00,2022-11-21T00:00:02.801+08:00),1000ms) ORDER BY DEVICE
  // DESC,TIME DESC";
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //      long lastTime = -1;
  //      String lastDevice = "";
  //      long[] expectedTime = new long[] {startTime, startTime + 1000, startTime + 1000 * 2};
  //      try (ResultSet resultSet = statement.executeQuery(sql)) {
  //        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
  //        checkHeader(
  //            resultSetMetaData,
  //
  // "Time,Device,AVG(precipitation),AVG(temperature),COUNT(precipitation),COUNT(temperature),MAX_VALUE(precipitation),MAX_VALUE(temperature)");
  //        int cnt = 2;
  //        while (resultSet.next()) {
  //          long actualTime = resultSet.getLong(1);
  //          String actualDevice = resultSet.getString(2);
  //          assertEquals(expectedTime[cnt], actualTime);
  //          if (!Objects.equals(lastDevice, "") && Objects.equals(actualDevice, lastDevice)) {
  //            assertTrue(actualTime <= lastTime);
  //          }
  //          if (!Objects.equals(lastDevice, "")) {
  //            assertTrue(actualDevice.compareTo(lastDevice) <= 0);
  //          }
  //          lastTime = actualTime;
  //          lastDevice = actualDevice;
  //
  //          double avgPrecipitation = resultSet.getDouble(3);
  //          assertTrue(deviceToAvgPrecipitation.get(actualDevice)[cnt] - avgPrecipitation <
  // 0.00001);
  //
  //          double avgTemperature = resultSet.getDouble(4);
  //          assertTrue(deviceToAvgTemperature.get(actualDevice)[cnt] - avgTemperature < 0.00001);
  //
  //          int countPrecipitation = resultSet.getInt(5);
  //          assertEquals(getCountNum(actualDevice, cnt), countPrecipitation);
  //          int countTemperature = resultSet.getInt(6);
  //          assertEquals(getCountNum(actualDevice, cnt), countTemperature);
  //
  //          long maxPrecipitation = resultSet.getLong(7);
  //          assertEquals(deviceToMaxPrecipitation.get(actualDevice)[cnt], maxPrecipitation);
  //
  //          double maxTemperature = resultSet.getDouble(8);
  //          assertTrue(deviceToMaxTemperature.get(actualDevice)[cnt] - maxTemperature < 0.00001);
  //          cnt--;
  //          if (cnt < 0) {
  //            cnt = 2;
  //          }
  //        }
  //      }
  //    } catch (Exception e) {
  //      e.printStackTrace();
  //      fail(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  public void groupByTimeOrderByDeviceTest4() {
  //    String sql =
  //        "SELECT AVG(*),COUNT(*),MAX_VALUE(*) FROM weather GROUP
  // BY([2022-11-21T00:00:00.000+08:00,2022-11-21T00:00:02.801+08:00),1000ms) ORDER BY DEVICE
  // ASC,TIME DESC";
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //      long lastTime = -1;
  //      String lastDevice = "";
  //      long[] expectedTime = new long[] {startTime, startTime + 1000, startTime + 1000 * 2};
  //      try (ResultSet resultSet = statement.executeQuery(sql)) {
  //        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
  //        checkHeader(
  //            resultSetMetaData,
  //
  // "Time,Device,AVG(precipitation),AVG(temperature),COUNT(precipitation),COUNT(temperature),MAX_VALUE(precipitation),MAX_VALUE(temperature)");
  //        int cnt = 2;
  //        while (resultSet.next()) {
  //
  //          long actualTime = resultSet.getLong(1);
  //          String actualDevice = resultSet.getString(2);
  //          assertEquals(expectedTime[cnt], actualTime);
  //          if (!Objects.equals(lastDevice, "") && Objects.equals(actualDevice, lastDevice)) {
  //            assertTrue(actualTime <= lastTime);
  //          }
  //          if (!Objects.equals(lastDevice, "")) {
  //            assertTrue(actualDevice.compareTo(lastDevice) >= 0);
  //          }
  //          lastTime = actualTime;
  //          lastDevice = actualDevice;
  //
  //          double avgPrecipitation = resultSet.getDouble(3);
  //          assertTrue(deviceToAvgPrecipitation.get(actualDevice)[cnt] - avgPrecipitation <
  // 0.00001);
  //
  //          double avgTemperature = resultSet.getDouble(4);
  //          assertTrue(deviceToAvgTemperature.get(actualDevice)[cnt] - avgTemperature < 0.00001);
  //
  //          int countPrecipitation = resultSet.getInt(5);
  //          assertEquals(getCountNum(actualDevice, cnt), countPrecipitation);
  //          int countTemperature = resultSet.getInt(6);
  //          assertEquals(getCountNum(actualDevice, cnt), countTemperature);
  //
  //          long maxPrecipitation = resultSet.getLong(7);
  //          assertEquals(deviceToMaxPrecipitation.get(actualDevice)[cnt], maxPrecipitation);
  //
  //          double maxTemperature = resultSet.getDouble(8);
  //          assertTrue(deviceToMaxTemperature.get(actualDevice)[cnt] - maxTemperature < 0.00001);
  //          cnt--;
  //          if (cnt < 0) {
  //            cnt = 2;
  //          }
  //        }
  //      }
  //    } catch (Exception e) {
  //      e.printStackTrace();
  //      fail(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  public void groupByTimeOrderByTimeTest1() {
  //    String sql =
  //        "SELECT AVG(*),COUNT(*),MAX_VALUE(*) FROM weather GROUP
  // BY([2022-11-21T00:00:00.000+08:00,2022-11-21T00:00:02.801+08:00),1000ms) ORDER BY TIME";
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //      long lastTime = -1;
  //      String lastDevice = "";
  //      int index = 0;
  //      try (ResultSet resultSet = statement.executeQuery(sql)) {
  //        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
  //        checkHeader(
  //            resultSetMetaData,
  //
  // "Time,Device,AVG(precipitation),AVG(temperature),COUNT(precipitation),COUNT(temperature),MAX_VALUE(precipitation),MAX_VALUE(temperature)");
  //        int cnt = 0;
  //        while (resultSet.next()) {
  //          long actualTime = resultSet.getLong(1);
  //          String actualDevice = resultSet.getString(2);
  //          assertTrue(actualTime >= lastTime);
  //          if (!Objects.equals(lastDevice, "") && actualTime == lastTime) {
  //            assertTrue(actualDevice.compareTo(lastDevice) >= 0);
  //          }
  //          lastTime = actualTime;
  //          lastDevice = actualDevice;
  //          double avgPrecipitation = resultSet.getDouble(3);
  //          assertTrue(deviceToAvgPrecipitation.get(actualDevice)[cnt] - avgPrecipitation <
  // 0.00001);
  //
  //          double avgTemperature = resultSet.getDouble(4);
  //          assertTrue(deviceToAvgTemperature.get(actualDevice)[cnt] - avgTemperature < 0.00001);
  //
  //          int countPrecipitation = resultSet.getInt(5);
  //          assertEquals(getCountNum(actualDevice, cnt), countPrecipitation);
  //          int countTemperature = resultSet.getInt(6);
  //          assertEquals(getCountNum(actualDevice, cnt), countTemperature);
  //
  //          long maxPrecipitation = resultSet.getLong(7);
  //          assertEquals(deviceToMaxPrecipitation.get(actualDevice)[cnt], maxPrecipitation);
  //
  //          double maxTemperature = resultSet.getDouble(8);
  //          assertTrue(deviceToMaxTemperature.get(actualDevice)[cnt] - maxTemperature < 0.00001);
  //          index++;
  //          if (index % 10 == 0) {
  //            cnt++;
  //          }
  //        }
  //        assertEquals(30, index);
  //      }
  //    } catch (Exception e) {
  //      e.printStackTrace();
  //      fail(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  public void groupByTimeOrderByTimeTest2() {
  //    String sql =
  //        "SELECT AVG(*),COUNT(*),MAX_VALUE(*) FROM weather GROUP
  // BY([2022-11-21T00:00:00.000+08:00,2022-11-21T00:00:02.801+08:00),1000ms) ORDER BY TIME DESC";
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //      long lastTime = Long.MAX_VALUE;
  //      String lastDevice = "";
  //      int index = 0;
  //      try (ResultSet resultSet = statement.executeQuery(sql)) {
  //        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
  //        checkHeader(
  //            resultSetMetaData,
  //
  // "Time,Device,AVG(precipitation),AVG(temperature),COUNT(precipitation),COUNT(temperature),MAX_VALUE(precipitation),MAX_VALUE(temperature)");
  //        int cnt = 2;
  //        while (resultSet.next()) {
  //          long actualTime = resultSet.getLong(1);
  //          String actualDevice = resultSet.getString(2);
  //          assertTrue(actualTime <= lastTime);
  //          if (!Objects.equals(lastDevice, "") && actualTime == lastTime) {
  //            assertTrue(actualDevice.compareTo(lastDevice) >= 0);
  //          }
  //          lastTime = actualTime;
  //          lastDevice = actualDevice;
  //          double avgPrecipitation = resultSet.getDouble(3);
  //          assertTrue(deviceToAvgPrecipitation.get(actualDevice)[cnt] - avgPrecipitation <
  // 0.00001);
  //
  //          double avgTemperature = resultSet.getDouble(4);
  //          assertTrue(deviceToAvgTemperature.get(actualDevice)[cnt] - avgTemperature < 0.00001);
  //
  //          int countPrecipitation = resultSet.getInt(5);
  //          assertEquals(getCountNum(actualDevice, cnt), countPrecipitation);
  //          int countTemperature = resultSet.getInt(6);
  //          assertEquals(getCountNum(actualDevice, cnt), countTemperature);
  //
  //          long maxPrecipitation = resultSet.getLong(7);
  //          assertEquals(deviceToMaxPrecipitation.get(actualDevice)[cnt], maxPrecipitation);
  //
  //          double maxTemperature = resultSet.getDouble(8);
  //          assertTrue(deviceToMaxTemperature.get(actualDevice)[cnt] - maxTemperature < 0.00001);
  //          index++;
  //          if (index % 10 == 0) {
  //            cnt--;
  //          }
  //        }
  //        assertEquals(30, index);
  //      }
  //    } catch (Exception e) {
  //      e.printStackTrace();
  //      fail(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  public void groupByTimeOrderByTimeTest3() {
  //    String sql =
  //        "SELECT AVG(*),COUNT(*),MAX_VALUE(*) FROM weather GROUP
  // BY([2022-11-21T00:00:00.000+08:00,2022-11-21T00:00:02.801+08:00),1000ms) ORDER BY TIME
  // ASC,DEVICE DESC";
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //      long lastTime = -1;
  //      String lastDevice = "";
  //      int index = 0;
  //      try (ResultSet resultSet = statement.executeQuery(sql)) {
  //        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
  //        checkHeader(
  //            resultSetMetaData,
  //
  // "Time,Device,AVG(precipitation),AVG(temperature),COUNT(precipitation),COUNT(temperature),MAX_VALUE(precipitation),MAX_VALUE(temperature)");
  //        int cnt = 0;
  //        while (resultSet.next()) {
  //          long actualTime = resultSet.getLong(1);
  //          String actualDevice = resultSet.getString(2);
  //          assertTrue(actualTime >= lastTime);
  //          if (!Objects.equals(lastDevice, "") && actualTime == lastTime) {
  //            assertTrue(actualDevice.compareTo(lastDevice) <= 0);
  //          }
  //          lastTime = actualTime;
  //          lastDevice = actualDevice;
  //          double avgPrecipitation = resultSet.getDouble(3);
  //          assertTrue(deviceToAvgPrecipitation.get(actualDevice)[cnt] - avgPrecipitation <
  // 0.00001);
  //
  //          double avgTemperature = resultSet.getDouble(4);
  //          assertTrue(deviceToAvgTemperature.get(actualDevice)[cnt] - avgTemperature < 0.00001);
  //
  //          int countPrecipitation = resultSet.getInt(5);
  //          assertEquals(getCountNum(actualDevice, cnt), countPrecipitation);
  //          int countTemperature = resultSet.getInt(6);
  //          assertEquals(getCountNum(actualDevice, cnt), countTemperature);
  //
  //          long maxPrecipitation = resultSet.getLong(7);
  //          assertEquals(deviceToMaxPrecipitation.get(actualDevice)[cnt], maxPrecipitation);
  //
  //          double maxTemperature = resultSet.getDouble(8);
  //          assertTrue(deviceToMaxTemperature.get(actualDevice)[cnt] - maxTemperature < 0.00001);
  //          index++;
  //          if (index % 10 == 0) {
  //            cnt++;
  //          }
  //        }
  //        assertEquals(30, index);
  //      }
  //    } catch (Exception e) {
  //      e.printStackTrace();
  //      fail(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  public void groupByTimeOrderByTimeTest4() {
  //    String sql =
  //        "SELECT AVG(*),COUNT(*),MAX_VALUE(*) FROM weather GROUP
  // BY([2022-11-21T00:00:00.000+08:00,2022-11-21T00:00:02.801+08:00),1000ms) ORDER BY TIME
  // DESC,DEVICE DESC";
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //      long lastTime = Long.MAX_VALUE;
  //      String lastDevice = "";
  //      int index = 0;
  //      try (ResultSet resultSet = statement.executeQuery(sql)) {
  //        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
  //        checkHeader(
  //            resultSetMetaData,
  //
  // "Time,Device,AVG(precipitation),AVG(temperature),COUNT(precipitation),COUNT(temperature),MAX_VALUE(precipitation),MAX_VALUE(temperature)");
  //        int cnt = 2;
  //        while (resultSet.next()) {
  //          long actualTime = resultSet.getLong(1);
  //          String actualDevice = resultSet.getString(2);
  //          assertTrue(actualTime <= lastTime);
  //          if (!Objects.equals(lastDevice, "") && actualTime == lastTime) {
  //            assertTrue(actualDevice.compareTo(lastDevice) <= 0);
  //          }
  //          lastTime = actualTime;
  //          lastDevice = actualDevice;
  //          double avgPrecipitation = resultSet.getDouble(3);
  //          assertTrue(deviceToAvgPrecipitation.get(actualDevice)[cnt] - avgPrecipitation <
  // 0.00001);
  //
  //          double avgTemperature = resultSet.getDouble(4);
  //          assertTrue(deviceToAvgTemperature.get(actualDevice)[cnt] - avgTemperature < 0.00001);
  //
  //          int countPrecipitation = resultSet.getInt(5);
  //          assertEquals(getCountNum(actualDevice, cnt), countPrecipitation);
  //          int countTemperature = resultSet.getInt(6);
  //          assertEquals(getCountNum(actualDevice, cnt), countTemperature);
  //
  //          long maxPrecipitation = resultSet.getLong(7);
  //          assertEquals(deviceToMaxPrecipitation.get(actualDevice)[cnt], maxPrecipitation);
  //
  //          double maxTemperature = resultSet.getDouble(8);
  //          assertTrue(deviceToMaxTemperature.get(actualDevice)[cnt] - maxTemperature < 0.00001);
  //          index++;
  //          if (index % 10 == 0) {
  //            cnt--;
  //          }
  //        }
  //        assertEquals(30, index);
  //      }
  //    } catch (Exception e) {
  //      e.printStackTrace();
  //      fail(e.getMessage());
  //    }
  //  }

  // test the optimized plan
  public static String[] optimizedSQL =
      new String[] {
        "create table optimize(plant_id STRING ID, device_id STRING ID, temperature DOUBLE MEASUREMENT, status BOOLEAN MEASUREMENT, hardware STRING MEASUREMENT)",
        "insert into optimize(Time, plant_id, device_id, temperature, status) values(2017-11-01T00:00:00.000+08:00, 'wf01', 'wt01', 25.96, true)",
        "insert into optimize(Time, plant_id, device_id, temperature, status) values(2017-11-01T00:01:00.000+08:00, 'wf01', 'wt01', 24.36, true)",
        "insert into optimize(Time, plant_id, device_id, status, hardware) values(1970-01-01T08:00:00.001+08:00, 'wf02', 'wt02', true, 'v1')",
        "insert into optimize(Time, plant_id, device_id, status, hardware) values(1970-01-01T08:00:00.002+08:00, 'wf02', 'wt02', false, 'v2')",
        "insert into optimize(Time, plant_id, device_id, status, hardware) values(2017-11-01T00:00:00.000+08:00, 'wf02', 'wt02', false, 'v2')",
        "insert into optimize(Time, plant_id, device_id, status, hardware) values(2017-11-01T00:01:00.000+08:00, 'wf02', 'wt02', true, 'v2')",
      };

  @Test
  public void optimizedPlanTest() {

    String[] expectedHeader =
        new String[] {"time", "plant_id", "device_id", "temperature", "status", "hardware"};
    String[] retArray =
        new String[] {
          "2017-10-31T16:01:00.000Z,wf02,wt02,null,true,v2,",
          "2017-10-31T16:01:00.000Z,wf01,wt01,24.36,true,null,",
          "2017-10-31T16:00:00.000Z,wf02,wt02,null,false,v2,",
          "2017-10-31T16:00:00.000Z,wf01,wt01,25.96,true,null,",
          "1970-01-01T00:00:00.002Z,wf02,wt02,null,false,v2,",
          "1970-01-01T00:00:00.001Z,wf02,wt02,null,true,v1,",
        };

    tableResultSetEqualTest(
        "SELECT * FROM optimize ORDER BY Time DESC,plant_id DESC,device_id desc",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }
}
