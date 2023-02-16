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
package org.apache.iotdb.db.it.alignbydevice;

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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBOrderByWithAlignByDeviceIT {
  private static final String[] places =
      new String[] {
        "root.weather.London",
        "root.weather.Edinburgh",
        "root.weather.Belfast",
        "root.weather.Birmingham",
        "root.weather.Liverpool",
        "root.weather.Derby",
        "root.weather.Durham",
        "root.weather.Hereford",
        "root.weather.Manchester",
        "root.weather.Oxford"
      };
  private static final long startPrecipitation = 200;
  private static final double startTemperature = 20.0;
  private static final long startTime = 1668960000000L;
  private static final int numOfPointsInDevice = 20;
  private static final long timeGap = 100L;
  private static final Map<String, Long> deviceToStartTimestamp = new HashMap<>();
  public static final Map<String, double[]> deviceToMaxTemperature = new HashMap<>();
  public static final Map<String, double[]> deviceToAvgTemperature = new HashMap<>();
  public static final Map<String, long[]> deviceToMaxPrecipitation = new HashMap<>();
  public static final Map<String, double[]> deviceToAvgPrecipitation = new HashMap<>();

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
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
    try (Connection iotDBConnection = EnvFactory.getEnv().getConnection();
        Statement statement = iotDBConnection.createStatement()) {
      // create TimeSeries
      for (String place : places) {
        String PRE_PRECIPITATION = place + ".precipitation";
        String PRE_TEMPERATURE = place + ".temperature";
        String createPrecipitationSql =
            "CREATE TIMESERIES " + PRE_PRECIPITATION + " WITH DATATYPE=INT64, ENCODING=RLE";
        String createTemperatureSql =
            "CREATE TIMESERIES " + PRE_TEMPERATURE + " WITH DATATYPE=DOUBLE, ENCODING=RLE";
        statement.execute(createPrecipitationSql);
        statement.execute(createTemperatureSql);
      }
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
              "INSERT INTO "
                  + place
                  + "(timestamp,precipitation,temperature) VALUES("
                  + (start + i * timeGap)
                  + ","
                  + precipitation
                  + ","
                  + temperature
                  + ")";
          statement.execute(insertUniqueTime);
          if (i == 0) deviceToStartTimestamp.put(place, start);
        }
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

  private void checkHeader(ResultSetMetaData resultSetMetaData, String title) throws SQLException {
    String[] headers = title.split(",");
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      assertEquals(headers[i - 1], resultSetMetaData.getColumnName(i));
    }
  }

  // ORDER BY DEVICE
  @Test
  public void orderByDeviceTest1() {
    String sql = "SELECT * FROM root.weather.** ORDER BY DEVICE ALIGN BY DEVICE";
    Object[] expectedDevice = Arrays.stream(places.clone()).sorted().toArray();
    int index = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "Time,Device,precipitation,temperature");

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
    String sql = "SELECT * FROM root.weather.** ORDER BY DEVICE ASC ALIGN BY DEVICE";
    Object[] expectedDevice = Arrays.stream(places.clone()).sorted().toArray();
    int index = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "Time,Device,precipitation,temperature");

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
    String sql = "SELECT * FROM root.weather.** ORDER BY DEVICE DESC ALIGN BY DEVICE";
    Object[] expectedDevice =
        Arrays.stream(places.clone()).sorted(Comparator.reverseOrder()).toArray();
    int index = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "Time,Device,precipitation,temperature");

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
    String sql = "SELECT * FROM root.weather.** ORDER BY TIME ALIGN BY DEVICE";
    int total = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "Time,Device,precipitation,temperature");
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
        assertEquals(numOfPointsInDevice * places.length, total);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void orderByTimeTest2() {
    String sql = "SELECT * FROM root.weather.** ORDER BY TIME ASC ALIGN BY DEVICE";
    int total = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "Time,Device,precipitation,temperature");
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
        assertEquals(numOfPointsInDevice * places.length, total);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void orderByTimeTest3() {
    String sql = "SELECT * FROM root.weather.** ORDER BY TIME DESC ALIGN BY DEVICE";
    int total = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "Time,Device,precipitation,temperature");
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
        assertEquals(numOfPointsInDevice * places.length, total);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // ORDER BY DEVICE,TIME

  @Test
  public void orderByDeviceTimeTest1() {
    String sql = "SELECT * FROM root.weather.** ORDER BY DEVICE ASC,TIME DESC ALIGN BY DEVICE";
    Object[] expectedDevice = Arrays.stream(places.clone()).sorted().toArray();
    int index = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "Time,Device,precipitation,temperature");

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
        assertEquals(10, index);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void orderByDeviceTimeTest2() {
    String sql = "SELECT * FROM root.weather.** ORDER BY DEVICE ASC,TIME ASC ALIGN BY DEVICE";
    Object[] expectedDevice = Arrays.stream(places.clone()).sorted().toArray();
    int index = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "Time,Device,precipitation,temperature");

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
        assertEquals(10, index);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void orderByDeviceTimeTest3() {
    String sql = "SELECT * FROM root.weather.** ORDER BY DEVICE DESC,TIME DESC ALIGN BY DEVICE";
    Object[] expectedDevice =
        Arrays.stream(places.clone()).sorted(Comparator.reverseOrder()).toArray();
    int index = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "Time,Device,precipitation,temperature");

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
        assertEquals(10, index);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void orderByDeviceTimeTest4() {
    String sql = "SELECT * FROM root.weather.** ORDER BY DEVICE DESC,TIME ASC ALIGN BY DEVICE";
    Object[] expectedDevice =
        Arrays.stream(places.clone()).sorted(Comparator.reverseOrder()).toArray();
    int index = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "Time,Device,precipitation,temperature");

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
        assertEquals(10, index);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // ORDER BY TIME,DEVICE

  @Test
  public void orderByTimeDeviceTest1() {
    String sql = "SELECT * FROM root.weather.** ORDER BY TIME ASC,DEVICE DESC ALIGN BY DEVICE";
    int total = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "Time,Device,precipitation,temperature");
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
        assertEquals(numOfPointsInDevice * places.length, total);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void orderByTimeDeviceTest2() {
    String sql = "SELECT * FROM root.weather.** ORDER BY TIME ASC,DEVICE ASC ALIGN BY DEVICE";
    int total = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "Time,Device,precipitation,temperature");
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
        assertEquals(numOfPointsInDevice * places.length, total);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void orderByTimeDeviceTest3() {
    String sql = "SELECT * FROM root.weather.** ORDER BY TIME DESC,DEVICE DESC ALIGN BY DEVICE";
    int total = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "Time,Device,precipitation,temperature");
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
        assertEquals(numOfPointsInDevice * places.length, total);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void orderByTimeDeviceTest4() {
    String sql = "SELECT * FROM root.weather.** ORDER BY TIME DESC,DEVICE ASC ALIGN BY DEVICE";
    int total = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "Time,Device,precipitation,temperature");
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
        assertEquals(numOfPointsInDevice * places.length, total);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // aggregation query
  private static final int[][] countIn1000MSFiledWith100MSTimeGap =
      new int[][] {
        {10, 10, 0},
        {9, 10, 1},
        {8, 10, 2},
        {7, 10, 3},
        {6, 10, 4},
        {5, 10, 5},
        {4, 10, 6},
        {3, 10, 7},
        {2, 10, 8},
        {1, 10, 9}
      };

  private static int getCountNum(String device, int cnt) {
    int index = 0;
    for (int i = 0; i < places.length; i++) {
      if (places[i].equals(device)) {
        index = i;
        break;
      }
    }

    return countIn1000MSFiledWith100MSTimeGap[index][cnt];
  }

  @Test
  public void groupByTimeOrderByDeviceTest1() {
    String sql =
        "SELECT AVG(*),COUNT(*),MAX_VALUE(*) FROM root.weather.** GROUP BY([2022-11-21T00:00:00.000+08:00,2022-11-21T00:00:02.801+08:00),1000ms) ALIGN BY DEVICE";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      long lastTime = -1;
      String lastDevice = "";
      long[] expectedTime = new long[] {startTime, startTime + 1000, startTime + 1000 * 2};
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(
            resultSetMetaData,
            "Time,Device,AVG(precipitation),AVG(temperature),COUNT(precipitation),COUNT(temperature),MAX_VALUE(precipitation),MAX_VALUE(temperature)");
        int cnt = 0;
        while (resultSet.next()) {

          long actualTime = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertEquals(expectedTime[cnt], actualTime);
          if (!Objects.equals(lastDevice, "") && Objects.equals(actualDevice, lastDevice))
            assertTrue(actualTime >= lastTime);
          if (!Objects.equals(lastDevice, "")) assertTrue(actualDevice.compareTo(lastDevice) >= 0);
          lastTime = actualTime;
          lastDevice = actualDevice;

          double avgPrecipitation = resultSet.getDouble(3);
          assertTrue(deviceToAvgPrecipitation.get(actualDevice)[cnt] - avgPrecipitation < 0.00001);

          double avgTemperature = resultSet.getDouble(4);
          assertTrue(deviceToAvgTemperature.get(actualDevice)[cnt] - avgTemperature < 0.00001);

          int countPrecipitation = resultSet.getInt(5);
          assertEquals(getCountNum(actualDevice, cnt), countPrecipitation);
          int countTemperature = resultSet.getInt(6);
          assertEquals(getCountNum(actualDevice, cnt), countTemperature);

          long maxPrecipitation = resultSet.getLong(7);
          assertEquals(deviceToMaxPrecipitation.get(actualDevice)[cnt], maxPrecipitation);

          double maxTemperature = resultSet.getDouble(8);
          assertTrue(deviceToMaxTemperature.get(actualDevice)[cnt] - maxTemperature < 0.00001);
          cnt++;
          if (cnt % 3 == 0) cnt = 0;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void groupByTimeOrderByDeviceTest2() {
    String sql =
        "SELECT AVG(*),COUNT(*),MAX_VALUE(*) FROM root.weather.** GROUP BY([2022-11-21T00:00:00.000+08:00,2022-11-21T00:00:02.801+08:00),1000ms) ORDER BY DEVICE DESC ALIGN BY DEVICE";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      long lastTime = -1;
      String lastDevice = "";
      long[] expectedTime = new long[] {startTime, startTime + 1000, startTime + 1000 * 2};
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(
            resultSetMetaData,
            "Time,Device,AVG(precipitation),AVG(temperature),COUNT(precipitation),COUNT(temperature),MAX_VALUE(precipitation),MAX_VALUE(temperature)");
        int cnt = 0;
        while (resultSet.next()) {

          long actualTime = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertEquals(expectedTime[cnt], actualTime);
          if (!Objects.equals(lastDevice, "") && Objects.equals(actualDevice, lastDevice))
            assertTrue(actualTime >= lastTime);
          if (!Objects.equals(lastDevice, "")) assertTrue(actualDevice.compareTo(lastDevice) <= 0);
          lastTime = actualTime;
          lastDevice = actualDevice;

          double avgPrecipitation = resultSet.getDouble(3);
          assertTrue(deviceToAvgPrecipitation.get(actualDevice)[cnt] - avgPrecipitation < 0.00001);

          double avgTemperature = resultSet.getDouble(4);
          assertTrue(deviceToAvgTemperature.get(actualDevice)[cnt] - avgTemperature < 0.00001);

          int countPrecipitation = resultSet.getInt(5);
          assertEquals(getCountNum(actualDevice, cnt), countPrecipitation);
          int countTemperature = resultSet.getInt(6);
          assertEquals(getCountNum(actualDevice, cnt), countTemperature);

          long maxPrecipitation = resultSet.getLong(7);
          assertEquals(deviceToMaxPrecipitation.get(actualDevice)[cnt], maxPrecipitation);

          double maxTemperature = resultSet.getDouble(8);
          assertTrue(deviceToMaxTemperature.get(actualDevice)[cnt] - maxTemperature < 0.00001);
          cnt++;
          if (cnt % 3 == 0) cnt = 0;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void groupByTimeOrderByDeviceTest3() {
    String sql =
        "SELECT AVG(*),COUNT(*),MAX_VALUE(*) FROM root.weather.** GROUP BY([2022-11-21T00:00:00.000+08:00,2022-11-21T00:00:02.801+08:00),1000ms) ORDER BY DEVICE DESC,TIME DESC ALIGN BY DEVICE";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      long lastTime = -1;
      String lastDevice = "";
      long[] expectedTime = new long[] {startTime, startTime + 1000, startTime + 1000 * 2};
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(
            resultSetMetaData,
            "Time,Device,AVG(precipitation),AVG(temperature),COUNT(precipitation),COUNT(temperature),MAX_VALUE(precipitation),MAX_VALUE(temperature)");
        int cnt = 2;
        while (resultSet.next()) {
          long actualTime = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertEquals(expectedTime[cnt], actualTime);
          if (!Objects.equals(lastDevice, "") && Objects.equals(actualDevice, lastDevice))
            assertTrue(actualTime <= lastTime);
          if (!Objects.equals(lastDevice, "")) assertTrue(actualDevice.compareTo(lastDevice) <= 0);
          lastTime = actualTime;
          lastDevice = actualDevice;

          double avgPrecipitation = resultSet.getDouble(3);
          assertTrue(deviceToAvgPrecipitation.get(actualDevice)[cnt] - avgPrecipitation < 0.00001);

          double avgTemperature = resultSet.getDouble(4);
          assertTrue(deviceToAvgTemperature.get(actualDevice)[cnt] - avgTemperature < 0.00001);

          int countPrecipitation = resultSet.getInt(5);
          assertEquals(getCountNum(actualDevice, cnt), countPrecipitation);
          int countTemperature = resultSet.getInt(6);
          assertEquals(getCountNum(actualDevice, cnt), countTemperature);

          long maxPrecipitation = resultSet.getLong(7);
          assertEquals(deviceToMaxPrecipitation.get(actualDevice)[cnt], maxPrecipitation);

          double maxTemperature = resultSet.getDouble(8);
          assertTrue(deviceToMaxTemperature.get(actualDevice)[cnt] - maxTemperature < 0.00001);
          cnt--;
          if (cnt < 0) cnt = 2;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void groupByTimeOrderByDeviceTest4() {
    String sql =
        "SELECT AVG(*),COUNT(*),MAX_VALUE(*) FROM root.weather.** GROUP BY([2022-11-21T00:00:00.000+08:00,2022-11-21T00:00:02.801+08:00),1000ms) ORDER BY DEVICE ASC,TIME DESC ALIGN BY DEVICE";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      long lastTime = -1;
      String lastDevice = "";
      long[] expectedTime = new long[] {startTime, startTime + 1000, startTime + 1000 * 2};
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(
            resultSetMetaData,
            "Time,Device,AVG(precipitation),AVG(temperature),COUNT(precipitation),COUNT(temperature),MAX_VALUE(precipitation),MAX_VALUE(temperature)");
        int cnt = 2;
        while (resultSet.next()) {

          long actualTime = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertEquals(expectedTime[cnt], actualTime);
          if (!Objects.equals(lastDevice, "") && Objects.equals(actualDevice, lastDevice))
            assertTrue(actualTime <= lastTime);
          if (!Objects.equals(lastDevice, "")) assertTrue(actualDevice.compareTo(lastDevice) >= 0);
          lastTime = actualTime;
          lastDevice = actualDevice;

          double avgPrecipitation = resultSet.getDouble(3);
          assertTrue(deviceToAvgPrecipitation.get(actualDevice)[cnt] - avgPrecipitation < 0.00001);

          double avgTemperature = resultSet.getDouble(4);
          assertTrue(deviceToAvgTemperature.get(actualDevice)[cnt] - avgTemperature < 0.00001);

          int countPrecipitation = resultSet.getInt(5);
          assertEquals(getCountNum(actualDevice, cnt), countPrecipitation);
          int countTemperature = resultSet.getInt(6);
          assertEquals(getCountNum(actualDevice, cnt), countTemperature);

          long maxPrecipitation = resultSet.getLong(7);
          assertEquals(deviceToMaxPrecipitation.get(actualDevice)[cnt], maxPrecipitation);

          double maxTemperature = resultSet.getDouble(8);
          assertTrue(deviceToMaxTemperature.get(actualDevice)[cnt] - maxTemperature < 0.00001);
          cnt--;
          if (cnt < 0) cnt = 2;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void groupByTimeOrderByTimeTest1() {
    String sql =
        "SELECT AVG(*),COUNT(*),MAX_VALUE(*) FROM root.weather.** GROUP BY([2022-11-21T00:00:00.000+08:00,2022-11-21T00:00:02.801+08:00),1000ms) ORDER BY TIME ALIGN BY DEVICE";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      long lastTime = -1;
      String lastDevice = "";
      int index = 0;
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(
            resultSetMetaData,
            "Time,Device,AVG(precipitation),AVG(temperature),COUNT(precipitation),COUNT(temperature),MAX_VALUE(precipitation),MAX_VALUE(temperature)");
        int cnt = 0;
        while (resultSet.next()) {
          long actualTime = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertTrue(actualTime >= lastTime);
          if (!Objects.equals(lastDevice, "") && actualTime == lastTime)
            assertTrue(actualDevice.compareTo(lastDevice) >= 0);
          lastTime = actualTime;
          lastDevice = actualDevice;
          double avgPrecipitation = resultSet.getDouble(3);
          assertTrue(deviceToAvgPrecipitation.get(actualDevice)[cnt] - avgPrecipitation < 0.00001);

          double avgTemperature = resultSet.getDouble(4);
          assertTrue(deviceToAvgTemperature.get(actualDevice)[cnt] - avgTemperature < 0.00001);

          int countPrecipitation = resultSet.getInt(5);
          assertEquals(getCountNum(actualDevice, cnt), countPrecipitation);
          int countTemperature = resultSet.getInt(6);
          assertEquals(getCountNum(actualDevice, cnt), countTemperature);

          long maxPrecipitation = resultSet.getLong(7);
          assertEquals(deviceToMaxPrecipitation.get(actualDevice)[cnt], maxPrecipitation);

          double maxTemperature = resultSet.getDouble(8);
          assertTrue(deviceToMaxTemperature.get(actualDevice)[cnt] - maxTemperature < 0.00001);
          index++;
          if (index % 10 == 0) cnt++;
        }
        assertEquals(30, index);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void groupByTimeOrderByTimeTest2() {
    String sql =
        "SELECT AVG(*),COUNT(*),MAX_VALUE(*) FROM root.weather.** GROUP BY([2022-11-21T00:00:00.000+08:00,2022-11-21T00:00:02.801+08:00),1000ms) ORDER BY TIME DESC ALIGN BY DEVICE";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      long lastTime = Long.MAX_VALUE;
      String lastDevice = "";
      int index = 0;
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(
            resultSetMetaData,
            "Time,Device,AVG(precipitation),AVG(temperature),COUNT(precipitation),COUNT(temperature),MAX_VALUE(precipitation),MAX_VALUE(temperature)");
        int cnt = 2;
        while (resultSet.next()) {
          long actualTime = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertTrue(actualTime <= lastTime);
          if (!Objects.equals(lastDevice, "") && actualTime == lastTime)
            assertTrue(actualDevice.compareTo(lastDevice) >= 0);
          lastTime = actualTime;
          lastDevice = actualDevice;
          double avgPrecipitation = resultSet.getDouble(3);
          assertTrue(deviceToAvgPrecipitation.get(actualDevice)[cnt] - avgPrecipitation < 0.00001);

          double avgTemperature = resultSet.getDouble(4);
          assertTrue(deviceToAvgTemperature.get(actualDevice)[cnt] - avgTemperature < 0.00001);

          int countPrecipitation = resultSet.getInt(5);
          assertEquals(getCountNum(actualDevice, cnt), countPrecipitation);
          int countTemperature = resultSet.getInt(6);
          assertEquals(getCountNum(actualDevice, cnt), countTemperature);

          long maxPrecipitation = resultSet.getLong(7);
          assertEquals(deviceToMaxPrecipitation.get(actualDevice)[cnt], maxPrecipitation);

          double maxTemperature = resultSet.getDouble(8);
          assertTrue(deviceToMaxTemperature.get(actualDevice)[cnt] - maxTemperature < 0.00001);
          index++;
          if (index % 10 == 0) cnt--;
        }
        assertEquals(30, index);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void groupByTimeOrderByTimeTest3() {
    String sql =
        "SELECT AVG(*),COUNT(*),MAX_VALUE(*) FROM root.weather.** GROUP BY([2022-11-21T00:00:00.000+08:00,2022-11-21T00:00:02.801+08:00),1000ms) ORDER BY TIME ASC,DEVICE DESC ALIGN BY DEVICE";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      long lastTime = -1;
      String lastDevice = "";
      int index = 0;
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(
            resultSetMetaData,
            "Time,Device,AVG(precipitation),AVG(temperature),COUNT(precipitation),COUNT(temperature),MAX_VALUE(precipitation),MAX_VALUE(temperature)");
        int cnt = 0;
        while (resultSet.next()) {
          long actualTime = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertTrue(actualTime >= lastTime);
          if (!Objects.equals(lastDevice, "") && actualTime == lastTime)
            assertTrue(actualDevice.compareTo(lastDevice) <= 0);
          lastTime = actualTime;
          lastDevice = actualDevice;
          double avgPrecipitation = resultSet.getDouble(3);
          assertTrue(deviceToAvgPrecipitation.get(actualDevice)[cnt] - avgPrecipitation < 0.00001);

          double avgTemperature = resultSet.getDouble(4);
          assertTrue(deviceToAvgTemperature.get(actualDevice)[cnt] - avgTemperature < 0.00001);

          int countPrecipitation = resultSet.getInt(5);
          assertEquals(getCountNum(actualDevice, cnt), countPrecipitation);
          int countTemperature = resultSet.getInt(6);
          assertEquals(getCountNum(actualDevice, cnt), countTemperature);

          long maxPrecipitation = resultSet.getLong(7);
          assertEquals(deviceToMaxPrecipitation.get(actualDevice)[cnt], maxPrecipitation);

          double maxTemperature = resultSet.getDouble(8);
          assertTrue(deviceToMaxTemperature.get(actualDevice)[cnt] - maxTemperature < 0.00001);
          index++;
          if (index % 10 == 0) cnt++;
        }
        assertEquals(30, index);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void groupByTimeOrderByTimeTest4() {
    String sql =
        "SELECT AVG(*),COUNT(*),MAX_VALUE(*) FROM root.weather.** GROUP BY([2022-11-21T00:00:00.000+08:00,2022-11-21T00:00:02.801+08:00),1000ms) ORDER BY TIME DESC,DEVICE DESC ALIGN BY DEVICE";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      long lastTime = Long.MAX_VALUE;
      String lastDevice = "";
      int index = 0;
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(
            resultSetMetaData,
            "Time,Device,AVG(precipitation),AVG(temperature),COUNT(precipitation),COUNT(temperature),MAX_VALUE(precipitation),MAX_VALUE(temperature)");
        int cnt = 2;
        while (resultSet.next()) {
          long actualTime = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertTrue(actualTime <= lastTime);
          if (!Objects.equals(lastDevice, "") && actualTime == lastTime)
            assertTrue(actualDevice.compareTo(lastDevice) <= 0);
          lastTime = actualTime;
          lastDevice = actualDevice;
          double avgPrecipitation = resultSet.getDouble(3);
          assertTrue(deviceToAvgPrecipitation.get(actualDevice)[cnt] - avgPrecipitation < 0.00001);

          double avgTemperature = resultSet.getDouble(4);
          assertTrue(deviceToAvgTemperature.get(actualDevice)[cnt] - avgTemperature < 0.00001);

          int countPrecipitation = resultSet.getInt(5);
          assertEquals(getCountNum(actualDevice, cnt), countPrecipitation);
          int countTemperature = resultSet.getInt(6);
          assertEquals(getCountNum(actualDevice, cnt), countTemperature);

          long maxPrecipitation = resultSet.getLong(7);
          assertEquals(deviceToMaxPrecipitation.get(actualDevice)[cnt], maxPrecipitation);

          double maxTemperature = resultSet.getDouble(8);
          assertTrue(deviceToMaxTemperature.get(actualDevice)[cnt] - maxTemperature < 0.00001);
          index++;
          if (index % 10 == 0) cnt--;
        }
        assertEquals(30, index);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // test the optimized plan
  public static String[] optimizedSQL =
      new String[] {
        "insert into root.ln.wf01.wt01(timestamp,temperature,status) values(2017-11-01T00:00:00.000+08:00,25.96,true)",
        "insert into root.ln.wf01.wt01(timestamp,temperature,status) values(2017-11-01T00:01:00.000+08:00,24.36,true)",
        "insert into root.ln.wf02.wt02(timestamp,status,hardware) values(1970-01-01T08:00:00.001+08:00,true,'v1')",
        "insert into root.ln.wf02.wt02(timestamp,status,hardware) values(1970-01-01T08:00:00.002+08:00,false,'v2')",
        "insert into root.ln.wf02.wt02(timestamp,status,hardware) values(2017-11-01T00:00:00.000+08:00,true,'v2')",
        "insert into root.ln.wf02.wt02(timestamp,status,hardware) values(2017-11-01T00:01:00.000+08:00,true,'v2')"
      };

  String[] res =
      new String[] {
        "v2,true,null",
        "null,true,24.36",
        "v2,true,null",
        "null,true,25.96",
        "v2,false,null",
        "v1,true,null"
      };

  @Test
  public void optimizedPlanTest() {
    String sql = "SELECT * FROM root.ln.** ORDER BY TIME DESC,DEVICE DESC ALIGN BY DEVICE";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "Time,Device,hardware,status,temperature");
        long lastTimeStamp = Long.MAX_VALUE;
        String lastDevice = "";
        int i = 0;
        while (resultSet.next()) {
          long actualTimeStamp = resultSet.getLong(1);
          assertTrue(actualTimeStamp <= lastTimeStamp);
          String actualDevice = resultSet.getString(2);
          if (!lastDevice.equals("") && actualTimeStamp == lastTimeStamp) {
            assertTrue(actualDevice.compareTo(lastDevice) <= 0);
          }
          lastDevice = actualDevice;
          lastTimeStamp = actualTimeStamp;
          String actualHardware = resultSet.getString(3);
          String actualStatus = resultSet.getString(4);
          String actualTemperature = resultSet.getString(5);
          String stringBuilder = actualHardware + "," + actualStatus + "," + actualTemperature;
          assertEquals(res[i], stringBuilder);
          i++;
        }
        assertEquals(i, 6);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
