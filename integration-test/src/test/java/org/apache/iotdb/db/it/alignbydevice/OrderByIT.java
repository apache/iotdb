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
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
public class OrderByIT {
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
  private static final int startPrecipitation = 200;
  private static final double startTemperature = 20.0;

  private static final Map<String, Long> deviceToUniqueTimestamp = new HashMap<>();
  private static final long shareTime = 1668960000;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeClass();
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
  }

  static void arrayReverse(String[] arrays) {
    for (int i = 0; i < arrays.length / 2; i++) {
      String temp = arrays[i];
      arrays[i] = arrays[arrays.length - i - 1];
      arrays[arrays.length - i - 1] = temp;
    }
  }

  private static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      long uniqueTime = 1668960001;
      // create TimeSeries
      for (String place : places) {
        String PRE_PRECIPITATION = place + ".precipitation";
        String PRE_TEMPERATURE = place + ".temperature";
        String createPrecipitationSql =
            "CREATE TIMESERIES " + PRE_PRECIPITATION + " WITH DATATYPE=INT32, ENCODING=RLE";
        String createTemperatureSql =
            "CREATE TIMESERIES " + PRE_TEMPERATURE + " WITH DATATYPE=DOUBLE, ENCODING=RLE";
        statement.execute(createPrecipitationSql);
        statement.execute(createTemperatureSql);
      }

      // insert data
      for (String place : places) {
        int precipitation = place.hashCode() + startPrecipitation;
        double temperature = place.hashCode() + startTemperature;
        String insertShareTime =
            "INSERT INTO "
                + place
                + "(timestamp,precipitation,temperature) VALUES("
                + shareTime
                + ","
                + precipitation
                + ","
                + temperature
                + ")";
        String insertUniqueTime =
            "INSERT INTO "
                + place
                + "(timestamp,precipitation,temperature) VALUES("
                + uniqueTime
                + ","
                + (precipitation + 1)
                + ","
                + (temperature + 1)
                + ")";

        deviceToUniqueTimestamp.put(place, uniqueTime);
        uniqueTime++;
        statement.execute(insertShareTime);
        statement.execute(insertUniqueTime);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void checkHeader(ResultSetMetaData resultSetMetaData) throws SQLException {
    String[] headers = "Time,Device,precipitation,temperature".split(",");
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      assertEquals(headers[i - 1], resultSetMetaData.getColumnName(i));
    }
  }

  // ORDER BY DEVICE

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void orderByDeviceTest1() {
    String sql = "SELECT * FROM root.** ORDER BY DEVICE ALIGN BY DEVICE";
    Object[] expectedDevice = Arrays.stream(places.clone()).sorted().toArray();
    int index = 0;
    int expectedPrecipitation = startPrecipitation;
    double expectedTemperature = startTemperature;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData);

        int cnt = 0;
        while (resultSet.next()) {
          Long actualTimeStamp = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertEquals(expectedDevice[index], actualDevice);
          assertEquals(
              Optional.of(cnt == 0 ? shareTime : deviceToUniqueTimestamp.get(actualDevice)).get(),
              actualTimeStamp);

          int actualPrecipitation = resultSet.getInt(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(expectedPrecipitation + actualDevice.hashCode(), actualPrecipitation);
          assertTrue((expectedTemperature + actualDevice.hashCode()) - actualTemperature < 0.00001);

          cnt++;
          if (cnt % 2 == 0) {
            index++;
            cnt = 0;
            expectedTemperature = startTemperature;
            expectedPrecipitation = startPrecipitation;
          } else {
            expectedTemperature++;
            expectedPrecipitation++;
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
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void orderByDeviceTest2() {
    String sql = "SELECT * FROM root.** ORDER BY DEVICE ASC ALIGN BY DEVICE";
    Object[] expectedDevice = Arrays.stream(places.clone()).sorted().toArray();
    int index = 0;
    int expectedPrecipitation = startPrecipitation;
    double expectedTemperature = startTemperature;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData);

        int cnt = 0;
        while (resultSet.next()) {
          Long actualTimeStamp = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertEquals(expectedDevice[index], actualDevice);
          assertEquals(
              Optional.of(cnt == 0 ? shareTime : deviceToUniqueTimestamp.get(actualDevice)).get(),
              actualTimeStamp);

          int actualPrecipitation = resultSet.getInt(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(expectedPrecipitation + actualDevice.hashCode(), actualPrecipitation);
          assertTrue((expectedTemperature + actualDevice.hashCode()) - actualTemperature < 0.00001);

          cnt++;
          if (cnt % 2 == 0) {
            index++;
            cnt = 0;
            expectedTemperature = startTemperature;
            expectedPrecipitation = startPrecipitation;
          } else {
            expectedTemperature++;
            expectedPrecipitation++;
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
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void orderByDeviceTest3() {
    String sql = "SELECT * FROM root.** ORDER BY DEVICE DESC ALIGN BY DEVICE";
    Object[] expectedDevice =
        Arrays.stream(places.clone()).sorted(Comparator.reverseOrder()).toArray();
    int index = 0;
    int expectedPrecipitation = startPrecipitation;
    double expectedTemperature = startTemperature;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData);

        int cnt = 0;
        while (resultSet.next()) {
          Long actualTimeStamp = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertEquals(expectedDevice[index], actualDevice);
          assertEquals(
              Optional.of(cnt == 0 ? shareTime : deviceToUniqueTimestamp.get(actualDevice)).get(),
              actualTimeStamp);

          int actualPrecipitation = resultSet.getInt(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(expectedPrecipitation + actualDevice.hashCode(), actualPrecipitation);
          assertTrue((expectedTemperature + actualDevice.hashCode()) - actualTemperature < 0.00001);

          cnt++;
          if (cnt % 2 == 0) {
            index++;
            cnt = 0;
            expectedTemperature = startTemperature;
            expectedPrecipitation = startPrecipitation;
          } else {
            expectedTemperature++;
            expectedPrecipitation++;
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
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void orderByTimeTest1() {
    String sql = "SELECT * FROM root.** ORDER BY TIME ALIGN BY DEVICE";
    Object[] expectedOrderDevice = Arrays.stream(places.clone()).sorted().toArray();
    String[] expectedTimeOrderDevice = places.clone();
    int index = 0;
    int expectedPrecipitation = startPrecipitation;
    double expectedTemperature = startTemperature;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData);

        int cnt = 0;
        while (resultSet.next()) {
          Long actualTimeStamp = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertEquals(
              cnt < 10 ? expectedOrderDevice[index] : expectedTimeOrderDevice[index], actualDevice);
          assertEquals(
              Optional.of(cnt < 10 ? shareTime : deviceToUniqueTimestamp.get(actualDevice)).get(),
              actualTimeStamp);

          int actualPrecipitation = resultSet.getInt(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(expectedPrecipitation + actualDevice.hashCode(), actualPrecipitation);
          assertTrue((expectedTemperature + actualDevice.hashCode()) - actualTemperature < 0.00001);

          cnt++;
          if (cnt == 10) {
            index = 0;
            expectedTemperature++;
            expectedPrecipitation++;
          } else {
            index++;
          }
        }
        assertEquals(20, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void orderByTimeTest2() {
    String sql = "SELECT * FROM root.** ORDER BY TIME ASC ALIGN BY DEVICE";
    Object[] expectedOrderDevice = Arrays.stream(places.clone()).sorted().toArray();
    String[] expectedTimeOrderDevice = places.clone();
    int index = 0;
    int expectedPrecipitation = startPrecipitation;
    double expectedTemperature = startTemperature;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData);

        int cnt = 0;
        while (resultSet.next()) {
          Long actualTimeStamp = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertEquals(
              cnt < 10 ? expectedOrderDevice[index] : expectedTimeOrderDevice[index], actualDevice);
          assertEquals(
              Optional.of(cnt < 10 ? shareTime : deviceToUniqueTimestamp.get(actualDevice)).get(),
              actualTimeStamp);

          int actualPrecipitation = resultSet.getInt(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(expectedPrecipitation + actualDevice.hashCode(), actualPrecipitation);
          assertTrue((expectedTemperature + actualDevice.hashCode()) - actualTemperature < 0.00001);

          cnt++;
          if (cnt == 10) {
            index = 0;
            expectedTemperature++;
            expectedPrecipitation++;
          } else {
            index++;
          }
        }
        assertEquals(20, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void orderByTimeTest3() {
    String sql = "SELECT * FROM root.** ORDER BY TIME DESC ALIGN BY DEVICE";
    Object[] expectedOrderDevice = Arrays.stream(places.clone()).sorted().toArray();
    String[] expectedTimeOrderDevice = places.clone();
    arrayReverse(expectedTimeOrderDevice);
    int index = 0;
    int expectedPrecipitation = startPrecipitation + 1;
    double expectedTemperature = startTemperature + 1;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData);

        int cnt = 0;
        while (resultSet.next()) {
          Long actualTimeStamp = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertEquals(
              cnt >= 10 ? expectedOrderDevice[index] : expectedTimeOrderDevice[index],
              actualDevice);
          assertEquals(
              Optional.of(cnt >= 10 ? shareTime : deviceToUniqueTimestamp.get(actualDevice)).get(),
              actualTimeStamp);

          int actualPrecipitation = resultSet.getInt(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(expectedPrecipitation + actualDevice.hashCode(), actualPrecipitation);
          assertTrue((expectedTemperature + actualDevice.hashCode()) - actualTemperature < 0.00001);

          cnt++;
          if (cnt == 10) {
            index = 0;
            expectedTemperature--;
            expectedPrecipitation--;
          } else {
            index++;
          }
        }
        assertEquals(20, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // ORDER BY DEVICE,TIME

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void orderByDeviceTimeTest1() {
    String sql = "SELECT * FROM root.** ORDER BY DEVICE ASC,TIME DESC ALIGN BY DEVICE";
    Object[] expectedDevice = Arrays.stream(places.clone()).sorted().toArray();
    int index = 0;
    int expectedPrecipitation = startPrecipitation + 1;
    double expectedTemperature = startTemperature + 1;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData);

        int cnt = 0;
        while (resultSet.next()) {
          Long actualTimeStamp = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertEquals(expectedDevice[index], actualDevice);
          assertEquals(
              Optional.of(cnt == 1 ? shareTime : deviceToUniqueTimestamp.get(actualDevice)).get(),
              actualTimeStamp);

          int actualPrecipitation = resultSet.getInt(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(expectedPrecipitation + actualDevice.hashCode(), actualPrecipitation);
          assertTrue((expectedTemperature + actualDevice.hashCode()) - actualTemperature < 0.00001);

          cnt++;
          if (cnt % 2 == 0) {
            index++;
            cnt = 0;
            expectedTemperature = startTemperature + 1;
            expectedPrecipitation = startPrecipitation + 1;
          } else {
            expectedTemperature--;
            expectedPrecipitation--;
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
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void orderByDeviceTimeTest2() {
    String sql = "SELECT * FROM root.** ORDER BY DEVICE ASC,TIME ASC ALIGN BY DEVICE";
    Object[] expectedDevice = Arrays.stream(places.clone()).sorted().toArray();
    int index = 0;
    int expectedPrecipitation = startPrecipitation;
    double expectedTemperature = startTemperature;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData);

        int cnt = 0;
        while (resultSet.next()) {
          Long actualTimeStamp = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertEquals(expectedDevice[index], actualDevice);
          assertEquals(
              Optional.of(cnt == 0 ? shareTime : deviceToUniqueTimestamp.get(actualDevice)).get(),
              actualTimeStamp);

          int actualPrecipitation = resultSet.getInt(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(expectedPrecipitation + actualDevice.hashCode(), actualPrecipitation);
          assertTrue((expectedTemperature + actualDevice.hashCode()) - actualTemperature < 0.00001);

          cnt++;
          if (cnt % 2 == 0) {
            index++;
            cnt = 0;
            expectedTemperature = startTemperature;
            expectedPrecipitation = startPrecipitation;
          } else {
            expectedTemperature++;
            expectedPrecipitation++;
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
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void orderByDeviceTimeTest3() {
    String sql = "SELECT * FROM root.** ORDER BY DEVICE DESC,TIME DESC ALIGN BY DEVICE";
    Object[] expectedDevice =
        Arrays.stream(places.clone()).sorted(Comparator.reverseOrder()).toArray();
    int index = 0;
    int expectedPrecipitation = startPrecipitation + 1;
    double expectedTemperature = startTemperature + 1;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData);

        int cnt = 0;
        while (resultSet.next()) {
          Long actualTimeStamp = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertEquals(expectedDevice[index], actualDevice);
          assertEquals(
              Optional.of(cnt == 1 ? shareTime : deviceToUniqueTimestamp.get(actualDevice)).get(),
              actualTimeStamp);

          int actualPrecipitation = resultSet.getInt(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(expectedPrecipitation + actualDevice.hashCode(), actualPrecipitation);
          assertTrue((expectedTemperature + actualDevice.hashCode()) - actualTemperature < 0.00001);

          cnt++;
          if (cnt % 2 == 0) {
            index++;
            cnt = 0;
            expectedTemperature = startTemperature + 1;
            expectedPrecipitation = startPrecipitation + 1;
          } else {
            expectedTemperature--;
            expectedPrecipitation--;
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
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void orderByDeviceTimeTest4() {
    String sql = "SELECT * FROM root.** ORDER BY DEVICE DESC,TIME ASC ALIGN BY DEVICE";
    Object[] expectedDevice =
        Arrays.stream(places.clone()).sorted(Comparator.reverseOrder()).toArray();
    int index = 0;
    int expectedPrecipitation = startPrecipitation;
    double expectedTemperature = startTemperature;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData);

        int cnt = 0;
        while (resultSet.next()) {
          Long actualTimeStamp = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertEquals(expectedDevice[index], actualDevice);
          assertEquals(
              Optional.of(cnt == 0 ? shareTime : deviceToUniqueTimestamp.get(actualDevice)).get(),
              actualTimeStamp);

          int actualPrecipitation = resultSet.getInt(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(expectedPrecipitation + actualDevice.hashCode(), actualPrecipitation);
          assertTrue((expectedTemperature + actualDevice.hashCode()) - actualTemperature < 0.00001);

          cnt++;
          if (cnt % 2 == 0) {
            index++;
            cnt = 0;
            expectedTemperature = startTemperature;
            expectedPrecipitation = startPrecipitation;
          } else {
            expectedTemperature++;
            expectedPrecipitation++;
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
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void orderByTimeDeviceTest1() {
    String sql = "SELECT * FROM root.** ORDER BY TIME ASC,DEVICE DESC ALIGN BY DEVICE";
    Object[] expectedOrderDevice =
        Arrays.stream(places.clone()).sorted(Comparator.reverseOrder()).toArray();
    String[] expectedTimeOrderDevice = places.clone();
    int index = 0;
    int expectedPrecipitation = startPrecipitation;
    double expectedTemperature = startTemperature;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData);

        int cnt = 0;
        while (resultSet.next()) {
          Long actualTimeStamp = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertEquals(
              cnt < 10 ? expectedOrderDevice[index] : expectedTimeOrderDevice[index], actualDevice);
          assertEquals(
              Optional.of(cnt < 10 ? shareTime : deviceToUniqueTimestamp.get(actualDevice)).get(),
              actualTimeStamp);

          int actualPrecipitation = resultSet.getInt(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(expectedPrecipitation + actualDevice.hashCode(), actualPrecipitation);
          assertTrue((expectedTemperature + actualDevice.hashCode()) - actualTemperature < 0.00001);

          cnt++;
          if (cnt == 10) {
            index = 0;
            expectedTemperature++;
            expectedPrecipitation++;
          } else {
            index++;
          }
        }
        assertEquals(20, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void orderByTimeDeviceTest2() {
    String sql = "SELECT * FROM root.** ORDER BY TIME ASC,DEVICE ASC ALIGN BY DEVICE";
    Object[] expectedOrderDevice = Arrays.stream(places.clone()).sorted().toArray();
    String[] expectedTimeOrderDevice = places.clone();
    int index = 0;
    int expectedPrecipitation = startPrecipitation;
    double expectedTemperature = startTemperature;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData);

        int cnt = 0;
        while (resultSet.next()) {
          Long actualTimeStamp = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertEquals(
              cnt < 10 ? expectedOrderDevice[index] : expectedTimeOrderDevice[index], actualDevice);
          assertEquals(
              Optional.of(cnt < 10 ? shareTime : deviceToUniqueTimestamp.get(actualDevice)).get(),
              actualTimeStamp);

          int actualPrecipitation = resultSet.getInt(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(expectedPrecipitation + actualDevice.hashCode(), actualPrecipitation);
          assertTrue((expectedTemperature + actualDevice.hashCode()) - actualTemperature < 0.00001);

          cnt++;
          if (cnt == 10) {
            index = 0;
            expectedTemperature++;
            expectedPrecipitation++;
          } else {
            index++;
          }
        }
        assertEquals(20, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void orderByTimeDeviceTest3() {
    String sql = "SELECT * FROM root.** ORDER BY TIME DESC,DEVICE DESC ALIGN BY DEVICE";
    Object[] expectedOrderDevice =
        Arrays.stream(places.clone()).sorted(Comparator.reverseOrder()).toArray();
    String[] expectedTimeOrderDevice = places.clone();
    arrayReverse(expectedTimeOrderDevice);
    int index = 0;
    int expectedPrecipitation = startPrecipitation + 1;
    double expectedTemperature = startTemperature + 1;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData);

        int cnt = 0;
        while (resultSet.next()) {
          Long actualTimeStamp = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertEquals(
              cnt >= 10 ? expectedOrderDevice[index] : expectedTimeOrderDevice[index],
              actualDevice);
          assertEquals(
              Optional.of(cnt >= 10 ? shareTime : deviceToUniqueTimestamp.get(actualDevice)).get(),
              actualTimeStamp);

          int actualPrecipitation = resultSet.getInt(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(expectedPrecipitation + actualDevice.hashCode(), actualPrecipitation);
          assertTrue((expectedTemperature + actualDevice.hashCode()) - actualTemperature < 0.00001);

          cnt++;
          if (cnt == 10) {
            index = 0;
            expectedTemperature--;
            expectedPrecipitation--;
          } else {
            index++;
          }
        }
        assertEquals(20, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void orderByTimeDeviceTest4() {
    String sql = "SELECT * FROM root.** ORDER BY TIME DESC,DEVICE ASC ALIGN BY DEVICE";
    Object[] expectedOrderDevice = Arrays.stream(places.clone()).sorted().toArray();
    String[] expectedTimeOrderDevice = places.clone();
    arrayReverse(expectedTimeOrderDevice);
    int index = 0;
    int expectedPrecipitation = startPrecipitation + 1;
    double expectedTemperature = startTemperature + 1;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData);

        int cnt = 0;
        while (resultSet.next()) {
          Long actualTimeStamp = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          assertEquals(
              cnt >= 10 ? expectedOrderDevice[index] : expectedTimeOrderDevice[index],
              actualDevice);
          assertEquals(
              Optional.of(cnt >= 10 ? shareTime : deviceToUniqueTimestamp.get(actualDevice)).get(),
              actualTimeStamp);

          int actualPrecipitation = resultSet.getInt(3);
          double actualTemperature = resultSet.getDouble(4);
          assertEquals(expectedPrecipitation + actualDevice.hashCode(), actualPrecipitation);
          assertTrue((expectedTemperature + actualDevice.hashCode()) - actualTemperature < 0.00001);

          cnt++;
          if (cnt == 10) {
            index = 0;
            expectedTemperature--;
            expectedPrecipitation--;
          } else {
            index++;
          }
        }
        assertEquals(20, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
