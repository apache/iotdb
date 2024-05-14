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

package org.apache.iotdb;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

public class QiMingQueryClient {

  private static final SimpleDateFormat DATE_FORMATTER =
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

  static {
    DATE_FORMATTER.setTimeZone(TimeZone.getTimeZone("GMT+00:00"));
  }

  private static final String[] PREPARED_SQL =
      new String[] {
        "SELECT name, driver, max_time(latitude), last_value(latitude), last_value(longitude) FROM readings WHERE fleet = 'South' and name IS NOT NULL GROUP BY name, driver",
        "SELECT name, driver, max_time(fuel_state), last_value(fuel_state) FROM diagnostics WHERE fleet = 'South' and fuel_state <= 0.1 and name IS NOT NULL GROUP BY name, driver",
        "SELECT ts, name, driver, current_load, load_capacity FROM (SELECT name, driver, max_time(current_load) as ts, last_value(current_load) as current_load, load_capacity FROM diagnostics WHERE fleet = 'South' GROUP BY name, driver, load_capacity) WHERE current_load >= (0.9 * load_capacity)",
        "SELECT name, driver FROM readings WHERE time > '2016-01-01T12:07:21Z' AND time <= '2016-01-01T12:17:21Z' AND fleet = 'West' GROUP BY name, driver HAVING avg(velocity) < 1",
        "SELECT name, driver FROM (SELECT name, driver, avg(velocity) as mean_velocity FROM readings WHERE fleet = 'West' GROUP BY name, driver, TIME(['2016-03-30T13:46:34Z', '2016-03-30T17:46:34Z'), 10m)) WHERE mean_velocity > 1 GROUP BY name, driver HAVING count(*) > 22",
        "SELECT name, driver FROM (SELECT name, driver, avg(velocity) as mean_velocity FROM readings WHERE fleet = 'West' GROUP BY name, driver, TIME(['2016-03-30T12:31:37Z', '2016-03-31T12:31:37Z'), 10m)) WHERE mean_velocity > 1 GROUP BY name, driver HAVING count(*) > 60",
        "SELECT avg(fuel_consumption) as avg_fuel_consumption, avg(nominal_fuel_consumption) as nominal_fuel_consumption FROM readings GROUP BY fleet",
        "SELECT fleet, name, driver, count(avg_velocity) / 6 as hours_driven FROM (SELECT fleet, name, driver, avg(velocity) as avg_velocity FROM readings GROUP BY fleet, name, driver, TIME(['2016-01-01T00:00:00Z', '2016-01-10T00:00:00Z'), 10m)) GROUP BY fleet, name, driver, TIME(['2016-01-01T00:00:00Z', '2016-01-10T00:00:00Z'), 1d)",
        "SELECT name, avg(daily_drive) FROM (SELECT name, time_duration(flag) as daily_drive FROM (SELECT name, avg(velocity) > 5 as flag FROM readings GROUP BY name, TIME(['2016-03-23T00:00:00Z', '2016-04-02T00:00:00Z'), 10m)) GROUP BY name, TIME(['2016-03-23T00:00:00Z', '2016-04-02T00:00:00Z'), 1d), variation(flag) HAVING first_value(flag) = true)",
        "SELECT fleet, model, load_capacity, avg(ml / load_capacity) FROM (SELECT fleet, model, load_capacity, avg(current_load) AS ml FROM diagnostics WHERE name IS NOT NULL GROUP BY fleet, model, name, load_capacity) GROUP BY fleet, model, load_capacity",
        "SELECT model, fleet, count(avg_status) / 144 FROM (SELECT model, fleet, avg(status) AS avg_status FROM diagnostics GROUP BY model, fleet, name, TIME(['2016-01-01T00:00:00Z', '2016-01-10T00:00:00Z'), 10m)) WHERE avg_status < 1 GROUP BY model, fleet, TIME(['2016-01-01T00:00:00Z', '2016-01-10T00:00:00Z'), 1d)",
        "SELECT model, count(a1) FROM (SELECT model, name, first_value(active) as a1 FROM(SELECT model, name, (COUNT(case when status > 0 then 1 else null) / COUNT(*)) > 0.5 as active FROM diagnostics WHERE status > 0 GROUP BY name, TIME(['2016-01-01T00:00:00Z', '2016-01-10T00:00:00Z'), 10m)) GROUP BY model, name, VARIATION(active) HAVING first_value(active) = true)"
      };

  private static final String[] PREFIX =
      new String[] {
        // SELECT name, driver, max_time(latitude), last_value(latitude), last_value(longitude) FROM
        // readings
        PREPARED_SQL[0].substring(0, PREPARED_SQL[0].indexOf(" WHERE")),
        // SELECT name, driver, max_time(fuel_state), last_value(fuel_state) FROM diagnostics
        PREPARED_SQL[1].substring(0, PREPARED_SQL[1].indexOf(" WHERE")),
        // SELECT ts, name, driver, current_load, load_capacity
        PREPARED_SQL[2].substring(0, PREPARED_SQL[2].indexOf(" FROM (SELECT")),
        // SELECT name, driver FROM readings
        PREPARED_SQL[3].substring(0, PREPARED_SQL[3].indexOf(" WHERE")),
        // SELECT name, driver FROM (SELECT name, driver, avg(velocity) as mean_velocity
        PREPARED_SQL[4].substring(0, PREPARED_SQL[4].indexOf(" FROM readings")),
        // SELECT name, driver FROM (SELECT name, driver, avg(velocity) as mean_velocity
        PREPARED_SQL[5].substring(0, PREPARED_SQL[5].indexOf(" FROM readings")),
        // SELECT avg(fuel_consumption) as avg_fuel_consumption, avg(nominal_fuel_consumption) as
        // nominal_fuel_consumption
        PREPARED_SQL[6].substring(0, PREPARED_SQL[6].indexOf(" FROM readings")),
        // SELECT fleet, name, driver, count(avg_velocity) / 6 as hours_driven
        PREPARED_SQL[7].substring(0, PREPARED_SQL[7].indexOf(" FROM (SELECT")),
        // SELECT name, avg(daily_drive)
        PREPARED_SQL[8].substring(0, PREPARED_SQL[8].indexOf(" FROM (SELECT")),
        // SELECT fleet, model, load_capacity, avg(ml / load_capacity)
        PREPARED_SQL[9].substring(0, PREPARED_SQL[9].indexOf(" FROM (SELECT")),
        // SELECT model, fleet, count(avg_status) / 144
        PREPARED_SQL[10].substring(0, PREPARED_SQL[10].indexOf(" FROM (SELECT")),
        // SELECT model, count(a1)
        PREPARED_SQL[11].substring(0, PREPARED_SQL[11].indexOf(" FROM (SELECT"))
      };

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException, ParseException {
    String host = args[0];
    Session session =
        new Session.Builder()
            .host(host)
            .port(6667)
            .username("root")
            .password("root")
            .version(Version.V_1_0)
            .fetchSize(10_000)
            .timeOut(6_000_000)
            .build();
    session.open(false);

    int queryCount = Integer.parseInt(args[1]);
    if (queryCount <= 0) {
      System.out.println("Query Count should be larger than 0.");
      return;
    }
    List<Long> timeCostList = new ArrayList<>();

    String sql = args[2];
    String queryType = "Unknown";
    if (sql.startsWith(PREFIX[0])) {
      queryType = "last-loc";
      int startIndex = sql.indexOf("fleet = '");
      if (startIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      int endIndex = sql.indexOf("' and", startIndex);
      if (endIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      String fleet = sql.substring(startIndex + 9, endIndex);
      for (int i = 0; i < queryCount; i++) {
        long startTimeNano = System.nanoTime();
        try (SessionDataSet dataSet =
            session.executeQueryStatement(
                String.format(
                    "SELECT max_time(latitude), last_value(latitude), last_value(longitude) FROM root.readings.%s.**  align by device",
                    fleet))) {
          SessionDataSet.DataIterator iterator = dataSet.iterator();
          long count = 0;
          while (iterator.next()) {
            count++;
          }
          timeCostList.add(System.nanoTime() - startTimeNano);
        }
      }
    } else if (sql.startsWith(PREFIX[1])) {
      queryType = "low-fuel";
      int startIndex = sql.indexOf("fleet = '");
      if (startIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      int endIndex = sql.indexOf("' and", startIndex);
      if (endIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      String fleet = sql.substring(startIndex + 9, endIndex);
      for (int i = 0; i < queryCount; i++) {
        long startTimeNano = System.nanoTime();
        try (SessionDataSet dataSet =
            session.executeQueryStatement(
                String.format(
                    "SELECT last_value(fuel_state) FROM root.diagnostics.%s.** WHERE fuel_state <= 0.1 align by device;",
                    fleet))) {
          SessionDataSet.DataIterator iterator = dataSet.iterator();
          long count = 0;
          while (iterator.next()) {
            count++;
          }
          timeCostList.add(System.nanoTime() - startTimeNano);
        }
      }
    } else if (sql.startsWith(PREFIX[2])) {
      queryType = "high-load";

      int startIndex = sql.indexOf("fleet = '");
      if (startIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      int endIndex = sql.indexOf("' GROUP BY ", startIndex);
      if (endIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      String fleet = sql.substring(startIndex + 9, endIndex);

      for (int i = 0; i < queryCount; i++) {
        long startTimeNano = System.nanoTime();
        try (SessionDataSet dataSet = session.tsbsIoTHighLoad(fleet)) {
          SessionDataSet.DataIterator iterator = dataSet.iterator();
          long count = 0;
          while (iterator.next()) {
            count++;
          }
          timeCostList.add(System.nanoTime() - startTimeNano);
        }
      }
    } else if (sql.startsWith(PREFIX[3])) {
      queryType = "stationary-trucks";
      int startIndex = sql.indexOf("fleet = '");
      if (startIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      int endIndex = sql.indexOf("' GROUP BY ", startIndex);
      if (endIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      String fleet = sql.substring(startIndex + 9, endIndex);

      startIndex = sql.indexOf("time > '");
      if (startIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      endIndex = sql.indexOf("'", startIndex + 8);
      if (endIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      String startTimeString = sql.substring(startIndex + 8, endIndex);

      startIndex = sql.indexOf("time <= '");
      if (startIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      endIndex = sql.indexOf("'", startIndex + 9);
      if (endIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      String endTimeString = sql.substring(startIndex + 9, endIndex);

      long startTime = DATE_FORMATTER.parse(startTimeString).getTime();
      long endTime = DATE_FORMATTER.parse(endTimeString).getTime();

      for (int i = 0; i < queryCount; i++) {
        long startTimeNano = System.nanoTime();
        try (SessionDataSet dataSet =
            session.executeQueryStatement(
                String.format(
                    "select avg(velocity) from root.readings.%s.** where time >= %d and time < %d having avg(velocity) < 1 align by device",
                    fleet, startTime, endTime))) {
          SessionDataSet.DataIterator iterator = dataSet.iterator();
          long count = 0;
          while (iterator.next()) {
            count++;
          }
          timeCostList.add(System.nanoTime() - startTimeNano);
        }
      }
    } else if (sql.startsWith(PREFIX[4]) && (sql.endsWith("22") || sql.endsWith("60"))) { // 4 & 5

      int startIndex = sql.indexOf("fleet = '");
      if (startIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      int endIndex = sql.indexOf("' GROUP BY ", startIndex);
      if (endIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      String fleet = sql.substring(startIndex + 9, endIndex);

      startIndex = sql.indexOf("TIME(['");
      if (startIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      endIndex = sql.indexOf("'", startIndex + 7);
      if (endIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      String startTimeString = sql.substring(startIndex + 7, endIndex);

      startIndex = sql.indexOf(", '", endIndex);
      if (startIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      endIndex = sql.indexOf("'", startIndex + 3);
      if (endIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      String endTimeString = sql.substring(startIndex + 3, endIndex);

      long startTime = DATE_FORMATTER.parse(startTimeString).getTime();
      long endTime = DATE_FORMATTER.parse(endTimeString).getTime();

      if (sql.endsWith("22")) {
        queryType = "long-driving-sessions";
        for (int i = 0; i < queryCount; i++) {
          long startTimeNano = System.nanoTime();
          try (SessionDataSet dataSet =
              session.tsbsIoTLongDrivingSessions(fleet, startTime, endTime)) {
            SessionDataSet.DataIterator iterator = dataSet.iterator();
            long count = 0;
            while (iterator.next()) {
              count++;
            }
            timeCostList.add(System.nanoTime() - startTimeNano);
          }
        }
      } else {
        queryType = "long-daily-sessions";
        for (int i = 0; i < queryCount; i++) {
          long startTimeNano = System.nanoTime();
          try (SessionDataSet dataSet =
              session.tsbsIoTLongDailySessions(fleet, startTime, endTime)) {
            SessionDataSet.DataIterator iterator = dataSet.iterator();
            long count = 0;
            while (iterator.next()) {
              count++;
            }
            timeCostList.add(System.nanoTime() - startTimeNano);
          }
        }
      }
    } else if (sql.startsWith(PREFIX[6])) {
      queryType = "avg-vs-projected-fuel-consumption";
      for (int i = 0; i < queryCount; i++) {
        long startTimeNano = System.nanoTime();
        try (SessionDataSet dataSet = session.tsbsIoTAvgVsProjectedFuelConsumption()) {
          SessionDataSet.DataIterator iterator = dataSet.iterator();
          long count = 0;
          while (iterator.next()) {
            count++;
          }
          timeCostList.add(System.nanoTime() - startTimeNano);
        }
      }
    } else if (sql.startsWith(PREFIX[7])) {
      queryType = "avg-daily-driving-duration";

      int startIndex = sql.indexOf("TIME(['");
      if (startIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      int endIndex = sql.indexOf("'", startIndex + 7);
      if (endIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      String startTimeString = sql.substring(startIndex + 7, endIndex);

      startIndex = sql.indexOf(", '", endIndex);
      if (startIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      endIndex = sql.indexOf("'", startIndex + 3);
      if (endIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      String endTimeString = sql.substring(startIndex + 3, endIndex);

      long startTime = DATE_FORMATTER.parse(startTimeString).getTime();
      long endTime = DATE_FORMATTER.parse(endTimeString).getTime();

      for (int i = 0; i < queryCount; i++) {
        long startTimeNano = System.nanoTime();
        try (SessionDataSet dataSet = session.tsbsIoTAvgDailyDrivingDuration(startTime, endTime)) {
          SessionDataSet.DataIterator iterator = dataSet.iterator();
          long count = 0;
          while (iterator.next()) {
            count++;
          }
          timeCostList.add(System.nanoTime() - startTimeNano);
        }
      }
    } else if (sql.startsWith(PREFIX[8])) {
      queryType = "avg-daily-driving-session";

      int startIndex = sql.indexOf("TIME(['");
      if (startIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      int endIndex = sql.indexOf("'", startIndex + 7);
      if (endIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      String startTimeString = sql.substring(startIndex + 7, endIndex);

      startIndex = sql.indexOf(", '", endIndex);
      if (startIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      endIndex = sql.indexOf("'", startIndex + 3);
      if (endIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      String endTimeString = sql.substring(startIndex + 3, endIndex);

      long startTime = DATE_FORMATTER.parse(startTimeString).getTime();
      long endTime = DATE_FORMATTER.parse(endTimeString).getTime();

      for (int i = 0; i < queryCount; i++) {
        long startTimeNano = System.nanoTime();
        try (SessionDataSet dataSet = session.tsbsIoTAvgDailyDrivingSession(startTime, endTime)) {
          SessionDataSet.DataIterator iterator = dataSet.iterator();
          long count = 0;
          while (iterator.next()) {
            count++;
          }
          timeCostList.add(System.nanoTime() - startTimeNano);
        }
      }
    } else if (sql.startsWith(PREFIX[9])) {
      queryType = "avg-load";

      for (int i = 0; i < queryCount; i++) {
        long startTimeNano = System.nanoTime();
        try (SessionDataSet dataSet = session.tsbsIoTAvgLoad()) {
          SessionDataSet.DataIterator iterator = dataSet.iterator();
          long count = 0;
          while (iterator.next()) {
            count++;
          }
          timeCostList.add(System.nanoTime() - startTimeNano);
        }
      }
    } else if (sql.startsWith(PREFIX[10])) {
      queryType = "daily-activity";

      int startIndex = sql.indexOf("TIME(['");
      if (startIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      int endIndex = sql.indexOf("'", startIndex + 7);
      if (endIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      String startTimeString = sql.substring(startIndex + 7, endIndex);

      startIndex = sql.indexOf(", '", endIndex);
      if (startIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      endIndex = sql.indexOf("'", startIndex + 3);
      if (endIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      String endTimeString = sql.substring(startIndex + 3, endIndex);

      long startTime = DATE_FORMATTER.parse(startTimeString).getTime();
      long endTime = DATE_FORMATTER.parse(endTimeString).getTime();

      for (int i = 0; i < queryCount; i++) {
        long startTimeNano = System.nanoTime();
        try (SessionDataSet dataSet = session.tsbsIoTDailyActivity(startTime, endTime)) {
          SessionDataSet.DataIterator iterator = dataSet.iterator();
          long count = 0;
          while (iterator.next()) {
            count++;
          }
          timeCostList.add(System.nanoTime() - startTimeNano);
        }
      }
    } else if (sql.startsWith(PREFIX[11])) {
      queryType = "breakdown-frequency";

      int startIndex = sql.indexOf("TIME(['");
      if (startIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      int endIndex = sql.indexOf("'", startIndex + 7);
      if (endIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      String startTimeString = sql.substring(startIndex + 7, endIndex);

      startIndex = sql.indexOf(", '", endIndex);
      if (startIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      endIndex = sql.indexOf("'", startIndex + 3);
      if (endIndex < 0) {
        throw new IllegalArgumentException("Wrong sql: " + sql);
      }
      String endTimeString = sql.substring(startIndex + 3, endIndex);

      long startTime = DATE_FORMATTER.parse(startTimeString).getTime();
      long endTime = DATE_FORMATTER.parse(endTimeString).getTime();

      for (int i = 0; i < queryCount; i++) {
        long startTimeNano = System.nanoTime();
        try (SessionDataSet dataSet = session.tsbsIoTBreakdownFrequency(startTime, endTime)) {
          SessionDataSet.DataIterator iterator = dataSet.iterator();
          long count = 0;
          while (iterator.next()) {
            count++;
          }
          timeCostList.add(System.nanoTime() - startTimeNano);
        }
      }
    } else {
      for (int i = 0; i < queryCount; i++) {
        long startTimeNano = System.nanoTime();
        try (SessionDataSet dataSet = session.executeQueryStatement(sql)) {
          SessionDataSet.DataIterator iterator = dataSet.iterator();
          long count = 0;
          while (iterator.next()) {
            count++;
          }
          timeCostList.add(System.nanoTime() - startTimeNano);
        }
      }
    }

    session.close();

    printQueryTimeCost(queryType, queryCount, timeCostList);
  }

  private static void printQueryTimeCost(
      String queryType, int queryCount, List<Long> timeCostList) {
    System.out.printf("Running %s query %d times.%n", queryType, queryCount);
    System.out.printf("Time cost of first query is %d ms.%n", timeCostList.get(0) / 1_000_000L);
    long sum = 0;
    for (Long cost : timeCostList) {
      sum += cost;
    }
    System.out.printf("Avg time cost is %d ms.%n", sum / timeCostList.size() / 1_000_000L);
    long median;
    int index = timeCostList.size() / 2;
    if (timeCostList.size() % 2 == 0) {
      median = (timeCostList.get(index - 1) + timeCostList.get(index)) / 2 / 1_000_000L;
    } else {
      median = timeCostList.get(index) / 1_000_000L;
    }
    System.out.printf("Median time cost is %d ms.%n", median);
  }
}
