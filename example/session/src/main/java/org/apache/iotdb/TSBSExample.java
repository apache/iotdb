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

import java.util.ArrayList;
import java.util.List;

public class TSBSExample {

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
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

    String queryType = args[1];

    int queryCount = Integer.parseInt(args[2]);
    if (queryCount <= 0) {
      System.out.println("Query Count should be larger than 0.");
      return;
    }
    List<Long> timeCostList = new ArrayList<>();

    switch (queryType) {
      case "last-loc":
        for (int i = 0; i < queryCount; i++) {
          long startTimeNano = System.nanoTime();
          try (SessionDataSet dataSet =
              session.executeQueryStatement(
                  String.format(
                      "SELECT max_time(latitude), last_value(latitude), last_value(longitude) FROM root.readings.%s.**  align by device",
                      args[3]))) {
            SessionDataSet.DataIterator iterator = dataSet.iterator();
            long count = 0;
            while (iterator.next()) {
              count++;
            }
            timeCostList.add(System.nanoTime() - startTimeNano);
          }
        }
        break;
      case "low-fuel":
        for (int i = 0; i < queryCount; i++) {
          long startTimeNano = System.nanoTime();
          try (SessionDataSet dataSet =
              session.executeQueryStatement(
                  String.format(
                      "SELECT last_value(fuel_state) FROM root.diagnostics.%s.** WHERE fuel_state <= 0.1 align by device;",
                      args[3]))) {
            SessionDataSet.DataIterator iterator = dataSet.iterator();
            long count = 0;
            while (iterator.next()) {
              count++;
            }
            timeCostList.add(System.nanoTime() - startTimeNano);
          }
        }
        break;
      case "high-load":
        for (int i = 0; i < queryCount; i++) {
          long startTimeNano = System.nanoTime();
          try (SessionDataSet dataSet = session.tsbsIoTHighLoad(args[3])) {
            SessionDataSet.DataIterator iterator = dataSet.iterator();
            long count = 0;
            while (iterator.next()) {
              count++;
            }
            timeCostList.add(System.nanoTime() - startTimeNano);
          }
        }
        break;
      case "stationary-trucks":
        for (int i = 0; i < queryCount; i++) {
          long startTimeNano = System.nanoTime();
          try (SessionDataSet dataSet =
              session.executeQueryStatement(
                  String.format(
                      "select avg(velocity) from root.readings.%s.** where time >= %d and time < %d having avg(velocity) < 1 align by device",
                      args[3], Long.parseLong(args[4]), Long.parseLong(args[5])))) {
            SessionDataSet.DataIterator iterator = dataSet.iterator();
            long count = 0;
            while (iterator.next()) {
              count++;
            }
            timeCostList.add(System.nanoTime() - startTimeNano);
          }
        }
        break;
      case "long-driving-sessions":
        for (int i = 0; i < queryCount; i++) {
          long startTimeNano = System.nanoTime();
          try (SessionDataSet dataSet =
              session.tsbsIoTLongDrivingSessions(
                  args[3], Long.parseLong(args[4]), Long.parseLong(args[5]))) {
            SessionDataSet.DataIterator iterator = dataSet.iterator();
            long count = 0;
            while (iterator.next()) {
              count++;
            }
            timeCostList.add(System.nanoTime() - startTimeNano);
          }
        }
        break;
      case "long-daily-sessions":
        for (int i = 0; i < queryCount; i++) {
          long startTimeNano = System.nanoTime();
          try (SessionDataSet dataSet =
              session.tsbsIoTLongDailySessions(
                  args[3], Long.parseLong(args[4]), Long.parseLong(args[5]))) {
            SessionDataSet.DataIterator iterator = dataSet.iterator();
            long count = 0;
            while (iterator.next()) {
              count++;
            }
            timeCostList.add(System.nanoTime() - startTimeNano);
          }
        }
        break;
      case "avg-vs-projected-fuel-consumption":
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
        break;
      case "avg-daily-driving-duration":
        for (int i = 0; i < queryCount; i++) {
          long startTimeNano = System.nanoTime();
          try (SessionDataSet dataSet =
              session.tsbsIoTAvgDailyDrivingDuration(
                  Long.parseLong(args[3]), Long.parseLong(args[4]))) {
            SessionDataSet.DataIterator iterator = dataSet.iterator();
            long count = 0;
            while (iterator.next()) {
              count++;
            }
            timeCostList.add(System.nanoTime() - startTimeNano);
          }
        }
        break;
      case "avg-daily-driving-session":
        for (int i = 0; i < queryCount; i++) {
          long startTimeNano = System.nanoTime();
          try (SessionDataSet dataSet =
              session.tsbsIoTAvgDailyDrivingSession(
                  Long.parseLong(args[3]), Long.parseLong(args[4]))) {
            SessionDataSet.DataIterator iterator = dataSet.iterator();
            long count = 0;
            while (iterator.next()) {
              count++;
            }
            timeCostList.add(System.nanoTime() - startTimeNano);
          }
        }
        break;
      case "avg-load":
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
        break;
      case "daily-activity":
        for (int i = 0; i < queryCount; i++) {
          long startTimeNano = System.nanoTime();
          try (SessionDataSet dataSet =
              session.tsbsIoTDailyActivity(Long.parseLong(args[3]), Long.parseLong(args[4]))) {
            SessionDataSet.DataIterator iterator = dataSet.iterator();
            long count = 0;
            while (iterator.next()) {
              count++;
            }
            timeCostList.add(System.nanoTime() - startTimeNano);
          }
        }
        break;
      case "breakdown-frequency":
        for (int i = 0; i < queryCount; i++) {
          long startTimeNano = System.nanoTime();
          try (SessionDataSet dataSet =
              session.tsbsIoTBreakdownFrequency(Long.parseLong(args[3]), Long.parseLong(args[4]))) {
            SessionDataSet.DataIterator iterator = dataSet.iterator();
            long count = 0;
            while (iterator.next()) {
              count++;
            }
            timeCostList.add(System.nanoTime() - startTimeNano);
          }
        }
        break;
      default:
        throw new UnsupportedOperationException(queryType);
    }
    session.close();

    printQueryTimeCost(queryCount, timeCostList);
  }

  private static void printQueryTimeCost(int queryCount, List<Long> timeCostList) {
    System.out.printf("Running query %d times.%n", queryCount);
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
