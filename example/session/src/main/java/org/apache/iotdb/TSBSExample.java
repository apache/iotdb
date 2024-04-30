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

    switch (queryType) {
      case "last-loc":
        try (SessionDataSet dataSet =
            session.executeQueryStatement(
                String.format(
                    "SELECT max_time(latitude), last_value(latitude), last_value(longitude) FROM root.readings.%s.**  align by device",
                    args[2]))) {
          SessionDataSet.DataIterator iterator = dataSet.iterator();
          long count = 0;
          while (iterator.next()) {
            count++;
          }
          System.out.println("total lines: " + count);
        }
        break;
      case "low-fuel":
        try (SessionDataSet dataSet =
            session.executeQueryStatement(
                String.format(
                    "SELECT last_value(fuel_state) FROM root.diagnostics.%s.** WHERE fuel_state <= 0.1 align by device;",
                    args[2]))) {
          SessionDataSet.DataIterator iterator = dataSet.iterator();
          long count = 0;
          while (iterator.next()) {
            count++;
          }
          System.out.println("total lines: " + count);
        }
        break;
      case "high-load":
        try (SessionDataSet dataSet = session.tsbsIoTHighLoad(args[2])) {
          SessionDataSet.DataIterator iterator = dataSet.iterator();
          long count = 0;
          while (iterator.next()) {
            count++;
          }
          System.out.println("total lines: " + count);
        }
        break;
      case "stationary-trucks":
        try (SessionDataSet dataSet =
            session.executeQueryStatement(
                String.format(
                    "select avg(velocity) from root.readings.%s.** where time >= %d and time < %d having avg(velocity) < 1 align by device",
                    args[2], Long.parseLong(args[3]), Long.parseLong(args[4])))) {
          SessionDataSet.DataIterator iterator = dataSet.iterator();
          long count = 0;
          while (iterator.next()) {
            count++;
          }
          System.out.println("total lines: " + count);
        }
        break;
      case "long-driving-sessions":
        try (SessionDataSet dataSet =
            session.tsbsIoTLongDrivingSessions(
                args[2], Long.parseLong(args[3]), Long.parseLong(args[4]))) {
          SessionDataSet.DataIterator iterator = dataSet.iterator();
          long count = 0;
          while (iterator.next()) {
            count++;
          }
          System.out.println("total lines: " + count);
        }
        break;
      case "long-daily-sessions":
        try (SessionDataSet dataSet =
            session.tsbsIoTLongDailySessions(
                args[2], Long.parseLong(args[3]), Long.parseLong(args[4]))) {
          SessionDataSet.DataIterator iterator = dataSet.iterator();
          long count = 0;
          while (iterator.next()) {
            count++;
          }
          System.out.println("total lines: " + count);
        }
        break;
      case "avg-vs-projected-fuel-consumption":
        try (SessionDataSet dataSet = session.tsbsIoTAvgVsProjectedFuelConsumption()) {
          SessionDataSet.DataIterator iterator = dataSet.iterator();
          long count = 0;
          while (iterator.next()) {
            count++;
          }
          System.out.println("total lines: " + count);
        }
        break;
      case "avg-daily-driving-duration":
        try (SessionDataSet dataSet =
            session.tsbsIoTAvgDailyDrivingDuration(
                Long.parseLong(args[2]), Long.parseLong(args[3]))) {
          SessionDataSet.DataIterator iterator = dataSet.iterator();
          long count = 0;
          while (iterator.next()) {
            count++;
          }
          System.out.println("total lines: " + count);
        }
        break;
      case "avg-daily-driving-session":
        try (SessionDataSet dataSet =
            session.tsbsIoTAvgDailyDrivingSession(
                Long.parseLong(args[2]), Long.parseLong(args[3]))) {
          SessionDataSet.DataIterator iterator = dataSet.iterator();
          long count = 0;
          while (iterator.next()) {
            count++;
          }
          System.out.println("total lines: " + count);
        }
        break;
      case "avg-load":
        try (SessionDataSet dataSet = session.tsbsIoTAvgLoad()) {
          SessionDataSet.DataIterator iterator = dataSet.iterator();
          long count = 0;
          while (iterator.next()) {
            count++;
          }
          System.out.println("total lines: " + count);
        }
        break;
      case "daily-activity":
        try (SessionDataSet dataSet =
            session.tsbsIoTDailyActivity(Long.parseLong(args[2]), Long.parseLong(args[3]))) {
          SessionDataSet.DataIterator iterator = dataSet.iterator();
          long count = 0;
          while (iterator.next()) {
            count++;
          }
          System.out.println("total lines: " + count);
        }
        break;
      case "breakdown-frequency":
        try (SessionDataSet dataSet =
            session.tsbsIoTBreakdownFrequency(Long.parseLong(args[2]), Long.parseLong(args[3]))) {
          SessionDataSet.DataIterator iterator = dataSet.iterator();
          long count = 0;
          while (iterator.next()) {
            count++;
          }
          System.out.println("total lines: " + count);
        }
        break;
      default:
        throw new UnsupportedOperationException(queryType);
    }

    session.close();
  }
}
