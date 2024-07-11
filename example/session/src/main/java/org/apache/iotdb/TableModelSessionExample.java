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

public class TableModelSessionExample {

  private static final String LOCAL_HOST = "127.0.0.1";

  public static void main(String[] args) {

    // don't specify database in constructor
    Session session =
        new Session.Builder()
            .host(LOCAL_HOST)
            .port(6667)
            .username("root")
            .password("root")
            .version(Version.V_1_0)
            .sqlDialect("table")
            .build();

    try {
      session.open(false);

      session.executeNonQueryStatement("CREATE DATABASE test1");
      session.executeNonQueryStatement("CREATE DATABASE test2");

      session.executeNonQueryStatement("use test2");

      // or use full qualified table name
      session.executeNonQueryStatement(
          "create table test1.table1(region_id STRING ID, plant_id STRING ID, device_id STRING ID, model STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, humidity DOUBLE MEASUREMENT) with (TTL=3600000)");

      session.executeNonQueryStatement(
          "create table table2(region_id STRING ID, plant_id STRING ID, color STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, speed DOUBLE MEASUREMENT) with (TTL=6600000)");

      // show tables from current database
      try (SessionDataSet dataSet = session.executeQueryStatement("SHOW TABLES")) {
        System.out.println(dataSet.getColumnNames());
        while (dataSet.hasNext()) {
          System.out.println(dataSet.next());
        }
      }

      // show tables by specifying another database
      // using SHOW tables FROM
      try (SessionDataSet dataSet = session.executeQueryStatement("SHOW TABLES FROM test1")) {
        System.out.println(dataSet.getColumnNames());
        while (dataSet.hasNext()) {
          System.out.println(dataSet.next());
        }
      }

    } catch (IoTDBConnectionException e) {
      e.printStackTrace();
    } catch (StatementExecutionException e) {
      e.printStackTrace();
    } finally {
      try {
        session.close();
      } catch (IoTDBConnectionException e) {
        e.printStackTrace();
      }
    }

    // specify database in constructor
    session =
        new Session.Builder()
            .host(LOCAL_HOST)
            .port(6667)
            .username("root")
            .password("root")
            .version(Version.V_1_0)
            .sqlDialect("table")
            .database("test1")
            .build();

    try {
      session.open(false);

      // show tables from current database
      try (SessionDataSet dataSet = session.executeQueryStatement("SHOW TABLES")) {
        System.out.println(dataSet.getColumnNames());
        while (dataSet.hasNext()) {
          System.out.println(dataSet.next());
        }
      }

      // change database to test2
      session.executeNonQueryStatement("use test2");

      // show tables by specifying another database
      // using SHOW tables FROM
      try (SessionDataSet dataSet = session.executeQueryStatement("SHOW TABLES")) {
        System.out.println(dataSet.getColumnNames());
        while (dataSet.hasNext()) {
          System.out.println(dataSet.next());
        }
      }

    } catch (IoTDBConnectionException e) {
      e.printStackTrace();
    } catch (StatementExecutionException e) {
      e.printStackTrace();
    } finally {
      try {
        session.close();
      } catch (IoTDBConnectionException e) {
        e.printStackTrace();
      }
    }
  }
}
