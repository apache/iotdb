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

import org.apache.iotdb.jdbc.IoTDBSQLException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class JDBCExample {

  public static void main(String[] args) throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667", "root", "root");
        Statement statement = connection.createStatement()) {

      // set JDBC fetchSize
      statement.setFetchSize(10000);

      int loop = 100;
      long minRunTime = Long.MAX_VALUE;
      long maxRunTime = Long.MIN_VALUE;
      long totalRunTime = 0;

      for (int i = 0; i < loop; i++) {
        String sql = getSQL2(50);
        System.out.println(sql);
        long startTime = System.currentTimeMillis();

        ResultSet resultSet = statement.executeQuery(sql);

        // outputResult(resultSet);
        while (resultSet.next()) {}

        resultSet.close();
        long runTime = System.currentTimeMillis() - startTime;
        minRunTime = Math.min(minRunTime, runTime);
        maxRunTime = Math.max(maxRunTime, runTime);
        totalRunTime += runTime;
      }
      double avgTime = 1.0 * totalRunTime / loop;
      System.out.println("Avg run time : " + avgTime + "ms");
      System.out.println("Max run time : " + maxRunTime + "ms");
      System.out.println("Min run time : " + minRunTime + "ms");
    } catch (IoTDBSQLException e) {
      System.out.println(e.getMessage());
    }
  }

  // single device
  private static String getSQL1() {
    Random random = new Random(System.currentTimeMillis());
    int device_num = random.nextInt(100);
    return String.format("select * from root.test.g_0.d_%d", device_num);
  }

  // 10 device
  private static String getSQL2(int deviceNum) {
    Random random = new Random(System.currentTimeMillis());
    StringBuilder SQLBuilder = new StringBuilder();
    SQLBuilder.append("select s_0 from ");
    for (int i = 0; i < deviceNum; i++) {
      SQLBuilder.append(String.format("root.test.g_0.d_%d", random.nextInt(100)));
      if (i < deviceNum - 1) {
        SQLBuilder.append(", ");
      }
    }
    return SQLBuilder.toString();
  }

  private static String getSQL3(int deviceNum) {
    Random random = new Random(System.currentTimeMillis());
    StringBuilder SQLBuilder = new StringBuilder();
    Set<String> devices = new HashSet<>();
    SQLBuilder.append("select s_0 from ");
    for (int i = 0; i < deviceNum; i++) {
      String device = String.format("root.test.g_0.d_%d", random.nextInt(100));
      while (devices.contains(device)) {
        device = String.format("root.test.g_0.d_%d", random.nextInt(100));
      }
      devices.add(device);
      SQLBuilder.append(device);
      if (i < deviceNum - 1) {
        SQLBuilder.append(", ");
      }
    }
    return SQLBuilder.toString();
  }

  private static void outputResult(ResultSet resultSet) throws SQLException {
    if (resultSet != null) {
      System.out.println("--------------------------");
      final ResultSetMetaData metaData = resultSet.getMetaData();
      final int columnCount = metaData.getColumnCount();
      for (int i = 0; i < columnCount; i++) {
        System.out.print(metaData.getColumnLabel(i + 1) + " ");
      }
      System.out.println();
      while (resultSet.next()) {
        for (int i = 1; ; i++) {
          System.out.print(resultSet.getString(i));
          if (i < columnCount) {
            System.out.print(", ");
          } else {
            System.out.println();
            break;
          }
        }
      }
      System.out.println("--------------------------\n");
    }
  }
}
