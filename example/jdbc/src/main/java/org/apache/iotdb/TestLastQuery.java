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
import java.sql.SQLException;
import java.sql.Statement;

public class TestLastQuery {
  public static void main(String[] args) throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    try (Connection connection =
            DriverManager.getConnection(
                "jdbc:iotdb://127.0.0.1:6667?version=V_1_0", "root", "root");
        Statement statement = connection.createStatement()) {

      // set JDBC fetchSize
      statement.setFetchSize(10000);
      int loop = 3;
      long maxRunTime = Long.MIN_VALUE;
      long minRunTime = Long.MAX_VALUE;
      long totalRunTime = 0;

      for (int target = 0; target < 11; target++) {
        String sql = String.format("select last s%d from root.sg", target);
        for (int i = 0; i < loop; i++) {
          long startTime = System.currentTimeMillis();
          ResultSet resultSet = statement.executeQuery(sql);
          while (resultSet.next()) {}

          resultSet.close();
          long runTime = System.currentTimeMillis() - startTime;
          maxRunTime = Math.max(maxRunTime, runTime);
          minRunTime = Math.min(minRunTime, runTime);
          totalRunTime += runTime;
        }

        double avgRunTime = 1.0 * totalRunTime / loop;
        System.out.println("SQL is: " + sql);
        System.out.println("Min run time is: " + minRunTime + "ms");
        System.out.println("Max run time is: " + maxRunTime + "ms");
        System.out.println("Avg run time is: " + avgRunTime + "ms");
      }

    } catch (IoTDBSQLException e) {
      System.out.println("error");
      System.out.println(e.getMessage());
    }
  }
}
