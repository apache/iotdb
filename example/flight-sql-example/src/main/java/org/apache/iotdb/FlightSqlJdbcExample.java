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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Example demonstrating how to query IoTDB via Arrow Flight SQL JDBC driver.
 *
 * <p>The Arrow Flight SQL JDBC driver ({@code org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver})
 * provides a standard JDBC interface over the Arrow Flight SQL protocol, allowing you to use
 * familiar JDBC APIs with Arrow's high-performance columnar data transfer.
 *
 * <p>Before running this example, make sure IoTDB is running with Arrow Flight SQL enabled:
 *
 * <ul>
 *   <li>Set <code>enable_arrow_flight_sql_service=true</code> in iotdb-system.properties
 *   <li>Default Flight SQL port is 8904
 * </ul>
 */
public class FlightSqlJdbcExample {

  private static final String HOST = "127.0.0.1";
  private static final int PORT = 8904;
  private static final String USER = "root";
  private static final String PASSWORD = "root";

  public static void main(String[] args) throws Exception {
    // The Arrow Flight SQL JDBC driver URL format
    String jdbcUrl =
        String.format(
            "jdbc:arrow-flight-sql://%s:%d?useEncryption=false&useSystemTrustStore=false",
            HOST, PORT);

    // Set connection properties
    Properties properties = new Properties();
    properties.put("user", USER);
    properties.put("password", PASSWORD);

    // Load the Arrow Flight SQL JDBC Driver
    Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");

    try (Connection connection = DriverManager.getConnection(jdbcUrl, properties)) {
      System.out.println("=== Connected to IoTDB via Arrow Flight SQL JDBC ===");
      System.out.println("URL: " + jdbcUrl);

      try (Statement statement = connection.createStatement()) {
        // 1. Create database and table
        System.out.println("\n--- Setting up test data ---");
        statement.execute("CREATE DATABASE IF NOT EXISTS jdbc_flight_db");
        statement.execute(
            "CREATE TABLE IF NOT EXISTS jdbc_flight_db.sensor_data ("
                + "region STRING TAG, "
                + "device_id STRING TAG, "
                + "temperature FLOAT FIELD, "
                + "humidity DOUBLE FIELD, "
                + "status BOOLEAN FIELD)");

        // 2. Insert data
        statement.execute(
            "INSERT INTO jdbc_flight_db.sensor_data"
                + "(time, region, device_id, temperature, humidity, status) "
                + "VALUES(1, 'north', 'dev001', 25.5, 60.2, true)");
        statement.execute(
            "INSERT INTO jdbc_flight_db.sensor_data"
                + "(time, region, device_id, temperature, humidity, status) "
                + "VALUES(2, 'north', 'dev001', 26.1, 58.5, true)");
        statement.execute(
            "INSERT INTO jdbc_flight_db.sensor_data"
                + "(time, region, device_id, temperature, humidity, status) "
                + "VALUES(3, 'south', 'dev002', 30.2, 45.0, false)");
        statement.execute(
            "INSERT INTO jdbc_flight_db.sensor_data"
                + "(time, region, device_id, temperature, humidity, status) "
                + "VALUES(4, 'south', 'dev002', 31.5, 42.3, false)");
        System.out.println("Test data inserted successfully.");

        // 3. Query: Full table scan
        System.out.println("\n=== Query 1: Full table scan (JDBC ResultSet) ===");
        try (ResultSet rs =
            statement.executeQuery(
                "SELECT time, region, device_id, temperature, humidity, status "
                    + "FROM jdbc_flight_db.sensor_data ORDER BY time")) {
          printResultSet(rs);
        }

        // 4. Filtered query
        System.out.println("\n=== Query 2: Filtered query (region = 'north') ===");
        try (ResultSet rs =
            statement.executeQuery(
                "SELECT time, device_id, temperature, humidity "
                    + "FROM jdbc_flight_db.sensor_data WHERE region = 'north' ORDER BY time")) {
          printResultSet(rs);
        }

        // 5. Aggregation query
        System.out.println("\n=== Query 3: Aggregation ===");
        try (ResultSet rs =
            statement.executeQuery(
                "SELECT region, COUNT(*) as cnt, AVG(temperature) as avg_temp "
                    + "FROM jdbc_flight_db.sensor_data GROUP BY region ORDER BY region")) {
          printResultSet(rs);
        }

        // 6. Cleanup
        statement.execute("DROP DATABASE IF EXISTS jdbc_flight_db");
      }

      System.out.println("\n=== JDBC Example completed successfully ===");
    }
  }

  /** Print a JDBC ResultSet in table format. */
  private static void printResultSet(ResultSet rs) throws SQLException {
    ResultSetMetaData metaData = rs.getMetaData();
    int columnCount = metaData.getColumnCount();

    // Print column headers
    StringBuilder header = new StringBuilder();
    StringBuilder separator = new StringBuilder();
    for (int i = 1; i <= columnCount; i++) {
      header.append(String.format("%-20s", metaData.getColumnName(i)));
      separator.append("--------------------");
    }
    System.out.println(header);
    System.out.println(separator);

    // Print rows
    int rowCount = 0;
    while (rs.next()) {
      StringBuilder row = new StringBuilder();
      for (int i = 1; i <= columnCount; i++) {
        Object value = rs.getObject(i);
        row.append(String.format("%-20s", value == null ? "null" : value.toString()));
      }
      System.out.println(row);
      rowCount++;
    }
    System.out.println("Total rows: " + rowCount);
  }
}
