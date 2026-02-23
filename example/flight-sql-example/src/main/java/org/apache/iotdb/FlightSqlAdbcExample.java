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

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Example demonstrating how to query IoTDB using ADBC (Arrow Database Connectivity) with the Flight
 * SQL driver in Java.
 *
 * <p>ADBC provides a database-agnostic API that returns data natively as Apache Arrow columnar
 * format, avoiding the overhead of row-by-row conversion. This is the recommended approach for
 * analytics and data science workloads.
 *
 * <p>Before running this example, make sure IoTDB is running with Arrow Flight SQL enabled:
 *
 * <ul>
 *   <li>Set <code>enable_arrow_flight_sql_service=true</code> in iotdb-system.properties
 *   <li>Default Flight SQL port is 8904
 * </ul>
 */
public class FlightSqlAdbcExample {

  private static final String HOST = "127.0.0.1";
  private static final int PORT = 8904;
  private static final String USER = "root";
  private static final String PASSWORD = "root";

  public static void main(String[] args) throws Exception {
    // 1. Create Arrow memory allocator
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {

      // 2. Configure ADBC Flight SQL driver
      AdbcDriver driver = new FlightSqlDriver(allocator);
      Map<String, Object> dbParams = new HashMap<>();
      dbParams.put(AdbcDriver.PARAM_URI.getKey(), String.format("grpc+tcp://%s:%d", HOST, PORT));
      dbParams.put(AdbcDriver.PARAM_USERNAME.getKey(), USER);
      dbParams.put(AdbcDriver.PARAM_PASSWORD.getKey(), PASSWORD);

      // 3. Open ADBC database and connection
      try (AdbcDatabase db = driver.open(dbParams);
          AdbcConnection connection = db.connect()) {
        System.out.println("=== Connected to IoTDB via ADBC Flight SQL ===");
        System.out.println("URI: grpc+tcp://" + HOST + ":" + PORT);

        // 4. Create database and table, insert data
        System.out.println("\n--- Setting up test data ---");
        executeUpdate(connection, "CREATE DATABASE IF NOT EXISTS adbc_example_db");
        executeUpdate(
            connection,
            "CREATE TABLE IF NOT EXISTS adbc_example_db.sensor_data ("
                + "region STRING TAG, "
                + "device_id STRING TAG, "
                + "temperature FLOAT FIELD, "
                + "humidity DOUBLE FIELD, "
                + "status BOOLEAN FIELD)");

        executeUpdate(
            connection,
            "INSERT INTO adbc_example_db.sensor_data"
                + "(time, region, device_id, temperature, humidity, status) "
                + "VALUES(1, 'north', 'dev001', 25.5, 60.2, true)");
        executeUpdate(
            connection,
            "INSERT INTO adbc_example_db.sensor_data"
                + "(time, region, device_id, temperature, humidity, status) "
                + "VALUES(2, 'north', 'dev001', 26.1, 58.5, true)");
        executeUpdate(
            connection,
            "INSERT INTO adbc_example_db.sensor_data"
                + "(time, region, device_id, temperature, humidity, status) "
                + "VALUES(3, 'south', 'dev002', 30.2, 45.0, false)");
        executeUpdate(
            connection,
            "INSERT INTO adbc_example_db.sensor_data"
                + "(time, region, device_id, temperature, humidity, status) "
                + "VALUES(4, 'south', 'dev002', 31.5, 42.3, false)");
        System.out.println("Test data inserted successfully.");

        // 5. Query: Full table scan — results returned as Arrow VectorSchemaRoot
        System.out.println("\n=== Query 1: Full table scan (Arrow VectorSchemaRoot) ===");
        executeQuery(
            connection,
            "SELECT time, region, device_id, temperature, humidity, status "
                + "FROM adbc_example_db.sensor_data ORDER BY time");

        // 6. Filtered query
        System.out.println("\n=== Query 2: Filtered query (region = 'north') ===");
        executeQuery(
            connection,
            "SELECT time, device_id, temperature, humidity "
                + "FROM adbc_example_db.sensor_data WHERE region = 'north' ORDER BY time");

        // 7. Aggregation query
        System.out.println("\n=== Query 3: Aggregation ===");
        executeQuery(
            connection,
            "SELECT region, COUNT(*) as cnt, AVG(temperature) as avg_temp "
                + "FROM adbc_example_db.sensor_data GROUP BY region ORDER BY region");

        // 8. Cleanup
        executeUpdate(connection, "DROP DATABASE IF EXISTS adbc_example_db");
        System.out.println("\n=== ADBC Example completed successfully ===");
      }
    }
  }

  /** Execute a non-query SQL statement via ADBC. */
  private static void executeUpdate(AdbcConnection connection, String sql) throws Exception {
    try (AdbcStatement statement = connection.createStatement()) {
      statement.setSqlQuery(sql);
      statement.executeUpdate();
    }
  }

  /** Execute a query via ADBC and print Arrow columnar results. */
  private static void executeQuery(AdbcConnection connection, String sql) throws Exception {
    try (AdbcStatement statement = connection.createStatement()) {
      statement.setSqlQuery(sql);
      AdbcStatement.QueryResult result = statement.executeQuery();
      try (ArrowReader reader = result.getReader()) {
        System.out.println("SQL: " + sql);

        int totalRows = 0;
        boolean headerPrinted = false;

        while (reader.loadNextBatch()) {
          VectorSchemaRoot root = reader.getVectorSchemaRoot();

          // Print header once
          if (!headerPrinted) {
            List<Field> fields = root.getSchema().getFields();
            System.out.println("Schema: " + fields.size() + " columns");
            StringBuilder header = new StringBuilder();
            StringBuilder separator = new StringBuilder();
            for (Field field : fields) {
              header.append(String.format("%-20s", field.getName()));
              separator.append("--------------------");
            }
            System.out.println(header);
            System.out.println(separator);
            headerPrinted = true;
          }

          // Print rows from this batch
          int rowCount = root.getRowCount();
          for (int i = 0; i < rowCount; i++) {
            StringBuilder row = new StringBuilder();
            for (FieldVector vector : root.getFieldVectors()) {
              Object value = vector.getObject(i);
              row.append(String.format("%-20s", value == null ? "null" : value.toString()));
            }
            System.out.println(row);
            totalRows++;
          }
        }
        System.out.println("Total rows: " + totalRows);
      }
    }
  }
}
