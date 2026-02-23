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

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.BasicAuthCredentialWriter;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Example demonstrating how to use Arrow Flight SQL to query IoTDB.
 *
 * <p>Before running this example, make sure IoTDB is running with Arrow Flight SQL enabled:
 *
 * <ul>
 *   <li>Set <code>enable_arrow_flight_sql_service=true</code> in iotdb-system.properties
 *   <li>Default Flight SQL port is 8904
 * </ul>
 */
public class FlightSqlExample {

  private static final String HOST = "127.0.0.1";
  private static final int PORT = 8904;
  private static final String USER = "root";
  private static final String PASSWORD = "root";

  public static void main(String[] args) throws Exception {
    // 1. Create Arrow allocator and Flight SQL client
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Location location = Location.forGrpcInsecure(HOST, PORT);

      // Set up Bearer token authentication middleware
      ClientIncomingAuthHeaderMiddleware.Factory authFactory =
          new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());

      try (FlightClient flightClient =
              FlightClient.builder(allocator, location).intercept(authFactory).build();
          FlightSqlClient sqlClient = new FlightSqlClient(flightClient)) {

        // 2. Authenticate with username/password to get Bearer token
        CredentialCallOption bearerToken =
            new CredentialCallOption(new BasicAuthCredentialWriter(USER, PASSWORD));
        System.out.println("=== Connected to IoTDB Flight SQL service ===");
        System.out.println("Host: " + HOST + ", Port: " + PORT);

        // 3. Create database and table, insert data
        System.out.println("\n--- Setting up test data ---");
        executeUpdate(sqlClient, "CREATE DATABASE IF NOT EXISTS flight_example_db", bearerToken);
        executeUpdate(
            sqlClient,
            "CREATE TABLE IF NOT EXISTS flight_example_db.sensor_data ("
                + "region STRING TAG, "
                + "device_id STRING TAG, "
                + "temperature FLOAT FIELD, "
                + "humidity DOUBLE FIELD, "
                + "status BOOLEAN FIELD, "
                + "description TEXT FIELD)",
            bearerToken);

        // Insert sample data
        executeUpdate(
            sqlClient,
            "INSERT INTO flight_example_db.sensor_data"
                + "(time, region, device_id, temperature, humidity, status, description) "
                + "VALUES(1, 'north', 'dev001', 25.5, 60.2, true, 'normal')",
            bearerToken);
        executeUpdate(
            sqlClient,
            "INSERT INTO flight_example_db.sensor_data"
                + "(time, region, device_id, temperature, humidity, status, description) "
                + "VALUES(2, 'north', 'dev001', 26.1, 58.5, true, 'normal')",
            bearerToken);
        executeUpdate(
            sqlClient,
            "INSERT INTO flight_example_db.sensor_data"
                + "(time, region, device_id, temperature, humidity, status, description) "
                + "VALUES(3, 'south', 'dev002', 30.2, 45.0, false, 'high temp')",
            bearerToken);
        executeUpdate(
            sqlClient,
            "INSERT INTO flight_example_db.sensor_data"
                + "(time, region, device_id, temperature, humidity, status, description) "
                + "VALUES(4, 'south', 'dev002', 31.5, 42.3, false, 'high temp')",
            bearerToken);
        System.out.println("Test data inserted successfully.");

        // 4. Query: SELECT all data
        System.out.println("\n=== Query 1: Full table scan ===");
        executeQuery(
            sqlClient,
            "SELECT time, region, device_id, temperature, humidity, status, description "
                + "FROM flight_example_db.sensor_data ORDER BY time",
            bearerToken);

        // 5. Query with filter
        System.out.println("\n=== Query 2: Filtered query (region = 'north') ===");
        executeQuery(
            sqlClient,
            "SELECT time, device_id, temperature, humidity "
                + "FROM flight_example_db.sensor_data WHERE region = 'north' ORDER BY time",
            bearerToken);

        // 6. Aggregation query
        System.out.println("\n=== Query 3: Aggregation (AVG temperature by region) ===");
        executeQuery(
            sqlClient,
            "SELECT region, COUNT(*) as cnt, AVG(temperature) as avg_temp "
                + "FROM flight_example_db.sensor_data GROUP BY region ORDER BY region",
            bearerToken);

        // 7. Cleanup
        executeUpdate(sqlClient, "DROP DATABASE IF EXISTS flight_example_db", bearerToken);
        System.out.println("\n=== Example completed successfully ===");
      }
    }
  }

  /** Execute a non-query SQL statement (DDL/DML). */
  private static void executeUpdate(
      FlightSqlClient sqlClient, String sql, CredentialCallOption bearerToken) throws Exception {
    FlightInfo info = sqlClient.execute(sql, bearerToken);
    // Consume the stream to ensure execution
    for (FlightEndpoint endpoint : info.getEndpoints()) {
      try (FlightStream stream = sqlClient.getStream(endpoint.getTicket(), bearerToken)) {
        while (stream.next()) {
          // consume
        }
      }
    }
  }

  /** Execute a query and print results in table format. */
  private static void executeQuery(
      FlightSqlClient sqlClient, String sql, CredentialCallOption bearerToken) throws Exception {
    FlightInfo flightInfo = sqlClient.execute(sql, bearerToken);

    // Print schema
    Schema schema = flightInfo.getSchema();
    List<Field> fields = schema.getFields();
    System.out.println("SQL: " + sql);
    System.out.println("Schema: " + fields.size() + " columns");

    // Print column headers
    StringBuilder header = new StringBuilder();
    StringBuilder separator = new StringBuilder();
    for (Field field : fields) {
      String colName = field.getName();
      header.append(String.format("%-20s", colName));
      separator.append("--------------------");
    }
    System.out.println(header);
    System.out.println(separator);

    // Fetch and print rows
    int totalRows = 0;
    for (FlightEndpoint endpoint : flightInfo.getEndpoints()) {
      try (FlightStream stream = sqlClient.getStream(endpoint.getTicket(), bearerToken)) {
        while (stream.next()) {
          VectorSchemaRoot root = stream.getRoot();
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
      }
    }
    System.out.println("Total rows: " + totalRows);
  }
}
