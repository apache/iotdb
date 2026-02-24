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

package org.apache.iotdb.relational.it.flightsql;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for Arrow Flight SQL service in IoTDB. Tests the end-to-end flow: client
 * connects via Flight SQL protocol, authenticates via auth2 Bearer token, executes SQL queries, and
 * receives Arrow-formatted results.
 *
 * <p>Uses the standard auth2 pattern: ClientIncomingAuthHeaderMiddleware intercepts the first
 * call's response to cache the Bearer token, which is then automatically sent on subsequent calls.
 * All queries use fully qualified table names (database.table) for clarity.
 */
@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBArrowFlightSqlIT {

  private static final String DATABASE = "flightsql_test_db";
  private static final String TABLE = DATABASE + ".test_table";

  private static BufferAllocator allocator;
  private static FlightClient flightClient;
  private static FlightSqlClient flightSqlClient;
  private static CredentialCallOption credentials;

  @BeforeClass
  public static void setUpClass() throws Exception {
    // Configure and start the cluster with Arrow Flight SQL enabled
    BaseEnv baseEnv = EnvFactory.getEnv();
    baseEnv.getConfig().getCommonConfig().setEnableArrowFlightSqlService(true);
    baseEnv.initClusterEnvironment();

    // Get the Flight SQL port from the data node
    int port = EnvFactory.getEnv().getArrowFlightSqlPort();

    // Create Arrow allocator and Flight client with Bearer token auth middleware.
    // The ClientIncomingAuthHeaderMiddleware captures the Bearer token from the
    // server's
    // response on the first authenticated call, and automatically attaches it to
    // all
    // subsequent calls — ensuring they reuse the same server-side session.
    allocator = new RootAllocator(Long.MAX_VALUE);
    Location location = Location.forGrpcInsecure("127.0.0.1", port);
    ClientIncomingAuthHeaderMiddleware.Factory authFactory =
        new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());
    flightClient = FlightClient.builder(allocator, location).intercept(authFactory).build();

    // Create credentials — passed on every call per the auth2 pattern
    credentials =
        new CredentialCallOption(
            new BasicAuthCredentialWriter(
                SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD));

    // Wrap in FlightSqlClient for Flight SQL protocol operations
    flightSqlClient = new FlightSqlClient(flightClient);

    // Use the standard session to create the test database and table with data
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS " + DATABASE);
    }
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB(DATABASE)) {
      session.executeNonQueryStatement(
          "CREATE TABLE test_table ("
              + "id1 STRING TAG, "
              + "s1 INT32 FIELD, "
              + "s2 INT64 FIELD, "
              + "s3 FLOAT FIELD, "
              + "s4 DOUBLE FIELD, "
              + "s5 BOOLEAN FIELD, "
              + "s6 TEXT FIELD)");
      session.executeNonQueryStatement(
          "INSERT INTO test_table(time, id1, s1, s2, s3, s4, s5, s6) "
              + "VALUES(1, 'device1', 100, 1000, 1.5, 2.5, true, 'hello')");
      session.executeNonQueryStatement(
          "INSERT INTO test_table(time, id1, s1, s2, s3, s4, s5, s6) "
              + "VALUES(2, 'device1', 200, 2000, 3.5, 4.5, false, 'world')");
      session.executeNonQueryStatement(
          "INSERT INTO test_table(time, id1, s1, s2, s3, s4, s5, s6) "
              + "VALUES(3, 'device2', 300, 3000, 5.5, 6.5, true, 'iotdb')");
    }
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    if (flightSqlClient != null) {
      try {
        flightSqlClient.close();
      } catch (Exception e) {
        // ignore
      }
    }
    if (flightClient != null) {
      try {
        flightClient.close();
      } catch (Exception e) {
        // ignore
      }
    }
    if (allocator != null) {
      allocator.close();
    }
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testQueryWithAllDataTypes() throws Exception {
    FlightInfo flightInfo =
        flightSqlClient.execute(
            "SELECT time, id1, s1, s2, s3, s4, s5, s6 FROM " + TABLE + " ORDER BY time",
            credentials);

    // Validate schema
    Schema schema = flightInfo.getSchemaOptional().orElse(null);
    assertNotNull("Schema should not be null", schema);
    List<Field> fields = schema.getFields();
    assertEquals("Should have 8 columns", 8, fields.size());

    // Fetch all data
    List<List<String>> rows = fetchAllRows(flightInfo);
    assertEquals("Should have 3 rows", 3, rows.size());
  }

  @Test
  public void testQueryWithFilter() throws Exception {
    FlightInfo flightInfo =
        flightSqlClient.execute(
            "SELECT id1, s1 FROM " + TABLE + " WHERE id1 = 'device1' ORDER BY time", credentials);

    List<List<String>> rows = fetchAllRows(flightInfo);
    assertEquals("Should have 2 rows for device1", 2, rows.size());
  }

  @Test
  public void testQueryWithAggregation() throws Exception {
    FlightInfo flightInfo =
        flightSqlClient.execute(
            "SELECT id1, COUNT(*) as cnt, SUM(s1) as s1_sum "
                + "FROM "
                + TABLE
                + " GROUP BY id1 ORDER BY id1",
            credentials);

    List<List<String>> rows = fetchAllRows(flightInfo);
    assertEquals("Should have 2 groups", 2, rows.size());
  }

  @Test
  public void testEmptyResult() throws Exception {
    FlightInfo flightInfo =
        flightSqlClient.execute(
            "SELECT * FROM " + TABLE + " WHERE id1 = 'nonexistent'", credentials);

    List<List<String>> rows = fetchAllRows(flightInfo);
    assertEquals("Should have 0 rows", 0, rows.size());
  }

  @Test
  public void testShowDatabases() throws Exception {
    FlightInfo flightInfo = flightSqlClient.execute("SHOW DATABASES", credentials);

    List<List<String>> rows = fetchAllRows(flightInfo);
    assertTrue("Should have at least 1 database", rows.size() >= 1);

    boolean found = false;
    for (List<String> row : rows) {
      for (String val : row) {
        if (val.contains(DATABASE)) {
          found = true;
          break;
        }
      }
    }
    assertTrue("Should find test database " + DATABASE, found);
  }

  /**
   * Fetches all rows from all endpoints in a FlightInfo. Each row is a list of string
   * representations of the column values.
   */
  private List<List<String>> fetchAllRows(FlightInfo flightInfo) throws Exception {
    List<List<String>> rows = new ArrayList<>();
    for (FlightEndpoint endpoint : flightInfo.getEndpoints()) {
      try (FlightStream stream = flightSqlClient.getStream(endpoint.getTicket(), credentials)) {
        while (stream.next()) {
          VectorSchemaRoot root = stream.getRoot();
          int rowCount = root.getRowCount();
          for (int i = 0; i < rowCount; i++) {
            List<String> row = new ArrayList<>();
            for (FieldVector vector : root.getFieldVectors()) {
              Object value = vector.getObject(i);
              row.add(value == null ? "null" : value.toString());
            }
            rows.add(row);
          }
        }
      }
    }
    return rows;
  }
}
