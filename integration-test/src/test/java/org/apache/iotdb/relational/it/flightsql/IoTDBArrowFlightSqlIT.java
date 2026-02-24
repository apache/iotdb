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
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightClientMiddleware;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Integration tests for Arrow Flight SQL service in IoTDB. Tests the end-to-end flow: client
 * connects via Flight SQL protocol, authenticates, executes SQL queries, and receives
 * Arrow-formatted results.
 */
@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBArrowFlightSqlIT {

  private static final String DATABASE = "flightsql_test_db";
  private static final String TABLE = DATABASE + ".test_table";
  private static final String USER = "root";
  private static final String PASSWORD = "root";

  private String clientId;
  private BufferAllocator allocator;
  private FlightSqlClient flightSqlClient;
  private CredentialCallOption bearerToken;

  @Before
  public void setUp() throws Exception {
    // Configure and start the cluster with Arrow Flight SQL enabled
    BaseEnv baseEnv = EnvFactory.getEnv();
    baseEnv.getConfig().getDataNodeConfig().setEnableArrowFlightSqlService(true);
    baseEnv.initClusterEnvironment();

    int port = EnvFactory.getEnv().getArrowFlightSqlPort();
    allocator = new RootAllocator(Long.MAX_VALUE);
    Location location = Location.forGrpcInsecure("127.0.0.1", port);

    clientId = UUID.randomUUID().toString();
    flightSqlClient = createFlightSqlClient(clientId);
    bearerToken = new CredentialCallOption(new BasicAuthCredentialWriter(USER, PASSWORD));

    // Create test data via native session (not Flight SQL)
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

  @After
  public void tearDown() throws Exception {
    if (flightSqlClient != null) {
      try {
        flightSqlClient.close();
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
  public void testShowDatabases() throws Exception {
    FlightInfo flightInfo = flightSqlClient.execute("SHOW DATABASES", bearerToken);

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

  @Test
  public void testQueryWithAllDataTypes() throws Exception {
    FlightInfo flightInfo =
        flightSqlClient.execute(
            "SELECT time, id1, s1, s2, s3, s4, s5, s6 FROM " + TABLE + " ORDER BY time",
            bearerToken);

    // Validate schema
    Schema schema = flightInfo.getSchema();
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
            "SELECT id1, s1 FROM " + TABLE + " WHERE id1 = 'device1' ORDER BY time", bearerToken);

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
            bearerToken);

    List<List<String>> rows = fetchAllRows(flightInfo);
    assertEquals("Should have 2 groups", 2, rows.size());
  }

  @Test
  public void testEmptyResult() throws Exception {
    FlightInfo flightInfo =
        flightSqlClient.execute(
            "SELECT * FROM " + TABLE + " WHERE id1 = 'nonexistent'", bearerToken);

    List<List<String>> rows = fetchAllRows(flightInfo);
    assertEquals("Should have 0 rows", 0, rows.size());
  }

  @Test
  public void testUseDbSessionPersistence() throws Exception {
    // Connection 1: USE database (same clientId shares the session)
    flightSqlClient.execute("USE " + DATABASE, bearerToken);

    // Connection 2: query without fully-qualified table name.
    // Same clientId ensures the same session is reused, so USE context persists.
    FlightSqlClient client2 = createFlightSqlClient(clientId);
    try {
      CredentialCallOption token2 =
          new CredentialCallOption(new BasicAuthCredentialWriter(USER, PASSWORD));
      FlightInfo flightInfo = client2.execute("SELECT * FROM test_table ORDER BY time", token2);
      List<List<String>> rows = fetchAllRows(flightInfo, client2, token2);
      assertEquals("Should have 3 rows from unqualified query after USE", 3, rows.size());
    } finally {
      client2.close();
    }
  }

  @Test
  public void testUseDbWithFullyQualifiedFallback() throws Exception {
    // Connection 1: USE database
    flightSqlClient.execute("USE " + DATABASE, bearerToken);

    // Connection 2: unqualified query (same clientId → same session)
    FlightSqlClient client2 = createFlightSqlClient(clientId);
    try {
      CredentialCallOption token2 =
          new CredentialCallOption(new BasicAuthCredentialWriter(USER, PASSWORD));
      FlightInfo infoUnqualified =
          client2.execute("SELECT * FROM test_table ORDER BY time", token2);
      List<List<String>> rowsUnqualified = fetchAllRows(infoUnqualified, client2, token2);
      assertEquals("Unqualified query should return 3 rows", 3, rowsUnqualified.size());
    } finally {
      client2.close();
    }

    // Connection 3: fully-qualified query
    FlightSqlClient client3 = createFlightSqlClient(clientId);
    try {
      CredentialCallOption token3 =
          new CredentialCallOption(new BasicAuthCredentialWriter(USER, PASSWORD));
      FlightInfo infoQualified =
          client3.execute("SELECT * FROM " + TABLE + " ORDER BY time", token3);
      List<List<String>> rowsQualified = fetchAllRows(infoQualified, client3, token3);
      assertEquals("Fully-qualified query should also return 3 rows", 3, rowsQualified.size());
    } finally {
      client3.close();
    }
  }

  @Test
  public void testUseDbIsolationAcrossClients() throws Exception {
    // Client A (clientId from setUp): USE DATABASE
    flightSqlClient.execute("USE " + DATABASE, bearerToken);

    // Client B (different clientId): gets its own independent session with NO USE context.
    // Querying an unqualified table name should fail because no database is selected.
    String clientIdB = UUID.randomUUID().toString();
    FlightSqlClient clientB = createFlightSqlClient(clientIdB);
    CredentialCallOption tokenB =
        new CredentialCallOption(new BasicAuthCredentialWriter(USER, PASSWORD));
    try {
      clientB.execute("SELECT * FROM test_table", tokenB);
      fail("Client B should fail on unqualified table query without USE");
    } catch (Exception expected) {
      // Expected: Client B has no database context, so unqualified table query fails.
      // Arrow Flight wraps the actual error, so we just verify the query did fail.
      assertNotNull("Exception should have a message", expected.getMessage());
    } finally {
      clientB.close();
    }

    // Client A's USE context is preserved (same clientId → same session)
    FlightSqlClient clientA2 = createFlightSqlClient(clientId);
    CredentialCallOption tokenA2 =
        new CredentialCallOption(new BasicAuthCredentialWriter(USER, PASSWORD));
    try {
      FlightInfo infoA = clientA2.execute("SELECT * FROM test_table ORDER BY time", tokenA2);
      List<List<String>> rowsA = fetchAllRows(infoA, clientA2, tokenA2);
      assertEquals("Client A should still see 3 rows after Client B's queries", 3, rowsA.size());
    } finally {
      clientA2.close();
    }
  }

  @Test
  public void testInvalidClientIdRejected() throws Exception {
    // A non-empty clientId with invalid characters (contains @) should be rejected (fail-closed).
    // Only null/empty clientId should fall back to shared session keying.
    String invalidClientId = "bad@client!id";
    FlightSqlClient invalidClient = createFlightSqlClient(invalidClientId);
    CredentialCallOption token =
        new CredentialCallOption(new BasicAuthCredentialWriter(USER, PASSWORD));
    try {
      invalidClient.execute("SHOW DATABASES", token);
      fail("Server should reject invalid clientId during authentication");
    } catch (Exception expected) {
      // Expected: server rejects the invalid clientId
      assertNotNull("Exception should have a message", expected.getMessage());
    } finally {
      invalidClient.close();
    }
  }

  // ===================== Helper Methods =====================

  /**
   * Fetches all rows from all endpoints in a FlightInfo using the shared client. Each row is a list
   * of string representations of the column values.
   */
  private List<List<String>> fetchAllRows(FlightInfo flightInfo) throws Exception {
    return fetchAllRows(flightInfo, flightSqlClient, bearerToken);
  }

  private List<List<String>> fetchAllRows(
      FlightInfo flightInfo, FlightSqlClient client, CredentialCallOption token) throws Exception {
    List<List<String>> rows = new ArrayList<>();
    for (FlightEndpoint endpoint : flightInfo.getEndpoints()) {
      try (FlightStream stream = client.getStream(endpoint.getTicket(), token)) {
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

  private FlightSqlClient createFlightSqlClient(String flightClientId) {
    int port = EnvFactory.getEnv().getArrowFlightSqlPort();
    Location location = Location.forGrpcInsecure("127.0.0.1", port);
    ClientIncomingAuthHeaderMiddleware.Factory authFactory =
        new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());
    FlightClient client =
        FlightClient.builder(allocator, location)
            .intercept(authFactory)
            .intercept(new ClientIdMiddlewareFactory(flightClientId))
            .build();
    return new FlightSqlClient(client);
  }

  /**
   * FlightClientMiddleware that injects the x-flight-sql-client-id header on every call. This
   * allows the server to key sessions per logical client, enabling per-client USE database
   * isolation.
   */
  private static class ClientIdMiddlewareFactory implements FlightClientMiddleware.Factory {
    private final String flightClientId;

    ClientIdMiddlewareFactory(String flightClientId) {
      this.flightClientId = flightClientId;
    }

    @Override
    public FlightClientMiddleware onCallStarted(CallInfo info) {
      return new FlightClientMiddleware() {
        @Override
        public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
          outgoingHeaders.insert("x-flight-sql-client-id", flightClientId);
        }

        @Override
        public void onHeadersReceived(CallHeaders incomingHeaders) {
          // no-op
        }

        @Override
        public void onCallCompleted(CallStatus status) {
          // no-op
        }
      };
    }
  }
}
