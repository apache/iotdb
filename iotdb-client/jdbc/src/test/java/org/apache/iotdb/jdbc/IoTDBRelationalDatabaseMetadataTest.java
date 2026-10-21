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

package org.apache.iotdb.jdbc;

import org.apache.iotdb.jdbc.relational.IoTDBRelationalDatabaseMetadata;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.tsfile.utils.Binary;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class IoTDBRelationalDatabaseMetadataTest {

  private static final String TABLES_QUERY = "select * from information_schema.tables";
  private static final String COLUMNS_QUERY = "select * from information_schema.columns";

  // Result headers as the server produces them for each metadata source.
  private static final List<String> INFORMATION_SCHEMA_TABLES_HEADER =
      Arrays.asList(
          "database",
          "table_name",
          "ttl(ms)",
          "status",
          "comment",
          "table_type",
          "need_last_cache");
  private static final List<TSDataType> INFORMATION_SCHEMA_TABLES_TYPES =
      Arrays.asList(
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.BOOLEAN);
  private static final List<String> INFORMATION_SCHEMA_COLUMNS_HEADER =
      Arrays.asList(
          "database", "table_name", "column_name", "datatype", "category", "status", "comment");
  private static final List<String> SHOW_DATABASES_HEADER =
      Arrays.asList(
          "Database",
          "TTL(ms)",
          "SchemaReplicationFactor",
          "DataReplicationFactor",
          "TimePartitionInterval");
  private static final List<TSDataType> SHOW_DATABASES_TYPES =
      Arrays.asList(
          TSDataType.TEXT, TSDataType.TEXT, TSDataType.INT32, TSDataType.INT32, TSDataType.INT64);
  private static final List<String> SHOW_TABLES_DETAILS_HEADER =
      Arrays.asList("TableName", "TTL(ms)", "Status", "Comment", "TableType", "NeedLastCache");
  private static final List<TSDataType> SHOW_TABLES_DETAILS_TYPES =
      Arrays.asList(
          TSDataType.TEXT,
          TSDataType.TEXT,
          TSDataType.TEXT,
          TSDataType.TEXT,
          TSDataType.TEXT,
          TSDataType.BOOLEAN);
  private static final List<String> DESC_DETAILS_HEADER =
      Arrays.asList("ColumnName", "DataType", "Category", "Status", "Comment");

  @Mock private IoTDBConnection connection;
  @Mock private IClientRPCService.Iface client;

  private final long sessionId = 1L;
  private final ZoneId zoneId = ZoneId.systemDefault();
  private DatabaseMetaData metadata;

  /** Canned server responses, picked by the prefix of the statement the driver sends. */
  private final Map<String, TSExecuteStatementResp> responses = new LinkedHashMap<>();

  private final List<String> executedStatements = new ArrayList<>();

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(connection.isClosed()).thenReturn(false);
    when(connection.getTimeFactor()).thenReturn(1_000);
    when(connection.createStatement())
        .thenAnswer(invocation -> new IoTDBStatement(connection, client, sessionId, zoneId, 0, 1L));
    when(client.closeOperation(any(TSCloseOperationReq.class))).thenReturn(RpcUtils.SUCCESS_STATUS);
    when(client.executeQueryStatementV2(any(TSExecuteStatementReq.class)))
        .thenAnswer(
            invocation -> {
              String statement = ((TSExecuteStatementReq) invocation.getArgument(0)).getStatement();
              executedStatements.add(statement);
              return responses.entrySet().stream()
                  .filter(entry -> statement.startsWith(entry.getKey()))
                  .map(entry -> withUnreadBuffers(entry.getValue()))
                  .findFirst()
                  .orElseGet(() -> failure("unexpected statement: " + statement));
            });
    metadata = new IoTDBRelationalDatabaseMetadata(connection, client, sessionId, zoneId);
  }

  // ========== getTables ==========

  @Test
  public void testGetTablesReportsRealNamesForEscapedPatterns() throws Exception {
    responses.put(
        TABLES_QUERY,
        informationSchemaTables(informationSchemaTable("db_1", "sensor_data", "sensor table")));

    try (ResultSet rs = metadata.getTables("Apache IoTDB", "db\\_1", "sensor\\_data", null)) {
      assertTrue(rs.next());
      assertNull(rs.getString("TABLE_CAT"));
      assertEquals("db_1", rs.getString("TABLE_SCHEM"));
      assertEquals("sensor_data", rs.getString("TABLE_NAME"));
      assertEquals("TABLE", rs.getString("TABLE_TYPE"));
      assertEquals("sensor table", rs.getString("REMARKS"));
      assertFalse(rs.next());
    }
  }

  @Test
  public void testGetTablesStartsWithStandardColumns() throws Exception {
    responses.put(
        TABLES_QUERY, informationSchemaTables(informationSchemaTable("db_1", "sensor_data", null)));

    try (ResultSet rs = metadata.getTables(null, "db_1", null, null)) {
      assertEquals(1, rs.findColumn("TABLE_CAT"));
      assertEquals(2, rs.findColumn("TABLE_SCHEM"));
      assertEquals(3, rs.findColumn("TABLE_NAME"));
      assertEquals(4, rs.findColumn("TABLE_TYPE"));
      assertEquals(5, rs.findColumn("REMARKS"));
      assertTrue(rs.next());
      assertEquals("db_1", rs.getString(2));
      assertEquals("sensor_data", rs.getString(3));
    }
  }

  @Test
  public void testGetTablesFiltersByTableNamePatternAndEscapesQuotes() throws Exception {
    responses.put(TABLES_QUERY, informationSchemaTables());

    metadata.getTables(null, "db\\_1", "it's%", null).close();

    assertEquals(
        Collections.singletonList(
            "select * from information_schema.tables"
                + " where database like 'db\\_1' escape '\\'"
                + " and table_name like 'it''s%' escape '\\'"),
        executedStatements);
  }

  @Test
  public void testGetTablesWithNullPatternsListsEveryDatabase() throws Exception {
    responses.put(
        TABLES_QUERY,
        informationSchemaTables(
            informationSchemaTable("db_1", "t1", null),
            informationSchemaTable("db_2", "t2", null)));

    try (ResultSet rs = metadata.getTables(null, null, null, null)) {
      assertTrue(rs.next());
      assertEquals("db_1", rs.getString("TABLE_SCHEM"));
      assertEquals("t1", rs.getString("TABLE_NAME"));
      assertTrue(rs.next());
      assertEquals("db_2", rs.getString("TABLE_SCHEM"));
      assertEquals("t2", rs.getString("TABLE_NAME"));
      assertFalse(rs.next());
    }
    assertEquals(
        Collections.singletonList("select * from information_schema.tables"), executedStatements);
  }

  @Test
  public void testGetTablesReturnsEmptyResultSetWhenNothingMatches() throws Exception {
    responses.put(TABLES_QUERY, informationSchemaTables());

    try (ResultSet rs = metadata.getTables(null, "no_such_db", null, null)) {
      assertNotNull(rs);
      assertEquals(3, rs.findColumn("TABLE_NAME"));
      assertFalse(rs.next());
    }
  }

  @Test
  public void testGetTablesHonorsTableTypes() throws Exception {
    responses.put(
        TABLES_QUERY, informationSchemaTables(informationSchemaTable("db_1", "sensor_data", null)));

    try (ResultSet rs = metadata.getTables(null, "db_1", null, new String[] {"VIEW"})) {
      assertFalse(rs.next());
    }
    // getTableTypes() reports the type in lower case, so the match must ignore case.
    try (ResultSet rs = metadata.getTables(null, "db_1", null, new String[] {"table"})) {
      assertTrue(rs.next());
      assertEquals("sensor_data", rs.getString("TABLE_NAME"));
    }
    try (ResultSet rs = metadata.getTables(null, "db_1", null, new String[] {"TABLE", "VIEW"})) {
      assertTrue(rs.next());
      assertEquals("sensor_data", rs.getString("TABLE_NAME"));
    }
  }

  @Test
  public void testGetTablesFallsBackToShowTablesForLiteralSchema() throws Exception {
    responses.put(TABLES_QUERY, failure("no permission on information_schema"));
    responses.put(
        "show tables details from \"db_1\"",
        showTablesDetails(
            showTablesRow("sensor_data", "sensor table"), showTablesRow("other", "")));

    try (ResultSet rs = metadata.getTables(null, "db\\_1", "sensor%", null)) {
      assertTrue(rs.next());
      assertNull(rs.getString("TABLE_CAT"));
      assertEquals("db_1", rs.getString("TABLE_SCHEM"));
      assertEquals("sensor_data", rs.getString("TABLE_NAME"));
      assertEquals("sensor table", rs.getString("REMARKS"));
      assertFalse(rs.next());
    }
  }

  @Test
  public void testGetTablesFallbackListsEveryDatabaseForNullSchema() throws Exception {
    responses.put(TABLES_QUERY, failure("no permission on information_schema"));
    responses.put("SHOW DATABASES", showDatabases("db_1", "db_2"));
    responses.put("show tables details from \"db_1\"", showTablesDetails(showTablesRow("t1", "")));
    responses.put("show tables details from \"db_2\"", showTablesDetails(showTablesRow("t2", "")));

    try (ResultSet rs = metadata.getTables(null, null, null, null)) {
      assertTrue(rs.next());
      assertEquals("db_1", rs.getString("TABLE_SCHEM"));
      assertEquals("t1", rs.getString("TABLE_NAME"));
      assertTrue(rs.next());
      assertEquals("db_2", rs.getString("TABLE_SCHEM"));
      assertEquals("t2", rs.getString("TABLE_NAME"));
      assertFalse(rs.next());
    }
  }

  @Test
  public void testRemoveEscapeDropsSearchStringEscapes() {
    assertEquals(
        "information_schema", IoTDBRelationalDatabaseMetadata.removeEscape("information\\_schema"));
    assertEquals("a%b\\c", IoTDBRelationalDatabaseMetadata.removeEscape("a\\%b\\\\c"));
    assertEquals("plain", IoTDBRelationalDatabaseMetadata.removeEscape("plain"));
  }

  // ========== getColumns ==========

  @Test
  public void testGetColumnsReportsRealNamesForEscapedPatterns() throws Exception {
    responses.put(
        COLUMNS_QUERY,
        informationSchemaColumns(
            informationSchemaColumn("db_1", "sensor_data", "ts", "TIMESTAMP", "TIME"),
            informationSchemaColumn("db_1", "sensor_data", "region", "STRING", "TAG")));

    try (ResultSet rs = metadata.getColumns(null, "db\\_1", "sensor\\_data", null)) {
      assertTrue(rs.next());
      assertNull(rs.getString("TABLE_CAT"));
      assertEquals("db_1", rs.getString("TABLE_SCHEM"));
      assertEquals("sensor_data", rs.getString("TABLE_NAME"));
      assertEquals("ts", rs.getString("COLUMN_NAME"));
      assertEquals(Types.TIMESTAMP, rs.getInt("DATA_TYPE"));
      assertEquals("TIMESTAMP", rs.getString("TYPE_NAME"));
      assertTrue(rs.next());
      assertNull(rs.getString("TABLE_CAT"));
      assertEquals("db_1", rs.getString("TABLE_SCHEM"));
      assertEquals("sensor_data", rs.getString("TABLE_NAME"));
      assertEquals("region", rs.getString("COLUMN_NAME"));
      assertEquals(Types.VARCHAR, rs.getInt("DATA_TYPE"));
      assertFalse(rs.next());
    }
  }

  @Test
  public void testGetColumnsReportsObjectAsDatabaseSpecificType() throws Exception {
    responses.put(
        COLUMNS_QUERY,
        informationSchemaColumns(
            informationSchemaColumn("db_1", "files", "content", "OBJECT", "FIELD")));

    try (ResultSet rs = metadata.getColumns(null, "db_1", "files", null)) {
      assertTrue(rs.next());
      assertEquals("OBJECT", rs.getString("TYPE_NAME"));
      assertEquals(Types.OTHER, rs.getInt("DATA_TYPE"));
    }
  }

  @Test
  public void testGetColumnsDerivesNullabilityFromTimeCategory() throws Exception {
    responses.put(
        COLUMNS_QUERY,
        informationSchemaColumns(
            informationSchemaColumn("db_1", "sensor_data", "ts", "TIMESTAMP", "TIME"),
            informationSchemaColumn("db_1", "sensor_data", "temperature", "FLOAT", "FIELD")));

    try (ResultSet rs = metadata.getColumns(null, "db_1", "sensor_data", null)) {
      assertTrue(rs.next());
      assertEquals("ts", rs.getString("COLUMN_NAME"));
      assertEquals(ResultSetMetaData.columnNoNulls, rs.getInt("NULLABLE"));
      assertEquals("NO", rs.getString("IS_NULLABLE"));
      assertTrue(rs.next());
      assertEquals("temperature", rs.getString("COLUMN_NAME"));
      assertEquals(ResultSetMetaData.columnNullableUnknown, rs.getInt("NULLABLE"));
      assertEquals("YES", rs.getString("IS_NULLABLE"));
      assertFalse(rs.next());
    }
  }

  @Test
  public void testGetColumnsNumbersColumnsWithinEachTable() throws Exception {
    responses.put(
        COLUMNS_QUERY,
        informationSchemaColumns(
            informationSchemaColumn("db_1", "t1", "time", "TIMESTAMP", "TIME"),
            informationSchemaColumn("db_1", "t1", "a", "INT32", "FIELD"),
            informationSchemaColumn("db_1", "t1", "b", "INT64", "FIELD"),
            informationSchemaColumn("db_1", "t2", "time", "TIMESTAMP", "TIME"),
            informationSchemaColumn("db_1", "t2", "c", "DOUBLE", "FIELD")));

    List<String> columns = new ArrayList<>();
    try (ResultSet rs = metadata.getColumns(null, "db_1", "t%", null)) {
      while (rs.next()) {
        columns.add(
            rs.getString("TABLE_NAME")
                + "."
                + rs.getString("COLUMN_NAME")
                + "#"
                + rs.getInt("ORDINAL_POSITION"));
      }
    }
    assertEquals(Arrays.asList("t1.time#1", "t1.a#2", "t1.b#3", "t2.time#1", "t2.c#2"), columns);
  }

  @Test
  public void testGetColumnsFiltersByColumnNamePatternKeepingOrdinalPosition() throws Exception {
    responses.put(
        COLUMNS_QUERY,
        informationSchemaColumns(
            informationSchemaColumn("db_1", "t1", "time", "TIMESTAMP", "TIME"),
            informationSchemaColumn("db_1", "t1", "a", "INT32", "FIELD"),
            informationSchemaColumn("db_1", "t1", "b", "INT64", "FIELD")));

    try (ResultSet rs = metadata.getColumns(null, "db_1", "t1", "b")) {
      assertTrue(rs.next());
      assertEquals("b", rs.getString("COLUMN_NAME"));
      assertEquals(3, rs.getInt("ORDINAL_POSITION"));
      assertFalse(rs.next());
    }
  }

  @Test
  public void testGetColumnsBuildsNullSafeQuotedQuery() throws Exception {
    responses.put(COLUMNS_QUERY, informationSchemaColumns());

    metadata.getColumns(null, "db\\_1", "it's", "c%").close();
    metadata.getColumns(null, null, null, null).close();

    assertEquals(
        Arrays.asList(
            "select * from information_schema.columns"
                + " where database like 'db\\_1' escape '\\'"
                + " and table_name like 'it''s' escape '\\'",
            "select * from information_schema.columns"),
        executedStatements);
  }

  @Test
  public void testGetColumnsReturnsEmptyResultSetForUnknownTable() throws Exception {
    responses.put(COLUMNS_QUERY, informationSchemaColumns());

    try (ResultSet rs = metadata.getColumns(null, "db_1", "no_such_table", null)) {
      assertFalse(rs.next());
    }
  }

  @Test
  public void testGetColumnsFallsBackToDescribeTable() throws Exception {
    responses.put(COLUMNS_QUERY, failure("no permission on information_schema"));
    responses.put(
        "show tables details from \"db_1\"",
        showTablesDetails(showTablesRow("sensor_data", ""), showTablesRow("sensorXdata", "")));
    responses.put(
        "desc \"db_1\".\"sensor_data\" details",
        describeTableDetails(
            new Object[] {"ts", "TIMESTAMP", "TIME", "USING", null},
            new Object[] {"temperature", "FLOAT", "FIELD", "USING", "celsius"}));

    try (ResultSet rs = metadata.getColumns(null, "db\\_1", "sensor\\_data", null)) {
      assertTrue(rs.next());
      assertNull(rs.getString("TABLE_CAT"));
      assertEquals("db_1", rs.getString("TABLE_SCHEM"));
      assertEquals("sensor_data", rs.getString("TABLE_NAME"));
      assertEquals("ts", rs.getString("COLUMN_NAME"));
      assertEquals(ResultSetMetaData.columnNoNulls, rs.getInt("NULLABLE"));
      assertEquals(1, rs.getInt("ORDINAL_POSITION"));
      assertTrue(rs.next());
      assertEquals("temperature", rs.getString("COLUMN_NAME"));
      assertEquals("celsius", rs.getString("REMARKS"));
      assertEquals("YES", rs.getString("IS_NULLABLE"));
      assertEquals(2, rs.getInt("ORDINAL_POSITION"));
      assertFalse(rs.next());
    }
  }

  // ========== getPrimaryKeys ==========

  @Test
  public void testGetPrimaryKeysReportsRealNamesOfTimeAndTagColumns() throws Exception {
    responses.put(
        COLUMNS_QUERY,
        informationSchemaColumns(
            informationSchemaColumn("db_1", "sensor_data", "ts", "TIMESTAMP", "TIME"),
            informationSchemaColumn("db_1", "sensor_data", "region", "STRING", "TAG"),
            informationSchemaColumn("db_1", "sensor_data", "model", "STRING", "ATTRIBUTE"),
            informationSchemaColumn("db_1", "sensor_data", "plant", "STRING", "TAG"),
            informationSchemaColumn("db_1", "sensor_data", "temperature", "FLOAT", "FIELD")));

    List<String> keys = new ArrayList<>();
    // Some tools escape the names as if they were patterns
    try (ResultSet rs = metadata.getPrimaryKeys(null, "db\\_1", "sensor\\_data")) {
      while (rs.next()) {
        assertNull(rs.getString("TABLE_CAT"));
        assertEquals("db_1", rs.getString("TABLE_SCHEM"));
        assertEquals("sensor_data", rs.getString("TABLE_NAME"));
        assertEquals("PRIMARY", rs.getString("PK_NAME"));
        keys.add(rs.getString("COLUMN_NAME") + "#" + rs.getInt("KEY_SEQ"));
      }
    }
    assertEquals(Arrays.asList("ts#1", "region#2", "plant#3"), keys);
  }

  @Test
  public void testGetPrimaryKeysMatchesExactNamesWhetherEscapedOrNot() throws Exception {
    responses.put(COLUMNS_QUERY, informationSchemaColumns());

    metadata.getPrimaryKeys(null, "db_1", "it's").close();
    metadata.getPrimaryKeys(null, "db\\_1", "sensor\\_data").close();
    metadata.getPrimaryKeys(null, null, "t1").close();

    assertEquals(
        Arrays.asList(
            "select * from information_schema.columns"
                + " where database like 'db\\_1' escape '\\'"
                + " and table_name like 'it''s' escape '\\'",
            "select * from information_schema.columns"
                + " where database like 'db\\_1' escape '\\'"
                + " and table_name like 'sensor\\_data' escape '\\'",
            "select * from information_schema.columns where table_name like 't1' escape '\\'"),
        executedStatements);
  }

  @Test
  public void testGetPrimaryKeysReturnsEmptyResultSetForUnknownTable() throws Exception {
    responses.put(COLUMNS_QUERY, informationSchemaColumns());

    try (ResultSet rs = metadata.getPrimaryKeys(null, "db_1", "no_such_table")) {
      assertFalse(rs.next());
    }
  }

  @Test
  public void testGetPrimaryKeysFallsBackToDescribeTable() throws Exception {
    responses.put(COLUMNS_QUERY, failure("no permission on information_schema"));
    responses.put(
        "show tables details from \"db_1\"",
        showTablesDetails(showTablesRow("sensor_data", ""), showTablesRow("sensorXdata", "")));
    responses.put(
        "desc \"db_1\".\"sensor_data\" details",
        describeTableDetails(
            new Object[] {"ts", "TIMESTAMP", "TIME", "USING", null},
            new Object[] {"region", "STRING", "TAG", "USING", null},
            new Object[] {"temperature", "FLOAT", "FIELD", "USING", null}));

    List<String> keys = new ArrayList<>();
    try (ResultSet rs = metadata.getPrimaryKeys(null, "db_1", "sensor_data")) {
      while (rs.next()) {
        assertNull(rs.getString("TABLE_CAT"));
        assertEquals("db_1", rs.getString("TABLE_SCHEM"));
        assertEquals("sensor_data", rs.getString("TABLE_NAME"));
        keys.add(rs.getString("COLUMN_NAME") + "#" + rs.getInt("KEY_SEQ"));
      }
    }
    assertEquals(Arrays.asList("ts#1", "region#2"), keys);
  }

  // ========== fake server responses ==========

  private static Object[] informationSchemaTable(String database, String table, String comment) {
    return new Object[] {database, table, "INF", "USING", comment, "BASE TABLE", false};
  }

  private static TSExecuteStatementResp informationSchemaTables(Object[]... rows)
      throws IOException {
    return queryResult(INFORMATION_SCHEMA_TABLES_HEADER, INFORMATION_SCHEMA_TABLES_TYPES, rows);
  }

  private static Object[] informationSchemaColumn(
      String database, String table, String column, String dataType, String category) {
    return new Object[] {database, table, column, dataType, category, "USING", null};
  }

  private static TSExecuteStatementResp informationSchemaColumns(Object[]... rows)
      throws IOException {
    return queryResult(
        INFORMATION_SCHEMA_COLUMNS_HEADER,
        Collections.nCopies(INFORMATION_SCHEMA_COLUMNS_HEADER.size(), TSDataType.STRING),
        rows);
  }

  private static TSExecuteStatementResp showDatabases(String... databases) throws IOException {
    Object[][] rows = new Object[databases.length][];
    for (int i = 0; i < databases.length; i++) {
      rows[i] = new Object[] {databases[i], "INF", 1, 1, 604_800_000L};
    }
    return queryResult(SHOW_DATABASES_HEADER, SHOW_DATABASES_TYPES, rows);
  }

  private static Object[] showTablesRow(String table, String comment) {
    return new Object[] {table, "INF", "USING", comment, "BASE TABLE", false};
  }

  private static TSExecuteStatementResp showTablesDetails(Object[]... rows) throws IOException {
    return queryResult(SHOW_TABLES_DETAILS_HEADER, SHOW_TABLES_DETAILS_TYPES, rows);
  }

  private static TSExecuteStatementResp describeTableDetails(Object[]... rows) throws IOException {
    return queryResult(
        DESC_DETAILS_HEADER,
        Collections.nCopies(DESC_DETAILS_HEADER.size(), TSDataType.TEXT),
        rows);
  }

  /** A successful table-model query response carrying the given rows in one TsBlock. */
  private static TSExecuteStatementResp queryResult(
      List<String> header, List<TSDataType> types, Object[]... rows) throws IOException {
    TsBlockBuilder builder = new TsBlockBuilder(types);
    for (Object[] row : rows) {
      builder.getTimeColumnBuilder().writeLong(0);
      for (int i = 0; i < row.length; i++) {
        ColumnBuilder columnBuilder = builder.getColumnBuilder(i);
        Object value = row[i];
        if (value == null) {
          columnBuilder.appendNull();
        } else if (value instanceof Boolean) {
          columnBuilder.writeBoolean((Boolean) value);
        } else if (value instanceof Integer) {
          columnBuilder.writeInt((Integer) value);
        } else if (value instanceof Long) {
          columnBuilder.writeLong((Long) value);
        } else {
          columnBuilder.writeBinary(new Binary((String) value, TSFileConfig.STRING_CHARSET));
        }
      }
      builder.declarePosition();
    }

    TSExecuteStatementResp resp = new TSExecuteStatementResp(RpcUtils.SUCCESS_STATUS);
    resp.setQueryId(100L);
    resp.setColumns(header);
    resp.setDataTypeList(types.stream().map(TSDataType::name).collect(Collectors.toList()));
    resp.setIgnoreTimeStamp(true);
    resp.setTableModel(true);
    resp.setMoreData(false);
    resp.setColumnIndex2TsBlockColumnIndexList(
        IntStream.range(0, header.size()).boxed().collect(Collectors.toList()));
    resp.setQueryResult(
        rows.length == 0
            ? Collections.emptyList()
            : Collections.singletonList(new TsBlockSerde().serialize(builder.build())));
    return resp;
  }

  /** Reading a result consumes its TsBlock buffers, so every execution gets its own view. */
  private static TSExecuteStatementResp withUnreadBuffers(TSExecuteStatementResp resp) {
    TSExecuteStatementResp copy = resp.deepCopy();
    if (copy.isSetQueryResult()) {
      copy.setQueryResult(
          copy.getQueryResult().stream().map(ByteBuffer::duplicate).collect(Collectors.toList()));
    }
    return copy;
  }

  private static TSExecuteStatementResp failure(String message) {
    return RpcUtils.getTSExecuteStatementResp(TSStatusCode.NO_PERMISSION, message);
  }
}
