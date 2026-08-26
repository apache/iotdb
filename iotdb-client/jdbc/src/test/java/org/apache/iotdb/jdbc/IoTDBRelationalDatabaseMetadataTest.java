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
import org.apache.iotdb.service.rpc.thrift.IClientRPCService.Iface;

import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IoTDBRelationalDatabaseMetadataTest {

  @Test
  public void testReadsSourceResultSetsBeforeClosingInternalStatements() throws SQLException {
    IoTDBConnection connection = mock(IoTDBConnection.class);
    Iface client = mock(Iface.class);
    long sessionId = 1L;
    ZoneId zoneId = ZoneId.systemDefault();
    when(connection.getTimeFactor()).thenReturn(1_000);

    CloseAwareStatement tablesStatement =
        new CloseAwareStatement(connection, client, sessionId, zoneId);
    ResultSet tablesSource =
        sourceResultSet(tablesStatement, values("table_name", "table1", "comment", ""));
    tablesStatement.setResultSet(tablesSource);

    CloseAwareStatement columnsStatement =
        new CloseAwareStatement(connection, client, sessionId, zoneId);
    ResultSet columnsSource =
        sourceResultSet(
            columnsStatement, values("column_name", "tag1", "datatype", "INT32", "comment", ""));
    columnsStatement.setResultSet(columnsSource);

    CloseAwareStatement primaryKeysStatement =
        new CloseAwareStatement(connection, client, sessionId, zoneId);
    ResultSet primaryKeysSource =
        sourceResultSet(primaryKeysStatement, values("column_name", "tag1", "category", "TAG"));
    primaryKeysStatement.setResultSet(primaryKeysSource);

    when(connection.createStatement())
        .thenReturn(tablesStatement, columnsStatement, primaryKeysStatement);

    IoTDBRelationalDatabaseMetadata metadata =
        new IoTDBRelationalDatabaseMetadata(connection, client, sessionId, zoneId);

    ResultSet tables = metadata.getTables(null, "rootdb", null, null);
    assertNotNull(tables);
    assertEquals("TABLE_SCHEM", tables.getMetaData().getColumnName(1));

    ResultSet columns = metadata.getColumns(null, "rootdb", "table1", null);
    assertEquals("COLUMN_NAME", columns.getMetaData().getColumnName(4));

    ResultSet primaryKeys = metadata.getPrimaryKeys(null, "rootdb", "table1");
    assertEquals("PK_NAME", primaryKeys.getMetaData().getColumnName(6));

    verify(tablesSource).close();
    verify(columnsSource).close();
    verify(primaryKeysSource).close();
  }

  private static ResultSet sourceResultSet(
      CloseAwareStatement statement, Map<String, String> values) throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    AtomicInteger nextCalls = new AtomicInteger();
    when(resultSet.next())
        .thenAnswer(
            invocation -> {
              if (statement.wasClosed()) {
                throw new SQLException("source result set was closed with its statement");
              }
              return nextCalls.getAndIncrement() == 0;
            });
    when(resultSet.getString(anyString()))
        .thenAnswer(invocation -> values.get(invocation.getArgument(0)));
    return resultSet;
  }

  private static Map<String, String> values(String... entries) {
    Map<String, String> values = new HashMap<>();
    for (int i = 0; i < entries.length; i += 2) {
      values.put(entries[i], entries[i + 1]);
    }
    return values;
  }

  private static class CloseAwareStatement extends IoTDBStatement {
    private ResultSet resultSet;
    private boolean closed;

    private CloseAwareStatement(
        IoTDBConnection connection, Iface client, long sessionId, ZoneId zoneId) {
      super(connection, client, sessionId, zoneId, 0, -1L);
    }

    private void setResultSet(ResultSet resultSet) {
      this.resultSet = resultSet;
    }

    private boolean wasClosed() {
      return closed;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
      return resultSet;
    }

    @Override
    public void close() throws SQLException {
      closed = true;
      super.close();
    }
  }
}
