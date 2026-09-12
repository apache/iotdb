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

package org.apache.iotdb.cli.fs.sql;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JdbcSqlExecutorTest {

  @Mock private Connection connection;
  @Mock private Statement statement;
  @Mock private ResultSet resultSet;
  @Mock private ResultSetMetaData metaData;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void queryConvertsResultSetToSqlRows() throws Exception {
    when(connection.createStatement()).thenReturn(statement);
    when(statement.executeQuery("SHOW DATABASES")).thenReturn(resultSet);
    when(resultSet.getMetaData()).thenReturn(metaData);
    when(metaData.getColumnCount()).thenReturn(2);
    when(metaData.getColumnLabel(1)).thenReturn("Database");
    when(metaData.getColumnLabel(2)).thenReturn("TTL");
    when(resultSet.next()).thenReturn(true, true, false);
    when(resultSet.getString(1)).thenReturn("root.sg", "root.ln");
    when(resultSet.getString(2)).thenReturn("INF", "INF");

    List<SqlRow> rows = new JdbcSqlExecutor(connection).query("SHOW DATABASES");

    assertEquals(2, rows.size());
    assertEquals("root.sg", rows.get(0).get("Database"));
    assertEquals("INF", rows.get(0).get("TTL"));
    assertEquals("root.ln", rows.get(1).get("Database"));
    verify(statement).executeQuery("SHOW DATABASES");
    verify(resultSet).close();
    verify(statement).close();
  }

  @Test
  public void executeRunsStatement() throws Exception {
    when(connection.createStatement()).thenReturn(statement);

    new JdbcSqlExecutor(connection).execute("CREATE DATABASE db1");

    verify(statement).execute("CREATE DATABASE db1");
    verify(statement).close();
  }

  @Test
  public void queryPreservesTimestampPrecisionAndSqlNull() throws Exception {
    when(connection.createStatement()).thenReturn(statement);
    when(statement.executeQuery("SELECT * FROM t")).thenReturn(resultSet);
    when(resultSet.getMetaData()).thenReturn(metaData);
    when(metaData.getColumnCount()).thenReturn(1);
    when(metaData.getColumnLabel(1)).thenReturn("time");
    when(metaData.getColumnTypeName(1)).thenReturn("TIMESTAMP");
    when(resultSet.next()).thenReturn(true, true, false);
    when(resultSet.getLong(1)).thenReturn(9007199254740993L, 0L);
    when(resultSet.wasNull()).thenReturn(false, true);
    List<SqlRow> rows = new JdbcSqlExecutor(connection).query("SELECT * FROM t");
    assertEquals("9007199254740993", rows.get(0).get("time"));
    assertEquals("TIMESTAMP", rows.get(0).getDataType("time"));
    assertNull(rows.get(1).get("time"));
  }

  @Test
  public void arbitrarySqlSupportsResultSetsAndUpdates() throws Exception {
    when(connection.createStatement()).thenReturn(statement);
    when(statement.execute("SHOW DATABASES")).thenReturn(true);
    when(statement.getResultSet()).thenReturn(resultSet);
    when(resultSet.getMetaData()).thenReturn(metaData);
    when(metaData.getColumnCount()).thenReturn(1);
    when(metaData.getColumnLabel(1)).thenReturn("Database");
    when(metaData.getColumnTypeName(1)).thenReturn("STRING");
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getString(1)).thenReturn("db1");
    JdbcSqlExecutor executor = new JdbcSqlExecutor(connection);
    assertEquals("db1", executor.executeQueryOrUpdate("SHOW DATABASES").get(0).get("Database"));
    assertEquals(0, executor.executeQueryOrUpdate("CREATE DATABASE db2").size());
    verify(statement).execute("CREATE DATABASE db2");
  }
}
