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

package org.apache.iotdb.cli.fs.provider;

import org.apache.iotdb.cli.fs.node.FsNode;
import org.apache.iotdb.cli.fs.node.FsNodeType;
import org.apache.iotdb.cli.fs.path.FsPath;
import org.apache.iotdb.cli.fs.sql.SqlExecutor;
import org.apache.iotdb.cli.fs.sql.SqlRow;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TableFilesystemSchemaProviderTest {

  @Mock private SqlExecutor executor;

  private TableFilesystemSchemaProvider provider;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    provider = new TableFilesystemSchemaProvider(executor);
  }

  @Test
  public void listRootReturnsDatabases() throws SQLException {
    when(executor.query("SHOW DATABASES"))
        .thenReturn(SqlRow.list(SqlRow.of("Database", "db1"), SqlRow.of("Database", "db2")));

    List<FsNode> children = provider.list(FsPath.absolute("/"));

    assertEquals(2, children.size());
    assertEquals("/db1", children.get(0).getPath().toString());
    assertEquals(FsNodeType.TABLE_DATABASE, children.get(0).getType());
    assertEquals("/db2", children.get(1).getPath().toString());
    verify(executor).query("SHOW DATABASES");
  }

  @Test
  public void listDatabaseReturnsTables() throws SQLException {
    when(executor.query("SHOW TABLES FROM db1"))
        .thenReturn(SqlRow.list(SqlRow.of("TableName", "table1"), SqlRow.of("TableName", "view1")));

    List<FsNode> children = provider.list(FsPath.absolute("/db1"));

    assertEquals(2, children.size());
    assertEquals("/db1/table1", children.get(0).getPath().toString());
    assertEquals(FsNodeType.TABLE_TABLE, children.get(0).getType());
    assertEquals("/db1/view1", children.get(1).getPath().toString());
    verify(executor).query("SHOW TABLES FROM db1");
  }

  @Test
  public void listTableReturnsColumns() throws SQLException {
    when(executor.query("DESC db1.table1 DETAILS"))
        .thenReturn(
            SqlRow.list(
                SqlRow.of("ColumnName", "tag1", "DataType", "STRING", "Category", "TAG"),
                SqlRow.of("ColumnName", "s1", "DataType", "INT32", "Category", "FIELD")));

    List<FsNode> children = provider.list(FsPath.absolute("/db1/table1"));

    assertEquals(2, children.size());
    assertEquals("tag1", children.get(0).getName());
    assertEquals(FsNodeType.TABLE_COLUMN, children.get(0).getType());
    assertEquals("TAG", children.get(0).getMetadata().get("Category"));
    verify(executor).query("DESC db1.table1 DETAILS");
  }

  @Test
  public void describeColumnReturnsColumnMetadata() throws SQLException {
    when(executor.query("DESC db1.table1 DETAILS"))
        .thenReturn(
            SqlRow.list(
                SqlRow.of("ColumnName", "tag1", "DataType", "STRING", "Category", "TAG"),
                SqlRow.of("ColumnName", "s1", "DataType", "INT32", "Category", "FIELD")));

    FsNode node = provider.describe(FsPath.absolute("/db1/table1/s1"));

    assertEquals("s1", node.getName());
    assertEquals("/db1/table1/s1", node.getPath().toString());
    assertEquals(FsNodeType.TABLE_COLUMN, node.getType());
    assertEquals("INT32", node.getMetadata().get("DataType"));
    verify(executor).query("DESC db1.table1 DETAILS");
  }

  @Test
  public void describeVirtualRootReturnsDirectoryNode() throws SQLException {
    FsNode node = provider.describe(FsPath.absolute("/"));

    assertEquals("/", node.getName());
    assertEquals("/", node.getPath().toString());
    assertEquals(FsNodeType.VIRTUAL_ROOT, node.getType());
  }

  @Test
  public void describeDatabaseReturnsDirectoryNode() throws SQLException {
    when(executor.query("SHOW DATABASES")).thenReturn(SqlRow.list(SqlRow.of("Database", "db1")));

    FsNode node = provider.describe(FsPath.absolute("/db1"));

    assertEquals("db1", node.getName());
    assertEquals("/db1", node.getPath().toString());
    assertEquals(FsNodeType.TABLE_DATABASE, node.getType());
    verify(executor).query("SHOW DATABASES");
  }

  @Test
  public void describeTableReturnsDirectoryNode() throws SQLException {
    when(executor.query("SHOW TABLES FROM db1"))
        .thenReturn(SqlRow.list(SqlRow.of("TableName", "table1")));

    FsNode node = provider.describe(FsPath.absolute("/db1/table1"));

    assertEquals("table1", node.getName());
    assertEquals("/db1/table1", node.getPath().toString());
    assertEquals(FsNodeType.TABLE_TABLE, node.getType());
    verify(executor).query("SHOW TABLES FROM db1");
  }

  @Test
  public void readColumnSelectsColumnFromTable() throws SQLException {
    when(executor.query("SELECT s1 FROM db1.table1 LIMIT 5"))
        .thenReturn(SqlRow.list(SqlRow.of("Time", "1", "s1", "42")));

    List<SqlRow> rows = provider.read(FsPath.absolute("/db1/table1/s1"), 5);

    assertEquals(1, rows.size());
    assertEquals("42", rows.get(0).get("s1"));
    verify(executor).query("SELECT s1 FROM db1.table1 LIMIT 5");
  }

  @Test
  public void readTableSelectsAllColumnsFromTable() throws SQLException {
    when(executor.query("SELECT * FROM db1.table1 LIMIT 5"))
        .thenReturn(SqlRow.list(SqlRow.of("Time", "1", "tag1", "a", "s1", "42")));

    List<SqlRow> rows = provider.read(FsPath.absolute("/db1/table1"), 5);

    assertEquals(1, rows.size());
    assertEquals("a", rows.get(0).get("tag1"));
    assertEquals("42", rows.get(0).get("s1"));
    verify(executor).query("SELECT * FROM db1.table1 LIMIT 5");
  }

  @Test
  public void tailTableSelectsNewestRowsAndReturnsOriginalOrder() throws SQLException {
    when(executor.query("SELECT * FROM db1.table1 ORDER BY time DESC LIMIT 2"))
        .thenReturn(
            SqlRow.list(
                SqlRow.of("Time", "2", "tag1", "b", "s1", "43"),
                SqlRow.of("Time", "1", "tag1", "a", "s1", "42")));

    List<SqlRow> rows = provider.tail(FsPath.absolute("/db1/table1"), 2);

    assertEquals("1", rows.get(0).get("Time"));
    assertEquals("2", rows.get(1).get("Time"));
    verify(executor).query("SELECT * FROM db1.table1 ORDER BY time DESC LIMIT 2");
  }

  @Test
  public void countTableSelectsRowCount() throws SQLException {
    when(executor.query("SELECT COUNT(*) FROM db1.table1"))
        .thenReturn(SqlRow.list(SqlRow.of("count", "2")));

    long count = provider.count(FsPath.absolute("/db1/table1"));

    assertEquals(2L, count);
    verify(executor).query("SELECT COUNT(*) FROM db1.table1");
  }

  @Test
  public void readColumnsSelectsMultipleColumnsFromSameTable() throws SQLException {
    when(executor.query("SELECT tag1, s1 FROM db1.table1 LIMIT 5"))
        .thenReturn(SqlRow.list(SqlRow.of("Time", "1", "tag1", "a", "s1", "42")));

    List<SqlRow> rows =
        provider.read(
            Arrays.asList(FsPath.absolute("/db1/table1/tag1"), FsPath.absolute("/db1/table1/s1")),
            5);

    assertEquals(1, rows.size());
    assertEquals("a", rows.get(0).get("tag1"));
    assertEquals("42", rows.get(0).get("s1"));
    verify(executor).query("SELECT tag1, s1 FROM db1.table1 LIMIT 5");
  }
}
