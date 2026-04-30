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
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
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

    assertEquals(6, children.size());
    assertEquals("table1.csv", children.get(0).getName());
    assertEquals("/db1/table1.csv", children.get(0).getPath().toString());
    assertEquals(FsNodeType.TABLE_DATA_FILE, children.get(0).getType());
    assertEquals("table1.schema", children.get(1).getName());
    assertEquals(FsNodeType.TABLE_SCHEMA_FILE, children.get(1).getType());
    assertEquals("table1.meta", children.get(2).getName());
    assertEquals(FsNodeType.TABLE_META_FILE, children.get(2).getType());
    assertEquals("view1.csv", children.get(3).getName());
    verify(executor).query("SHOW TABLES FROM db1");
  }

  @Test
  public void describeTableCsvReturnsDataFileNode() throws SQLException {
    when(executor.query("SHOW TABLES FROM db1"))
        .thenReturn(SqlRow.list(SqlRow.of("TableName", "table1")));

    FsNode node = provider.describe(FsPath.absolute("/db1/table1.csv"));

    assertEquals("table1.csv", node.getName());
    assertEquals(FsNodeType.TABLE_DATA_FILE, node.getType());
    assertEquals("table1", node.getMetadata().get("table"));
    assertEquals("csv", node.getMetadata().get("format"));
  }

  @Test
  public void bareTablePathIsNotDirectoryInSidecarModel() throws SQLException {
    List<FsNode> children = provider.list(FsPath.absolute("/db1/table1"));
    FsNode node = provider.describe(FsPath.absolute("/db1/table1"));

    assertEquals(0, children.size());
    assertEquals("table1", node.getName());
    assertEquals(FsNodeType.UNKNOWN, node.getType());
    verifyZeroInteractions(executor);
  }

  @Test
  public void columnPathIsNotAFileInSidecarModel() throws SQLException {
    FsNode node = provider.describe(FsPath.absolute("/db1/table1/s1"));

    assertEquals("s1", node.getName());
    assertEquals("/db1/table1/s1", node.getPath().toString());
    assertEquals(FsNodeType.UNKNOWN, node.getType());
    verifyZeroInteractions(executor);
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
  public void readBareTableAndColumnPathsAreRejected() throws SQLException {
    assertSqlError(
        () -> provider.read(FsPath.absolute("/db1/table1"), 5),
        "Path is not readable: /db1/table1");
    assertSqlError(
        () -> provider.read(FsPath.absolute("/db1/table1/s1"), 5),
        "Path is not readable: /db1/table1/s1");
  }

  @Test
  public void readTableCsvReturnsCsvLinesWithHeader() throws SQLException {
    mockTableExists("db1", "table1");
    when(executor.query("SELECT * FROM db1.table1 LIMIT 5"))
        .thenReturn(SqlRow.list(SqlRow.of("Time", "1", "tag1", "a", "s1", "42")));

    List<String> lines = provider.readLines(FsPath.absolute("/db1/table1.csv"), 5);

    assertEquals("Time,tag1,s1", lines.get(0));
    assertEquals("1,a,42", lines.get(1));
    verify(executor).query("SELECT * FROM db1.table1 LIMIT 5");
  }

  @Test
  public void readTableCsvQuotesSpecialIdentifiers() throws SQLException {
    when(executor.query("SHOW TABLES FROM \"db-1\""))
        .thenReturn(SqlRow.list(SqlRow.of("TableName", "table-1")));
    when(executor.query("SELECT * FROM \"db-1\".\"table-1\" LIMIT 5"))
        .thenReturn(SqlRow.list(SqlRow.of("Time", "1", "tag1", "a")));

    List<String> lines = provider.readLines(FsPath.absolute("/db-1/table-1.csv"), 5);

    assertEquals("Time,tag1", lines.get(0));
    assertEquals("1,a", lines.get(1));
    verify(executor).query("SHOW TABLES FROM \"db-1\"");
    verify(executor).query("SELECT * FROM \"db-1\".\"table-1\" LIMIT 5");
  }

  @Test
  public void readTableSchemaReturnsIoTDBDescCsvLines() throws SQLException {
    mockTableExists("db1", "table1");
    when(executor.query("DESC db1.table1 DETAILS"))
        .thenReturn(
            SqlRow.list(
                SqlRow.of("ColumnName", "tag1", "DataType", "STRING", "Category", "TAG"),
                SqlRow.of("ColumnName", "s1", "DataType", "INT32", "Category", "FIELD")));

    List<String> lines = provider.readLines(FsPath.absolute("/db1/table1.schema"), 5);

    assertEquals("ColumnName,DataType,Category", lines.get(0));
    assertEquals("tag1,STRING,TAG", lines.get(1));
    assertEquals("s1,INT32,FIELD", lines.get(2));
    verify(executor).query("DESC db1.table1 DETAILS");
  }

  @Test
  public void readTableMetaReturnsIoTDBTableCsvLines() throws SQLException {
    mockTableExists("db1", "table1");
    when(executor.query("SHOW TABLES DETAILS FROM db1"))
        .thenReturn(
            SqlRow.list(
                SqlRow.of(
                    "TableName",
                    "table1",
                    "TTL(ms)",
                    "3600000",
                    "Status",
                    "USING",
                    "Comment",
                    "main table"),
                SqlRow.of(
                    "TableName",
                    "table2",
                    "TTL(ms)",
                    "INF",
                    "Status",
                    "USING",
                    "Comment",
                    "archive")));

    List<String> lines = provider.readLines(FsPath.absolute("/db1/table1.meta"), 5);

    assertEquals("TableName,TTL(ms),Status,Comment", lines.get(0));
    assertEquals("table1,3600000,USING,main table", lines.get(1));
    assertEquals(2, lines.size());
    verify(executor).query("SHOW TABLES DETAILS FROM db1");
  }

  @Test
  public void tailTableCsvReturnsLastPhysicalLines() throws SQLException {
    mockTableExists("db1", "table1");
    when(executor.query("SELECT * FROM db1.table1 ORDER BY time DESC LIMIT 2"))
        .thenReturn(
            SqlRow.list(
                SqlRow.of("Time", "3", "tag1", "c", "s1", "44"),
                SqlRow.of("Time", "2", "tag1", "b", "s1", "43")));

    List<String> lines = provider.tailLines(FsPath.absolute("/db1/table1.csv"), 2);

    assertEquals("2,b,43", lines.get(0));
    assertEquals("3,c,44", lines.get(1));
    verify(executor).query("SELECT * FROM db1.table1 ORDER BY time DESC LIMIT 2");
  }

  @Test
  public void countTableCsvCountsHeaderAndRows() throws SQLException {
    mockTableExists("db1", "table1");
    when(executor.query("SELECT COUNT(*) FROM db1.table1"))
        .thenReturn(SqlRow.list(SqlRow.of("count", "2")));

    long count = provider.count(FsPath.absolute("/db1/table1.csv"));

    assertEquals(3L, count);
    verify(executor).query("SELECT COUNT(*) FROM db1.table1");
  }

  @Test
  public void countSchemaAndMetaCountsCsvLines() throws SQLException {
    mockTableExists("db1", "table1");
    when(executor.query("DESC db1.table1 DETAILS"))
        .thenReturn(
            SqlRow.list(
                SqlRow.of("ColumnName", "tag1", "DataType", "STRING", "Category", "TAG"),
                SqlRow.of("ColumnName", "s1", "DataType", "INT32", "Category", "FIELD")));
    when(executor.query("SHOW TABLES DETAILS FROM db1"))
        .thenReturn(
            SqlRow.list(
                SqlRow.of("TableName", "table1", "Status", "USING"),
                SqlRow.of("TableName", "table2", "Status", "USING")));

    assertEquals(3L, provider.count(FsPath.absolute("/db1/table1.schema")));
    assertEquals(2L, provider.count(FsPath.absolute("/db1/table1.meta")));
    verify(executor).query("DESC db1.table1 DETAILS");
    verify(executor).query("SHOW TABLES DETAILS FROM db1");
  }

  @Test
  public void missingSidecarFilesAreRejectedBeforeReadingContent() throws SQLException {
    when(executor.query("SHOW TABLES FROM db1"))
        .thenReturn(SqlRow.list(SqlRow.of("TableName", "table2")));

    assertSqlError(
        () -> provider.readLines(FsPath.absolute("/db1/table1.csv"), 5),
        "Path does not exist: /db1/table1.csv");
    assertSqlError(
        () -> provider.readLines(FsPath.absolute("/db1/table1.schema"), 5),
        "Path does not exist: /db1/table1.schema");
    assertSqlError(
        () -> provider.readLines(FsPath.absolute("/db1/table1.meta"), 5),
        "Path does not exist: /db1/table1.meta");

    verify(executor, times(3)).query("SHOW TABLES FROM db1");
  }

  @Test
  public void missingTableCsvTailAndCountAreRejectedBeforeDataQueries() throws SQLException {
    when(executor.query("SHOW TABLES FROM db1"))
        .thenReturn(SqlRow.list(SqlRow.of("TableName", "table2")));

    assertSqlError(
        () -> provider.tailLines(FsPath.absolute("/db1/table1.csv"), 2),
        "Path does not exist: /db1/table1.csv");
    assertSqlError(
        () -> provider.count(FsPath.absolute("/db1/table1.csv")),
        "Path does not exist: /db1/table1.csv");

    verify(executor, times(2)).query("SHOW TABLES FROM db1");
  }

  private static void assertSqlError(SqlOperation operation, String message) throws SQLException {
    try {
      operation.run();
      fail();
    } catch (SQLException e) {
      assertEquals(message, e.getMessage());
    }
  }

  private void mockTableExists(String database, String table) throws SQLException {
    when(executor.query("SHOW TABLES FROM " + database))
        .thenReturn(SqlRow.list(SqlRow.of("TableName", table)));
  }

  private interface SqlOperation {
    void run() throws SQLException;
  }
}
