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

import org.apache.iotdb.cli.fs.path.FsPath;
import org.apache.iotdb.cli.fs.sql.SqlExecutor;
import org.apache.iotdb.cli.fs.sql.SqlRow;
import org.apache.iotdb.cli.i18n.CliMessages;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TableFilesystemMutationSafetyTest {

  @Mock private SqlExecutor executor;
  private TableFilesystemMutationProvider provider;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    AtomicInteger sequence = new AtomicInteger();
    provider =
        new TableFilesystemMutationProvider(executor, () -> "_fs_" + sequence.incrementAndGet());
  }

  @Test
  public void rmdirChecksThatDatabaseIsEmptyBeforeDropping() throws SQLException {
    provider.rmdir(FsPath.absolute("/db1"));

    InOrder order = inOrder(executor);
    order.verify(executor).query("SHOW TABLES FROM db1");
    order.verify(executor).execute("DROP DATABASE db1");
  }

  @Test
  public void rmdirNonemptyDatabaseNeverDropsIt() throws SQLException {
    when(executor.query("SHOW TABLES FROM db1"))
        .thenReturn(SqlRow.list(SqlRow.of("TableName", "table1")));

    expectFailure(
        () -> provider.rmdir(FsPath.absolute("/db1")),
        String.format(CliMessages.FS_DIRECTORY_NOT_EMPTY, "/db1"));

    verify(executor, never()).execute("DROP DATABASE db1");
  }

  @Test
  public void rmdirDoesNotDropDatabaseWhenListingFails() throws SQLException {
    when(executor.query("SHOW TABLES FROM db1")).thenThrow(new SQLException("denied"));

    expectFailure(() -> provider.rmdir(FsPath.absolute("/db1")), "denied");

    verify(executor, never()).execute("DROP DATABASE db1");
  }

  @Test
  public void removeRecursiveAlsoAcceptsDataFile() throws SQLException {
    provider.removeRecursive(FsPath.absolute("/db1/table1.csv"));
    verify(executor).execute("DROP TABLE db1.table1");
  }

  @Test
  public void copyCreatesSchemaThenCopiesEveryColumn() throws SQLException {
    mockTableSchema();

    provider.copy(FsPath.absolute("/db1/table1.csv"), FsPath.absolute("/db2/table2.csv"));

    InOrder order = inOrder(executor);
    order.verify(executor).execute(createTable("db2.table2"));
    order
        .verify(executor)
        .execute(
            "INSERT INTO db2.table2 (time, key, value) SELECT time, key, value FROM db1.table1");
  }

  @Test
  public void copyIntoDatabaseUsesSourceFileName() throws SQLException {
    mockTableSchema();

    provider.copy(FsPath.absolute("/db1/table1.csv"), FsPath.absolute("/db2"));

    verify(executor).execute(createTable("db2.table1"));
  }

  @Test
  public void copyDoesNotModifyPreexistingDestination() throws SQLException {
    mockTableSchema();
    doThrow(new SQLException("already exists")).when(executor).execute(createTable("db2.table1"));

    expectFailure(
        () -> provider.copy(FsPath.absolute("/db1/table1.csv"), FsPath.absolute("/db2")),
        "already exists");

    verify(executor, never()).execute("DROP TABLE db2.table1");
    verify(executor, never())
        .execute(
            "INSERT INTO db2.table1 (time, key, value) SELECT time, key, value FROM db1.table1");
  }

  @Test
  public void failedCopyCleansOnlyNewDestinationAndKeepsSource() throws SQLException {
    mockTableSchema();
    doThrow(new SQLException("insert failed"))
        .when(executor)
        .execute(
            "INSERT INTO db2.table1 (time, key, value) SELECT time, key, value FROM db1.table1");

    expectFailure(
        () -> provider.move(FsPath.absolute("/db1/table1.csv"), FsPath.absolute("/db2")),
        "insert failed");

    verify(executor).execute("DROP TABLE db2.table1");
    verify(executor, never()).execute("DROP TABLE db1.table1");
  }

  @Test
  public void moveAcrossDatabasesDropsSourceOnlyAfterCopy() throws SQLException {
    mockTableSchema();

    provider.move(FsPath.absolute("/db1/table1.csv"), FsPath.absolute("/db2"));

    InOrder order = inOrder(executor);
    order.verify(executor).execute(createTable("db2.table1"));
    order
        .verify(executor)
        .execute(
            "INSERT INTO db2.table1 (time, key, value) SELECT time, key, value FROM db1.table1");
    order.verify(executor).execute("DROP TABLE db1.table1");
  }

  @Test
  public void copyRejectsSameFileIncludingDatabaseDestination() throws SQLException {
    expectFailure(
        () -> provider.copy(FsPath.absolute("/db1/table1.csv"), FsPath.absolute("/db1")),
        String.format(CliMessages.FS_SAME_FILE, "/db1/table1.csv"));
  }

  @Test
  public void copyPreservesMetadataAndQuotesIdentifiers() throws SQLException {
    when(executor.query("DESC \"db-1\".\"source-table\" DETAILS"))
        .thenReturn(
            SqlRow.list(
                SqlRow.of("ColumnName", "time", "DataType", "TIMESTAMP", "Category", "TIME"),
                SqlRow.of("ColumnName", "a-tag", "DataType", "STRING", "Category", "TAG"),
                SqlRow.of(
                    "ColumnName",
                    "value",
                    "DataType",
                    "DOUBLE",
                    "Category",
                    "FIELD",
                    "Comment",
                    "it's a value")));
    when(executor.query("SHOW TABLES DETAILS FROM \"db-1\""))
        .thenReturn(
            SqlRow.list(
                SqlRow.of(
                    "TableName",
                    "source-table",
                    "TTL(ms)",
                    "1234",
                    "NeedLastCache",
                    "false",
                    "Comment",
                    "a table")));

    provider.copy(
        FsPath.absolute("/db-1/source-table.csv"), FsPath.absolute("/db-2/target-table.csv"));

    verify(executor)
        .execute(
            "CREATE TABLE \"db-2\".\"target-table\" (time TIMESTAMP TIME, \"a-tag\" STRING TAG,"
                + " value DOUBLE FIELD COMMENT 'it''s a value') COMMENT 'a table' WITH (ttl=1234,"
                + " need_last_cache=false)");
    verify(executor)
        .execute(
            "INSERT INTO \"db-2\".\"target-table\" (time, \"a-tag\", value) SELECT time, \"a-tag\","
                + " value FROM \"db-1\".\"source-table\"");
  }

  @Test
  public void overwritePublishesPopulatedReplacementBeforeRemovingOriginal() throws SQLException {
    mockTableSchema();

    provider.write(
        FsPath.absolute("/db1/table1.csv"), Arrays.asList("time,key,value", "1,a,2.0"), false);

    InOrder order = inOrder(executor);
    order.verify(executor).execute(createTable("db1._fs_1"));
    order.verify(executor).execute("INSERT INTO db1._fs_1(time, key, value) VALUES (1, 'a', 2.0)");
    order.verify(executor).execute("ALTER TABLE db1.table1 RENAME TO _fs_2");
    order.verify(executor).execute("ALTER TABLE db1._fs_1 RENAME TO table1");
    order.verify(executor).execute("DROP TABLE db1._fs_2");
    verify(executor, never()).execute("DELETE FROM db1.table1");
  }

  @Test
  public void overwriteEmptyInputPublishesEmptyTable() throws SQLException {
    mockTableSchema();

    provider.write(FsPath.absolute("/db1/table1.csv"), Collections.emptyList(), false);

    verify(executor).execute(createTable("db1._fs_1"));
    verify(executor).execute("ALTER TABLE db1._fs_1 RENAME TO table1");
  }

  @Test
  public void overwriteInvalidCsvDoesNotRenameOriginal() throws SQLException {
    mockTableSchema();
    expectFailure(
        () ->
            provider.write(
                FsPath.absolute("/db1/table1.csv"), Arrays.asList("time,key,value", "1,a"), false),
        CliMessages.FS_INVALID_WRITE_OPERATION);

    verify(executor, never()).execute(createTable("db1._fs_1"));
    verify(executor, never()).execute("ALTER TABLE db1.table1 RENAME TO _fs_2");
  }

  @Test
  public void overwriteFailedInsertKeepsOriginal() throws SQLException {
    mockTableSchema();
    doThrow(new SQLException("insert failed"))
        .when(executor)
        .execute("INSERT INTO db1._fs_1(time, key, value) VALUES (1, 'a', 2.0)");

    expectFailure(
        () ->
            provider.write(
                FsPath.absolute("/db1/table1.csv"),
                Arrays.asList("time,key,value", "1,a,2.0"),
                false),
        "insert failed");

    verify(executor).execute("DROP TABLE db1._fs_1");
    verify(executor, never()).execute("ALTER TABLE db1.table1 RENAME TO _fs_2");
  }

  @Test
  public void overwriteFailedPublishRestoresOriginal() throws SQLException {
    mockTableSchema();
    doThrow(new SQLException("rename failed"))
        .when(executor)
        .execute("ALTER TABLE db1._fs_1 RENAME TO table1");

    expectFailure(
        () -> provider.write(FsPath.absolute("/db1/table1.csv"), Collections.emptyList(), false),
        "rename failed");

    InOrder order = inOrder(executor);
    order.verify(executor).execute("ALTER TABLE db1._fs_1 RENAME TO table1");
    order.verify(executor).execute("ALTER TABLE db1._fs_2 RENAME TO table1");
    order.verify(executor).execute("DROP TABLE db1._fs_1");
    verify(executor, never()).execute("DROP TABLE db1._fs_2");
  }

  @Test
  public void writeAppendKeepsExistingSchemaAndRows() throws SQLException {
    mockTableSchema();

    provider.write(FsPath.absolute("/db1/table1.csv"), Arrays.asList("time,value", "1,2.0"), true);

    verify(executor).execute("INSERT INTO db1.table1(time, value) VALUES (1, 2.0)");
    verify(executor, never()).execute(createTable("db1._fs_1"));
  }

  @Test
  public void appendDistinguishesQuotedNullMarkerAndNull() throws SQLException {
    mockTableSchema();

    provider.append(
        FsPath.absolute("/db1/table1.csv"),
        Arrays.asList("time,key,value", "1,\"\\N\",\\N", "2,\\N,3.0"));

    verify(executor)
        .execute(
            "INSERT INTO db1.table1(time, key, value) VALUES (1, '\\N', NULL), (2, NULL, 3.0)");
  }

  @Test
  public void appendPreservesEmbeddedNullMarkerInText() throws SQLException {
    mockTableSchema();

    provider.append(
        FsPath.absolute("/db1/table1.csv"), Arrays.asList("time,key,value", "1,a\\Nb,2.0"));

    verify(executor).execute("INSERT INTO db1.table1(time, key, value) VALUES (1, 'a\\Nb', 2.0)");
  }

  @Test
  public void appendRejectsSqlExpressionsInNumericFields() throws SQLException {
    mockTableSchema();

    expectFailure(
        () ->
            provider.append(
                FsPath.absolute("/db1/table1.csv"), Arrays.asList("time,key,value", "1,a,2 + 3")),
        CliMessages.FS_INVALID_WRITE_OPERATION);
  }

  @Test
  public void appendAcceptsRenamedTimeColumn() throws SQLException {
    when(executor.query("DESC db1.table1 DETAILS"))
        .thenReturn(
            SqlRow.list(
                SqlRow.of("ColumnName", "event_time", "DataType", "TIMESTAMP", "Category", "TIME"),
                SqlRow.of("ColumnName", "value", "DataType", "DOUBLE", "Category", "FIELD")));

    provider.append(FsPath.absolute("/db1/table1.csv"), Arrays.asList("event_time,value", "1,2.0"));

    verify(executor).execute("INSERT INTO db1.table1(event_time, value) VALUES (1, 2.0)");
  }

  @Test
  public void replaceCopyStagesSourceBeforeReplacingExistingTarget() throws SQLException {
    mockTableSchema();
    when(executor.query("SHOW TABLES FROM db2"))
        .thenReturn(SqlRow.list(SqlRow.of("TableName", "table2")));

    provider.copy(FsPath.absolute("/db1/table1.csv"), FsPath.absolute("/db2/table2.csv"), true);

    InOrder order = inOrder(executor);
    order.verify(executor).execute(createTable("db2._fs_1"));
    order
        .verify(executor)
        .execute(
            "INSERT INTO db2._fs_1 (time, key, value) SELECT time, key, value FROM db1.table1");
    order.verify(executor).execute("ALTER TABLE db2.table2 RENAME TO _fs_2");
    order.verify(executor).execute("ALTER TABLE db2._fs_1 RENAME TO table2");
    order.verify(executor).execute("DROP TABLE db2._fs_2");
    verify(executor, never()).execute("DROP TABLE db1.table1");
  }

  @Test
  public void replaceMoveKeepsSourceUntilTargetIsPublished() throws SQLException {
    mockTableSchema();
    when(executor.query("SHOW TABLES FROM db2"))
        .thenReturn(SqlRow.list(SqlRow.of("TableName", "table2")));

    provider.move(FsPath.absolute("/db1/table1.csv"), FsPath.absolute("/db2/table2.csv"), true);

    InOrder order = inOrder(executor);
    order.verify(executor).execute("ALTER TABLE db2._fs_1 RENAME TO table2");
    order.verify(executor).execute("DROP TABLE db2._fs_2");
    order.verify(executor).execute("DROP TABLE db1.table1");
  }

  private void mockTableSchema() throws SQLException {
    when(executor.query("DESC db1.table1 DETAILS"))
        .thenReturn(
            SqlRow.list(
                SqlRow.of("ColumnName", "time", "DataType", "TIMESTAMP", "Category", "TIME"),
                SqlRow.of("ColumnName", "key", "DataType", "STRING", "Category", "TAG"),
                SqlRow.of("ColumnName", "value", "DataType", "DOUBLE", "Category", "FIELD")));
  }

  private static String createTable(String path) {
    return "CREATE TABLE " + path + " (time TIMESTAMP TIME, key STRING TAG, value DOUBLE FIELD)";
  }

  private static void expectFailure(SqlOperation operation, String message) throws SQLException {
    try {
      operation.run();
      fail();
    } catch (SQLException e) {
      assertEquals(message, e.getMessage());
    }
  }

  private interface SqlOperation {
    void run() throws SQLException;
  }
}
