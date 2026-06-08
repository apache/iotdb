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

package org.apache.iotdb.cli.fs.virtualdir;

import org.apache.iotdb.cli.fs.node.FsNode;
import org.apache.iotdb.cli.fs.node.FsNodeType;
import org.apache.iotdb.cli.fs.path.FsPath;
import org.apache.iotdb.cli.fs.provider.FilesystemSchemaProvider;
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

public class TableVirtualDirectoryResolverTest {

  @Mock private SqlExecutor executor;
  @Mock private FilesystemSchemaProvider delegate;

  private ByDatabaseVirtualDirectoryResolver byDatabase;
  private TableByTableVirtualDirectoryResolver byTable;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    byDatabase = new ByDatabaseVirtualDirectoryResolver(executor, delegate, false);
    byTable = new TableByTableVirtualDirectoryResolver(executor, delegate);
  }

  @Test
  public void byDatabaseListsDatabasesAsVirtualDirectories() throws SQLException {
    when(executor.query("SHOW DATABASES"))
        .thenReturn(SqlRow.list(SqlRow.of("Database", "db1"), SqlRow.of("Database", "db2")));

    List<FsNode> nodes = byDatabase.list(FsPath.absolute("/.virtual/by-database"));

    assertEquals(2, nodes.size());
    assertEquals("db1", nodes.get(0).getName());
    assertEquals("/db1", nodes.get(0).getMetadata().get("canonicalPath"));
  }

  @Test
  public void byDatabaseListsCanonicalChildrenUnderVirtualPath() throws SQLException {
    when(delegate.list(FsPath.absolute("/db1")))
        .thenReturn(
            Arrays.asList(
                new FsNode(
                    "table1.csv", FsPath.absolute("/db1/table1.csv"), FsNodeType.TABLE_DATA_FILE)));

    List<FsNode> nodes = byDatabase.list(FsPath.absolute("/.virtual/by-database/db1"));

    assertEquals(1, nodes.size());
    assertEquals("/.virtual/by-database/db1/table1.csv", nodes.get(0).getPath().toString());
    assertEquals("/db1/table1.csv", nodes.get(0).getMetadata().get("canonicalPath"));
  }

  @Test
  public void byTableGroupsTablesBeforeDatabasesAndSidecars() throws SQLException {
    when(executor.query("SHOW DATABASES"))
        .thenReturn(SqlRow.list(SqlRow.of("Database", "db1"), SqlRow.of("Database", "db2")));
    when(executor.query("SHOW TABLES FROM db1"))
        .thenReturn(SqlRow.list(SqlRow.of("TableName", "t1")));
    when(executor.query("SHOW TABLES FROM db2"))
        .thenReturn(SqlRow.list(SqlRow.of("TableName", "t1")));
    when(delegate.describe(FsPath.absolute("/db1/t1.csv")))
        .thenReturn(
            new FsNode("t1.csv", FsPath.absolute("/db1/t1.csv"), FsNodeType.TABLE_DATA_FILE));
    when(delegate.describe(FsPath.absolute("/db1/t1.schema")))
        .thenReturn(
            new FsNode(
                "t1.schema", FsPath.absolute("/db1/t1.schema"), FsNodeType.TABLE_SCHEMA_FILE));
    when(delegate.describe(FsPath.absolute("/db1/t1.meta")))
        .thenReturn(
            new FsNode("t1.meta", FsPath.absolute("/db1/t1.meta"), FsNodeType.TABLE_META_FILE));

    List<FsNode> tables = byTable.list(FsPath.absolute("/.virtual/by-table"));
    List<FsNode> databases = byTable.list(FsPath.absolute("/.virtual/by-table/t1"));
    List<FsNode> files = byTable.list(FsPath.absolute("/.virtual/by-table/t1/db1"));

    assertEquals("t1", tables.get(0).getName());
    assertEquals(2, databases.size());
    assertEquals("db1", databases.get(0).getName());
    assertEquals(3, files.size());
    assertEquals("/.virtual/by-table/t1/db1/t1.csv", files.get(0).getPath().toString());
    assertEquals("/db1/t1.csv", files.get(0).getMetadata().get("canonicalPath"));
  }

  @Test
  public void byTableReadsThroughCanonicalTableFilePath() throws SQLException {
    when(delegate.readLines(FsPath.absolute("/db1/t1.csv"), 5))
        .thenReturn(Arrays.asList("Time,s1", "1,42"));

    List<String> lines = byTable.readLines(FsPath.absolute("/.virtual/by-table/t1/db1/t1.csv"), 5);

    assertEquals("1,42", lines.get(1));
    verify(delegate).readLines(FsPath.absolute("/db1/t1.csv"), 5);
  }
}
