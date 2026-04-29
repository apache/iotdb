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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TreeFilesystemSchemaProviderTest {

  @Mock private SqlExecutor executor;

  private TreeFilesystemSchemaProvider provider;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    provider = new TreeFilesystemSchemaProvider(executor);
  }

  @Test
  public void listRootDiscoversTreeRootFromDatabases() throws SQLException {
    when(executor.query("SHOW DATABASES"))
        .thenReturn(
            SqlRow.list(SqlRow.of("Database", "root.sg"), SqlRow.of("Database", "root.ln")));

    List<FsNode> children = provider.list(FsPath.absolute("/"));

    assertEquals(1, children.size());
    assertEquals("root", children.get(0).getName());
    assertEquals("/root", children.get(0).getPath().toString());
    assertEquals(FsNodeType.TREE_ROOT, children.get(0).getType());
    verify(executor).query("SHOW DATABASES");
  }

  @Test
  public void listTreeRootReturnsDatabases() throws SQLException {
    when(executor.query("SHOW DATABASES"))
        .thenReturn(
            SqlRow.list(SqlRow.of("Database", "root.sg"), SqlRow.of("Database", "root.ln")));

    List<FsNode> children = provider.list(FsPath.absolute("/root"));

    assertEquals(2, children.size());
    assertEquals("/root/sg", children.get(0).getPath().toString());
    assertEquals(FsNodeType.TREE_DATABASE, children.get(0).getType());
    assertEquals("/root/ln", children.get(1).getPath().toString());
    verify(executor).query("SHOW DATABASES");
  }

  @Test
  public void listInternalTreePathReturnsChildren() throws SQLException {
    when(executor.query("SHOW CHILD PATHS root.sg"))
        .thenReturn(
            SqlRow.list(
                SqlRow.of("ChildPaths", "root.sg.d1"), SqlRow.of("ChildPaths", "root.sg.d2")));

    List<FsNode> children = provider.list(FsPath.absolute("/root/sg"));

    assertEquals(2, children.size());
    assertEquals("d1", children.get(0).getName());
    assertEquals("/root/sg/d1", children.get(0).getPath().toString());
    assertEquals(FsNodeType.TREE_INTERNAL_PATH, children.get(0).getType());
    verify(executor).query("SHOW CHILD PATHS root.sg");
  }

  @Test
  public void describeTimeseriesReturnsMetadataNode() throws SQLException {
    when(executor.query("SHOW TIMESERIES root.sg.d1.s1"))
        .thenReturn(
            SqlRow.list(
                SqlRow.of(
                    "Timeseries",
                    "root.sg.d1.s1",
                    "Alias",
                    "",
                    "Database",
                    "root.sg",
                    "DataType",
                    "INT32")));

    FsNode node = provider.describe(FsPath.absolute("/root/sg/d1/s1"));

    assertEquals("s1", node.getName());
    assertEquals("/root/sg/d1/s1", node.getPath().toString());
    assertEquals(FsNodeType.TREE_TIMESERIES, node.getType());
    assertEquals("INT32", node.getMetadata().get("DataType"));
    verify(executor).query("SHOW TIMESERIES root.sg.d1.s1");
  }

  @Test
  public void describeVirtualRootReturnsDirectoryNode() throws SQLException {
    FsNode node = provider.describe(FsPath.absolute("/"));

    assertEquals("/", node.getName());
    assertEquals("/", node.getPath().toString());
    assertEquals(FsNodeType.VIRTUAL_ROOT, node.getType());
  }

  @Test
  public void describeTreeRootReturnsDirectoryNode() throws SQLException {
    FsNode node = provider.describe(FsPath.absolute("/root"));

    assertEquals("root", node.getName());
    assertEquals("/root", node.getPath().toString());
    assertEquals(FsNodeType.TREE_ROOT, node.getType());
  }

  @Test
  public void describeDatabaseReturnsDirectoryNode() throws SQLException {
    when(executor.query("SHOW DATABASES"))
        .thenReturn(SqlRow.list(SqlRow.of("Database", "root.sg")));

    FsNode node = provider.describe(FsPath.absolute("/root/sg"));

    assertEquals("sg", node.getName());
    assertEquals("/root/sg", node.getPath().toString());
    assertEquals(FsNodeType.TREE_DATABASE, node.getType());
    verify(executor).query("SHOW DATABASES");
  }

  @Test
  public void readTimeseriesSelectsMeasurementFromDevice() throws SQLException {
    when(executor.query("SELECT s1 FROM root.sg.d1 LIMIT 10"))
        .thenReturn(SqlRow.list(SqlRow.of("Time", "1", "s1", "42")));

    List<SqlRow> rows = provider.read(FsPath.absolute("/root/sg/d1/s1"), 10);

    assertEquals(1, rows.size());
    assertEquals("42", rows.get(0).get("s1"));
    verify(executor).query("SELECT s1 FROM root.sg.d1 LIMIT 10");
  }
}
