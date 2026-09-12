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
import static org.junit.Assert.assertNull;
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
  public void describeDeviceIsADirectoryEvenWhenShowTimeseriesUsesExactMatching()
      throws SQLException {
    when(executor.query("SHOW CHILD PATHS root.sg.d1"))
        .thenReturn(SqlRow.list(SqlRow.of("ChildPaths", "root.sg.d1.s1")));
    assertEquals(
        FsNodeType.TREE_INTERNAL_PATH, provider.describe(FsPath.absolute("/root/sg/d1")).getType());
  }

  @Test
  public void schemaReturnsTimeseriesRows() throws SQLException {
    when(executor.query("SHOW TIMESERIES root.sg.d1.s1"))
        .thenReturn(
            SqlRow.list(
                SqlRow.of(
                    "Timeseries", "root.sg.d1.s1", "DataType", "INT32", "Encoding", "PLAIN")));

    List<SqlRow> rows = provider.schema(FsPath.absolute("/root/sg/d1/s1"));

    assertEquals(1, rows.size());
    assertEquals("INT32", rows.get(0).get("data_type"));
    assertEquals("root.sg.d1", rows.get(0).get("object"));
    assertEquals("s1", rows.get(0).get("column"));
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
    mockTimeseries("s1", "INT32");
    when(executor.query("SELECT s1 FROM root.sg.d1 ORDER BY time ASC LIMIT 10"))
        .thenReturn(SqlRow.list(SqlRow.of("Time", "1", "root.sg.d1.s1", "42")));

    List<SqlRow> rows = provider.read(FsPath.absolute("/root/sg/d1/s1"), 10);

    assertEquals(1, rows.size());
    assertEquals("42", rows.get(0).get("s1"));
    assertEquals("1", rows.get(0).get("time"));
    verify(executor).query("SELECT s1 FROM root.sg.d1 ORDER BY time ASC LIMIT 10");
  }

  @Test
  public void countMeasurementUsesFullDeviceTimelineAndNullEntityCount() throws SQLException {
    mockTimeseries("s1", "INT32");
    when(executor.query("SELECT * FROM root.sg.d1 ORDER BY time ASC"))
        .thenReturn(
            SqlRow.list(
                SqlRow.of("Time", "1", "root.sg.d1.s1", "42", "root.sg.d1.s2", null),
                SqlRow.of("Time", "2", "root.sg.d1.s1", null, "root.sg.d1.s2", "other"),
                SqlRow.of("Time", "3", "root.sg.d1.s1", "43", "root.sg.d1.s2", null)));
    SqlRow row = provider.countRows(FsPath.absolute("/root/sg/d1/s1")).get(0);
    assertEquals("root.sg.d1", row.get("object"));
    assertEquals("FIELD", row.get("category"));
    assertEquals("3", row.get("row_count"));
    assertEquals("2", row.get("non_null_count"));
    assertEquals("1", row.get("null_count"));
    assertNull(row.get("entity_count"));
    assertEquals("1", row.get("min_time"));
    assertEquals("3", row.get("max_time"));
  }

  @Test
  public void stringStatisticsAreTypeAwareAndUseNonNullTimeRange() throws SQLException {
    mockTimeseries("s1", "STRING");
    when(executor.query("SELECT * FROM root.sg.d1 ORDER BY time ASC"))
        .thenReturn(
            SqlRow.list(
                SqlRow.of("Time", "1", "root.sg.d1.s1", null, "root.sg.d1.s2", "other"),
                SqlRow.of("Time", "2", "root.sg.d1.s1", "z"),
                SqlRow.of("Time", "3", "root.sg.d1.s1", "a")));
    SqlRow row = provider.stats(FsPath.absolute("/root/sg/d1/s1")).get(0);
    assertEquals("a", row.get("min"));
    assertEquals("z", row.get("max"));
    assertEquals("z", row.get("first"));
    assertEquals("a", row.get("last"));
    assertNull(row.get("sum"));
    assertEquals("2", row.get("min_time"));
    assertEquals("1", row.get("null_count"));
  }

  @Test
  public void deviceReadAndUnlimitedTailUseAllMeasurements() throws SQLException {
    when(executor.query("SHOW TIMESERIES root.sg.d1.**"))
        .thenReturn(
            SqlRow.list(
                SqlRow.of("Timeseries", "root.sg.d1.s1", "DataType", "INT32"),
                SqlRow.of("Timeseries", "root.sg.d1.s2", "DataType", "STRING")));
    when(executor.query("SELECT * FROM root.sg.d1 ORDER BY time DESC"))
        .thenReturn(
            SqlRow.list(
                SqlRow.of("Time", "2", "root.sg.d1.s1", "2"),
                SqlRow.of("Time", "1", "root.sg.d1.s1", "1")));
    assertEquals(3, provider.columns(FsPath.absolute("/root/sg/d1")).size());
    List<SqlRow> rows = provider.tail(FsPath.absolute("/root/sg/d1"), -1);
    assertEquals("1", rows.get(0).get("s1"));
    assertEquals("2", rows.get(1).get("s1"));
  }

  private void mockTimeseries(String name, String type) throws SQLException {
    when(executor.query("SHOW TIMESERIES root.sg.d1." + name))
        .thenReturn(SqlRow.list(SqlRow.of("Timeseries", "root.sg.d1." + name, "DataType", type)));
  }
}
