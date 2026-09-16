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
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TreeVirtualDirectoryResolverTest {

  @Mock private SqlExecutor executor;
  @Mock private FilesystemSchemaProvider delegate;

  private TreeByMeasurementVirtualDirectoryResolver byMeasurement;
  private TreeByTagVirtualDirectoryResolver byTag;
  private TreeByAttributeVirtualDirectoryResolver byAttribute;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    byMeasurement = new TreeByMeasurementVirtualDirectoryResolver(executor, delegate);
    byTag = new TreeByTagVirtualDirectoryResolver(executor, delegate);
    byAttribute = new TreeByAttributeVirtualDirectoryResolver(executor, delegate);
  }

  @Test
  public void byMeasurementListsMeasurementDirectories() throws SQLException {
    when(executor.query("SHOW TIMESERIES root.**"))
        .thenReturn(
            SqlRow.list(
                SqlRow.of("Timeseries", "root.sg.d1.s1"),
                SqlRow.of("Timeseries", "root.sg.d2.s2"),
                SqlRow.of("Timeseries", "root.sg.d3.s1")));

    List<FsNode> nodes = byMeasurement.list(FsPath.absolute("/.virtual/by-measurement"));

    assertEquals(2, nodes.size());
    assertEquals("s1", nodes.get(0).getName());
    assertEquals("s2", nodes.get(1).getName());
    assertEquals(FsNodeType.VIRTUAL_DIRECTORY, nodes.get(0).getType());
  }

  @Test
  public void byMeasurementListsTimeseriesFiles() throws SQLException {
    when(executor.query("SHOW TIMESERIES root.**.s1"))
        .thenReturn(SqlRow.list(SqlRow.of("Timeseries", "root.sg.d1.s1", "DataType", "INT32")));

    List<FsNode> nodes = byMeasurement.list(FsPath.absolute("/.virtual/by-measurement/s1"));

    assertEquals(1, nodes.size());
    assertEquals("root.sg.d1.s1", nodes.get(0).getName());
    assertEquals("/.virtual/by-measurement/s1/root.sg.d1.s1", nodes.get(0).getPath().toString());
    assertEquals("/root/sg/d1/s1", nodes.get(0).getMetadata().get("canonicalPath"));
  }

  @Test
  public void byMeasurementReadsThroughCanonicalTimeseriesPath() throws SQLException {
    when(delegate.read(FsPath.absolute("/root/sg/d1/s1"), 10))
        .thenReturn(SqlRow.list(SqlRow.of("Time", "1", "s1", "42")));

    List<SqlRow> rows =
        byMeasurement.read(FsPath.absolute("/.virtual/by-measurement/s1/root.sg.d1.s1"), 10);

    assertEquals("42", rows.get(0).get("s1"));
    verify(delegate).read(FsPath.absolute("/root/sg/d1/s1"), 10);
  }

  @Test
  public void byMeasurementEncodesMeasurementAndTimeseriesSegments() throws SQLException {
    when(executor.query("SHOW TIMESERIES root.**"))
        .thenReturn(SqlRow.list(SqlRow.of("Timeseries", "root.sg.d1.`sensor temp`")));
    when(executor.query("SHOW TIMESERIES root.**.`sensor temp`"))
        .thenReturn(SqlRow.list(SqlRow.of("Timeseries", "root.sg.d1.`sensor temp`")));

    List<FsNode> measurements = byMeasurement.list(FsPath.absolute("/.virtual/by-measurement"));
    List<FsNode> files =
        byMeasurement.list(FsPath.absolute("/.virtual/by-measurement/%60sensor%20temp%60"));

    assertEquals("%60sensor%20temp%60", measurements.get(0).getName());
    assertEquals("`sensor temp`", measurements.get(0).getMetadata().get("measurement"));
    assertEquals("root.sg.d1.%60sensor%20temp%60", files.get(0).getName());
    assertEquals("root.sg.d1.`sensor temp`", files.get(0).getMetadata().get("timeseries"));
  }

  @Test
  public void byTagListsTagKeysAndValues() throws SQLException {
    when(executor.query("SHOW TIMESERIES root.**"))
        .thenReturn(
            SqlRow.list(
                SqlRow.of("Timeseries", "root.sg.d1.s1", "Tags", "{unit=c, site=A}"),
                SqlRow.of("Timeseries", "root.sg.d2.s1", "Tags", "{unit=f}")));

    List<FsNode> keys = byTag.list(FsPath.absolute("/.virtual/by-tag"));
    List<FsNode> values = byTag.list(FsPath.absolute("/.virtual/by-tag/unit"));

    assertEquals("unit", keys.get(0).getName());
    assertEquals("site", keys.get(1).getName());
    assertEquals("c", values.get(0).getName());
    assertEquals("f", values.get(1).getName());
  }

  @Test
  public void byTagListsMatchingTimeseriesFiles() throws SQLException {
    when(executor.query("SHOW TIMESERIES root.** WHERE 'unit' = 'c'"))
        .thenReturn(
            SqlRow.list(
                SqlRow.of("Timeseries", "root.sg.d1.s1", "Tags", "{unit=c}", "DataType", "INT32")));

    List<FsNode> nodes = byTag.list(FsPath.absolute("/.virtual/by-tag/unit/c"));

    assertEquals(1, nodes.size());
    assertEquals("root.sg.d1.s1", nodes.get(0).getName());
    assertEquals("/root/sg/d1/s1", nodes.get(0).getMetadata().get("canonicalPath"));
    verify(executor).query("SHOW TIMESERIES root.** WHERE 'unit' = 'c'");
  }

  @Test
  public void byTagEncodesKeyAndValuePathSegments() throws SQLException {
    when(executor.query("SHOW TIMESERIES root.**"))
        .thenReturn(
            SqlRow.list(
                SqlRow.of("Timeseries", "root.sg.d1.s1", "Tags", "{site zone=building/1}")));

    List<FsNode> keys = byTag.list(FsPath.absolute("/.virtual/by-tag"));
    List<FsNode> values = byTag.list(FsPath.absolute("/.virtual/by-tag/site%20zone"));

    assertEquals("site%20zone", keys.get(0).getName());
    assertEquals("site zone", keys.get(0).getMetadata().get("metadataKey"));
    assertEquals("building%2F1", values.get(0).getName());
    assertEquals("building/1", values.get(0).getMetadata().get("metadataValue"));
  }

  @Test
  public void byAttributeListsMatchingTimeseriesFiles() throws SQLException {
    when(executor.query("SHOW TIMESERIES root.** WHERE 'owner' = 'ops'"))
        .thenReturn(
            SqlRow.list(
                SqlRow.of(
                    "Timeseries",
                    "root.sg.d1.s1",
                    "Attributes",
                    "{owner=ops}",
                    "DataType",
                    "INT32")));

    List<FsNode> nodes = byAttribute.list(FsPath.absolute("/.virtual/by-attribute/owner/ops"));

    assertEquals(1, nodes.size());
    assertEquals("root.sg.d1.s1", nodes.get(0).getName());
    assertEquals("/root/sg/d1/s1", nodes.get(0).getMetadata().get("canonicalPath"));
    assertEquals("root.sg.d1.s1", nodes.get(0).getMetadata().get("timeseries"));
    verify(executor).query("SHOW TIMESERIES root.** WHERE 'owner' = 'ops'");
  }
}
