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
import org.apache.iotdb.cli.fs.sql.SqlRow;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class VirtualDirectorySchemaProviderTest {

  @Mock private FilesystemSchemaProvider delegate;
  @Mock private VirtualDirectoryResolver resolver;

  private VirtualDirectorySchemaProvider provider;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(resolver.name()).thenReturn("by-test");
    when(resolver.rootNode())
        .thenReturn(
            new FsNode(
                "by-test", FsPath.absolute("/.virtual/by-test"), FsNodeType.VIRTUAL_DIRECTORY));
    provider = new VirtualDirectorySchemaProvider(delegate, Arrays.asList(resolver));
  }

  @Test
  public void listRootAddsVirtualDirectoryRoot() throws SQLException {
    when(delegate.list(FsPath.absolute("/")))
        .thenReturn(
            Arrays.asList(
                new FsNode("root", FsPath.absolute("/root"), FsNodeType.TREE_ROOT),
                new FsNode(".virtual", FsPath.absolute("/.virtual"), FsNodeType.TABLE_DATABASE)));

    List<FsNode> nodes = provider.list(FsPath.absolute("/"));

    assertEquals(2, nodes.size());
    assertEquals("root", nodes.get(0).getName());
    assertEquals(".virtual", nodes.get(1).getName());
    assertEquals(FsNodeType.VIRTUAL_DIRECTORY, nodes.get(1).getType());
  }

  @Test
  public void listVirtualRootReturnsRegisteredResolvers() throws SQLException {
    List<FsNode> nodes = provider.list(FsPath.absolute("/.virtual"));

    assertEquals(1, nodes.size());
    assertEquals("by-test", nodes.get(0).getName());
    verify(resolver).rootNode();
  }

  @Test
  public void virtualPathDispatchesToResolver() throws SQLException {
    FsPath path = FsPath.absolute("/.virtual/by-test/value");
    when(resolver.describe(path))
        .thenReturn(new FsNode("value", path, FsNodeType.VIRTUAL_DIRECTORY));

    FsNode node = provider.describe(path);

    assertEquals("value", node.getName());
    verify(resolver).describe(path);
  }

  @Test
  public void virtualReadDispatchesToResolver() throws SQLException {
    FsPath path = FsPath.absolute("/.virtual/by-test/value");
    when(resolver.read(path, 5)).thenReturn(SqlRow.list(SqlRow.of("value", "42")));

    List<SqlRow> rows = provider.read(path, 5);

    assertEquals("42", rows.get(0).get("value"));
    verify(resolver).read(path, 5);
  }

  @Test
  public void unknownVirtualPathDoesNotDelegateRead() throws SQLException {
    FsPath path = FsPath.absolute("/.virtual/missing/value");

    try {
      provider.read(path, 1);
      fail("Expected unknown virtual path to be rejected");
    } catch (SQLException e) {
      assertEquals("Path is not readable: " + path, e.getMessage());
    }
    verify(delegate, never()).read(path, 1);
  }

  @Test
  public void multipleReadRejectsVirtualPathBeforeDelegating() throws SQLException {
    List<FsPath> paths =
        Arrays.asList(FsPath.absolute("/db/table.csv"), FsPath.absolute("/.virtual/by-test/value"));

    try {
      provider.read(paths, 1);
      fail("Expected mixed virtual multi-read to be rejected");
    } catch (SQLException e) {
      assertEquals(
          "Multiple paths are not readable when virtual paths are included", e.getMessage());
    }
    verify(delegate, never()).read(paths, 1);
  }
}
