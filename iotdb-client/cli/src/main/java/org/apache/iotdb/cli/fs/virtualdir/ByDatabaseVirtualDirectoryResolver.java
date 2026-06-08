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
import org.apache.iotdb.cli.fs.path.FsPath;
import org.apache.iotdb.cli.fs.provider.FilesystemSchemaProvider;
import org.apache.iotdb.cli.fs.sql.SqlExecutor;
import org.apache.iotdb.cli.fs.sql.SqlRow;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ByDatabaseVirtualDirectoryResolver implements VirtualDirectoryResolver {

  static final String NAME = "by-database";

  private final SqlExecutor executor;
  private final FilesystemSchemaProvider delegate;
  private final boolean treeModel;

  public ByDatabaseVirtualDirectoryResolver(
      SqlExecutor executor, FilesystemSchemaProvider delegate, boolean treeModel) {
    this.executor = executor;
    this.delegate = delegate;
    this.treeModel = treeModel;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public FsNode rootNode() {
    return VirtualDirectoryNodes.root(NAME, "Browse canonical objects grouped by database");
  }

  @Override
  public List<FsNode> list(FsPath path) throws SQLException {
    List<String> segments = VirtualDirectoryPaths.resolverSegments(path);
    if (segments.isEmpty()) {
      return listDatabases();
    }
    List<FsNode> nodes = new ArrayList<>();
    for (FsNode node : delegate.list(canonicalPath(segments))) {
      nodes.add(VirtualDirectoryNodes.rewrite(node, path.resolve(node.getName()), NAME));
    }
    return nodes;
  }

  @Override
  public FsNode describe(FsPath path) throws SQLException {
    List<String> segments = VirtualDirectoryPaths.resolverSegments(path);
    if (segments.isEmpty()) {
      return rootNode();
    }
    if (segments.size() == 1) {
      return describeDatabase(segments.get(0));
    }
    FsNode node = delegate.describe(canonicalPath(segments));
    return VirtualDirectoryNodes.rewrite(node, path, NAME);
  }

  @Override
  public List<SqlRow> read(FsPath path, int limit) throws SQLException {
    return delegate.read(canonicalLeafPath(path), limit);
  }

  @Override
  public List<String> readLines(FsPath path, int limit) throws SQLException {
    return delegate.readLines(canonicalLeafPath(path), limit);
  }

  @Override
  public List<SqlRow> tail(FsPath path, int limit) throws SQLException {
    return delegate.tail(canonicalLeafPath(path), limit);
  }

  @Override
  public List<String> tailLines(FsPath path, int limit) throws SQLException {
    return delegate.tailLines(canonicalLeafPath(path), limit);
  }

  @Override
  public long count(FsPath path) throws SQLException {
    return delegate.count(canonicalLeafPath(path));
  }

  private List<FsNode> listDatabases() throws SQLException {
    List<FsNode> nodes = new ArrayList<>();
    for (SqlRow row : executor.query("SHOW DATABASES")) {
      String database = row.get("Database");
      if (database == null) {
        continue;
      }
      FsPath path = VirtualDirectoryPaths.resolverRootPath(NAME).resolve(database);
      nodes.add(
          VirtualDirectoryNodes.directory(
              database, path, NAME, canonicalDatabasePath(database).toString(), "database"));
    }
    return nodes;
  }

  private FsNode describeDatabase(String database) throws SQLException {
    for (SqlRow row : executor.query("SHOW DATABASES")) {
      if (database.equals(row.get("Database"))) {
        FsPath path = VirtualDirectoryPaths.resolverRootPath(NAME).resolve(database);
        return VirtualDirectoryNodes.directory(
            database, path, NAME, canonicalDatabasePath(database).toString(), "database");
      }
    }
    return VirtualDirectoryPaths.unknown(
        VirtualDirectoryPaths.resolverRootPath(NAME).resolve(database));
  }

  private FsPath canonicalLeafPath(FsPath path) throws SQLException {
    List<String> segments = VirtualDirectoryPaths.resolverSegments(path);
    if (segments.size() <= 1) {
      throw new SQLException("Path is not readable: " + path);
    }
    return canonicalPath(segments);
  }

  private FsPath canonicalPath(List<String> segments) {
    if (segments.isEmpty()) {
      return FsPath.absolute("/");
    }
    StringBuilder builder = new StringBuilder(canonicalDatabasePath(segments.get(0)).toString());
    for (int i = 1; i < segments.size(); i++) {
      builder.append('/').append(segments.get(i));
    }
    return FsPath.absolute(builder.toString());
  }

  private FsPath canonicalDatabasePath(String database) {
    if (treeModel) {
      return FsPath.absolute("/" + database.replace('.', '/'));
    }
    return FsPath.absolute("/" + database);
  }
}
