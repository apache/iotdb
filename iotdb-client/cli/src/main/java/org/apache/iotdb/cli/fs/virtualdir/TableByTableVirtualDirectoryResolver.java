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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class TableByTableVirtualDirectoryResolver implements VirtualDirectoryResolver {

  static final String NAME = "by-table";

  private final SqlExecutor executor;
  private final FilesystemSchemaProvider delegate;

  public TableByTableVirtualDirectoryResolver(
      SqlExecutor executor, FilesystemSchemaProvider delegate) {
    this.executor = executor;
    this.delegate = delegate;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public FsNode rootNode() {
    return VirtualDirectoryNodes.root(NAME, "Browse table-model files grouped by table name");
  }

  @Override
  public List<FsNode> list(FsPath path) throws SQLException {
    List<String> segments = VirtualDirectoryPaths.resolverSegments(path);
    if (segments.isEmpty()) {
      return listTableDirectories();
    }
    if (segments.size() == 1) {
      return listDatabaseDirectories(path, segments.get(0));
    }
    if (segments.size() == 2) {
      return listTableFiles(path, segments.get(0), segments.get(1));
    }
    return new ArrayList<>();
  }

  @Override
  public FsNode describe(FsPath path) throws SQLException {
    List<String> segments = VirtualDirectoryPaths.resolverSegments(path);
    if (segments.isEmpty()) {
      return rootNode();
    }
    if (segments.size() == 1) {
      return tableExistsInAnyDatabase(segments.get(0))
          ? VirtualDirectoryNodes.directory(segments.get(0), path, NAME, "", "table")
          : VirtualDirectoryPaths.unknown(path);
    }
    if (segments.size() == 2) {
      return tableExists(segments.get(1), segments.get(0))
          ? VirtualDirectoryNodes.directory(segments.get(1), path, NAME, "", "database")
          : VirtualDirectoryPaths.unknown(path);
    }
    if (segments.size() == 3) {
      return VirtualDirectoryNodes.rewrite(
          delegate.describe(canonicalTableFilePath(segments)), path, NAME);
    }
    return VirtualDirectoryPaths.unknown(path);
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

  private List<FsNode> listTableDirectories() throws SQLException {
    Set<String> tables = new LinkedHashSet<>();
    for (String database : databases()) {
      tables.addAll(tables(database));
    }
    List<FsNode> nodes = new ArrayList<>();
    for (String table : tables) {
      nodes.add(
          VirtualDirectoryNodes.directory(
              table,
              VirtualDirectoryPaths.resolverRootPath(NAME).resolve(table),
              NAME,
              "",
              "table"));
    }
    return nodes;
  }

  private List<FsNode> listDatabaseDirectories(FsPath path, String table) throws SQLException {
    List<FsNode> nodes = new ArrayList<>();
    for (String database : databases()) {
      if (!tables(database).contains(table)) {
        continue;
      }
      nodes.add(
          VirtualDirectoryNodes.directory(database, path.resolve(database), NAME, "", "database"));
    }
    return nodes;
  }

  private List<FsNode> listTableFiles(FsPath path, String table, String database)
      throws SQLException {
    List<FsNode> nodes = new ArrayList<>();
    if (!tableExists(database, table)) {
      return nodes;
    }
    String[] fileNames = {table + ".csv", table + ".schema", table + ".meta"};
    for (String fileName : fileNames) {
      FsPath canonicalPath = FsPath.absolute("/" + database + "/" + fileName);
      FsNode node = delegate.describe(canonicalPath);
      nodes.add(VirtualDirectoryNodes.rewrite(node, path.resolve(fileName), NAME));
    }
    return nodes;
  }

  private boolean tableExistsInAnyDatabase(String table) throws SQLException {
    for (String database : databases()) {
      if (tables(database).contains(table)) {
        return true;
      }
    }
    return false;
  }

  private boolean tableExists(String database, String table) throws SQLException {
    return tables(database).contains(table);
  }

  private List<String> databases() throws SQLException {
    List<String> databases = new ArrayList<>();
    for (SqlRow row : executor.query("SHOW DATABASES")) {
      String database = row.get("Database");
      if (database != null) {
        databases.add(database);
      }
    }
    return databases;
  }

  private List<String> tables(String database) throws SQLException {
    List<String> tables = new ArrayList<>();
    for (SqlRow row : executor.query("SHOW TABLES FROM " + identifier(database))) {
      String table = row.get("TableName");
      if (table != null) {
        tables.add(table);
      }
    }
    return tables;
  }

  private FsPath canonicalLeafPath(FsPath path) throws SQLException {
    List<String> segments = VirtualDirectoryPaths.resolverSegments(path);
    if (segments.size() != 3) {
      throw new SQLException("Path is not readable: " + path);
    }
    return canonicalTableFilePath(segments);
  }

  private static FsPath canonicalTableFilePath(List<String> segments) {
    return FsPath.absolute("/" + segments.get(1) + "/" + segments.get(2));
  }

  private static String identifier(String value) {
    if (value.matches("[A-Za-z_][A-Za-z0-9_]*")) {
      return value;
    }
    return "\"" + value.replace("\"", "\"\"") + "\"";
  }
}
