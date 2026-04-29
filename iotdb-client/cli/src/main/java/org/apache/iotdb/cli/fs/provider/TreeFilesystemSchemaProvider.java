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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class TreeFilesystemSchemaProvider implements FilesystemSchemaProvider {

  private static final String ROOT = "root";

  private final SqlExecutor executor;

  public TreeFilesystemSchemaProvider(SqlExecutor executor) {
    this.executor = executor;
  }

  @Override
  public List<FsNode> list(FsPath path) throws SQLException {
    if (path.isRoot()) {
      return listTreeRoots();
    }
    if (ROOT.equals(path.toString().substring(1))) {
      return listDatabases();
    }
    return listChildPaths(path);
  }

  @Override
  public FsNode describe(FsPath path) throws SQLException {
    if (path.isRoot()) {
      return new FsNode("/", path, FsNodeType.VIRTUAL_ROOT);
    }
    if (isTreeRoot(path)) {
      return new FsNode(ROOT, path, FsNodeType.TREE_ROOT);
    }
    if (isDatabase(path)) {
      return new FsNode(path.getFileName(), path, FsNodeType.TREE_DATABASE);
    }
    List<SqlRow> rows = executor.query("SHOW TIMESERIES " + toTreePath(path));
    if (rows.isEmpty()) {
      return new FsNode(path.getFileName(), path, FsNodeType.UNKNOWN);
    }
    return new FsNode(path.getFileName(), path, FsNodeType.TREE_TIMESERIES, rows.get(0).asMap());
  }

  @Override
  public List<SqlRow> read(FsPath path, int limit) throws SQLException {
    String measurement = path.getFileName();
    FsPath devicePath = parent(path);
    return executor.query(
        "SELECT " + measurement + " FROM " + toTreePath(devicePath) + " LIMIT " + limit);
  }

  @Override
  public List<SqlRow> tail(FsPath path, int limit) throws SQLException {
    String measurement = path.getFileName();
    FsPath devicePath = parent(path);
    List<SqlRow> rows =
        executor.query(
            "SELECT "
                + measurement
                + " FROM "
                + toTreePath(devicePath)
                + " ORDER BY time DESC LIMIT "
                + limit);
    Collections.reverse(rows);
    return rows;
  }

  @Override
  public long count(FsPath path) throws SQLException {
    String measurement = path.getFileName();
    FsPath devicePath = parent(path);
    List<SqlRow> rows =
        executor.query("SELECT COUNT(" + measurement + ") FROM " + toTreePath(devicePath));
    if (rows.isEmpty() || rows.get(0).asMap().isEmpty()) {
      return 0;
    }
    return Long.parseLong(rows.get(0).asMap().values().iterator().next());
  }

  private List<FsNode> listTreeRoots() throws SQLException {
    Set<String> roots = new LinkedHashSet<>();
    for (SqlRow row : executor.query("SHOW DATABASES")) {
      String database = row.get("Database");
      if (database != null && database.startsWith(ROOT)) {
        roots.add(ROOT);
      }
    }
    List<FsNode> nodes = new ArrayList<>();
    for (String root : roots) {
      nodes.add(new FsNode(root, FsPath.absolute("/" + root), FsNodeType.TREE_ROOT));
    }
    return nodes;
  }

  private List<FsNode> listDatabases() throws SQLException {
    List<FsNode> nodes = new ArrayList<>();
    for (SqlRow row : executor.query("SHOW DATABASES")) {
      String database = row.get("Database");
      if (database == null || !database.startsWith(ROOT + ".")) {
        continue;
      }
      String name = database.substring((ROOT + ".").length());
      if (!name.contains(".")) {
        nodes.add(
            new FsNode(name, FsPath.absolute("/" + ROOT + "/" + name), FsNodeType.TREE_DATABASE));
      }
    }
    return nodes;
  }

  private List<FsNode> listChildPaths(FsPath path) throws SQLException {
    List<FsNode> nodes = new ArrayList<>();
    for (SqlRow row : executor.query("SHOW CHILD PATHS " + toTreePath(path))) {
      String childPath = row.get("ChildPaths");
      if (childPath == null) {
        continue;
      }
      FsPath fsPath = fromTreePath(childPath);
      nodes.add(new FsNode(fsPath.getFileName(), fsPath, FsNodeType.TREE_INTERNAL_PATH));
    }
    return nodes;
  }

  private boolean isTreeRoot(FsPath path) {
    List<String> segments = path.getSegments();
    return segments.size() == 1 && ROOT.equals(segments.get(0));
  }

  private boolean isDatabase(FsPath path) throws SQLException {
    if (path.getSegments().size() != 2 || !ROOT.equals(path.getSegments().get(0))) {
      return false;
    }
    String treePath = toTreePath(path);
    for (SqlRow row : executor.query("SHOW DATABASES")) {
      if (treePath.equals(row.get("Database"))) {
        return true;
      }
    }
    return false;
  }

  private static String toTreePath(FsPath path) {
    StringBuilder builder = new StringBuilder();
    for (String segment : path.getSegments()) {
      if (builder.length() > 0) {
        builder.append('.');
      }
      builder.append(segment);
    }
    return builder.toString();
  }

  private static FsPath fromTreePath(String treePath) {
    return FsPath.absolute("/" + treePath.replace('.', '/'));
  }

  private static FsPath parent(FsPath path) {
    List<String> segments = path.getSegments();
    StringBuilder builder = new StringBuilder("/");
    for (int i = 0; i < segments.size() - 1; i++) {
      if (i > 0) {
        builder.append('/');
      }
      builder.append(segments.get(i));
    }
    return FsPath.absolute(builder.toString());
  }
}
