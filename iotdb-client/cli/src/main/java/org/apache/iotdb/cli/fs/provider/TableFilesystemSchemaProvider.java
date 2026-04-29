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
import java.util.List;

public class TableFilesystemSchemaProvider implements FilesystemSchemaProvider {

  private final SqlExecutor executor;

  public TableFilesystemSchemaProvider(SqlExecutor executor) {
    this.executor = executor;
  }

  @Override
  public List<FsNode> list(FsPath path) throws SQLException {
    int depth = path.getSegments().size();
    if (depth == 0) {
      return listDatabases();
    }
    if (depth == 1) {
      return listTables(path);
    }
    if (depth == 2) {
      return listColumns(path);
    }
    return new ArrayList<>();
  }

  @Override
  public FsNode describe(FsPath path) throws SQLException {
    int depth = path.getSegments().size();
    if (depth == 0) {
      return new FsNode("/", path, FsNodeType.VIRTUAL_ROOT);
    }
    if (depth == 1) {
      return describeDatabase(path);
    }
    if (depth == 2) {
      return describeTable(path);
    }
    String columnName = path.getFileName();
    for (FsNode node : listColumns(parent(path))) {
      if (columnName.equals(node.getName())) {
        return node;
      }
    }
    return new FsNode(columnName, path, FsNodeType.UNKNOWN);
  }

  @Override
  public List<SqlRow> read(FsPath path, int limit) throws SQLException {
    int depth = path.getSegments().size();
    if (depth == 2) {
      return executor.query("SELECT * FROM " + toTablePath(path) + " LIMIT " + limit);
    }
    if (depth == 3) {
      String tablePath = toTablePath(parent(path));
      return executor.query(
          "SELECT " + path.getFileName() + " FROM " + tablePath + " LIMIT " + limit);
    }
    throw new SQLException("Path is not readable: " + path);
  }

  @Override
  public List<SqlRow> read(List<FsPath> paths, int limit) throws SQLException {
    if (paths.size() == 1) {
      return read(paths.get(0), limit);
    }
    FsPath tablePath = parent(paths.get(0));
    for (FsPath path : paths) {
      if (path.getSegments().size() != 3 || !tablePath.equals(parent(path))) {
        throw new SQLException("Paths must be columns from the same table");
      }
    }
    return executor.query(
        "SELECT " + columnList(paths) + " FROM " + toTablePath(tablePath) + " LIMIT " + limit);
  }

  private List<FsNode> listDatabases() throws SQLException {
    List<FsNode> nodes = new ArrayList<>();
    for (SqlRow row : executor.query("SHOW DATABASES")) {
      String database = row.get("Database");
      if (database != null) {
        nodes.add(new FsNode(database, FsPath.absolute("/" + database), FsNodeType.TABLE_DATABASE));
      }
    }
    return nodes;
  }

  private FsNode describeDatabase(FsPath path) throws SQLException {
    String database = path.getFileName();
    for (FsNode node : listDatabases()) {
      if (database.equals(node.getName())) {
        return node;
      }
    }
    return new FsNode(database, path, FsNodeType.UNKNOWN);
  }

  private FsNode describeTable(FsPath path) throws SQLException {
    String table = path.getFileName();
    for (FsNode node : listTables(parent(path))) {
      if (table.equals(node.getName())) {
        return node;
      }
    }
    return new FsNode(table, path, FsNodeType.UNKNOWN);
  }

  private List<FsNode> listTables(FsPath databasePath) throws SQLException {
    String database = databasePath.getFileName();
    List<FsNode> nodes = new ArrayList<>();
    for (SqlRow row : executor.query("SHOW TABLES FROM " + database)) {
      String table = row.get("TableName");
      if (table != null) {
        nodes.add(
            new FsNode(
                table, FsPath.absolute("/" + database + "/" + table), FsNodeType.TABLE_TABLE));
      }
    }
    return nodes;
  }

  private List<FsNode> listColumns(FsPath tablePath) throws SQLException {
    List<String> segments = tablePath.getSegments();
    String database = segments.get(0);
    String table = segments.get(1);
    List<FsNode> nodes = new ArrayList<>();
    for (SqlRow row : executor.query("DESC " + database + "." + table + " DETAILS")) {
      String column = row.get("ColumnName");
      if (column != null) {
        nodes.add(
            new FsNode(
                column,
                FsPath.absolute("/" + database + "/" + table + "/" + column),
                FsNodeType.TABLE_COLUMN,
                row.asMap()));
      }
    }
    return nodes;
  }

  private static String toTablePath(FsPath path) {
    List<String> segments = path.getSegments();
    return segments.get(0) + "." + segments.get(1);
  }

  private static String columnList(List<FsPath> paths) {
    StringBuilder builder = new StringBuilder();
    for (FsPath path : paths) {
      if (builder.length() > 0) {
        builder.append(", ");
      }
      builder.append(path.getFileName());
    }
    return builder.toString();
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
