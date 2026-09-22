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

import org.apache.iotdb.cli.fs.FsRowReader;
import org.apache.iotdb.cli.fs.command.ReadOptions;
import org.apache.iotdb.cli.fs.node.FsColumn;
import org.apache.iotdb.cli.fs.node.FsNode;
import org.apache.iotdb.cli.fs.node.FsNodeType;
import org.apache.iotdb.cli.fs.path.FsPath;
import org.apache.iotdb.cli.fs.sql.SqlExecutor;
import org.apache.iotdb.cli.fs.sql.SqlRow;
import org.apache.iotdb.cli.i18n.FsReadMessages;

import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.parser.PathNodesGenerator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TreeFilesystemSchemaProvider implements FilesystemSchemaProvider {

  private static final String ROOT = "root";

  private final SqlExecutor executor;

  public TreeFilesystemSchemaProvider(SqlExecutor executor) {
    this.executor = executor;
  }

  @Override
  public String model() {
    return "tree";
  }

  @Override
  public List<SqlRow> executeSql(String sql) throws SQLException {
    return executor.executeQueryOrUpdate(sql);
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
    for (SqlRow row : rows) {
      if (toTreePath(path).equals(row.get("Timeseries"))) {
        return new FsNode(path.getFileName(), path, FsNodeType.TREE_TIMESERIES, row.asMap());
      }
    }
    if (!rows.isEmpty() || !listChildPaths(path).isEmpty()) {
      return new FsNode(path.getFileName(), path, FsNodeType.TREE_INTERNAL_PATH);
    }
    return new FsNode(path.getFileName(), path, FsNodeType.UNKNOWN);
  }

  @Override
  public List<SqlRow> schema(FsPath path) throws SQLException {
    List<SqlRow> result = new ArrayList<>();
    for (SqlRow row : rawSchema(path)) {
      Path series = new Path(row.get("Timeseries"), true);
      result.add(FsStatistics.schema(model(), series.getDeviceString(), column(row)));
    }
    return result;
  }

  private List<SqlRow> rawSchema(FsPath path) throws SQLException {
    String scope = path.isRoot() ? ROOT + ".**" : toTreePath(path);
    List<SqlRow> rows = executor.query("SHOW TIMESERIES " + scope);
    if (rows.isEmpty() && !path.isRoot()) {
      rows = executor.query("SHOW TIMESERIES " + scope + ".**");
    }
    return rows;
  }

  private static FsColumn column(SqlRow row) {
    Path series = new Path(row.get("Timeseries"), true);
    return new FsColumn(
        series.getMeasurement(),
        "FIELD",
        row.get("DataType"),
        row.get("Encoding"),
        row.get("Compression"));
  }

  @Override
  public List<FsColumn> columns(FsPath path) throws SQLException {
    List<FsColumn> columns = new ArrayList<>();
    columns.add(new FsColumn("time", "TIME", "TIMESTAMP"));
    List<SqlRow> schema = rawSchema(path);
    boolean multipleDevices = multipleDevices(schema);
    for (SqlRow row : schema) {
      FsColumn column = column(row);
      columns.add(
          multipleDevices
              ? new FsColumn(
                  row.get("Timeseries"),
                  column.getCategory(),
                  column.getDataType(),
                  column.getEncoding(),
                  column.getCompression())
              : column);
    }
    return columns;
  }

  @Override
  public List<SqlRow> meta(FsPath path) throws SQLException {
    if (path.isRoot()) {
      throw new SQLException("Path is not a metadata object: " + path);
    }
    return executor.query("SHOW TIMESERIES " + toTreePath(path));
  }

  @Override
  public List<SqlRow> stats(FsPath path) throws SQLException {
    return collectStatistics(path, false, null);
  }

  @Override
  public List<SqlRow> stats(FsPath path, ReadOptions options) throws SQLException {
    return collectStatistics(path, false, options);
  }

  @Override
  public List<SqlRow> countRows(FsPath path) throws SQLException {
    return collectStatistics(path, true, null);
  }

  private List<SqlRow> collectStatistics(FsPath path, boolean count, ReadOptions options)
      throws SQLException {
    Map<String, List<FsColumn>> devices = new LinkedHashMap<>();
    for (SqlRow row : rawSchema(path)) {
      Path series = new Path(row.get("Timeseries"), true);
      devices
          .computeIfAbsent(series.getDeviceString(), ignored -> new ArrayList<>())
          .add(column(row));
    }
    List<SqlRow> result = new ArrayList<>();
    for (Map.Entry<String, List<FsColumn>> device : devices.entrySet()) {
      // The union of every measurement's timestamps is the device timeline,
      // including timestamps where the selected measurement is NULL.
      List<SqlRow> rows =
          normalize(
              executor.query("SELECT * FROM " + device.getKey() + " ORDER BY time ASC"), true);
      if (options != null) {
        rows = FsRowReader.filterRows(rows, device.getValue(), options);
      }
      result.addAll(
          count
              ? FsStatistics.count(model(), device.getKey(), device.getValue(), rows)
              : FsStatistics.stats(model(), device.getKey(), device.getValue(), rows));
    }
    return result;
  }

  @Override
  public List<SqlRow> read(FsPath path, int limit) throws SQLException {
    return read(path, limit, false);
  }

  @Override
  public List<SqlRow> tail(FsPath path, int limit) throws SQLException {
    return read(path, limit, true);
  }

  private List<SqlRow> read(FsPath path, int limit, boolean descending) throws SQLException {
    if (path.isRoot()) {
      throw new SQLException(String.format(FsReadMessages.INVALID_SCOPE, path));
    }
    List<SqlRow> schema = rawSchema(path);
    String scope = toTreePath(path);
    boolean measurement = schema.size() == 1 && scope.equals(schema.get(0).get("Timeseries"));
    String sql =
        "SELECT "
            + (measurement ? identifier(path.getFileName()) : "*")
            + " FROM "
            + (measurement ? toTreePath(parent(path)) : scope)
            + " ORDER BY time "
            + (descending ? "DESC" : "ASC")
            + (limit < 0 ? "" : " LIMIT " + limit);
    List<SqlRow> rows = normalize(executor.query(sql), !multipleDevices(schema));
    if (descending) {
      Collections.reverse(rows);
    }
    return rows;
  }

  private static boolean multipleDevices(List<SqlRow> schema) {
    Set<String> devices = new LinkedHashSet<>();
    for (SqlRow row : schema) {
      devices.add(new Path(row.get("Timeseries"), true).getDeviceString());
    }
    return devices.size() > 1;
  }

  private static List<SqlRow> normalize(List<SqlRow> rows, boolean shortNames) {
    List<SqlRow> result = new ArrayList<>();
    for (SqlRow row : rows) {
      Map<String, String> cells = new LinkedHashMap<>();
      Map<String, String> types = new LinkedHashMap<>();
      for (Map.Entry<String, String> entry : row.asMap().entrySet()) {
        String name = entry.getKey();
        String normalized =
            "time".equalsIgnoreCase(name)
                ? "time"
                : shortNames && name.startsWith("root.")
                    ? new Path(name, true).getMeasurement()
                    : name;
        cells.put(normalized, entry.getValue());
        types.put(normalized, row.getDataType(name));
      }
      result.add(new SqlRow(cells, types));
    }
    return result;
  }

  @Override
  public long count(FsPath path) throws SQLException {
    return read(path, -1).size();
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
      builder.append(identifier(segment));
    }
    return builder.toString();
  }

  private static FsPath fromTreePath(String treePath) {
    return FsPath.absolute("/" + String.join("/", PathNodesGenerator.splitPathToNodes(treePath)));
  }

  private static String identifier(String value) {
    return value.matches("[A-Za-z_][A-Za-z0-9_]*") || value.matches("`(?:[^`]|``)+`")
        ? value
        : "`" + value.replace("`", "``") + "`";
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
