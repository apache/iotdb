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
import org.apache.iotdb.cli.fs.FsRowRenderer;
import org.apache.iotdb.cli.fs.command.ReadOptions;
import org.apache.iotdb.cli.fs.node.FsColumn;
import org.apache.iotdb.cli.fs.node.FsNode;
import org.apache.iotdb.cli.fs.node.FsNodeType;
import org.apache.iotdb.cli.fs.path.FsPath;
import org.apache.iotdb.cli.fs.sql.SqlExecutor;
import org.apache.iotdb.cli.fs.sql.SqlRow;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TableFilesystemSchemaProvider implements FilesystemSchemaProvider {

  private enum TableFileKind {
    DATA_CSV,
    META,
    UNKNOWN
  }

  private static final String CSV_SUFFIX = ".csv";
  private static final String META_SUFFIX = ".meta";

  private final SqlExecutor executor;

  public TableFilesystemSchemaProvider(SqlExecutor executor) {
    this.executor = executor;
  }

  @Override
  public String model() {
    return "table";
  }

  @Override
  public List<SqlRow> executeSql(String sql) throws SQLException {
    return executor.executeQueryOrUpdate(sql);
  }

  @Override
  public List<FsNode> list(FsPath path) throws SQLException {
    int depth = path.getSegments().size();
    if (depth == 0) {
      return listDatabases();
    }
    if (depth == 1) {
      return listTableFiles(path);
    }
    if (depth == 2) {
      if (parseTableFile(path).kind != TableFileKind.UNKNOWN) {
        return new ArrayList<>();
      }
      return new ArrayList<>();
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
      TableFileRef file = parseTableFile(path);
      if (file.kind == TableFileKind.UNKNOWN) {
        file =
            new TableFileRef(path.getSegments().get(0), path.getFileName(), TableFileKind.DATA_CSV);
      }
      return describeTableFile(path, file);
    }
    return unknown(path);
  }

  @Override
  public List<SqlRow> schema(FsPath path) throws SQLException {
    TableFileRef file = tableFile(path);
    List<SqlRow> result = new ArrayList<>();
    for (FsColumn column : columns(file)) {
      result.add(FsStatistics.schema(model(), file.table, column));
    }
    return result;
  }

  @Override
  public List<FsColumn> columns(FsPath path) throws SQLException {
    return columns(tableFile(path));
  }

  private List<FsColumn> columns(TableFileRef file) throws SQLException {
    List<FsColumn> result = new ArrayList<>();
    for (SqlRow row : executor.query("DESC " + file.toTablePath() + " DETAILS")) {
      String name = FsStatistics.value(row, "ColumnName");
      if (name != null) {
        String category = FsStatistics.value(row, "Category");
        if ("time".equalsIgnoreCase(name)) {
          category = "TIME";
        }
        result.add(
            new FsColumn(
                name,
                category,
                FsStatistics.value(row, "DataType"),
                FsStatistics.value(row, "Encoding"),
                FsStatistics.value(row, "Compression")));
      }
    }
    return result;
  }

  @Override
  public List<SqlRow> meta(FsPath path) throws SQLException {
    TableFileRef file = parseTableFile(path);
    if (file.kind == TableFileKind.UNKNOWN && path.getSegments().size() == 2) {
      file =
          new TableFileRef(
              path.getSegments().get(0), path.getSegments().get(1), TableFileKind.DATA_CSV);
    }
    if (file.kind == TableFileKind.UNKNOWN || !tableExists(parent(path), file.table)) {
      throw new SQLException("Path does not exist: " + path);
    }
    List<SqlRow> result = new ArrayList<>();
    for (SqlRow row :
        executor.query(
            "SHOW TABLES DETAILS FROM " + TableFilesystemSql.identifier(file.database))) {
      if (file.table.equals(row.get("TableName"))) {
        result.add(row);
      }
    }
    return result;
  }

  @Override
  public List<SqlRow> stats(FsPath path) throws SQLException {
    TableFileRef file = tableFile(path);
    List<FsColumn> columns = columns(file);
    return FsStatistics.stats(model(), file.table, columns, read(file, columns, -1, false));
  }

  @Override
  public List<SqlRow> stats(FsPath path, ReadOptions options) throws SQLException {
    TableFileRef file = tableFile(path);
    List<FsColumn> columns = columns(file);
    List<SqlRow> rows = new FsRowReader(this).read(path, options, false).getRows();
    return FsStatistics.stats(model(), file.table, columns, rows);
  }

  @Override
  public List<SqlRow> countRows(FsPath path) throws SQLException {
    TableFileRef file = tableFile(path);
    List<FsColumn> columns = columns(file);
    return FsStatistics.count(model(), file.table, columns, read(file, columns, -1, false));
  }

  private TableFileRef tableFile(FsPath path) throws SQLException {
    TableFileRef file = parseTableFile(path);
    if (file.kind == TableFileKind.UNKNOWN && path.getSegments().size() == 2) {
      file =
          new TableFileRef(
              path.getSegments().get(0), path.getSegments().get(1), TableFileKind.DATA_CSV);
    }
    if (file.kind != TableFileKind.DATA_CSV) {
      throw new SQLException("Path is not a table: " + path);
    }
    ensureExists(path, file);
    return file;
  }

  @Override
  public List<SqlRow> read(FsPath path, int limit) throws SQLException {
    TableFileRef file = tableFile(path);
    return read(file, columns(file), limit, false);
  }

  private List<SqlRow> read(
      TableFileRef file, List<FsColumn> columns, int limit, boolean descending)
      throws SQLException {
    String direction = descending ? " DESC" : " ASC";
    StringBuilder order = new StringBuilder(" ORDER BY time").append(direction);
    for (FsColumn column : columns) {
      if ("TAG".equals(column.getCategory())) {
        order
            .append(", ")
            .append(TableFilesystemSql.identifier(column.getName()))
            .append(direction)
            .append(descending ? " NULLS FIRST" : " NULLS LAST");
      }
    }
    List<SqlRow> rows =
        new ArrayList<>(
            executor.query(selectWithLimit("SELECT * FROM " + file.toTablePath() + order, limit)));
    if (descending) {
      Collections.reverse(rows);
    }
    return rows;
  }

  @Override
  public List<String> readLines(FsPath path, int limit) throws SQLException {
    TableFileRef file = parseTableFile(path);
    if (file.kind == TableFileKind.DATA_CSV) {
      ensureExists(path, file);
      List<FsColumn> columns = columns(file);
      return head(rowsToCsvLines(read(file, columns, limit, false), columns), limit);
    }
    if (file.kind == TableFileKind.META) {
      ensureExists(path, file);
      return head(metaLines(file), limit);
    }
    throw new SQLException("Path is not readable as text: " + path);
  }

  @Override
  public List<SqlRow> tail(FsPath path, int limit) throws SQLException {
    TableFileRef file = tableFile(path);
    return read(file, columns(file), limit, true);
  }

  @Override
  public List<String> tailLines(FsPath path, int limit) throws SQLException {
    TableFileRef file = parseTableFile(path);
    if (file.kind == TableFileKind.META) {
      return tail(readLines(path, Integer.MAX_VALUE), limit);
    }
    if (file.kind == TableFileKind.DATA_CSV) {
      ensureExists(path, file);
      List<FsColumn> columns = columns(file);
      return tail(rowsToCsvLines(read(file, columns, limit, true), columns), limit);
    }
    throw new SQLException("Path does not support tail: " + path);
  }

  @Override
  public long count(FsPath path) throws SQLException {
    int depth = path.getSegments().size();
    TableFileRef file = parseTableFile(path);
    List<SqlRow> rows;
    if (depth == 2 && file.kind == TableFileKind.DATA_CSV) {
      ensureExists(path, file);
      rows = executor.query("SELECT COUNT(*) FROM " + file.toTablePath());
      return countValue(rows) + 1;
    } else if (depth == 2 && file.kind == TableFileKind.META) {
      return readLines(path, Integer.MAX_VALUE).size();
    } else {
      throw new SQLException("Path is not countable: " + path);
    }
  }

  @Override
  public List<SqlRow> read(List<FsPath> paths, int limit) throws SQLException {
    if (paths.size() == 1) {
      return read(paths.get(0), limit);
    }
    throw new SQLException("Multiple paths should be read as regular files");
  }

  private void ensureExists(FsPath path, TableFileRef file) throws SQLException {
    if (!tableExists(parent(path), file.table)) {
      throw new SQLException("Path does not exist: " + path);
    }
  }

  private static String selectWithLimit(String query, int limit) {
    return limit < 0 ? query : query + " LIMIT " + limit;
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

  private FsNode describeTableFile(FsPath path, TableFileRef file) throws SQLException {
    for (String table : listTableNames(parent(path))) {
      if (table.equals(file.table)) {
        return new FsNode(path.getFileName(), path, file.nodeType(), file.metadata());
      }
    }
    return new FsNode(path.getFileName(), path, FsNodeType.UNKNOWN);
  }

  private List<FsNode> listTableFiles(FsPath databasePath) throws SQLException {
    String database = databasePath.getFileName();
    List<FsNode> nodes = new ArrayList<>();
    for (String table : listTableNames(databasePath)) {
      nodes.add(
          new FsNode(
              table + CSV_SUFFIX,
              FsPath.absolute("/" + database + "/" + table + CSV_SUFFIX),
              FsNodeType.TABLE_DATA_FILE,
              tableFileMetadata(database, table, TableFileKind.DATA_CSV)));
      nodes.add(
          new FsNode(
              table + META_SUFFIX,
              FsPath.absolute("/" + database + "/" + table + META_SUFFIX),
              FsNodeType.TABLE_META_FILE,
              tableFileMetadata(database, table, TableFileKind.META)));
    }
    return nodes;
  }

  private List<String> listTableNames(FsPath databasePath) throws SQLException {
    String database = databasePath.getFileName();
    List<String> tables = new ArrayList<>();
    for (SqlRow row :
        executor.query("SHOW TABLES FROM " + TableFilesystemSql.identifier(database))) {
      String table = row.get("TableName");
      if (table != null) {
        tables.add(table);
      }
    }
    return tables;
  }

  private boolean tableExists(FsPath databasePath, String table) throws SQLException {
    for (String tableName : listTableNames(databasePath)) {
      if (tableName.equals(table)) {
        return true;
      }
    }
    return false;
  }

  private List<String> metaLines(TableFileRef file) throws SQLException {
    List<SqlRow> rows = new ArrayList<>();
    for (SqlRow row :
        executor.query(
            "SHOW TABLES DETAILS FROM " + TableFilesystemSql.identifier(file.database))) {
      if (file.table.equals(row.get("TableName"))) {
        rows.add(row);
      }
    }
    return rowsToCsvLines(rows);
  }

  private static List<String> rowsToCsvLines(List<SqlRow> rows) throws SQLException {
    return rowsToCsvLines(rows, Collections.emptyList());
  }

  private static List<String> rowsToCsvLines(List<SqlRow> rows, List<FsColumn> columns) {
    if (rows.isEmpty() && columns.isEmpty()) {
      return new ArrayList<>();
    }
    List<String> lines = new ArrayList<>();
    List<String> headers = new ArrayList<>();
    if (columns.isEmpty()) {
      headers.addAll(rows.get(0).asMap().keySet());
    } else {
      for (FsColumn column : columns) {
        headers.add(column.getName());
      }
    }
    lines.add(csvRecord(headers));
    for (SqlRow row : rows) {
      List<String> values = new ArrayList<>();
      for (String header : headers) {
        values.add(FsStatistics.value(row, header));
      }
      lines.add(csvRecord(values));
    }
    return lines;
  }

  private static String csvRecord(List<String> values) {
    List<String> cells = new ArrayList<>();
    for (String value : values) {
      cells.add(FsRowRenderer.csvValue(value));
    }
    return String.join(",", cells);
  }

  private static TableFileRef parseTableFile(FsPath path) {
    List<String> segments = path.getSegments();
    if (segments.size() != 2) {
      return TableFileRef.unknown(path);
    }
    String fileName = segments.get(1);
    if (fileName.endsWith(CSV_SUFFIX)) {
      return new TableFileRef(
          segments.get(0), removeSuffix(fileName, CSV_SUFFIX), TableFileKind.DATA_CSV);
    }
    if (fileName.endsWith(META_SUFFIX)) {
      return new TableFileRef(
          segments.get(0), removeSuffix(fileName, META_SUFFIX), TableFileKind.META);
    }
    return TableFileRef.unknown(path);
  }

  private static String removeSuffix(String value, String suffix) {
    return value.substring(0, value.length() - suffix.length());
  }

  private static Map<String, String> tableFileMetadata(
      String database, String table, TableFileKind kind) {
    return new TableFileRef(database, table, kind).metadata();
  }

  private static long countValue(List<SqlRow> rows) {
    if (rows.isEmpty() || rows.get(0).asMap().isEmpty()) {
      return 0;
    }
    return Long.parseLong(rows.get(0).asMap().values().iterator().next());
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

  private static FsNode unknown(FsPath path) {
    return new FsNode(path.getFileName(), path, FsNodeType.UNKNOWN);
  }

  private static List<String> head(List<String> lines, int limit) {
    if (limit < 0 || lines.size() <= limit) {
      return lines;
    }
    return new ArrayList<>(lines.subList(0, limit));
  }

  private static List<String> tail(List<String> lines, int limit) {
    if (limit < 0 || lines.size() <= limit) {
      return lines;
    }
    return new ArrayList<>(lines.subList(lines.size() - limit, lines.size()));
  }

  private static class TableFileRef {
    private final String database;
    private final String table;
    private final TableFileKind kind;

    private TableFileRef(String database, String table, TableFileKind kind) {
      this.database = database;
      this.table = table;
      this.kind = kind;
    }

    private static TableFileRef unknown(FsPath path) {
      List<String> segments = path.getSegments();
      String database = segments.isEmpty() ? "" : segments.get(0);
      String table = segments.size() < 2 ? "" : segments.get(1);
      return new TableFileRef(database, table, TableFileKind.UNKNOWN);
    }

    private String fileName() {
      switch (kind) {
        case DATA_CSV:
          return table + CSV_SUFFIX;
        case META:
          return table + META_SUFFIX;
        case UNKNOWN:
        default:
          return table;
      }
    }

    private String toTablePath() {
      return TableFilesystemSql.tablePath(database, table);
    }

    private FsNodeType nodeType() {
      switch (kind) {
        case DATA_CSV:
          return FsNodeType.TABLE_DATA_FILE;
        case META:
          return FsNodeType.TABLE_META_FILE;
        case UNKNOWN:
        default:
          return FsNodeType.UNKNOWN;
      }
    }

    private Map<String, String> metadata() {
      Map<String, String> metadata = new LinkedHashMap<>();
      metadata.put("database", database);
      metadata.put("table", table);
      metadata.put("format", kind == TableFileKind.DATA_CSV ? "csv" : kind.name().toLowerCase());
      return metadata;
    }
  }
}
