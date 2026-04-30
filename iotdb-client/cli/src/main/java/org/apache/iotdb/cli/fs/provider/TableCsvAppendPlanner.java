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

import org.apache.iotdb.cli.fs.sql.SqlRow;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

class TableCsvAppendPlanner {

  private static final int INSERT_BATCH_SIZE = 1000;
  private static final String INVALID_WRITE_OPERATION =
      "Invalid filesystem write operation for this path";
  private static final String NULL_MARKER = "\\N";
  private static final CSVFormat CSV_FORMAT =
      CSVFormat.DEFAULT.builder().setIgnoreEmptyLines(true).build();

  private TableCsvAppendPlanner() {}

  static List<String> plan(
      String database, String table, List<SqlRow> schemaRows, List<String> lines)
      throws SQLException {
    List<CSVRecord> records = parse(lines);
    if (records.isEmpty()) {
      return new ArrayList<>();
    }
    List<TableColumn> schemaColumns = schemaColumns(schemaRows);
    if (schemaColumns.isEmpty()) {
      throw invalidOperation();
    }
    ParsedCsv csv = parseCsv(schemaColumns, records);
    if (csv.rows.isEmpty()) {
      return new ArrayList<>();
    }
    return buildStatements(database, table, csv.columns, csv.rows);
  }

  private static List<CSVRecord> parse(List<String> lines) throws SQLException {
    StringBuilder builder = new StringBuilder();
    for (String line : lines) {
      if (builder.length() > 0) {
        builder.append(System.lineSeparator());
      }
      builder.append(line);
    }
    try {
      try (CSVParser parser = CSVParser.parse(builder.toString(), CSV_FORMAT)) {
        return parser.getRecords();
      }
    } catch (IOException e) {
      throw new SQLException("Failed to parse CSV input", e);
    }
  }

  private static List<TableColumn> schemaColumns(List<SqlRow> rows) {
    List<TableColumn> columns = new ArrayList<>();
    for (SqlRow row : rows) {
      String name = row.get("ColumnName");
      String type = row.get("DataType");
      if (name != null && type != null) {
        columns.add(new TableColumn(name, type));
      }
    }
    return columns;
  }

  private static ParsedCsv parseCsv(List<TableColumn> schemaColumns, List<CSVRecord> records)
      throws SQLException {
    Map<String, TableColumn> columnsByName = columnsByName(schemaColumns);
    CSVRecord first = records.get(0);
    boolean hasHeader = looksLikeHeader(first, columnsByName);
    List<TableColumn> selectedColumns;
    int firstDataIndex;
    if (hasHeader) {
      selectedColumns = selectedColumns(first, columnsByName);
      firstDataIndex = 1;
    } else {
      selectedColumns = schemaColumns;
      firstDataIndex = 0;
    }
    if (!containsTime(selectedColumns)) {
      throw invalidOperation();
    }

    List<String> rows = new ArrayList<>();
    for (int i = firstDataIndex; i < records.size(); i++) {
      rows.add(recordValues(records.get(i), selectedColumns));
    }
    return new ParsedCsv(selectedColumns, rows);
  }

  private static Map<String, TableColumn> columnsByName(List<TableColumn> columns) {
    Map<String, TableColumn> columnsByName = new LinkedHashMap<>();
    for (TableColumn column : columns) {
      columnsByName.put(column.name.toLowerCase(Locale.ROOT), column);
    }
    return columnsByName;
  }

  private static boolean looksLikeHeader(CSVRecord record, Map<String, TableColumn> columnsByName) {
    if (record.size() == 0) {
      return false;
    }
    for (String value : record) {
      if (!columnsByName.containsKey(value.toLowerCase(Locale.ROOT))) {
        return false;
      }
    }
    return true;
  }

  private static List<TableColumn> selectedColumns(
      CSVRecord header, Map<String, TableColumn> columnsByName) throws SQLException {
    List<TableColumn> columns = new ArrayList<>();
    Set<String> selected = new HashSet<>();
    for (String value : header) {
      String normalized = value.toLowerCase(Locale.ROOT);
      TableColumn column = columnsByName.get(normalized);
      if (column == null || selected.contains(normalized)) {
        throw invalidOperation();
      }
      selected.add(normalized);
      columns.add(column);
    }
    return columns;
  }

  private static boolean containsTime(List<TableColumn> columns) {
    for (TableColumn column : columns) {
      if (column.isTime()) {
        return true;
      }
    }
    return false;
  }

  private static String recordValues(CSVRecord record, List<TableColumn> columns)
      throws SQLException {
    if (record.size() != columns.size()) {
      throw invalidOperation();
    }
    StringBuilder builder = new StringBuilder("(");
    for (int i = 0; i < columns.size(); i++) {
      if (i > 0) {
        builder.append(", ");
      }
      builder.append(sqlValue(columns.get(i), record.get(i)));
    }
    builder.append(')');
    return builder.toString();
  }

  private static String sqlValue(TableColumn column, String value) throws SQLException {
    if (column.isTime()) {
      if (isNull(value)) {
        throw invalidOperation();
      }
      return timestampValue(value);
    }
    if (NULL_MARKER.equals(value)) {
      return "NULL";
    }
    if (column.isTextual() || column.isDateLike()) {
      return quote(value);
    }
    if (value.isEmpty()) {
      throw invalidOperation();
    }
    return value;
  }

  private static boolean isNull(String value) {
    return value == null || value.isEmpty() || NULL_MARKER.equals(value);
  }

  private static String timestampValue(String value) {
    if (value.matches("[-+]?\\d+")) {
      return value;
    }
    return quote(value);
  }

  private static String quote(String value) {
    return "'" + value.replace("'", "''") + "'";
  }

  private static List<String> buildStatements(
      String database, String table, List<TableColumn> columns, List<String> rows) {
    List<String> statements = new ArrayList<>();
    for (int start = 0; start < rows.size(); start += INSERT_BATCH_SIZE) {
      int end = Math.min(start + INSERT_BATCH_SIZE, rows.size());
      statements.add(
          "INSERT INTO "
              + TableFilesystemSql.tablePath(database, table)
              + "("
              + columnList(columns)
              + ") VALUES "
              + join(rows.subList(start, end), ", "));
    }
    return statements;
  }

  private static String columnList(List<TableColumn> columns) {
    List<String> names = new ArrayList<>();
    for (TableColumn column : columns) {
      names.add(TableFilesystemSql.identifier(column.name));
    }
    return join(names, ", ");
  }

  private static String join(List<String> values, String delimiter) {
    StringBuilder builder = new StringBuilder();
    for (String value : values) {
      if (builder.length() > 0) {
        builder.append(delimiter);
      }
      builder.append(value);
    }
    return builder.toString();
  }

  private static SQLException invalidOperation() {
    return new SQLException(INVALID_WRITE_OPERATION);
  }

  private static class ParsedCsv {
    private final List<TableColumn> columns;
    private final List<String> rows;

    private ParsedCsv(List<TableColumn> columns, List<String> rows) {
      this.columns = columns;
      this.rows = rows;
    }
  }

  private static class TableColumn {
    private final String name;
    private final String type;

    private TableColumn(String name, String type) {
      this.name = name;
      this.type = type.toUpperCase(Locale.ROOT);
    }

    private boolean isTime() {
      return "TIME".equalsIgnoreCase(name);
    }

    private boolean isTextual() {
      return type.contains("STRING") || type.contains("TEXT") || type.contains("BLOB");
    }

    private boolean isDateLike() {
      return type.contains("DATE") || type.contains("TIMESTAMP");
    }
  }
}
