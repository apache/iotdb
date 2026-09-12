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
package org.apache.iotdb.cli.fs;

import org.apache.iotdb.cli.fs.node.FsColumn;
import org.apache.iotdb.cli.fs.sql.SqlRow;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Shared typed output for reads and exports, including schema-only results. */
public final class FsRowRenderer {
  private FsRowRenderer() {}

  public static void print(
      PrintStream out, List<FsColumn> columns, List<SqlRow> rows, String format) {
    if ("ndjson".equals(format)) {
      for (SqlRow row : rows) {
        List<String> cells = new ArrayList<>();
        for (FsColumn column : columns) {
          String type = row.getDataType(column.getName());
          cells.add(
              jsonString(column.getName())
                  + ":"
                  + jsonValue(
                      value(row, column.getName()), type == null ? column.getDataType() : type));
        }
        out.print("{" + String.join(",", cells) + "}\n");
      }
      return;
    }
    if ("csv".equals(format)) {
      List<String> header = new ArrayList<>();
      for (FsColumn column : columns) header.add(csvValue(column.getName()));
      if (!columns.isEmpty()) out.print(String.join(",", header) + "\n");
      for (SqlRow row : rows) {
        List<String> cells = new ArrayList<>();
        for (FsColumn column : columns) cells.add(csvValue(value(row, column.getName())));
        out.print(String.join(",", cells) + "\n");
      }
      return;
    }
    int[] widths = new int[columns.size()];
    for (int i = 0; i < widths.length; i++) {
      widths[i] = width(display(columns.get(i).getName()));
      for (SqlRow row : rows)
        widths[i] = Math.max(widths[i], width(display(value(row, columns.get(i).getName()))));
    }
    List<String> header = new ArrayList<>();
    for (FsColumn column : columns) header.add(column.getName());
    if (!columns.isEmpty()) out.print(tableLine(header, widths) + "\n");
    for (SqlRow row : rows) {
      List<String> cells = new ArrayList<>();
      for (FsColumn column : columns) cells.add(value(row, column.getName()));
      out.print(tableLine(cells, widths) + "\n");
    }
  }

  public static String value(SqlRow row, String name) {
    for (Map.Entry<String, String> entry : row.asMap().entrySet()) {
      if (name.equalsIgnoreCase(entry.getKey())) return entry.getValue();
    }
    return null;
  }

  public static String csvValue(String value) {
    if (value == null) return "\\N";
    if (value.isEmpty()
        || "\\N".equals(value)
        || value.indexOf(',') >= 0
        || value.indexOf('"') >= 0
        || value.indexOf('\n') >= 0
        || value.indexOf('\r') >= 0) {
      return "\"" + value.replace("\"", "\"\"") + "\"";
    }
    return value;
  }

  private static String jsonValue(String value, String dataType) {
    if (value == null) return "null";
    switch (dataType.toUpperCase(Locale.ROOT)) {
      case "BOOLEAN":
        return Boolean.parseBoolean(value) ? "true" : "false";
      case "INT32":
        return Integer.toString(Integer.parseInt(value));
      case "FLOAT":
      case "DOUBLE":
        double number = Double.parseDouble(value);
        return Double.isFinite(number) ? value : "null";
      default:
        return jsonString(value);
    }
  }

  public static String jsonString(String value) {
    StringBuilder result = new StringBuilder("\"");
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '"':
          result.append("\\\"");
          break;
        case '\\':
          result.append("\\\\");
          break;
        case '\b':
          result.append("\\b");
          break;
        case '\f':
          result.append("\\f");
          break;
        case '\n':
          result.append("\\n");
          break;
        case '\r':
          result.append("\\r");
          break;
        case '\t':
          result.append("\\t");
          break;
        default:
          if (c < 32) result.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
          else result.append(c);
      }
    }
    return result.append('"').toString();
  }

  private static String display(String value) {
    return value == null ? "" : value;
  }

  private static int width(String value) {
    return value.getBytes(StandardCharsets.UTF_8).length;
  }

  private static String tableLine(List<String> cells, int[] widths) {
    StringBuilder line = new StringBuilder();
    for (int i = 0; i < cells.size(); i++) {
      if (i > 0) line.append("  ");
      String cell = display(cells.get(i));
      line.append(cell);
      for (int padding = width(cell); padding < widths[i] && i + 1 < cells.size(); padding++)
        line.append(' ');
    }
    return line.toString();
  }
}
