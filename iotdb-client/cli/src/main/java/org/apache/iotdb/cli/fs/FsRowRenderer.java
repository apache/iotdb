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
import org.apache.iotdb.cli.fs.sql.SqlExecutor.RowVisitor;
import org.apache.iotdb.cli.fs.sql.SqlRow;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
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

  @FunctionalInterface
  public interface RowSource {
    void scan(RowVisitor visitor) throws SQLException;
  }

  /** Streams textual formats; exact table widths use a private disk spool, never a row list. */
  public static void printStream(
      PrintStream out, List<FsColumn> columns, String format, RowSource source)
      throws SQLException {
    if (!"table".equals(format)) {
      print(out, columns, Collections.emptyList(), format);
      source.scan(
          row -> {
            printRow(out, columns, row, format);
            return !out.checkError();
          });
      return;
    }
    Path path = null;
    try {
      path = Files.createTempFile("iotdb-fs-table-", ".rows");
      int[] widths = new int[columns.size()];
      List<String> header = new ArrayList<>();
      for (int i = 0; i < columns.size(); i++) {
        header.add(columns.get(i).getName());
        widths[i] = width(display(header.get(i)));
      }
      long[] count = {0};
      try (DataOutputStream spool =
          new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)))) {
        source.scan(
            row -> {
              try {
                for (int i = 0; i < columns.size(); i++) {
                  String value = value(row, columns.get(i).getName());
                  widths[i] = Math.max(widths[i], width(display(value)));
                  if (value == null) spool.writeInt(-1);
                  else {
                    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                    spool.writeInt(bytes.length);
                    spool.write(bytes);
                  }
                }
                count[0]++;
                return true;
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            });
      }
      if (!columns.isEmpty()) out.print(tableLine(header, widths) + "\n");
      try (DataInputStream spool =
          new DataInputStream(new BufferedInputStream(Files.newInputStream(path)))) {
        for (long i = 0; i < count[0] && !out.checkError(); i++) {
          List<String> cells = new ArrayList<>();
          for (int j = 0; j < columns.size(); j++) {
            int length = spool.readInt();
            if (length < 0) cells.add(null);
            else {
              byte[] bytes = new byte[length];
              spool.readFully(bytes);
              cells.add(new String(bytes, StandardCharsets.UTF_8));
            }
          }
          out.print(tableLine(cells, widths) + "\n");
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      if (path != null) {
        try {
          Files.deleteIfExists(path);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }
  }

  public static void printRow(PrintStream out, List<FsColumn> columns, SqlRow row, String format) {
    if ("csv".equals(format)) {
      List<String> cells = new ArrayList<>();
      for (FsColumn column : columns) cells.add(csvValue(value(row, column.getName())));
      out.print(String.join(",", cells) + "\n");
    } else {
      print(out, columns, Collections.singletonList(row), format);
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
