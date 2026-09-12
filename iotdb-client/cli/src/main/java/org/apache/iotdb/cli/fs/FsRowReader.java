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

import org.apache.iotdb.cli.fs.command.ReadOptions;
import org.apache.iotdb.cli.fs.node.FsColumn;
import org.apache.iotdb.cli.fs.path.FsPath;
import org.apache.iotdb.cli.fs.provider.FilesystemSchemaProvider;
import org.apache.iotdb.cli.fs.sql.SqlRow;
import org.apache.iotdb.cli.i18n.CliMessages;
import org.apache.iotdb.cli.i18n.FsReadMessages;

import org.apache.tsfile.exception.PathParseException;
import org.apache.tsfile.read.common.parser.PathNodesGenerator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/** Validates schema-dependent options before reading and applies the shared query semantics. */
public final class FsRowReader {
  private final FilesystemSchemaProvider provider;

  public FsRowReader(FilesystemSchemaProvider provider) {
    this.provider = provider;
  }

  public FsPath scope(FsPath path, ReadOptions options) {
    String model = provider.model();
    if (!options.getDevice().isEmpty()) {
      if ("table".equals(model)) throw invalid(CliMessages.FS_SCOPE_MODEL, "-d", model);
      FsPath device;
      try {
        device =
            FsPath.absolute(
                "/" + String.join("/", PathNodesGenerator.splitPathToNodes(options.getDevice())));
      } catch (PathParseException e) {
        throw invalid(FsReadMessages.INVALID_DEVICE, options.getDevice());
      }
      if (!path.isRoot()
          && !path.equals(device)
          && !path.toString().startsWith(device + "/")
          && !device.toString().startsWith(path + "/")) {
        throw invalid(CliMessages.FS_SCOPE_PATH, options.getDevice(), path);
      }
      return device;
    }
    if (!options.getTable().isEmpty()) {
      if ("tree".equals(model)) throw invalid(CliMessages.FS_SCOPE_MODEL, "-t", model);
      List<String> segments = path.getSegments();
      if (segments.size() == 1) return path.resolve(options.getTable() + ".csv");
      if (segments.size() != 2
          || !stripCsv(path.getFileName()).equalsIgnoreCase(options.getTable())) {
        throw invalid(CliMessages.FS_SCOPE_PATH, options.getTable(), path);
      }
    }
    return path;
  }

  public Result read(FsPath input, ReadOptions options, boolean tail) throws SQLException {
    FsPath path = scope(input, options);
    if (path.isRoot()) {
      throw invalid(FsReadMessages.INVALID_SCOPE, path);
    }
    List<FsColumn> schema = provider.columns(path);
    List<FsColumn> projected = selectColumns(schema, options.getColumns());
    List<TagPredicate> predicates = new ArrayList<>();
    for (String filter : options.getTagFilters()) predicates.add(new TagPredicate(filter, schema));
    boolean filtered =
        options.getStart() != null || options.getEnd() != null || !predicates.isEmpty();
    long requested =
        options.getLimit() < 0 || filtered
            ? -1
            : Math.min(
                Integer.MAX_VALUE,
                options.getLimit() + Math.min(Integer.MAX_VALUE, options.getOffset()));
    List<SqlRow> rows =
        tail ? provider.tail(path, (int) requested) : provider.read(path, (int) requested);
    List<SqlRow> matching = new ArrayList<>();
    for (SqlRow row : rows) {
      if (options.getStart() != null || options.getEnd() != null) {
        String timestamp = FsRowRenderer.value(row, "time");
        if (timestamp == null) continue;
        long time = Long.parseLong(timestamp);
        if (options.getStart() != null && time < options.getStart()) continue;
        if (options.getEnd() != null && time > options.getEnd()) continue;
      }
      boolean any = "any".equals(options.getTagMatch());
      boolean match = !any || predicates.isEmpty();
      for (TagPredicate predicate : predicates) {
        if (any) match |= predicate.matches(row);
        else match &= predicate.matches(row);
      }
      if (match) matching.add(row);
    }
    int offset = (int) Math.min(options.getOffset(), matching.size());
    int from = tail ? 0 : offset;
    int end = tail ? matching.size() - offset : matching.size();
    if (options.getLimit() >= 0) {
      if (tail) from = (int) Math.max(0, end - options.getLimit());
      else end = (int) Math.min(end, (long) from + options.getLimit());
    }
    return new Result(projected, new ArrayList<>(matching.subList(from, end)));
  }

  public static void validateTextOptions(ReadOptions options) {
    if (!options.getDevice().isEmpty()
        || !options.getTable().isEmpty()
        || !options.getColumns().isEmpty()
        || options.getStart() != null
        || options.getEnd() != null
        || !options.getTagFilters().isEmpty()
        || !"all".equals(options.getTagMatch())
        || (!"table".equals(options.getFormat()) && !"csv".equals(options.getFormat()))) {
      throw invalid(FsReadMessages.TEXT_OPTIONS);
    }
  }

  private static List<FsColumn> selectColumns(List<FsColumn> schema, List<String> requested) {
    List<FsColumn> selected = new ArrayList<>();
    for (FsColumn column : schema) {
      if ("TIME".equalsIgnoreCase(column.getCategory())
          || "TAG".equalsIgnoreCase(column.getCategory())) selected.add(column);
    }
    if (requested.isEmpty()) {
      for (FsColumn column : schema)
        if ("FIELD".equalsIgnoreCase(column.getCategory())) selected.add(column);
    } else {
      for (String name : requested) {
        FsColumn column = find(schema, name);
        if (column == null || !"FIELD".equalsIgnoreCase(column.getCategory()))
          throw invalid(CliMessages.FS_UNKNOWN_FIELD, name);
        selected.add(column);
      }
    }
    return selected;
  }

  private static FsColumn find(List<FsColumn> schema, String name) {
    for (FsColumn column : schema) if (column.getName().equalsIgnoreCase(name)) return column;
    return null;
  }

  private static String stripCsv(String name) {
    return name.endsWith(".csv") ? name.substring(0, name.length() - 4) : name;
  }

  private static IllegalArgumentException invalid(String message, Object... args) {
    return new IllegalArgumentException(String.format(message, args));
  }

  public static final class Result {
    private final List<FsColumn> columns;
    private final List<SqlRow> rows;

    public Result(List<FsColumn> columns, List<SqlRow> rows) {
      this.columns = columns;
      this.rows = rows;
    }

    public List<FsColumn> getColumns() {
      return columns;
    }

    public List<SqlRow> getRows() {
      return rows;
    }
  }

  private static final class TagPredicate {
    private final String column;
    private final String operator;
    private final String expected;
    private final Pattern pattern;

    private TagPredicate(String filter, List<FsColumn> schema) {
      String[] parts = filter.split("\\s+", 3);
      column = parts[0];
      FsColumn definition = find(schema, column);
      if (definition == null || !"TAG".equalsIgnoreCase(definition.getCategory()))
        throw invalid(CliMessages.FS_UNKNOWN_TAG, column);
      operator = parts[1];
      expected = parts.length == 3 ? parts[2] : null;
      try {
        pattern = "regexp".equals(operator) ? Pattern.compile(expected) : null;
      } catch (PatternSyntaxException e) {
        throw invalid(CliMessages.FS_INVALID_REGEX, expected);
      }
    }

    private boolean matches(SqlRow row) {
      String actual = FsRowRenderer.value(row, column);
      switch (operator) {
        case "eq":
          return actual != null && actual.equals(expected);
        case "neq":
          return actual != null && !actual.equals(expected);
        case "is-null":
          return actual == null;
        case "not-null":
          return actual != null;
        case "regexp":
          return actual != null && pattern.matcher(actual).matches();
        default:
          throw invalid(CliMessages.FS_INVALID_REGEX, operator);
      }
    }
  }
}
