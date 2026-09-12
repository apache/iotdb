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

import org.apache.iotdb.cli.fs.path.FsPath;
import org.apache.iotdb.cli.fs.sql.SqlExecutor;
import org.apache.iotdb.cli.fs.sql.SqlRow;
import org.apache.iotdb.cli.i18n.CliMessages;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

public class TableFilesystemMutationProvider implements FilesystemMutationProvider {

  private static final String CSV_SUFFIX = ".csv";

  private final SqlExecutor executor;
  private final Supplier<String> temporaryName;

  public TableFilesystemMutationProvider(SqlExecutor executor) {
    this(executor, () -> "_fs_" + UUID.randomUUID().toString().replace("-", ""));
  }

  TableFilesystemMutationProvider(SqlExecutor executor, Supplier<String> temporaryName) {
    this.executor = executor;
    this.temporaryName = temporaryName;
  }

  @Override
  public void mkdir(FsPath path) throws SQLException {
    if (path.getSegments().size() != 1) {
      throw invalidOperation();
    }
    executor.execute("CREATE DATABASE " + TableFilesystemSql.identifier(path.getFileName()));
  }

  @Override
  public void rmdir(FsPath path) throws SQLException {
    if (path.getSegments().size() != 1) {
      throw invalidOperation();
    }
    if (!executor
        .query("SHOW TABLES FROM " + TableFilesystemSql.identifier(path.getFileName()))
        .isEmpty()) {
      throw new SQLException(String.format(CliMessages.FS_DIRECTORY_NOT_EMPTY, path));
    }
    dropDatabase(path);
  }

  @Override
  public void remove(FsPath path) throws SQLException {
    if (!isDataFile(path)) {
      throw invalidOperation();
    }
    executor.execute("DROP TABLE " + toTablePath(path));
  }

  @Override
  public void removeRecursive(FsPath path) throws SQLException {
    if (isDataFile(path)) {
      remove(path);
      return;
    }
    dropDatabase(path);
  }

  @Override
  public void move(FsPath source, FsPath target) throws SQLException {
    target = destination(source, target);
    if (!parent(source).equals(parent(target))) {
      copy(source, target);
      remove(source);
      return;
    }
    rename(source, target);
  }

  @Override
  public void copy(FsPath source, FsPath target) throws SQLException {
    target = destination(source, target);
    List<SqlRow> schema = executor.query("DESC " + toTablePath(source) + " DETAILS");
    String create = createTable(source, target, schema);
    // CREATE without IF NOT EXISTS guarantees that a pre-existing target is never changed.
    executor.execute(create);
    try {
      executor.execute(
          "INSERT INTO "
              + toTablePath(target)
              + " ("
              + TableFilesystemCopyPlanner.columns(schema)
              + ") SELECT "
              + TableFilesystemCopyPlanner.columns(schema)
              + " FROM "
              + toTablePath(source));
    } catch (SQLException e) {
      cleanup(target, e);
      throw e;
    }
  }

  @Override
  public void copy(FsPath source, FsPath target, boolean replace) throws SQLException {
    target = destination(source, target);
    if (!replace || !tableExists(target)) {
      copy(source, target);
      return;
    }
    FsPath staged = temporaryPath(target);
    copy(source, staged);
    publish(staged, target);
  }

  @Override
  public void move(FsPath source, FsPath target, boolean replace) throws SQLException {
    target = destination(source, target);
    if (!replace || !tableExists(target)) {
      move(source, target);
      return;
    }
    copy(source, target, true);
    remove(source);
  }

  @Override
  public void append(FsPath path, List<String> lines) throws SQLException {
    if (!isDataFile(path)) {
      throw invalidOperation();
    }
    if (lines == null || lines.isEmpty()) {
      return;
    }
    List<String> statements =
        TableCsvAppendPlanner.plan(
            databaseName(path),
            tableName(path),
            executor.query("DESC " + toTablePath(path) + " DETAILS"),
            lines);
    for (String statement : statements) {
      executor.execute(statement);
    }
  }

  @Override
  public void write(FsPath path, List<String> lines, boolean append) throws SQLException {
    if (append) {
      append(path, lines);
      return;
    }
    if (!isDataFile(path)) {
      throw invalidOperation();
    }
    FsPath staged = temporaryPath(path);
    List<SqlRow> schema = executor.query("DESC " + toTablePath(path) + " DETAILS");
    List<String> statements =
        TableCsvAppendPlanner.plan(
            databaseName(staged),
            tableName(staged),
            schema,
            lines == null ? Collections.emptyList() : lines);
    String create = createTable(path, staged, schema);
    executor.execute(create);
    try {
      for (String statement : statements) {
        executor.execute(statement);
      }
    } catch (SQLException e) {
      cleanup(staged, e);
      throw e;
    }
    publish(staged, path);
  }

  private void publish(FsPath staged, FsPath path) throws SQLException {
    FsPath backup = temporaryPath(path);
    boolean originalRenamed = false;
    boolean published = false;
    try {
      // Keep the original table until the replacement is populated and ready to publish.
      rename(path, backup);
      originalRenamed = true;
      rename(staged, path);
      published = true;
      remove(backup);
    } catch (SQLException e) {
      if (originalRenamed && !published) {
        try {
          rename(backup, path);
        } catch (SQLException rollbackFailure) {
          e.addSuppressed(rollbackFailure);
        }
      }
      if (!published) {
        cleanup(staged, e);
      }
      throw e;
    }
  }

  private boolean tableExists(FsPath path) throws SQLException {
    for (SqlRow row :
        executor.query("SHOW TABLES FROM " + TableFilesystemSql.identifier(databaseName(path)))) {
      if (tableName(path).equalsIgnoreCase(row.get("TableName"))) {
        return true;
      }
    }
    return false;
  }

  private String createTable(FsPath source, FsPath target, List<SqlRow> schema)
      throws SQLException {
    SqlRow metadata = null;
    for (SqlRow row :
        executor.query(
            "SHOW TABLES DETAILS FROM " + TableFilesystemSql.identifier(databaseName(source)))) {
      if (tableName(source).equals(row.get("TableName"))) {
        metadata = row;
        break;
      }
    }
    return TableFilesystemCopyPlanner.create(
        databaseName(target), tableName(target), schema, metadata);
  }

  private static FsPath destination(FsPath source, FsPath target) throws SQLException {
    if (!isDataFile(source)) {
      throw invalidOperation();
    }
    if (target.getSegments().size() == 1) {
      target = target.resolve(source.getFileName());
    }
    if (!isDataFile(target)) {
      throw invalidOperation();
    }
    if (source.toString().equalsIgnoreCase(target.toString())) {
      throw new SQLException(String.format(CliMessages.FS_SAME_FILE, source));
    }
    return target;
  }

  private FsPath temporaryPath(FsPath path) {
    return parent(path).resolve(temporaryName.get() + CSV_SUFFIX);
  }

  private void rename(FsPath source, FsPath target) throws SQLException {
    executor.execute(
        "ALTER TABLE "
            + toTablePath(source)
            + " RENAME TO "
            + TableFilesystemSql.identifier(tableName(target)));
  }

  private void cleanup(FsPath path, SQLException failure) {
    try {
      remove(path);
    } catch (SQLException cleanupFailure) {
      failure.addSuppressed(cleanupFailure);
    }
  }

  private static SQLException invalidOperation() {
    return new SQLException(CliMessages.FS_INVALID_WRITE_OPERATION);
  }

  private void dropDatabase(FsPath path) throws SQLException {
    if (path.getSegments().size() != 1) {
      throw invalidOperation();
    }
    executor.execute("DROP DATABASE " + TableFilesystemSql.identifier(path.getFileName()));
  }

  private static String toTablePath(FsPath path) {
    return TableFilesystemSql.tablePath(databaseName(path), tableName(path));
  }

  private static String databaseName(FsPath path) {
    return path.getSegments().get(0);
  }

  private static boolean isDataFile(FsPath path) {
    return path.getSegments().size() == 2
        && path.getFileName().endsWith(CSV_SUFFIX)
        && path.getFileName().length() > CSV_SUFFIX.length();
  }

  private static String tableName(FsPath path) {
    String fileName = path.getFileName();
    return fileName.substring(0, fileName.length() - CSV_SUFFIX.length());
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
