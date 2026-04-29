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

import java.sql.SQLException;
import java.util.List;

public class TableFilesystemMutationProvider implements FilesystemMutationProvider {

  private static final String INVALID_WRITE_OPERATION =
      "Invalid filesystem write operation for this path";

  private final SqlExecutor executor;

  public TableFilesystemMutationProvider(SqlExecutor executor) {
    this.executor = executor;
  }

  @Override
  public void mkdir(FsPath path) throws SQLException {
    if (path.getSegments().size() != 1) {
      throw invalidOperation();
    }
    executor.execute("CREATE DATABASE " + path.getFileName());
  }

  @Override
  public void remove(FsPath path) throws SQLException {
    if (path.getSegments().size() != 2) {
      throw invalidOperation();
    }
    executor.execute("DROP TABLE " + toTablePath(path));
  }

  @Override
  public void move(FsPath source, FsPath target) throws SQLException {
    if (source.getSegments().size() != 2 || target.getSegments().size() != 2) {
      throw invalidOperation();
    }
    if (!parent(source).equals(parent(target))) {
      throw invalidOperation();
    }
    executor.execute("ALTER TABLE " + toTablePath(source) + " RENAME TO " + target.getFileName());
  }

  private static SQLException invalidOperation() {
    return new SQLException(INVALID_WRITE_OPERATION);
  }

  private static String toTablePath(FsPath path) {
    List<String> segments = path.getSegments();
    return segments.get(0) + "." + segments.get(1);
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
