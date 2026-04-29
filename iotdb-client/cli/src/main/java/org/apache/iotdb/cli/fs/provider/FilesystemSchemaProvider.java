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
import org.apache.iotdb.cli.fs.path.FsPath;
import org.apache.iotdb.cli.fs.sql.SqlRow;

import java.sql.SQLException;
import java.util.List;

public interface FilesystemSchemaProvider {

  List<FsNode> list(FsPath path) throws SQLException;

  FsNode describe(FsPath path) throws SQLException;

  List<SqlRow> read(FsPath path, int limit) throws SQLException;

  default List<SqlRow> tail(FsPath path, int limit) throws SQLException {
    throw new SQLException("Path does not support tail: " + path);
  }

  default long count(FsPath path) throws SQLException {
    throw new SQLException("Path does not support count: " + path);
  }

  default List<SqlRow> read(List<FsPath> paths, int limit) throws SQLException {
    if (paths.size() == 1) {
      return read(paths.get(0), limit);
    }
    throw new SQLException("Multiple paths are not readable by this provider");
  }
}
