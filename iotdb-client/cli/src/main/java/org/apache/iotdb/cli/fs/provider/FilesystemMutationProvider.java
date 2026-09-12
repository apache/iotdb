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
import org.apache.iotdb.cli.i18n.CliMessages;

import java.sql.SQLException;
import java.util.List;

public interface FilesystemMutationProvider {

  void mkdir(FsPath path) throws SQLException;

  void rmdir(FsPath path) throws SQLException;

  void remove(FsPath path) throws SQLException;

  void removeRecursive(FsPath path) throws SQLException;

  void move(FsPath source, FsPath target) throws SQLException;

  void copy(FsPath source, FsPath target) throws SQLException;

  default void copy(FsPath source, FsPath target, boolean replace) throws SQLException {
    if (replace) {
      throw new SQLException(CliMessages.FS_WRITE_UNSUPPORTED);
    }
    copy(source, target);
  }

  default void move(FsPath source, FsPath target, boolean replace) throws SQLException {
    if (replace) {
      throw new SQLException(CliMessages.FS_WRITE_UNSUPPORTED);
    }
    move(source, target);
  }

  void append(FsPath path, List<String> lines) throws SQLException;

  default void write(FsPath path, List<String> lines, boolean append) throws SQLException {
    if (!append) {
      throw new SQLException(CliMessages.FS_WRITE_UNSUPPORTED);
    }
    append(path, lines);
  }
}
