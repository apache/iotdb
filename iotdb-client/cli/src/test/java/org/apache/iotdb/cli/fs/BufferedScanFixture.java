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
import org.apache.iotdb.cli.fs.path.FsPath;
import org.apache.iotdb.cli.fs.provider.FilesystemSchemaProvider;
import org.apache.iotdb.cli.fs.sql.SqlExecutor.RowVisitor;
import org.apache.iotdb.cli.fs.sql.SqlRow;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;

/** Adapts historical buffered fixtures; Mockito in this module cannot invoke default methods. */
final class BufferedScanFixture {
  private BufferedScanFixture() {}

  static void install(FilesystemSchemaProvider provider) throws SQLException {
    doAnswer(
            invocation -> {
              FsPath path = invocation.getArgument(0);
              ReadOptions options = invocation.getArgument(1);
              boolean descending = invocation.getArgument(2);
              RowVisitor visitor = invocation.getArgument(3);
              boolean filtered =
                  options.getStart() != null
                      || options.getEnd() != null
                      || !options.getTagFilters().isEmpty();
              int limit =
                  options.getLimit() < 0 || filtered
                      ? -1
                      : (int)
                          Math.min(
                              Integer.MAX_VALUE,
                              options.getLimit()
                                  + Math.min(Integer.MAX_VALUE, options.getOffset()));
              List<SqlRow> rows =
                  descending ? provider.tail(path, limit) : provider.read(path, limit);
              if (descending) {
                rows = new ArrayList<>(rows);
                Collections.reverse(rows);
              }
              for (SqlRow row : rows) if (!visitor.visit(row)) break;
              return null;
            })
        .when(provider)
        .scan(any(), any(), anyBoolean(), any());
  }
}
