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

import org.apache.iotdb.cli.fs.command.FilesystemCommandParser;
import org.apache.iotdb.cli.fs.command.ReadOptions;
import org.apache.iotdb.cli.fs.path.FsPath;
import org.apache.iotdb.cli.fs.provider.FilesystemSchemaProvider;
import org.apache.iotdb.cli.fs.provider.TreeFilesystemSchemaProvider;
import org.apache.iotdb.cli.fs.sql.SqlExecutor;
import org.apache.iotdb.cli.i18n.FsReadMessages;

import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class FsReadScopeTest {
  @Test
  public void deviceSelectionPreservesQuotedDotsAndAllowsAncestorScopes() {
    FilesystemSchemaProvider provider = mock(FilesystemSchemaProvider.class);
    when(provider.model()).thenReturn("tree");
    ReadOptions options = options("cat -d 'root.db.`device.one`'");
    FsPath expected = FsPath.absolute("/root/db/`device.one`");
    assertEquals(expected, new FsRowReader(provider).scope(FsPath.absolute("/"), options));
    assertEquals(expected, new FsRowReader(provider).scope(FsPath.absolute("/root/db"), options));
  }

  @Test
  public void virtualRootReadsAreRejectedBeforeAnySql() throws Exception {
    FilesystemSchemaProvider provider = mock(FilesystemSchemaProvider.class);
    try {
      new FsRowReader(provider).read(FsPath.absolute("/"), options("cat"), false);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(String.format(FsReadMessages.INVALID_SCOPE, "/"), e.getMessage());
    }
    SqlExecutor executor = mock(SqlExecutor.class);
    try {
      new TreeFilesystemSchemaProvider(executor).read(FsPath.absolute("/"), -1);
      fail();
    } catch (SQLException e) {
      assertEquals(String.format(FsReadMessages.INVALID_SCOPE, "/"), e.getMessage());
    }
    verifyZeroInteractions(executor);
  }

  @Test
  public void textInputsRejectDataOptionsButAcceptLineLimitsAndOffsets() {
    for (String command :
        new String[] {
          "cat -m value",
          "cat -t t",
          "cat -d root.db.d",
          "cat --start 1",
          "cat --end 2",
          "cat --tag-filter site eq a",
          "cat -f ndjson"
        }) {
      try {
        FsRowReader.validateTextOptions(options(command));
        fail(command);
      } catch (IllegalArgumentException e) {
        assertEquals(FsReadMessages.TEXT_OPTIONS, e.getMessage());
      }
    }
    FsRowReader.validateTextOptions(options("head -n 5 --offset 2"));
    FsRowReader.validateTextOptions(options("cat -f csv"));
  }

  private static ReadOptions options(String command) {
    return FilesystemCommandParser.parse(command).getReadOptions();
  }
}
