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

package org.apache.iotdb.cli.fs.command;

import org.apache.iotdb.cli.i18n.FsHelpMessages;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FilesystemCommandHelpTest {
  @Test
  public void generalHelpListsAllCommandsAndBatchContract() throws Exception {
    String help = help("");
    for (FilesystemCommand.Type type : FilesystemCommand.Type.values()) {
      if (type != FilesystemCommand.Type.INVALID) {
        assertTrue(
            type.name(), help.contains("  " + type.name().toLowerCase(java.util.Locale.ROOT)));
      }
    }
    assertTrue(help.contains("--fs_write_mode enabled"));
    assertTrue(help.contains("stdout"));
    assertTrue(help.contains("stderr"));
    assertTrue(help.contains("--"));
  }

  @Test
  public void commandHelpContainsUsageDefaultsResultsAndExamples() throws Exception {
    String help = help("head");
    assertTrue(help.contains("head [-n count | -count] [path]"));
    assertTrue(help.contains("head -n 5 /db1/table1.csv"));
    assertTrue(help.contains("10"));
    assertFalse(help.contains("tree [-L depth]"));
    assertTrue(help("cut").contains("cut -d, -f1-3,5 /db1/table1.csv"));
    assertTrue(help("join").contains("join -t, -1 2 -2 1 /db1/table1.csv /db1/table2.csv"));
  }

  @Test
  public void helpDocumentsMatchingAndDestructiveOperationContracts() throws Exception {
    assertTrue(help("find").contains(FsHelpMessages.FIND[0]));
    assertTrue(help("grep").contains(FsHelpMessages.GREP[0]));
    assertTrue(help("rmdir").contains(FsHelpMessages.RMDIR[0]));
    assertTrue(help("cp").contains("/db2/table2.csv"));
    assertFalse(help("cp").contains(".schema"));
    assertFalse(help("tee").contains(":wq"));
    assertTrue(help("export").contains("--output-dir"));
    assertTrue(help("sketch").contains("<local.tsfile>"));
  }

  private static String help(String command) throws Exception {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (PrintStream stream = new PrintStream(output, false, StandardCharsets.UTF_8.name())) {
      FilesystemCommandHelp.print(stream, command);
    }
    return output.toString(StandardCharsets.UTF_8.name());
  }
}
