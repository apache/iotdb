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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FilesystemCommandParserTest {

  @Test
  public void parseSimpleCommands() {
    assertEquals(FilesystemCommand.Type.PWD, FilesystemCommandParser.parse("pwd").getType());
    assertEquals(FilesystemCommand.Type.HELP, FilesystemCommandParser.parse("help").getType());
    assertEquals(FilesystemCommand.Type.EXIT, FilesystemCommandParser.parse("exit").getType());
    assertEquals(FilesystemCommand.Type.EXIT, FilesystemCommandParser.parse("quit").getType());
  }

  @Test
  public void parseLlAsLongListCommand() {
    FilesystemCommand command = FilesystemCommandParser.parse("ll /db1");

    assertEquals(FilesystemCommand.Type.LL, command.getType());
    assertEquals("/db1", command.getPath());
  }

  @Test
  public void parsePathCommand() {
    FilesystemCommand command = FilesystemCommandParser.parse("  ls   /root/sg  ");

    assertEquals(FilesystemCommand.Type.LS, command.getType());
    assertEquals("/root/sg", command.getPath());
  }

  @Test
  public void parseCatTablePath() {
    FilesystemCommand command = FilesystemCommandParser.parse("cat /db1/table1");

    assertEquals(FilesystemCommand.Type.CAT, command.getType());
    assertEquals("/db1/table1", command.getPath());
  }

  @Test
  public void parsePastePaths() {
    FilesystemCommand command =
        FilesystemCommandParser.parse("paste /db1/table1/tag1 /db1/table1/s1");

    assertEquals(FilesystemCommand.Type.PASTE, command.getType());
    assertEquals(2, command.getPaths().size());
    assertEquals("/db1/table1/tag1", command.getPaths().get(0));
    assertEquals("/db1/table1/s1", command.getPaths().get(1));
  }

  @Test
  public void parseTreeDepthBeforePath() {
    FilesystemCommand command = FilesystemCommandParser.parse("tree -L 2 /root/sg");

    assertEquals(FilesystemCommand.Type.TREE, command.getType());
    assertEquals("/root/sg", command.getPath());
    assertEquals(2, command.getDepth());
  }

  @Test
  public void parseTreeDepthAfterPath() {
    FilesystemCommand command = FilesystemCommandParser.parse("tree /root/sg -L 3");

    assertEquals(FilesystemCommand.Type.TREE, command.getType());
    assertEquals("/root/sg", command.getPath());
    assertEquals(3, command.getDepth());
  }

  @Test
  public void parseSqlPreservesStatementBody() {
    FilesystemCommand command =
        FilesystemCommandParser.parse("sql SELECT * FROM root.sg.d1 WHERE s1 > 1");

    assertEquals(FilesystemCommand.Type.SQL, command.getType());
    assertEquals("SELECT * FROM root.sg.d1 WHERE s1 > 1", command.getStatement());
  }

  @Test
  public void parseInvalidCommand() {
    FilesystemCommand command = FilesystemCommandParser.parse("unknown /root");

    assertEquals(FilesystemCommand.Type.INVALID, command.getType());
    assertFalse(command.getErrorMessage().isEmpty());
  }

  @Test
  public void parseInvalidTreeDepth() {
    FilesystemCommand command = FilesystemCommandParser.parse("tree -L bad /root");

    assertEquals(FilesystemCommand.Type.INVALID, command.getType());
    assertTrue(command.getErrorMessage().contains("depth"));
  }
}
