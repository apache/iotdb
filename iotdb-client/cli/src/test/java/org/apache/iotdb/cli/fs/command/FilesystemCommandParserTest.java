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
  public void parseLlAllOptionAsCurrentDirectoryLongListCommand() {
    FilesystemCommand command = FilesystemCommandParser.parse("ll -a");

    assertEquals(FilesystemCommand.Type.LL, command.getType());
    assertEquals(".", command.getPath());
    assertEquals("-a", command.getOption());
  }

  @Test
  public void parseLlCombinedOptionsAndPath() {
    FilesystemCommand command = FilesystemCommandParser.parse("ll -al /db1");

    assertEquals(FilesystemCommand.Type.LL, command.getType());
    assertEquals("/db1", command.getPath());
    assertEquals("-a", command.getOption());
  }

  @Test
  public void parseLsLongOptionAsLongListCommand() {
    FilesystemCommand command = FilesystemCommandParser.parse("ls -la /db1");

    assertEquals(FilesystemCommand.Type.LL, command.getType());
    assertEquals("/db1", command.getPath());
    assertEquals("-a", command.getOption());
  }

  @Test
  public void parsePathCommand() {
    FilesystemCommand command = FilesystemCommandParser.parse("  ls   /root/sg  ");

    assertEquals(FilesystemCommand.Type.LS, command.getType());
    assertEquals("/root/sg", command.getPath());
  }

  @Test
  public void parseCatSidecarPath() {
    FilesystemCommand command = FilesystemCommandParser.parse("cat /db1/table1.csv");

    assertEquals(FilesystemCommand.Type.CAT, command.getType());
    assertEquals("/db1/table1.csv", command.getPath());
  }

  @Test
  public void parseCatMultiplePaths() {
    FilesystemCommand command =
        FilesystemCommandParser.parse("cat /db1/table1.csv /db1/table1.meta");

    assertEquals(FilesystemCommand.Type.CAT, command.getType());
    assertEquals(2, command.getPaths().size());
    assertEquals("/db1/table1.csv", command.getPaths().get(0));
    assertEquals("/db1/table1.meta", command.getPaths().get(1));
  }

  @Test
  public void parseHeadLimitAndPath() {
    FilesystemCommand command = FilesystemCommandParser.parse("head -n 5 /db1/table1.csv");

    assertEquals(FilesystemCommand.Type.HEAD, command.getType());
    assertEquals("/db1/table1.csv", command.getPath());
    assertEquals(5, command.getLimit());
  }

  @Test
  public void parseTailLimitAndPath() {
    FilesystemCommand command = FilesystemCommandParser.parse("tail -n 3 /db1/table1.csv");

    assertEquals(FilesystemCommand.Type.TAIL, command.getType());
    assertEquals("/db1/table1.csv", command.getPath());
    assertEquals(3, command.getLimit());
  }

  @Test
  public void parseWcLineCountAndPath() {
    FilesystemCommand command = FilesystemCommandParser.parse("wc -l /db1/table1.csv");

    assertEquals(FilesystemCommand.Type.WC, command.getType());
    assertEquals("/db1/table1.csv", command.getPath());
    assertEquals("-l", command.getOption());
  }

  @Test
  public void parseGrepPatternAndPath() {
    FilesystemCommand command = FilesystemCommandParser.parse("grep spricoder /db1/table1.csv");

    assertEquals(FilesystemCommand.Type.GREP, command.getType());
    assertEquals("/db1/table1.csv", command.getPath());
    assertEquals("spricoder", command.getPattern());
  }

  @Test
  public void parseFindNamePatternAndPath() {
    FilesystemCommand command = FilesystemCommandParser.parse("find /db1 -name table1.csv");

    assertEquals(FilesystemCommand.Type.FIND, command.getType());
    assertEquals("/db1", command.getPath());
    assertEquals("table1.csv", command.getPattern());
  }

  @Test
  public void parseLessMoreFileAndDu() {
    assertEquals(
        FilesystemCommand.Type.LESS,
        FilesystemCommandParser.parse("less /db1/table1.csv").getType());
    assertEquals(
        FilesystemCommand.Type.MORE,
        FilesystemCommandParser.parse("more /db1/table1.csv").getType());
    assertEquals(
        FilesystemCommand.Type.FILE,
        FilesystemCommandParser.parse("file /db1/table1.csv").getType());
    assertEquals(
        FilesystemCommand.Type.DU, FilesystemCommandParser.parse("du /db1/table1.csv").getType());
  }

  @Test
  public void parsePastePaths() {
    FilesystemCommand command =
        FilesystemCommandParser.parse("paste /db1/table1.csv /db1/table2.csv");

    assertEquals(FilesystemCommand.Type.PASTE, command.getType());
    assertEquals(2, command.getPaths().size());
    assertEquals("/db1/table1.csv", command.getPaths().get(0));
    assertEquals("/db1/table2.csv", command.getPaths().get(1));
  }

  @Test
  public void parseJoinPathsUsesDefaultDelimiterAndFields() {
    FilesystemCommand command =
        FilesystemCommandParser.parse("join /db1/table1.csv /db1/table2.csv");

    assertEquals(FilesystemCommand.Type.JOIN, command.getType());
    assertEquals("", command.getOption());
    assertEquals("1,1", command.getPattern());
    assertEquals(2, command.getPaths().size());
    assertEquals("/db1/table1.csv", command.getPaths().get(0));
    assertEquals("/db1/table2.csv", command.getPaths().get(1));
  }

  @Test
  public void parseJoinDelimiterAndFields() {
    FilesystemCommand command =
        FilesystemCommandParser.parse("join -t, -1 2 -2 1 /db1/table1.csv /db1/table2.csv");

    assertEquals(FilesystemCommand.Type.JOIN, command.getType());
    assertEquals(",", command.getOption());
    assertEquals("2,1", command.getPattern());
    assertEquals("/db1/table1.csv", command.getPaths().get(0));
    assertEquals("/db1/table2.csv", command.getPaths().get(1));
  }

  @Test
  public void parseJoinSeparatedDelimiter() {
    FilesystemCommand command =
        FilesystemCommandParser.parse("join -t , /db1/table1.csv /db1/table2.csv");

    assertEquals(FilesystemCommand.Type.JOIN, command.getType());
    assertEquals(",", command.getOption());
    assertEquals("1,1", command.getPattern());
  }

  @Test
  public void parseJoinRejectsInvalidArguments() {
    assertEquals(FilesystemCommand.Type.INVALID, FilesystemCommandParser.parse("join").getType());
    assertEquals(
        FilesystemCommand.Type.INVALID,
        FilesystemCommandParser.parse("join /db1/table1.csv").getType());
    assertEquals(
        FilesystemCommand.Type.INVALID,
        FilesystemCommandParser.parse("join /db1/a.csv /db1/b.csv /db1/c.csv").getType());
    assertEquals(
        FilesystemCommand.Type.INVALID,
        FilesystemCommandParser.parse("join -t /db1/a.csv /db1/b.csv").getType());
    assertEquals(
        FilesystemCommand.Type.INVALID,
        FilesystemCommandParser.parse("join -t:: /db1/a.csv /db1/b.csv").getType());
    assertEquals(
        FilesystemCommand.Type.INVALID,
        FilesystemCommandParser.parse("join -1 0 /db1/a.csv /db1/b.csv").getType());
    assertEquals(
        FilesystemCommand.Type.INVALID,
        FilesystemCommandParser.parse("join -x /db1/a.csv /db1/b.csv").getType());
  }

  @Test
  public void parseCutDelimiterFieldsAndPath() {
    FilesystemCommand command = FilesystemCommandParser.parse("cut -d, -f2,3 /db1/table1.csv");

    assertEquals(FilesystemCommand.Type.CUT, command.getType());
    assertEquals(",", command.getOption());
    assertEquals("2,3", command.getPattern());
    assertEquals("/db1/table1.csv", command.getPath());
  }

  @Test
  public void parseCutSeparatedOptionArguments() {
    FilesystemCommand command = FilesystemCommandParser.parse("cut -d , -f 1-2 /db1/table1.csv");

    assertEquals(FilesystemCommand.Type.CUT, command.getType());
    assertEquals(",", command.getOption());
    assertEquals("1-2", command.getPattern());
    assertEquals("/db1/table1.csv", command.getPath());
  }

  @Test
  public void parseCutRequiresFieldsAndPath() {
    assertEquals(
        FilesystemCommand.Type.INVALID,
        FilesystemCommandParser.parse("cut -d, /db1/table1.csv").getType());
    assertEquals(
        FilesystemCommand.Type.INVALID, FilesystemCommandParser.parse("cut -f2,3").getType());
  }

  @Test
  public void parseTeeAppendPath() {
    FilesystemCommand command = FilesystemCommandParser.parse("tee -a /db1/table1.csv");

    assertEquals(FilesystemCommand.Type.TEE, command.getType());
    assertEquals("-a", command.getOption());
    assertEquals("/db1/table1.csv", command.getPath());
  }

  @Test
  public void parseTeeRequiresAppendOptionAndPath() {
    assertEquals(
        FilesystemCommand.Type.INVALID,
        FilesystemCommandParser.parse("tee /db1/table1.csv").getType());
    assertEquals(FilesystemCommand.Type.INVALID, FilesystemCommandParser.parse("tee -a").getType());
    assertEquals(
        FilesystemCommand.Type.INVALID,
        FilesystemCommandParser.parse("tee -p /db1/table1.csv").getType());
  }

  @Test
  public void parseWriteCommands() {
    FilesystemCommand mkdir = FilesystemCommandParser.parse("mkdir /db1");
    assertEquals(FilesystemCommand.Type.MKDIR, mkdir.getType());
    assertEquals("/db1", mkdir.getPath());

    FilesystemCommand rm = FilesystemCommandParser.parse("rm /db1/table1.csv");
    assertEquals(FilesystemCommand.Type.RM, rm.getType());
    assertEquals("/db1/table1.csv", rm.getPath());

    FilesystemCommand mv = FilesystemCommandParser.parse("mv /db1/table1.csv /db1/table2.csv");
    assertEquals(FilesystemCommand.Type.MV, mv.getType());
    assertEquals(2, mv.getPaths().size());
    assertEquals("/db1/table1.csv", mv.getPaths().get(0));
    assertEquals("/db1/table2.csv", mv.getPaths().get(1));
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
