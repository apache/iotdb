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

import org.apache.iotdb.cli.i18n.CliMessages;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

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
  public void parseSchemaCommandUsesTablePath() {
    FilesystemCommand command = FilesystemCommandParser.parse("schema /db1/table1");

    assertEquals(FilesystemCommand.Type.SCHEMA, command.getType());
    assertEquals("/db1/table1", command.getPath());
  }

  @Test
  public void parseSchemaCommandDefaultsToCurrentDirectory() {
    FilesystemCommand command = FilesystemCommandParser.parse("schema");

    assertEquals(FilesystemCommand.Type.SCHEMA, command.getType());
    assertEquals(".", command.getPath());
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
  public void parseRmdirCommand() {
    FilesystemCommand command = FilesystemCommandParser.parse("rmdir /db1");

    assertEquals(FilesystemCommand.Type.RMDIR, command.getType());
    assertEquals("/db1", command.getPath());
  }

  @Test
  public void parseRmRecursiveCommand() {
    FilesystemCommand command = FilesystemCommandParser.parse("rm -r /db1");

    assertEquals(FilesystemCommand.Type.RM, command.getType());
    assertEquals("-r", command.getOption());
    assertEquals("/db1", command.getPath());
  }

  @Test
  public void parseCpCommand() {
    FilesystemCommand command =
        FilesystemCommandParser.parse("cp /db1/table1.schema /db1/table2.schema");

    assertEquals(FilesystemCommand.Type.CP, command.getType());
    assertEquals(2, command.getPaths().size());
    assertEquals("/db1/table1.schema", command.getPaths().get(0));
    assertEquals("/db1/table2.schema", command.getPaths().get(1));
  }

  @Test
  public void parseLsRecursiveAsTreeCommand() {
    FilesystemCommand command = FilesystemCommandParser.parse("ls -R /db1");

    assertEquals(FilesystemCommand.Type.TREE, command.getType());
    assertEquals("/db1", command.getPath());
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
    assertEquals(
        String.format(CliMessages.EXCEPTION_INVALID_TREE_DEPTH_ARG_EF544DD4, "bad"),
        command.getErrorMessage());
  }

  @Test
  public void parseQuotedAndEscapedOperands() {
    FilesystemCommand command =
        FilesystemCommandParser.parse(
            "cat '/db a/table.csv' \"/db b/table.csv\" /db\\ c/table.csv");

    assertEquals(FilesystemCommand.Type.CAT, command.getType());
    assertEquals("/db a/table.csv", command.getPaths().get(0));
    assertEquals("/db b/table.csv", command.getPaths().get(1));
    assertEquals("/db c/table.csv", command.getPaths().get(2));
    assertEquals(
        "device 1", FilesystemCommandParser.parse("grep 'device 1' '/db a/t.csv'").getPattern());
    assertEquals("", FilesystemCommandParser.parse("grep '' /db1/t.csv").getPattern());
  }

  @Test
  public void rejectUnclosedQuotesAndEscapes() {
    assertInvalid("cat '/db a/table.csv", "cat \"/db a/table.csv", "cat /db1/table.csv\\");
  }

  @Test
  public void operandDelimiterProtectsOptionLikePathsAndPatterns() {
    FilesystemCommand head = FilesystemCommandParser.parse("head -n 2 -- -table.csv");
    assertEquals(FilesystemCommand.Type.HEAD, head.getType());
    assertEquals("-table.csv", head.getPath());
    assertEquals(2, head.getLimit());
    FilesystemCommand grep = FilesystemCommandParser.parse("grep -- --help -table.csv");
    assertEquals(FilesystemCommand.Type.GREP, grep.getType());
    assertEquals("--help", grep.getPattern());
    assertEquals("-table.csv", grep.getPath());
    FilesystemCommand cat = FilesystemCommandParser.parse("cat -- --help --");
    assertEquals(2, cat.getPaths().size());
    assertEquals("--help", cat.getPaths().get(0));
    assertEquals("--", cat.getPaths().get(1));
    assertInvalid("cat --", "ls --", "head -n -- file", "head -- file -n 2");
  }

  @Test
  public void preserveOptionsAfterPaths() {
    assertEquals(3, FilesystemCommandParser.parse("head /db1/t.csv -n 3").getLimit());
    assertEquals(2, FilesystemCommandParser.parse("tail /db1/t.csv -2").getLimit());
    assertEquals("-r", FilesystemCommandParser.parse("rm /db1 -r").getOption());
    assertEquals("-a", FilesystemCommandParser.parse("tee /db1/t.csv -a").getOption());
    assertEquals("t.csv", FilesystemCommandParser.parse("find /db1 -name t.csv").getPattern());
    assertEquals("1-2", FilesystemCommandParser.parse("cut /db1/t.csv -d, -f1-2").getPattern());
    assertEquals(
        "2,1", FilesystemCommandParser.parse("join /db1/a.csv -1 2 /db1/b.csv -t,").getPattern());
  }

  @Test
  public void rejectUnexpectedOperandsInsteadOfIgnoringThem() {
    assertInvalid(
        "pwd /db1",
        "exit now",
        "quit now",
        "ls a b",
        "ll a b",
        "cd a b",
        "stat a b",
        "head a b",
        "tail a b",
        "wc a b",
        "grep pattern a b",
        "find a b",
        "less a b",
        "more a b",
        "file a b",
        "du a b",
        "mkdir a b",
        "rmdir a b",
        "rm a b",
        "rm -r a b",
        "mv a b c",
        "cp a b c",
        "cut -f1 a b",
        "tee -a a b",
        "tree a b");
  }

  @Test
  public void rejectUnsupportedOptionsAndDuplicateSingletons() {
    assertInvalid(
        "ls -x",
        "cd -x",
        "stat -x",
        "cat -x",
        "head -x",
        "tail -x",
        "wc -c",
        "grep -i pattern path",
        "find -x",
        "less -x",
        "more -x",
        "file -x",
        "du -x",
        "mkdir -p path",
        "rmdir -p path",
        "rm -f path",
        "mv -f a b",
        "cp -r a b",
        "cut -x -f1 path",
        "paste -d, a b",
        "join -x a b",
        "tee -p path",
        "tree -x",
        "ls -ll",
        "ls -a -a",
        "head -n 1 -2 path",
        "tail -2 -n 1 path",
        "wc -l -l",
        "find -name a -name b",
        "tree -L 1 -L 2",
        "rm -r -r path",
        "tee -a -a path",
        "cut -d, -d: -f1 path",
        "cut -f1 -f2 path",
        "join -t, -t: a b",
        "join -1 1 -1 2 a b",
        "join -2 1 -2 2 a b");
  }

  @Test
  public void rejectSignedNonAsciiLeadingZeroAndOverflowNumbers() {
    for (String number :
        new String[] {
          "+1", "-1", "01", "1x", "1.0", "\u0661", "2147483648", "999999999999999999999"
        }) {
      assertInvalid(
          "head -n " + number + " path",
          "tail -n " + number + " path",
          "tree -L " + number,
          "join -1 " + number + " a b",
          "join -2 " + number + " a b");
    }
    assertEquals(0, FilesystemCommandParser.parse("head -n 0 path").getLimit());
    assertEquals(0, FilesystemCommandParser.parse("tree -L 0").getDepth());
    assertEquals(
        Integer.MAX_VALUE, FilesystemCommandParser.parse("tail -n 2147483647 path").getLimit());
    assertInvalid("join -1 0 a b", "join -2 0 a b");
  }

  @Test
  public void validateCutFieldListsBeforeExecution() {
    for (String fields :
        new String[] {
          "0",
          "+1",
          "01",
          "\u0661",
          "2147483648",
          "1,",
          ",1",
          "1,,2",
          "-2",
          "2-",
          "3-1",
          "1-2-3",
          "1,0",
          "1-2147483648"
        }) {
      assertInvalid("cut -f" + fields + " path");
    }
    assertEquals(
        "1,3-5,2147483647",
        FilesystemCommandParser.parse("cut -f1,3-5,2147483647 path").getPattern());
    assertInvalid("cut -f '' path", "cut -d '' -f1 path", "cut -d:: -f1 path", "join -t '' a b");
  }

  @Test
  public void parseStandaloneAndPerCommandHelp() {
    assertEquals(FilesystemCommand.Type.HELP, FilesystemCommandParser.parse("--help").getType());
    for (String command :
        new String[] {
          "pwd", "ls", "ll", "cd", "stat", "cat", "head", "tail", "wc", "grep", "find", "less",
          "more", "file", "du", "mkdir", "rmdir", "rm", "mv", "cp", "cut", "paste", "join", "tee",
          "tree", "sql", "help", "exit", "quit"
        }) {
      FilesystemCommand help = FilesystemCommandParser.parse(command + " --help");
      assertEquals(command, FilesystemCommand.Type.HELP, help.getType());
      assertEquals(command, command, help.getPath());
      assertEquals(command, FilesystemCommandParser.parse("help " + command).getPath());
    }
    assertEquals("head", FilesystemCommandParser.parse("HELP HeAd").getPath());
    assertInvalid(
        "--help head",
        "unknown --help",
        "help unknown",
        "head --help path",
        "head path --help",
        "help head path",
        "sql --help SELECT 1");
  }

  @Test
  public void preserveSqlQuotingAndWhitespaceAfterCommand() {
    FilesystemCommand command =
        FilesystemCommandParser.parse("sql\tSELECT 'a b', '--help', '\\\\' FROM t");
    assertEquals(FilesystemCommand.Type.SQL, command.getType());
    assertEquals("SELECT 'a b', '--help', '\\\\' FROM t", command.getStatement());
    assertInvalid("sql", "sql   ");
  }

  private static void assertInvalid(String... inputs) {
    for (String input : inputs) {
      FilesystemCommand command = FilesystemCommandParser.parse(input);
      assertEquals(input, FilesystemCommand.Type.INVALID, command.getType());
      assertFalse(input, command.getErrorMessage().isEmpty());
    }
  }
}
