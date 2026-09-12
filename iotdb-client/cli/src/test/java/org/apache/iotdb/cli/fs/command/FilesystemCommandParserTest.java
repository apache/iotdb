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
  public void parseMetaCommandUsesTablePath() {
    FilesystemCommand command = FilesystemCommandParser.parse("meta /db1/table1.csv");

    assertEquals(FilesystemCommand.Type.META, command.getType());
    assertEquals("/db1/table1.csv", command.getPath());
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
  public void parseTsFileReadOptions() {
    FilesystemCommand command =
        FilesystemCommandParser.parse(
            "head -t sensors -m temperature -m humidity -f ndjson --offset 2 --start -10 --end 20 "
                + "--tag-filter site eq north --tag-filter rack eq r1 --tag-match any data.tsfile");
    assertEquals(FilesystemCommand.Type.HEAD, command.getType());
    assertEquals("ndjson", command.getFormat());
    assertEquals("sensors", command.getTable());
    assertEquals(2, command.getColumns().size());
    assertEquals(2, command.getOffset());
    assertEquals("-10", command.getStart());
    assertEquals("any", command.getTagMatch());
    assertEquals(2, command.getTagFilters().size());
  }

  @Test
  public void parseLongMeasurementsAliasAndTagFilterOperators() {
    FilesystemCommand command =
        FilesystemCommandParser.parse(
            "cat --measurements temperature --measurements humidity "
                + "--tag-filter site is-null data.tsfile");
    assertEquals(2, command.getColumns().size());
    assertEquals("temperature", command.getColumns().get(0));
    assertEquals("site is-null", command.getTagFilters().get(0));
  }

  @Test
  public void rejectInvalidTagFilterCombinations() {
    assertInvalid(
        "cat --tag-filter site contains north data.tsfile",
        "cat --tag-match all --tag-filter site eq north data.tsfile",
        "cat --tag-filter site eq north --tag-filter rack eq r1 data.tsfile");
  }

  @Test
  public void parseTailLimitAndPath() {
    FilesystemCommand command = FilesystemCommandParser.parse("tail -n 3 /db1/table1.csv");

    assertEquals(FilesystemCommand.Type.TAIL, command.getType());
    assertEquals("/db1/table1.csv", command.getPath());
    assertEquals(3, command.getLimit());
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
  public void parseLessMoreFileAndWc() {
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
        FilesystemCommand.Type.STATS,
        FilesystemCommandParser.parse("stats /db1/table1.csv").getType());
    assertEquals(
        FilesystemCommand.Type.COUNT,
        FilesystemCommandParser.parse("count /db1/table1.csv").getType());
    FilesystemCommand wc = FilesystemCommandParser.parse("wc -c /db1/table1.csv");
    assertEquals(FilesystemCommand.Type.WC, wc.getType());
    assertEquals("-c", wc.getOption());
    assertEquals(
        FilesystemCommand.Type.INVALID,
        FilesystemCommandParser.parse("du /db1/table1.csv").getType());
    assertEquals(
        FilesystemCommand.Type.INVALID,
        FilesystemCommandParser.parse("wc -l /db1/table1.csv").getType());
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
  public void parseCutRequiresSelectionAndDefaultsToStdin() {
    assertEquals(
        FilesystemCommand.Type.INVALID,
        FilesystemCommandParser.parse("cut -d, /db1/table1.csv").getType());
    assertEquals("-", FilesystemCommandParser.parse("cut -f2,3").getPath());
  }

  @Test
  public void parseTeeAppendPath() {
    FilesystemCommand command = FilesystemCommandParser.parse("tee -a /db1/table1.csv");

    assertEquals(FilesystemCommand.Type.TEE, command.getType());
    assertEquals("-a", command.getOption());
    assertEquals("/db1/table1.csv", command.getPath());
  }

  @Test
  public void parseTeeAllowsOverwriteAndStandardOutputOnly() {
    assertEquals(
        FilesystemCommand.Type.TEE, FilesystemCommandParser.parse("tee /db1/table1.csv").getType());
    assertEquals(FilesystemCommand.Type.TEE, FilesystemCommandParser.parse("tee -a").getType());
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
  public void parseLsPreservesRecursiveAndDisplayFlags() {
    FilesystemCommand command = FilesystemCommandParser.parse("ls -R /db1");

    assertEquals(FilesystemCommand.Type.LS, command.getType());
    assertEquals("/db1", command.getPath());
    assertTrue(command.hasOption("-R"));
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
        "find a b",
        "less a b",
        "more a b",
        "file a b",
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
        "find -x",
        "less -x",
        "more -x",
        "file -x",
        "rmdir -p path",
        "cp -r a b",
        "cut -x -f1 path",
        "join -x a b",
        "tee -p path",
        "tree -x",
        "ls -ll",
        "ls -a -a",
        "head -n 1 -2 path",
        "tail -2 -n 1 path",
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
          "tree -L " + number, "join -1 " + number + " a b", "join -2 " + number + " a b");
      if (!"+1".equals(number)) assertInvalid("tail -n " + number + " path");
      assertInvalid("head -n " + number + " path");
    }
    assertEquals(0, FilesystemCommandParser.parse("head -n 0 path").getLimit());
    assertEquals(0, FilesystemCommandParser.parse("tree -L 0").getDepth());
    assertEquals(
        Integer.MAX_VALUE, FilesystemCommandParser.parse("tail -n 2147483647 path").getLimit());
    assertInvalid("join -1 0 a b", "join -2 0 a b");
  }

  @Test
  public void rejectNonCanonicalReadTimestampsAndOffsets() {
    assertInvalid(
        "head --start 01 path",
        "head --start -0 path",
        "head --start +1 path",
        "head --offset 01 path",
        "head --offset +1 path",
        "head --offset -1 path");
    assertEquals("-10", FilesystemCommandParser.parse("head --start -10 path").getStart());
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
          "pwd", "ls", "ll", "cd", "stat", "meta", "schema", "stats", "count", "cat", "head",
          "tail", "grep", "find", "less", "more", "file", "mkdir", "rmdir", "rm", "mv", "cp", "cut",
          "paste", "join", "tee", "tree", "sql", "help", "exit", "quit"
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

  @Test
  public void parseUnixFlagsAndStandardInput() {
    FilesystemCommand grep = FilesystemCommandParser.parse("grep -Ein 'a|b' - second");
    assertEquals(FilesystemCommand.Type.GREP, grep.getType());
    assertTrue(grep.hasOption("-E"));
    assertTrue(grep.hasOption("-i"));
    assertTrue(grep.hasOption("-n"));
    assertEquals(2, grep.getPaths().size());
    assertEquals("-", FilesystemCommandParser.parse("grep pattern").getPath());
    assertEquals("-", FilesystemCommandParser.parse("wc -c").getPath());
    assertEquals(2, FilesystemCommandParser.parse("wc -c first second").getPaths().size());
    assertEquals("-2,4-", FilesystemCommandParser.parse("cut -c-2,4-").getPattern());
    assertTrue(FilesystemCommandParser.parse("cut -s -d, -f2-").hasOption("-s"));
    assertTrue(FilesystemCommandParser.parse("paste -s -d, first second").hasOption("-s"));
    assertInvalid("cut -b1 -f1", "cut -c1 -s", "grep -FE pattern");
  }

  @Test
  public void parseTailFollowAndCountFromStart() {
    FilesystemCommand tail = FilesystemCommandParser.parse("tail -f -c +12 --format csv table.csv");
    assertEquals(FilesystemCommand.Type.TAIL, tail.getType());
    assertEquals(12, tail.getLimit());
    assertEquals("csv", tail.getFormat());
    assertTrue(tail.hasOption("-f"));
    assertTrue(tail.hasOption("--from-start"));
    assertInvalid("tail -n2 -c3 table.csv");
    assertEquals("/", FilesystemCommandParser.parse("cd").getPath());
    assertEquals("-", FilesystemCommandParser.parse("cd -").getPath());
    assertEquals(7, FilesystemCommandParser.parse("exit 7").getLimit());
    assertEquals(FilesystemCommand.Type.HELP, FilesystemCommandParser.parse("-h").getType());
    assertEquals("head", FilesystemCommandParser.parse("head -h").getPath());
  }

  @Test
  public void parseStructuredMetadataAndExportOptions() {
    FilesystemCommand metadata =
        FilesystemCommandParser.parse("count -t sensors -m value -f csv /db");
    assertEquals(FilesystemCommand.Type.COUNT, metadata.getType());
    assertEquals("sensors", metadata.getTable());
    assertEquals("value", metadata.getColumns().get(0));
    assertEquals("csv", metadata.getFormat());
    FilesystemCommand export =
        FilesystemCommandParser.parse("export -t sensors --type ndjson -o out.json --force /db");
    assertEquals(FilesystemCommand.Type.EXPORT, export.getType());
    assertEquals("ndjson", export.getFormat());
    assertEquals("out.json", export.optionValue("-o", ""));
    assertTrue(export.hasOption("--force"));
    assertEquals(
        FilesystemCommand.Type.SKETCH,
        FilesystemCommandParser.parse("sketch -o out.txt --force data.tsfile").getType());
    assertEquals("csv", FilesystemCommandParser.parse("ls --format csv /db").getFormat());
    assertInvalid(
        "export --type csv -o out /db",
        "export -t sensors -o out /db",
        "sketch --force data.tsfile");
    FilesystemCommand multiple =
        FilesystemCommandParser.parse(
            "export -t sensors -t meters --type csv --output-dir result /db");
    assertEquals(2, multiple.getScopeValues("-t").size());
    assertEquals("meters", multiple.getScopeValues("-t").get(1));
    assertInvalid("export -t sensors -t meters --type csv -o result /db");
  }

  @Test
  public void validateReadBoundsAndFindPredicates() {
    assertInvalid("cat -n -1 /db/table.csv", "head -n -1 /db/table.csv");
    assertInvalid("cat --start 20 --end 10 /db/table.csv", "head -n0 --offset 1 /db/table.csv");
    FilesystemCommand find =
        FilesystemCommandParser.parse("find /db -name '*.csv' -type f -maxdepth 2");
    assertEquals(FilesystemCommand.Type.FIND, find.getType());
    assertEquals("2", find.optionValue("-maxdepth", ""));
    assertInvalid("find -type x", "find -maxdepth -1");
  }

  @Test
  public void parseMutationAndJoinOptions() {
    FilesystemCommand remove = FilesystemCommandParser.parse("rm -rf one two");
    assertTrue(remove.hasOption("-r"));
    assertTrue(remove.hasOption("-f"));
    assertEquals(2, remove.getPaths().size());
    assertTrue(FilesystemCommandParser.parse("mkdir -p -m755 /db").hasOption("-p"));
    assertTrue(FilesystemCommandParser.parse("cp -n first second target").hasOption("-n"));
    FilesystemCommand join = FilesystemCommandParser.parse("join -a1 -eNA -o0,1.2,2.2 a b");
    assertEquals(FilesystemCommand.Type.JOIN, join.getType());
    assertEquals("1", join.optionValue("-a", ""));
    assertEquals("NA", join.optionValue("-e", ""));
    assertInvalid("join -a3 a b", "join -o1.0 a b", "mkdir", "mkdir -m888 /db");
  }

  private static void assertInvalid(String... inputs) {
    for (String input : inputs) {
      FilesystemCommand command = FilesystemCommandParser.parse(input);
      assertEquals(input, FilesystemCommand.Type.INVALID, command.getType());
      assertFalse(input, command.getErrorMessage().isEmpty());
    }
  }
}
