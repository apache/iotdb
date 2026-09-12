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

import org.apache.iotdb.cli.fs.command.FilesystemCommand;
import org.apache.iotdb.cli.fs.command.FilesystemCommandParser;
import org.apache.iotdb.cli.fs.command.WriteOptions;
import org.apache.iotdb.cli.fs.provider.FilesystemMutationProvider;
import org.apache.iotdb.cli.fs.provider.FilesystemSchemaProvider;
import org.apache.iotdb.cli.type.ExitType;
import org.apache.iotdb.cli.utils.CliContext;

import org.jline.reader.Candidate;
import org.jline.reader.LineReader;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;
import org.jline.utils.NonBlocking;
import org.jline.utils.NonBlockingReader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/** Public command contract shared with the C++ TsFile-Cli write command. */
public class FilesystemWriteCommandTest {
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final String VALID_WRITE =
      "write --table sensors --tag site STRING --field temperature DOUBLE --stdin -o out.tsfile";

  @Test
  public void parsesExplicitSchemaAndLocalFiles() {
    FilesystemCommand command =
        FilesystemCommandParser.parse(
            "write --table Sensors --tag Site STRING --tag Rack STRING "
                + "--field Temperature DOUBLE --field Status BOOLEAN "
                + "--encoding DOUBLE GORILLA --compression DOUBLE ZSTD "
                + "-i 'input data.csv' -o 'output data.tsfile' --verbose");
    assertEquals(FilesystemCommand.Type.WRITE, command.getType());
    WriteOptions options = command.getWriteOptions();
    assertEquals("sensors", options.getTable());
    assertEquals("input data.csv", options.getInput());
    assertEquals("output data.tsfile", options.getOutput());
    assertFalse(options.isStdin());
    assertTrue(options.isVerbose());
    assertEquals(4, options.getColumns().size());
    assertEquals("site", options.getColumns().get(0).getName());
    assertEquals("TAG", options.getColumns().get(0).getCategory());
    assertEquals("temperature", options.getColumns().get(2).getName());
    assertEquals("DOUBLE", options.getColumns().get(2).getType());
    assertEquals("FIELD", options.getColumns().get(2).getCategory());
    assertEquals("GORILLA", options.getEncodings().get("DOUBLE"));
    assertEquals("ZSTD", options.getCompressions().get("DOUBLE"));
  }

  @Test
  public void parsesAliasesAndStandardInputWithoutTags() {
    FilesystemCommand command =
        FilesystemCommandParser.parse(
            "write -t sensors --field temperature DOUBLE --input - --output out.tsfile -v");
    assertEquals(FilesystemCommand.Type.WRITE, command.getType());
    assertTrue(command.getWriteOptions().isStdin());
    assertTrue(command.getWriteOptions().isVerbose());
    assertEquals(1, command.getWriteOptions().getColumns().size());

    WriteOptions defaults = FilesystemCommandParser.parse(VALID_WRITE).getWriteOptions();
    assertTrue(defaults.isStdin());
    assertFalse(defaults.isVerbose());
    assertTrue(defaults.getEncodings().isEmpty());
    assertTrue(defaults.getCompressions().isEmpty());
  }

  @Test
  public void acceptsEachCanonicalFieldType() {
    for (String type :
        Arrays.asList(
            "BOOLEAN",
            "INT32",
            "INT64",
            "FLOAT",
            "DOUBLE",
            "STRING",
            "TEXT",
            "TIMESTAMP",
            "DATE",
            "BLOB")) {
      FilesystemCommand command =
          FilesystemCommandParser.parse(
              "write --table t --field value " + type + " --stdin -o out.tsfile");
      assertEquals(type, FilesystemCommand.Type.WRITE, command.getType());
      assertEquals(type, command.getWriteOptions().getColumns().get(0).getType());
    }
  }

  @Test
  public void requiresTableFieldInputAndOutput() {
    assertInvalid(
        "write",
        "write --field value INT64 --stdin -o out.tsfile",
        "write --table t --stdin -o out.tsfile",
        "write --table t --tag site STRING --stdin -o out.tsfile",
        "write --table t --field value INT64 -o out.tsfile",
        "write --table t --field value INT64 --stdin");
  }

  @Test
  public void rejectsRepeatedSingletonOptionsAndInputConflicts() {
    assertInvalid(
        VALID_WRITE + " --table other",
        VALID_WRITE + " -t other",
        VALID_WRITE + " -o other.tsfile",
        VALID_WRITE + " --output other.tsfile",
        VALID_WRITE + " --stdin",
        VALID_WRITE + " -i input.csv",
        "write --table t --field value INT64 -i a.csv --input b.csv -o out.tsfile");
  }

  @Test
  public void rejectsIncompleteSchemaAndPhysicalOptions() {
    assertInvalid(
        "write --table",
        "write --tag site",
        "write --field value",
        "write --encoding DOUBLE",
        "write --compression DOUBLE",
        "write --input",
        "write --output");
  }

  @Test
  public void rejectsHelpMixedWithWriteArguments() {
    assertInvalid(
        VALID_WRITE + " --help",
        "write --table --help --field value INT64 --stdin -o out.tsfile",
        "write --table t --field value INT64 --stdin -o --help");
  }

  @Test
  public void rejectsReadOptionsAndObsoleteWriteSyntax() {
    for (String option :
        Arrays.asList(
            "--format csv",
            "-f csv",
            "-m temperature",
            "-d root.d1",
            "-n 10",
            "--offset 1",
            "--start 0",
            "--end 1",
            "--tag-filter site eq north",
            "--tag-match all",
            "--no-header",
            "--header-match",
            "--columns value:INT64",
            "--force",
            "--model table",
            "input.csv")) {
      assertInvalid(VALID_WRITE + " " + option);
    }
  }

  @Test
  public void rejectsInvalidColumnDeclarationsAndReservedNames() {
    assertInvalid(
        "write --table t --field v int64 --stdin -o out.tsfile",
        "write --table t --field v UNKNOWN --stdin -o out.tsfile",
        "write --table t --tag site INT64 --field v INT64 --stdin -o out.tsfile",
        "write --table t --field v INT64 --field V DOUBLE --stdin -o out.tsfile",
        "write --table t --tag id STRING --field ID STRING --stdin -o out.tsfile",
        "write --table t --field time INT64 --stdin -o out.tsfile",
        "write --table t --field '' INT64 --stdin -o out.tsfile",
        "write --table '' --field v INT64 --stdin -o out.tsfile",
        "write --table 'bad\nname' --field v INT64 --stdin -o out.tsfile",
        "write --table t --field 'bad\uFEFFname' INT64 --stdin -o out.tsfile");
  }

  @Test
  public void validatesPhysicalOverridesAgainstDeclaredTypes() {
    assertInvalid(
        VALID_WRITE + " --encoding INT32 RLE",
        VALID_WRITE + " --encoding double GORILLA",
        VALID_WRITE + " --encoding DOUBLE RLE",
        VALID_WRITE + " --encoding DOUBLE UNKNOWN",
        VALID_WRITE + " --compression DOUBLE UNKNOWN",
        VALID_WRITE + " --compression DOUBLE zstd",
        VALID_WRITE + " --encoding DOUBLE PLAIN --encoding DOUBLE GORILLA",
        VALID_WRITE + " --compression DOUBLE GZIP --compression DOUBLE ZSTD");
  }

  @Test
  public void helpIsAvailableBeforeConnection() {
    for (String input : Arrays.asList("write --help", "help write")) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ByteArrayOutputStream err = new ByteArrayOutputStream();
      CliContext context = context("", out, err);
      Integer status = FilesystemShell.runOffline(context, input);
      assertNotNull(status);
      assertEquals(FilesystemShell.SUCCESS, status.intValue());
      String help = new String(out.toByteArray(), StandardCharsets.UTF_8);
      assertTrue(help, help.contains("write --table <name>"));
      assertTrue(help, help.contains("--field <name> <type>"));
      assertTrue(help, help.contains("--encoding"));
      assertTrue(help, help.contains("--compression"));
      assertTrue(help, help.contains("--stdin"));
      assertTrue(help, help.contains("--output"));
      assertEquals("", new String(err.toByteArray(), StandardCharsets.UTF_8));
    }
  }

  @Test
  public void readOnlyModeRejectsWriteBeforeOpeningInput() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    CliContext context = context("time,value\n0,1\n", out, err);
    int status = new FilesystemShell(context, null).runNonInteractive(VALID_WRITE);
    assertEquals(FilesystemShell.RUNTIME_ERROR, status);
    assertEquals("", new String(out.toByteArray(), StandardCharsets.UTF_8));
    assertTrue(new String(err.toByteArray(), StandardCharsets.UTF_8).contains("Read-only"));
  }

  @Test
  public void invalidWriteFailsBeforeConnection() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    Integer status = FilesystemShell.runOffline(context("", out, err), "write --table t");
    assertNotNull(status);
    assertEquals(FilesystemShell.USAGE_ERROR, status.intValue());
    assertEquals("", new String(out.toByteArray(), StandardCharsets.UTF_8));
    assertTrue(err.size() > 0);
  }

  @Test
  public void interactiveWriteUsesBufferedTerminalInputWithoutClosingIt() throws Exception {
    Path output = temporaryFolder.getRoot().toPath().resolve("interactive.tsfile");
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    byte[] unrelatedInput = "this is not CSV\n".getBytes(StandardCharsets.UTF_8);
    ByteArrayInputStream rawInput = new ByteArrayInputStream(unrelatedInput);
    CliContext context =
        new CliContext(rawInput, new PrintStream(out), new PrintStream(err), ExitType.EXCEPTION);
    LineReader lineReader = mock(LineReader.class);
    Terminal terminal = mock(Terminal.class);
    NonBlockingReader terminalReader =
        spy(NonBlocking.nonBlocking("write-test", new StringReader("time,value\n1,42\n")));
    when(lineReader.getTerminal()).thenReturn(terminal);
    when(terminal.reader()).thenReturn(terminalReader);
    context.setLineReader(lineReader);
    FilesystemSchemaProvider provider = mock(FilesystemSchemaProvider.class);
    FilesystemMutationProvider mutations = mock(FilesystemMutationProvider.class);
    FilesystemShell shell = new FilesystemShell(context, provider, mutations, true);

    try {
      assertTrue(
          shell.execute("write --table sensors --field value INT64 --stdin -o '" + output + "'"));

      assertTrue(Files.isRegularFile(output));
      assertTrue(Files.size(output) > 12);
      assertEquals(unrelatedInput.length, rawInput.available());
      assertEquals("", new String(out.toByteArray(), StandardCharsets.UTF_8));
      assertEquals("", new String(err.toByteArray(), StandardCharsets.UTF_8));
      verify(terminalReader, never()).close();
      verifyZeroInteractions(provider, mutations);
    } finally {
      terminalReader.close();
    }
  }

  @Test
  public void completerIncludesWriteCommand() {
    FilesystemSchemaProvider provider = mock(FilesystemSchemaProvider.class);
    FilesystemShell shell =
        new FilesystemShell(
            context("", new ByteArrayOutputStream(), new ByteArrayOutputStream()), provider);
    List<Candidate> candidates = new ArrayList<>();

    shell.createCompleter().complete(null, new DefaultParser().parse("wr", 2), candidates);

    assertTrue(candidates.stream().anyMatch(candidate -> "write".equals(candidate.value())));
    verifyZeroInteractions(provider);
  }

  private static CliContext context(
      String input, ByteArrayOutputStream out, ByteArrayOutputStream err) {
    return new CliContext(
        new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)),
        new PrintStream(out),
        new PrintStream(err),
        ExitType.EXCEPTION);
  }

  private static void assertInvalid(String... inputs) {
    for (String input : inputs) {
      FilesystemCommand command = FilesystemCommandParser.parse(input);
      assertEquals(input, FilesystemCommand.Type.INVALID, command.getType());
      assertFalse(input, command.getErrorMessage().isEmpty());
    }
  }
}
