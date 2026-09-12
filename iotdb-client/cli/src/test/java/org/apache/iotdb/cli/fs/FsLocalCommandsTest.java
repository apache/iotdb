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
import org.apache.iotdb.cli.fs.node.FsColumn;
import org.apache.iotdb.cli.fs.path.FsPath;
import org.apache.iotdb.cli.fs.provider.FilesystemSchemaProvider;
import org.apache.iotdb.cli.fs.sql.SqlRow;
import org.apache.iotdb.cli.fs.write.TsFileWriteExecutor;

import org.apache.tsfile.write.TsFileWriter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FsLocalCommandsTest {
  @Rule public TemporaryFolder folder = new TemporaryFolder();
  private final ByteArrayOutputStream output = new ByteArrayOutputStream();
  private final FilesystemSchemaProvider provider = mock(FilesystemSchemaProvider.class);
  private final FsLocalCommands local = new FsLocalCommands(new PrintStream(output), provider);

  @Test
  public void sketchMatchesTsFileMarkersAndModelFromActualFile() throws Exception {
    Path table = createTable();
    local.sketch(parse("sketch '" + table + "'"));
    assertEquals(sketch(table, "table"), output.toString("UTF-8"));
    output.reset();
    Path tree = folder.getRoot().toPath().resolve("tree.tsfile");
    try (TsFileWriter ignored = new TsFileWriter(tree.toFile())) {
      // A schema-only tree file still carries the tree model.
    }
    local.sketch(parse("sketch '" + tree + "'"));
    assertEquals(sketch(tree, "tree"), output.toString("UTF-8"));
  }

  @Test
  public void sketchNeverOverwritesItsSourceOrAnExistingTargetWithoutForce() throws Exception {
    Path source = createTable();
    byte[] original = Files.readAllBytes(source);
    expectSqlFailure(
        () -> local.sketch(parse("sketch '" + source + "' -o '" + source + "' --force")));
    assertArrayEquals(original, Files.readAllBytes(source));
    Path alias = folder.getRoot().toPath().resolve("alias.tsfile");
    Files.createLink(alias, source);
    expectSqlFailure(
        () -> local.sketch(parse("sketch '" + source + "' -o '" + alias + "' --force")));
    assertArrayEquals(original, Files.readAllBytes(source));
    Path target = folder.newFile("sketch.txt").toPath();
    Files.write(target, "existing".getBytes(StandardCharsets.UTF_8));
    expectSqlFailure(() -> local.sketch(parse("sketch '" + source + "' -o '" + target + "'")));
    assertEquals("existing", Files.readString(target));
    local.sketch(parse("sketch '" + source + "' -o '" + target + "' --force"));
    assertEquals(sketch(source, "table"), Files.readString(target));
  }

  @Test
  public void missingAndCorruptSketchInputsLeaveNoOutput() throws Exception {
    Path target = folder.getRoot().toPath().resolve("sketch.txt");
    Path missing = folder.getRoot().toPath().resolve("missing.tsfile");
    expectSqlFailure(() -> local.sketch(parse("sketch '" + missing + "' -o '" + target + "'")));
    Path corrupt = folder.newFile("corrupt.tsfile").toPath();
    Files.write(corrupt, "invalid".getBytes(StandardCharsets.UTF_8));
    expectSqlFailure(() -> local.sketch(parse("sketch '" + corrupt + "' -o '" + target + "'")));
    assertFalse(Files.exists(target));
    assertEquals("", output.toString("UTF-8"));
  }

  @Test
  public void exportDirectoryRecordsOnlyCompletedFilesAndRefusesExistingDirectory()
      throws Exception {
    mockTable("t1");
    when(provider.columns(FsPath.absolute("/db/t2.csv")))
        .thenThrow(new SQLException("unavailable"));
    Path target = folder.getRoot().toPath().resolve("export");
    expectSqlFailure(
        () ->
            local.export(
                parse("export --type csv -t t1 -t t2 --output-dir '" + target + "' /db"),
                FsPath.absolute("/")));
    String manifest = Files.readString(target.resolve("_manifest.json"));
    assertTrue(manifest.contains("\"complete\": false"));
    assertTrue(manifest.contains("\"object\":\"t1\""));
    assertFalse(manifest.contains("\"object\":\"t2\""));
    assertEquals("time,value\n1,9007199254740993\n", Files.readString(target.resolve("0001.csv")));
    assertFalse(Files.exists(target.resolve("0002.csv")));
    expectSqlFailure(
        () ->
            local.export(
                parse("export --type csv -t t1 --output-dir '" + target + "' /db"),
                FsPath.absolute("/")));
    assertEquals(manifest, Files.readString(target.resolve("_manifest.json")));
  }

  @Test
  public void exportSuccessManifestAndSingleFileNoClobber() throws Exception {
    mockTable("t1");
    Path directory = folder.getRoot().toPath().resolve("export");
    local.export(
        parse("export --type ndjson -t t1 --output-dir '" + directory + "' /db"),
        FsPath.absolute("/"));
    assertTrue(
        Files.readString(directory.resolve("_manifest.json")).contains("\"complete\": true"));
    assertTrue(
        Files.readString(directory.resolve("0001.ndjson"))
            .contains("\"value\":\"9007199254740993\""));
    Path target = folder.newFile("existing.csv").toPath();
    Files.writeString(target, "preserved");
    expectSqlFailure(
        () ->
            local.export(
                parse("export --type csv -t t1 -o '" + target + "' /db"), FsPath.absolute("/")));
    assertEquals("preserved", Files.readString(target));
  }

  private void mockTable(String name) throws Exception {
    when(provider.model()).thenReturn("table");
    when(provider.columns(FsPath.absolute("/db/" + name + ".csv")))
        .thenReturn(
            Arrays.asList(
                new FsColumn("time", "TIME", "TIMESTAMP"),
                new FsColumn("value", "FIELD", "INT64")));
    when(provider.read(FsPath.absolute("/db/" + name + ".csv"), -1))
        .thenReturn(SqlRow.list(SqlRow.of("time", "1", "value", "9007199254740993")));
  }

  private Path createTable() {
    Path target = folder.getRoot().toPath().resolve("table.tsfile");
    FilesystemCommand write =
        parse("write --table t --field value INT64 --stdin -o '" + target + "'");
    ByteArrayOutputStream errors = new ByteArrayOutputStream();
    assertEquals(
        errors.toString(),
        0,
        new TsFileWriteExecutor()
            .run(
                write.getWriteOptions(),
                new ByteArrayInputStream("time,value\n1,42\n".getBytes(StandardCharsets.UTF_8)),
                new PrintStream(errors)));
    return target;
  }

  private static FilesystemCommand parse(String input) {
    FilesystemCommand command = FilesystemCommandParser.parse(input);
    assertFalse(command.getErrorMessage(), command.getType() == FilesystemCommand.Type.INVALID);
    return command;
  }

  private static String sketch(Path path, String model) {
    return "-------------------------------- TsFile Sketch --------------------------------\n"
        + "file path: "
        + path
        + "\nmodel: "
        + model
        + "\n"
        + "---------------------------------- TsFile Sketch End"
        + " ----------------------------------\n";
  }

  private static void expectSqlFailure(SqlOperation operation) throws Exception {
    try {
      operation.run();
      fail("Expected SQL exception");
    } catch (SQLException expected) {
      assertTrue(expected.getMessage() != null);
    }
  }

  private interface SqlOperation {
    void run() throws Exception;
  }
}
