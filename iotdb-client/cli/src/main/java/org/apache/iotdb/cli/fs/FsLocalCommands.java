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
import org.apache.iotdb.cli.fs.command.ReadOptions;
import org.apache.iotdb.cli.fs.path.FsPath;
import org.apache.iotdb.cli.fs.provider.FilesystemSchemaProvider;
import org.apache.iotdb.cli.i18n.CliMessages;
import org.apache.iotdb.cli.i18n.FsLocalMessages;

import org.apache.tsfile.exception.TsFileRuntimeException;
import org.apache.tsfile.read.TsFileSequenceReader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** Local output commands follow TsFile CLI's target and manifest contract. */
public final class FsLocalCommands {
  private final PrintStream out;
  private final FilesystemSchemaProvider provider;

  public FsLocalCommands(PrintStream out, FilesystemSchemaProvider provider) {
    this.out = out;
    this.provider = provider;
  }

  public void sketch(FilesystemCommand command) throws SQLException {
    Path source = Paths.get(command.getPath()).toAbsolutePath();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(source.toString())) {
      if (!reader.isComplete()) {
        throw new IOException(String.format(FsLocalMessages.INCOMPLETE_TSFILE, source));
      }
      String model = reader.getTableSchemaMap().isEmpty() ? "tree" : "table";
      String content =
          "-------------------------------- TsFile Sketch --------------------------------\n"
              + "file path: "
              + command.getPath()
              + "\nmodel: "
              + model
              + "\n"
              + "---------------------------------- TsFile Sketch End ----------------------------------\n";
      if (!command.hasOption("-o")) out.print(content);
      else {
        Path target = Paths.get(command.optionValue("-o", "")).toAbsolutePath();
        if (target.equals(source) || (Files.exists(target) && Files.isSameFile(target, source))) {
          throw new IOException(String.format(CliMessages.FS_SAME_FILE, source));
        }
        publish(target, content.getBytes(StandardCharsets.UTF_8), command.hasOption("--force"));
      }
    } catch (IOException | TsFileRuntimeException e) {
      throw new SQLException(
          String.format(FsLocalMessages.LOCAL_IO_FAILED, "sketch", source, e.getMessage()), e);
    }
  }

  public void export(FilesystemCommand command, FsPath currentPath) throws SQLException {
    List<String> objects =
        new ArrayList<>(command.getScopeValues(command.getTable().isEmpty() ? "-d" : "-t"));
    if (objects.isEmpty())
      objects.add(command.getTable().isEmpty() ? command.getDevice() : command.getTable());
    boolean directory = command.hasOption("--output-dir");
    if (!directory && objects.size() != 1)
      throw new IllegalArgumentException(CliMessages.FS_EXPORT_TARGET);
    List<String> manifest = new ArrayList<>();
    Path target =
        Paths.get(command.optionValue(directory ? "--output-dir" : "-o", "")).toAbsolutePath();
    try {
      if (directory) {
        Files.createDirectory(target);
        manifest(target, false, manifest);
      }
      for (int i = 0; i < objects.size(); i++) {
        String object = objects.get(i);
        ReadOptions old = command.getReadOptions();
        ReadOptions options =
            new ReadOptions(
                old.getFormat(),
                command.getTable().isEmpty() ? object : "",
                command.getTable().isEmpty() ? "" : object,
                old.getColumns(),
                old.getLimit(),
                old.getOffset(),
                old.getStart(),
                old.getEnd(),
                old.getTagFilters(),
                old.getTagMatch());
        FsRowReader.Result result =
            new FsRowReader(provider).read(currentPath.resolve(command.getPath()), options, false);
        String extension = "table".equals(options.getFormat()) ? "txt" : options.getFormat();
        String name = String.format(Locale.ROOT, "%04d.%s", i + 1, extension);
        Path output = directory ? target.resolve(name) : target;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        FsRowRenderer.print(
            new PrintStream(bytes, false, "UTF-8"),
            result.getColumns(),
            result.getRows(),
            options.getFormat());
        publish(output, bytes.toByteArray(), command.hasOption("--force"));
        if (directory) {
          manifest.add(
              "    {\"file\":"
                  + FsRowRenderer.jsonString(name)
                  + ",\"model\":"
                  + FsRowRenderer.jsonString(provider.model())
                  + ",\"object\":"
                  + FsRowRenderer.jsonString(object)
                  + ",\"type\":"
                  + FsRowRenderer.jsonString(options.getFormat())
                  + ",\"rows\":"
                  + FsRowRenderer.jsonString(Integer.toString(result.getRows().size()))
                  + "}");
          manifest(target, false, manifest);
        }
      }
      if (directory) manifest(target, true, manifest);
    } catch (IOException e) {
      throw new SQLException(
          String.format(FsLocalMessages.LOCAL_IO_FAILED, "export", target, e.getMessage()), e);
    }
  }

  private static void manifest(Path directory, boolean complete, List<String> entries)
      throws IOException {
    String json =
        "{\n  \"complete\": "
            + complete
            + ",\n  \"files\": [\n"
            + String.join(",\n", entries)
            + "\n  ]\n}\n";
    publish(directory.resolve("_manifest.json"), json.getBytes(StandardCharsets.UTF_8), true);
  }

  static void publish(Path target, byte[] content, boolean replace) throws IOException {
    if (!replace && Files.exists(target, LinkOption.NOFOLLOW_LINKS))
      throw new IOException(String.format(CliMessages.FS_FILE_EXISTS, target));
    Path temporary = Files.createTempFile(target.getParent(), ".iotdb-export-", ".tmp");
    try {
      Files.write(temporary, content);
      if (replace)
        Files.move(
            temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      else Files.createLink(target, temporary);
    } finally {
      Files.deleteIfExists(temporary);
    }
  }
}
