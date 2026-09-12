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

package org.apache.iotdb.cli.fs.write;

import org.apache.iotdb.cli.fs.FilesystemShell;
import org.apache.iotdb.cli.fs.command.FilesystemCommand;
import org.apache.iotdb.cli.fs.command.FilesystemCommandParser;
import org.apache.iotdb.cli.fs.command.WriteOptions;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.v4.DeviceTableModelReader;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TsFileWriteExecutorTest {
  @Rule public TemporaryFolder folder = new TemporaryFolder();

  private final ByteArrayOutputStream diagnostics = new ByteArrayOutputStream();

  @Test
  public void writesAllFieldTypesFromReorderedHeader() throws Exception {
    Path target = target();
    WriteOptions options =
        options(
            "--field b BOOLEAN --tag id STRING --field i INT32 --field l INT64 "
                + "--field f FLOAT --field d DOUBLE --field s STRING --field txt TEXT "
                + "--field ts TIMESTAMP --field day DATE --field blob BLOB --stdin",
            target);
    String csv =
        "d,time,id,b,i,l,f,s,txt,ts,day,blob\n"
            + "2.5,0,dev,true,42,9223372036854775807,1.25,hello,world,1700000000000,2024-02-29,0xCAFE\n";
    assertEquals(message(), FilesystemShell.SUCCESS, run(options, csv));
    assertEquals("", message());
    try (DeviceTableModelReader reader = new DeviceTableModelReader(target.toFile());
        ResultSet rows =
            reader.query(
                "t",
                Arrays.asList("id", "b", "i", "l", "f", "d", "s", "txt", "ts", "day", "blob"),
                Long.MIN_VALUE,
                Long.MAX_VALUE)) {
      assertTrue(rows.next());
      assertEquals(0, rows.getLong("time"));
      assertEquals("dev", rows.getString("id"));
      assertTrue(rows.getBoolean("b"));
      assertEquals(42, rows.getInt("i"));
      assertEquals(Long.MAX_VALUE, rows.getLong("l"));
      assertEquals(1.25f, rows.getFloat("f"), 0);
      assertEquals(2.5, rows.getDouble("d"), 0);
      assertEquals("hello", rows.getString("s"));
      assertEquals("world", rows.getString("txt"));
      assertEquals(1700000000000L, rows.getLong("ts"));
      assertEquals(LocalDate.of(2024, 2, 29), rows.getDate("day"));
      assertArrayEquals("0xCAFE".getBytes(StandardCharsets.UTF_8), rows.getBinary("blob"));
      assertFalse(rows.next());
    }
    assertOnly(target);
  }

  @Test
  public void preservesQuotedEmptyTextAndNullValues() throws Exception {
    Path target = target();
    WriteOptions options =
        options("--tag id STRING --field note STRING --field value INT64 --stdin", target);
    assertEquals(
        message(),
        FilesystemShell.SUCCESS,
        run(
            options,
            "time,id,note,value\n0,dev,\"\",1\n1,dev,,2\n2,dev,\"line1\nline2\",\\N\n3,dev,\"\\N\",3\n4,dev,\\N,4\n"));
    try (DeviceTableModelReader reader = new DeviceTableModelReader(target.toFile());
        ResultSet rows = reader.query("t", Arrays.asList("id", "note", "value"), 0, 5)) {
      assertTrue(rows.next());
      assertEquals("", rows.getString("note"));
      assertFalse(rows.isNull("note"));
      assertEquals(1, rows.getLong("value"));
      assertTrue(rows.next());
      assertEquals("", rows.getString("note"));
      assertFalse(rows.isNull("note"));
      assertEquals(2, rows.getLong("value"));
      assertTrue(rows.next());
      assertEquals("line1\nline2", rows.getString("note"));
      assertTrue(rows.isNull("value"));
      assertTrue(rows.next());
      assertEquals("\\N", rows.getString("note"));
      assertFalse(rows.isNull("note"));
      assertTrue(rows.next());
      assertTrue(rows.isNull("note"));
      assertFalse(rows.next());
    }
  }

  @Test
  public void streamsMultipleBatchesAndInterleavedDevices() throws Exception {
    Path target = target();
    StringBuilder csv = new StringBuilder("time,id,value\n");
    for (int i = 0; i < 2050; i++) {
      csv.append(i / 2)
          .append(',')
          .append(i % 2 == 0 ? "a" : "b")
          .append(',')
          .append(i)
          .append('\n');
    }
    assertEquals(
        message(),
        FilesystemShell.SUCCESS,
        run(options("--tag id STRING --field value INT64 --stdin", target), csv.toString()));
    Map<String, Long> previous = new HashMap<>();
    int count = 0;
    try (DeviceTableModelReader reader = new DeviceTableModelReader(target.toFile());
        ResultSet rows = reader.query("t", Arrays.asList("id", "value"), 0, Long.MAX_VALUE)) {
      while (rows.next()) {
        String id = rows.getString("id");
        long time = rows.getLong("time");
        if (previous.containsKey(id)) assertTrue(time > previous.get(id));
        previous.put(id, time);
        assertEquals(time * 2 + ("a".equals(id) ? 0 : 1), rows.getLong("value"));
        count++;
      }
    }
    assertEquals(2050, count);
    assertEquals(2, previous.size());
  }

  @Test
  public void readsLocalCsvAndReportsResolvedSettingsOnlyAfterSuccess() throws Exception {
    Path source = folder.getRoot().toPath().resolve("input data.csv");
    Files.write(source, "time,id,b,value\n0,dev,true,1\n".getBytes(StandardCharsets.UTF_8));
    Path target = target();
    WriteOptions options =
        options(
            "--tag id STRING --field b BOOLEAN --field value INT64 "
                + "--encoding INT64 RLE --compression INT64 GZIP -i '"
                + source
                + "' -v",
            target);
    assertEquals(message(), FilesystemShell.SUCCESS, run(options, "invalid stdin"));
    assertTrue(message(), message().contains("created model=table object=t rows=1 output="));
    assertTrue(
        message(),
        message()
            .contains("encoding=RLE source=type-override compression=GZIP source=type-override"));
    try (DeviceTableModelReader reader = new DeviceTableModelReader(target.toFile())) {
      TableSchema schema = reader.getTableSchemas("t").get();
      assertEquals(ColumnCategory.TAG, schema.getColumnTypes().get(0));
      IMeasurementSchema bool = schema.findColumnSchema("b");
      assertEquals(TSEncoding.PLAIN, bool.getEncodingType());
      assertEquals(CompressionType.LZ4, bool.getCompressor());
      IMeasurementSchema value = schema.findColumnSchema("value");
      assertEquals(TSEncoding.RLE, value.getEncodingType());
      assertEquals(CompressionType.GZIP, value.getCompressor());
    }
  }

  @Test
  public void protectsExistingTargetAndDanglingSymlink() throws Exception {
    Path target = target();
    byte[] original = "existing content".getBytes(StandardCharsets.UTF_8);
    Files.write(target, original);
    WriteOptions options = options("--field value INT64 --stdin", target);
    assertEquals(FilesystemShell.RUNTIME_ERROR, run(options, "time,value\n0,1\n"));
    assertArrayEquals(original, Files.readAllBytes(target));
    assertOnly(target);
    Files.delete(target);
    Files.createSymbolicLink(target, folder.getRoot().toPath().resolve("missing"));
    assertEquals(FilesystemShell.RUNTIME_ERROR, run(options, "time,value\n0,1\n"));
    assertTrue(Files.isSymbolicLink(target));
    assertOnly(target);
  }

  @Test
  public void discardsAllOutputOnCsvFailureAfterFullBatch() throws Exception {
    Path target = target();
    StringBuilder csv = new StringBuilder("time,value\n");
    for (int i = 0; i < 1024; i++) csv.append(i).append(',').append(i).append('\n');
    csv.append("1024,bad\n");
    assertEquals(
        FilesystemShell.INPUT_ERROR,
        run(options("--field value INT64 --stdin -v", target), csv.toString()));
    assertFalse(message().contains("created model="));
    assertFalse(Files.exists(target));
    assertDirectoryEmpty();
  }

  @Test
  public void rejectsBadHeaderInvalidUtf8AndMissingInputWithoutOutput() throws Exception {
    Path target = target();
    WriteOptions options = options("--field value INT64 --stdin", target);
    assertEquals(FilesystemShell.INPUT_ERROR, run(options, "time,other\n0,1\n"));
    assertDirectoryEmpty();
    byte[] invalid =
        new byte[] {'t', 'i', 'm', 'e', ',', 'v', 'a', 'l', 'u', 'e', '\n', (byte) 0xC0, '\n'};
    assertEquals(
        FilesystemShell.INPUT_ERROR,
        new TsFileWriteExecutor()
            .run(options, new ByteArrayInputStream(invalid), new PrintStream(diagnostics)));
    assertDirectoryEmpty();
    assertEquals(
        FilesystemShell.INPUT_ERROR,
        run(options("--field value INT64 -i '" + target + "'", target), ""));
    assertDirectoryEmpty();
  }

  @Test
  public void publishesEmptyTableFromHeaderOnlyInput() throws Exception {
    Path target = target();
    assertEquals(
        message(),
        FilesystemShell.SUCCESS,
        run(options("--field value INT64 --stdin", target), "time,value\n"));
    try (DeviceTableModelReader reader = new DeviceTableModelReader(target.toFile())) {
      assertTrue(reader.getTableSchemas("t").isPresent());
    }
    assertOnly(target);
  }

  @Test
  public void doesNotCloseCallerInputStreamOrReader() throws Exception {
    final boolean[] closed = {false};
    InputStream input =
        new ByteArrayInputStream("time,value\n0,1\n".getBytes(StandardCharsets.UTF_8)) {
          @Override
          public void close() {
            closed[0] = true;
          }
        };
    Path target = target();
    WriteOptions options = options("--field value INT64 --stdin", target);
    assertEquals(
        FilesystemShell.SUCCESS,
        new TsFileWriteExecutor().run(options, input, new PrintStream(diagnostics)));
    assertFalse(closed[0]);
    Files.delete(target);
    StringReader reader =
        new StringReader("time,value\n0,1\n") {
          @Override
          public void close() {
            closed[0] = true;
          }
        };
    assertEquals(
        FilesystemShell.SUCCESS,
        new TsFileWriteExecutor().run(options, reader, new PrintStream(diagnostics)));
    assertFalse(closed[0]);
  }

  @Test
  public void unavailableCompressionFailsBeforeCreatingOutput() throws Exception {
    Path target = target();
    assertEquals(
        FilesystemShell.USAGE_ERROR,
        run(
            options("--field value INT64 --compression INT64 LZO --stdin", target),
            "time,value\n0,1\n"));
    assertTrue(message(), message().contains("LZO"));
    assertDirectoryEmpty();
  }

  @Test
  public void unavailableBlobEncodingRejectsEvenHeaderOnlyInput() throws Exception {
    Path target = target();
    assertEquals(
        FilesystemShell.USAGE_ERROR,
        run(
            options("--field value BLOB --encoding BLOB DICTIONARY --stdin", target),
            "time,value\n"));
    assertTrue(message(), message().contains("DICTIONARY"));
    assertDirectoryEmpty();
  }

  @Test
  public void concurrentTargetCreationIsNeverOverwritten() throws Exception {
    Path target = target();
    byte[] existing = "created concurrently".getBytes(StandardCharsets.UTF_8);
    StringReader input =
        new StringReader("time,value\n0,1\n") {
          private boolean created;

          @Override
          public int read(char[] buffer, int offset, int length) throws IOException {
            int count = super.read(buffer, offset, length);
            if (count < 0 && !created) {
              Files.write(target, existing);
              created = true;
            }
            return count;
          }
        };
    assertEquals(
        FilesystemShell.RUNTIME_ERROR,
        new TsFileWriteExecutor()
            .run(
                options("--field value INT64 --stdin", target),
                input,
                new PrintStream(diagnostics)));
    assertArrayEquals(existing, Files.readAllBytes(target));
    assertOnly(target);
  }

  @Test
  public void rejectsIdentifiersThatTheWriterWouldRenameBeforeReadingInput() throws Exception {
    Path target = target();
    InputStream input =
        new InputStream() {
          @Override
          public int read() {
            throw new AssertionError("Input must not be read for an unsupported identifier");
          }
        };
    for (String command :
        Arrays.asList(
            "write --table '\u00C4' --field value INT64",
            "write --table t --field '\u00C4' INT64")) {
      FilesystemCommand parsed =
          FilesystemCommandParser.parse(command + " --stdin -o '" + target + "'");
      assertEquals(parsed.getErrorMessage(), FilesystemCommand.Type.WRITE, parsed.getType());
      diagnostics.reset();
      assertEquals(
          FilesystemShell.USAGE_ERROR,
          new TsFileWriteExecutor()
              .run(parsed.getWriteOptions(), input, new PrintStream(diagnostics)));
      assertTrue(message(), message().contains("\u00C4"));
      assertTrue(message(), message().contains("\u00E4"));
      assertDirectoryEmpty();
    }
  }

  @Test
  public void preservesChineseTableAndColumnNames() throws Exception {
    Path target = target();
    FilesystemCommand command =
        FilesystemCommandParser.parse(
            "write --table '\u4F20\u611F\u5668' --field '\u6E29\u5EA6' INT64 --stdin -o '"
                + target
                + "'");
    assertEquals(command.getErrorMessage(), FilesystemCommand.Type.WRITE, command.getType());
    assertEquals(
        FilesystemShell.SUCCESS, run(command.getWriteOptions(), "time,\u6E29\u5EA6\n0,25\n"));
    try (DeviceTableModelReader reader = new DeviceTableModelReader(target.toFile());
        ResultSet rows = reader.query("\u4F20\u611F\u5668", Arrays.asList("\u6E29\u5EA6"), 0, 1)) {
      assertTrue(rows.next());
      assertEquals(25, rows.getLong("\u6E29\u5EA6"));
      assertFalse(rows.next());
    }
    assertOnly(target);
  }

  @Test
  public void verboseOutputFailurePreservesTheCommittedFile() throws Exception {
    Path target = target();
    PrintStream broken =
        new PrintStream(
            new OutputStream() {
              @Override
              public void write(int value) throws IOException {
                throw new IOException("broken diagnostic stream");
              }
            });
    assertEquals(
        FilesystemShell.RUNTIME_ERROR,
        new TsFileWriteExecutor()
            .run(
                options("--field value INT64 --stdin -v", target),
                new ByteArrayInputStream("time,value\n0,1\n".getBytes(StandardCharsets.UTF_8)),
                broken));
    try (DeviceTableModelReader reader = new DeviceTableModelReader(target.toFile());
        ResultSet rows = reader.query("t", Arrays.asList("value"), 0, 1)) {
      assertTrue(rows.next());
      assertEquals(1, rows.getLong("value"));
      assertFalse(rows.next());
    }
    assertOnly(target);
  }

  private WriteOptions options(String arguments, Path target) {
    FilesystemCommand command =
        FilesystemCommandParser.parse("write --table t " + arguments + " -o '" + target + "'");
    assertEquals(command.getErrorMessage(), FilesystemCommand.Type.WRITE, command.getType());
    return command.getWriteOptions();
  }

  private int run(WriteOptions options, String csv) {
    return new TsFileWriteExecutor()
        .run(
            options,
            new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)),
            new PrintStream(diagnostics));
  }

  private Path target() {
    return folder.getRoot().toPath().resolve("out.tsfile");
  }

  private String message() {
    return new String(diagnostics.toByteArray(), StandardCharsets.UTF_8);
  }

  private void assertDirectoryEmpty() throws IOException {
    try (Stream<Path> entries = Files.list(folder.getRoot().toPath())) {
      assertEquals(0, entries.count());
    }
  }

  private void assertOnly(Path target) throws IOException {
    try (Stream<Path> entries = Files.list(folder.getRoot().toPath())) {
      assertArrayEquals(new Path[] {target}, entries.toArray(Path[]::new));
    }
  }
}
