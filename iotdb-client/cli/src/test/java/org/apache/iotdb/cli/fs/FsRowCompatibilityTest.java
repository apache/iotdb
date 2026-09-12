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

import org.apache.iotdb.cli.fs.node.FsColumn;
import org.apache.iotdb.cli.fs.provider.TableFilesystemSchemaProvider;
import org.apache.iotdb.cli.fs.sql.SqlExecutor;
import org.apache.iotdb.cli.fs.sql.SqlRow;
import org.apache.iotdb.cli.type.ExitType;
import org.apache.iotdb.cli.utils.CliContext;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FsRowCompatibilityTest {
  @Rule public TemporaryFolder temporary = new TemporaryFolder();
  private final List<SqlRow> data = new ArrayList<>();
  private final ByteArrayOutputStream output = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errors = new ByteArrayOutputStream();
  private FilesystemShell shell;

  @Before
  public void setUp() {
    for (int i = 0; i < 30; i++)
      data.add(
          SqlRow.of(
              "time",
              Integer.toString(i),
              "site",
              "north",
              "label",
              "123",
              "value",
              Integer.toString(i)));
    SqlExecutor executor =
        new SqlExecutor() {
          @Override
          public List<SqlRow> query(String sql) {
            if (sql.startsWith("SHOW TABLES FROM"))
              return SqlRow.list(SqlRow.of("TableName", "sensors"));
            if (sql.startsWith("DESC "))
              return SqlRow.list(
                  SqlRow.of("ColumnName", "time", "DataType", "TIMESTAMP", "Category", "TIME"),
                  SqlRow.of("ColumnName", "site", "DataType", "STRING", "Category", "TAG"),
                  SqlRow.of("ColumnName", "label", "DataType", "STRING", "Category", "FIELD"),
                  SqlRow.of("ColumnName", "value", "DataType", "INT64", "Category", "FIELD"));
            if (sql.startsWith("SELECT * FROM")) {
              List<SqlRow> rows = new ArrayList<>(data);
              if (sql.contains("time DESC")) Collections.reverse(rows);
              int position = sql.lastIndexOf(" LIMIT ");
              if (position >= 0)
                rows =
                    new ArrayList<>(
                        rows.subList(
                            0,
                            Math.min(rows.size(), Integer.parseInt(sql.substring(position + 7)))));
              return rows;
            }
            throw new AssertionError(sql);
          }

          @Override
          public void execute(String sql) {
            throw new AssertionError(sql);
          }
        };
    shell =
        new FilesystemShell(
            new CliContext(
                new ByteArrayInputStream(new byte[0]),
                new PrintStream(output),
                new PrintStream(errors),
                ExitType.EXCEPTION),
            new TableFilesystemSchemaProvider(executor));
  }

  private String run(String command) {
    output.reset();
    errors.reset();
    assertEquals(errors.toString(), 0, shell.runNonInteractive(command));
    return new String(output.toByteArray(), StandardCharsets.UTF_8);
  }

  @Test
  public void catCsvReadsAllThirtyDataRowsAndHeader() {
    String text = run("cat -f csv /db/sensors.csv");
    assertEquals(31, text.split("\n").length);
    assertTrue(text.endsWith("29,north,123,29\n"));
    assertEquals(text, run("cat -f csv /db/sensors"));
  }

  @Test
  public void headCsvAppliesFilterOffsetProjectionAndFormatOnActualProvider() {
    assertEquals(
        "{\"time\":\"11\",\"site\":\"north\",\"value\":\"11\"}\n{\"time\":\"12\",\"site\":\"north\",\"value\":\"12\"}\n",
        run(
            "head -n 2 --start 10 --end 20 --offset 1 -m value -f ndjson --tag-filter site eq north /db/sensors.csv"));
  }

  @Test
  public void headCountsDataRowsAndKeepsAllTags() {
    assertEquals(
        "time,site,value\n0,north,0\n1,north,1\n",
        run("head -n 2 -m value -f csv /db/sensors.csv"));
  }

  @Test
  public void scopeSelectsTableAndRejectsWrongModel() {
    assertTrue(run("head -t sensors -n 1 -f csv /db").contains("0,north"));
    assertEquals(1, shell.runNonInteractive("head -t other /db/sensors.csv"));
    assertEquals(1, shell.runNonInteractive("head -d root.db.device /db/sensors.csv"));
  }

  @Test
  public void invalidColumnsAndRegexFailEvenForEmptyInput() {
    data.clear();
    assertEquals(1, shell.runNonInteractive("head -m missing /db/sensors.csv"));
    assertEquals(1, shell.runNonInteractive("head -m site /db/sensors.csv"));
    assertEquals(1, shell.runNonInteractive("head --tag-filter value eq 1 /db/sensors.csv"));
    assertEquals(1, shell.runNonInteractive("head --tag-filter site regexp '[' /db/sensors.csv"));
  }

  @Test
  public void emptyRowsStillPrintSchemaHeader() {
    data.clear();
    assertEquals("time,site,label,value\n", run("cat -f csv /db/sensors.csv"));
    assertTrue(run("head /db/sensors.csv").startsWith("time"));
    assertEquals("", run("head -f ndjson /db/sensors.csv"));
  }

  @Test
  public void jsonUsesSchemaAndPreservesInt64Precision() {
    data.clear();
    data.add(
        SqlRow.of("time", "1", "site", "true", "label", "123", "value", "9223372036854775807"));
    assertEquals(
        "{\"time\":\"1\",\"site\":\"true\",\"label\":\"123\",\"value\":\"9223372036854775807\"}\n",
        run("cat -f ndjson /db/sensors.csv"));
  }

  @Test
  public void csvDistinguishesNullEmptyAndLiteralNullToken() {
    data.clear();
    data.add(SqlRow.of("time", "1", "site", "", "label", "\\N", "value", null));
    assertEquals("time,site,label,value\n1,\"\",\"\\N\",\\N\n", run("cat -f csv /db/sensors.csv"));
  }

  @Test
  public void neqDoesNotMatchNullTags() {
    data.clear();
    data.add(SqlRow.of("time", "1", "site", null, "label", "x", "value", "1"));
    assertEquals("", run("cat -f ndjson --tag-filter site neq south /db/sensors.csv"));
  }

  @Test
  public void statisticsUseEachRowsType() {
    output.reset();
    List<FsColumn> columns = Arrays.asList(new FsColumn("min", "FIELD", "INT32"));
    List<SqlRow> rows =
        Arrays.asList(
            new SqlRow(
                Collections.singletonMap("min", "1"), Collections.singletonMap("min", "INT32")),
            new SqlRow(
                Collections.singletonMap("min", "2"), Collections.singletonMap("min", "STRING")));
    FsRowRenderer.print(new PrintStream(output), columns, rows, "ndjson");
    assertEquals("{\"min\":1}\n{\"min\":\"2\"}\n", output.toString());
  }

  @Test
  public void exportMatchesCatAndProtectsExistingOutput() throws Exception {
    Path target = temporary.getRoot().toPath().resolve("rows.csv");
    String expected = run("cat -f csv -m value --start 25 /db/sensors.csv");
    assertEquals(
        "", run("export -t sensors --type csv -m value --start 25 -o '" + target + "' /db"));
    assertEquals(expected, new String(Files.readAllBytes(target), StandardCharsets.UTF_8));
    assertEquals(
        3, shell.runNonInteractive("export -t sensors --type csv -o '" + target + "' /db"));
    assertEquals(expected, new String(Files.readAllBytes(target), StandardCharsets.UTF_8));
  }

  @Test
  public void exportDirectoryIncludesCompletionManifest() throws Exception {
    Path target = temporary.getRoot().toPath().resolve("export");
    run("export -t sensors --type ndjson --output-dir '" + target + "' /db");
    String manifest =
        new String(Files.readAllBytes(target.resolve("_manifest.json")), StandardCharsets.UTF_8);
    assertTrue(manifest.contains("\"complete\": true"));
    assertTrue(manifest.contains("\"rows\":\"30\""));
    assertTrue(Files.exists(target.resolve("0001.ndjson")));
  }

  @Test
  public void pipelineReadsAllMatchingRows() {
    assertEquals("128\n", run("cat -f csv /db/sensors.csv | grep '^2[0-7],' | wc -c"));
    assertEquals(
        "20,north,123,20\n21,north,123,21\n", run("cat -f csv /db/sensors.csv | grep '^2[01],'"));
  }

  @Test
  public void countAndSchemaAllowTagSelectionButStatsRequiresFields() {
    assertTrue(run("count -m site -f csv /db/sensors.csv").contains("site,TAG,30,1,30,0"));
    assertTrue(run("schema -m site -f csv /db/sensors.csv").contains("site,TAG,STRING"));
    assertEquals(1, shell.runNonInteractive("stats -m site /db/sensors.csv"));
    assertTrue(run("stats -m value -f csv /db/sensors.csv").contains("value,INT64"));
  }

  @Test
  public void grepInvalidRegexAndNoMatchHaveUnixStatuses() {
    assertEquals(2, shell.runNonInteractive("grep '[' /db/sensors.csv"));
    assertEquals(1, shell.runNonInteractive("grep missing /db/sensors.csv"));
    assertEquals(0, shell.runNonInteractive("grep north /db/sensors.csv"));
  }

  @Test
  public void tailRejectsConflictingModesAndHonorsTableSelector() {
    assertEquals(1, shell.runNonInteractive("tail -f --format csv /db/sensors.csv"));
    assertEquals(1, shell.runNonInteractive("tail -c 3 -m value /db/sensors.csv"));
    assertEquals(1, shell.runNonInteractive("tail -n +2 --format csv /db/sensors.csv"));
    assertEquals(1, shell.runNonInteractive("tail -d root.db.device /db/sensors.csv"));
    assertTrue(run("tail -n 1 -t sensors --format csv /db").endsWith("29,north,123,29\n"));
  }

  @Test
  public void localInputRedirectionFeedsPipeline() throws Exception {
    Path input = temporary.newFile("stdin.txt").toPath();
    Files.write(input, "first\nsecond\n".getBytes(StandardCharsets.UTF_8));
    assertEquals("first\n", run("head -n 1 < '" + input + "'"));
  }
}
