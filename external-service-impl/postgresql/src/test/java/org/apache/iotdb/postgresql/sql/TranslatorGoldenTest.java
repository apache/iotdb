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

package org.apache.iotdb.postgresql.sql;

import org.apache.iotdb.postgresql.protocol.PgException;
import org.apache.iotdb.postgresql.session.CursorCommand;
import org.apache.iotdb.postgresql.session.SessionCommands;

import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Golden tests: every statement of {@code corpus/<client>.sql} either translates to the table-model
 * SQL in {@code corpus/expected/<client>/<n>.sql}, is a session command, or is refused with the
 * SQLSTATE and message recorded there.
 *
 * <p>Run with {@code -Dgolden.update=true} to rewrite the expected files after a deliberate change;
 * the maintainer reviews the diff.
 */
public class TranslatorGoldenTest {

  /** The source directory (not the copy in target/), so that updates land in the sources. */
  private static final Path CORPUS = corpusDirectory();

  private static Path corpusDirectory() {
    try {
      // target/test-classes -> module directory
      Path classes =
          Paths.get(
              TranslatorGoldenTest.class
                  .getProtectionDomain()
                  .getCodeSource()
                  .getLocation()
                  .toURI());
      return classes
          .getParent()
          .getParent()
          .resolve(Paths.get("src", "test", "resources", "corpus"));
    } catch (java.net.URISyntaxException e) {
      throw new IllegalStateException(e);
    }
  }

  private static final String LICENSE =
      "--\n"
          + "-- Licensed to the Apache Software Foundation (ASF) under one\n"
          + "-- or more contributor license agreements.  See the NOTICE file\n"
          + "-- distributed with this work for additional information\n"
          + "-- regarding copyright ownership.  The ASF licenses this file\n"
          + "-- to you under the Apache License, Version 2.0 (the\n"
          + "-- \"License\"); you may not use this file except in compliance\n"
          + "-- with the License.  You may obtain a copy of the License at\n"
          + "--\n"
          + "--     http://www.apache.org/licenses/LICENSE-2.0\n"
          + "--\n"
          + "-- Unless required by applicable law or agreed to in writing,\n"
          + "-- software distributed under the License is distributed on an\n"
          + "-- \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\n"
          + "-- KIND, either express or implied.  See the License for the\n"
          + "-- specific language governing permissions and limitations\n"
          + "-- under the License.\n"
          + "--\n";

  /** The session of the recordings: database refdb (spec corpus/README.md), no search path. */
  static final TranslationContext CONTEXT =
      new TranslationContext() {
        @Override
        public List<String> searchPath() {
          return Collections.emptyList();
        }

        @Override
        public boolean tableExists(String database, String table) {
          return database.equals("refdb") && Arrays.asList("sensors", "events").contains(table);
        }

        @Override
        public String user() {
          return "admin";
        }

        @Override
        public String version() {
          return "PostgreSQL 14.0 (IoTDB), read-only adapter";
        }

        @Override
        public String currentDatabase() {
          return "refdb";
        }

        @Override
        public int backendPid() {
          return 4242;
        }
      };

  /** The statements of a corpus file, comments removed. */
  static List<String> statements(Path file) throws IOException {
    List<String> statements = new ArrayList<>();
    StringBuilder current = null;
    for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
      if (line.equals("-- next")) {
        if (current != null) {
          statements.add(current.toString().trim());
        }
        current = new StringBuilder();
      } else if (current != null && !line.startsWith("--")) {
        current.append(line).append('\n');
      }
    }
    if (current != null) {
      statements.add(current.toString().trim());
    }
    return statements;
  }

  /** What the adapter does with a statement, as written to the expected file. */
  static String outcome(String statement) {
    try {
      java.util.Optional<CursorCommand> cursor = CursorCommand.parse(statement);
      if (cursor.isPresent()) {
        if (cursor.get().kind() == CursorCommand.Kind.DECLARE) {
          return "-- cursor DECLARE "
              + cursor.get().name()
              + " FOR\n"
              + PgToTableSql.translate(cursor.get().query(), CONTEXT).sql()
              + "\n";
        }
        return "-- cursor " + cursor.get().kind() + " " + cursor.get().name() + "\n";
      }
    } catch (PgException e) {
      return "-- refused " + e.sqlState() + ": " + e.getMessage() + "\n";
    }
    if (SessionCommands.isCommand(statement)) {
      return "-- session command\n";
    }
    try {
      return PgToTableSql.translate(statement, CONTEXT).sql() + "\n";
    } catch (PgException e) {
      return "-- refused " + e.sqlState() + ": " + e.getMessage() + "\n";
    }
  }

  @Test
  public void theCorpusTranslatesAsRecorded() throws IOException {
    boolean update = Boolean.getBoolean("golden.update");
    List<Path> files;
    try (Stream<Path> list = Files.list(CORPUS)) {
      files = list.filter(p -> p.toString().endsWith(".sql")).sorted().collect(Collectors.toList());
    }
    assertFalse("no corpus files in " + CORPUS.toAbsolutePath(), files.isEmpty());
    List<String> mismatches = new ArrayList<>();
    int checked = 0;
    for (Path file : files) {
      String client = file.getFileName().toString().replace(".sql", "");
      Path expectedDir = CORPUS.resolve("expected").resolve(client);
      List<String> statements = statements(file);
      for (int i = 0; i < statements.size(); i++) {
        String actual = outcome(statements.get(i));
        Path expectedFile = expectedDir.resolve((i + 1) + ".sql");
        if (update) {
          Files.createDirectories(expectedDir);
          Files.write(expectedFile, (LICENSE + actual).getBytes(StandardCharsets.UTF_8));
          continue;
        }
        assertTrue(
            "missing " + expectedFile + "; run with -Dgolden.update=true",
            Files.exists(expectedFile));
        String expected =
            new String(Files.readAllBytes(expectedFile), StandardCharsets.UTF_8)
                .substring(LICENSE.length());
        checked++;
        if (!expected.equals(actual)) {
          mismatches.add(
              client + "/" + (i + 1) + "\n  expected: " + expected + "  actual:   " + actual);
        }
      }
    }
    assertEquals(String.join("\n", mismatches), Collections.emptyList(), mismatches);
    if (!update) {
      assertTrue(checked > 0);
    }
  }
}
