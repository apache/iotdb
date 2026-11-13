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

package org.apache.iotdb.tool;

import org.apache.iotdb.session.Session;
import org.apache.iotdb.tool.data.ImportData;
import org.apache.iotdb.tool.data.ImportDataTable;
import org.apache.iotdb.tool.data.ImportDataTree;
import org.apache.iotdb.tool.schema.ImportSchemaTree;

import org.junit.Test;

import java.io.File;
import java.lang.reflect.Method;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Every CSV import path opens a parser over a {@code FileInputStream}. On the early-return paths
 * (an empty file, a header that fails validation) that parser used to be abandoned without being
 * closed, so each such file leaked a file descriptor.
 *
 * <p>These tests drive the real import methods on an empty CSV many times and assert that the
 * number of open descriptors in the process does not grow with the number of calls. Without the fix
 * it grows by one per call.
 */
public class ReadCsvFileResourceTest {

  private static final int CALLS = 50;

  /**
   * Without the fix the growth is one per call. A leaked stream is only released once a GC lets the
   * JVM's Cleaner reap it, so a collection mid-loop could hide some leaks; a quarter of the
   * expected growth leaves room for that without letting a real leak through.
   */
  private static final int MAX_GROWTH = CALLS / 4;

  @Test
  public void importDataTreeClosesParserOnEarlyReturn() throws Exception {
    TreeProbe tool = new TreeProbe();
    assertNoDescriptorGrowth("ImportDataTree.importFromCsvFile", tool::importCsv);
  }

  @Test
  public void importDataTableClosesParserOnEarlyReturn() throws Exception {
    TableProbe tool = new TableProbe();
    assertNoDescriptorGrowth("ImportDataTable.importFromCsvFile", tool::importCsv);
  }

  @Test
  public void importSchemaTreeClosesParserOnEarlyReturn() throws Exception {
    SchemaProbe tool = new SchemaProbe();
    assertNoDescriptorGrowth("ImportSchemaTree.importSchemaFromCsvFile", tool::importCsv);
  }

  @Test
  public void importDataClosesParserOnEarlyReturn() throws Exception {
    Method importFromSingleFile =
        ImportData.class.getDeclaredMethod("importFromSingleFile", Session.class, File.class);
    importFromSingleFile.setAccessible(true);
    // The session is not reached on the empty-file path.
    assertNoDescriptorGrowth(
        "ImportData.importFromSingleFile", file -> importFromSingleFile.invoke(null, null, file));
  }

  private static void assertNoDescriptorGrowth(String site, CsvImport importCsv) throws Exception {
    // An empty file has no header, so every import path takes its "Empty file!" early return
    // without needing a connection.
    File csv = File.createTempFile("readCsvFileResource", ".csv");
    csv.deleteOnExit();

    importCsv.run(csv); // warm-up: class loading may open descriptors of its own
    long before = openDescriptors();
    for (int i = 0; i < CALLS; i++) {
      importCsv.run(csv);
    }
    long growth = openDescriptors() - before;

    assertTrue(
        site + " leaked " + growth + " file descriptors over " + CALLS + " calls",
        growth < MAX_GROWTH);
  }

  private static long openDescriptors() {
    File dir = new File("/proc/self/fd");
    if (!dir.isDirectory()) {
      dir = new File("/dev/fd");
    }
    String[] entries = dir.list();
    assumeTrue("no per-process descriptor directory on this platform", entries != null);
    return entries.length;
  }

  @FunctionalInterface
  private interface CsvImport {
    void run(File file) throws Exception;
  }

  private static final class TreeProbe extends ImportDataTree {
    void importCsv(File file) {
      importFromCsvFile(file);
    }
  }

  private static final class TableProbe extends ImportDataTable {
    void importCsv(File file) {
      importFromCsvFile(file);
    }
  }

  private static final class SchemaProbe extends ImportSchemaTree {
    void importCsv(File file) {
      importSchemaFromCsvFile(file);
    }
  }
}
