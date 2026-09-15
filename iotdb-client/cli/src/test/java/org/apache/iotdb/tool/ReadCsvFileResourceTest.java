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

import org.apache.iotdb.tool.data.AbstractDataTool;

import org.apache.commons.csv.CSVParser;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Resource-management guard for the CSV import paths. {@link AbstractDataTool#readCsvFile(String)}
 * returns a {@link CSVParser} that owns the underlying {@code FileInputStream}; the import call
 * sites (ImportData / ImportDataTree / ImportDataTable / ImportSchemaTree) now consume it inside a
 * try-with-resources, so the parser — and the file descriptor it wraps — is released on every exit,
 * including the empty-file / bad-header early returns that previously leaked it (bulk import over a
 * directory could otherwise exhaust file descriptors with "Too many open files").
 */
public class ReadCsvFileResourceTest {

  /** Reaches the protected static {@link AbstractDataTool#readCsvFile(String)} for the assertion. */
  private static final class Probe extends AbstractDataTool {
    static CSVParser open(String path) throws IOException {
      return readCsvFile(path);
    }
  }

  @Test
  public void readCsvFileParserIsClosedByTryWithResources() throws IOException {
    File csv = File.createTempFile("readCsvFileResource", ".csv");
    csv.deleteOnExit();
    try (FileWriter writer = new FileWriter(csv)) {
      writer.write("Time,root.sg.d.s0\n1,10\n2,20\n");
    }

    CSVParser parser;
    try (CSVParser opened = Probe.open(csv.getAbsolutePath())) {
      parser = opened;
      assertFalse("parser must be open inside the try block", opened.isClosed());
      List<String> headerNames = opened.getHeaderNames();
      assertFalse("header must be parsed", headerNames.isEmpty());
      assertEquals("both data rows must be readable before close", 2L, opened.stream().count());
    }

    assertTrue(
        "readCsvFile's parser (and the FileInputStream it wraps) must be closed on scope exit",
        parser.isClosed());
  }
}
