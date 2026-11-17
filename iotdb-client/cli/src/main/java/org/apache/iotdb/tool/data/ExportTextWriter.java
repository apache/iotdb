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

package org.apache.iotdb.tool.data;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Shared CSV/SQL splitting and reliable IO. Partial files are closed without a completion event.
 */
final class ExportTextWriter implements AutoCloseable {
  private final ExportConfig config;
  private final ExportTask task;
  private final ExportProgressReporter progress;
  private final List<String> headers;
  private BufferedWriter writer;
  private CSVPrinter csv;
  private Path file;
  private int index;
  private long fileRows;

  ExportTextWriter(
      ExportConfig config, ExportTask task, ExportProgressReporter progress, List<String> headers) {
    this.config = config;
    this.task = task;
    this.progress = progress;
    this.headers = headers;
  }

  private void open() throws Exception {
    if (writer != null) {
      return;
    }
    progress.checkActive();
    file = Path.of(task.outputBase() + "_" + index + (headers == null ? ".sql" : ".csv"));
    ExportFileNames.checkOutputCollision(file, config);
    progress.fileStarted(file);
    writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8);
    fileRows = 0;
    if (headers != null) {
      csv =
          CSVFormat.Builder.create(CSVFormat.DEFAULT)
              .setHeader()
              .setSkipHeaderRecord(true)
              .setEscape('\\')
              .setQuoteMode(QuoteMode.NONE)
              .build()
              .print(writer);
      csv.printRecord(headers);
    }
  }

  synchronized void csvRow(List<Object> values) throws Exception {
    open();
    csv.printRecord(values);
    rowWritten();
  }

  synchronized void sqlRow(String sql) throws Exception {
    open();
    writer.write(sql);
    rowWritten();
  }

  private void rowWritten() throws IOException {
    fileRows++;
    progress.rowsWritten(1);
    if (fileRows >= config.linesPerFile) {
      completeFile();
    }
  }

  synchronized void complete() throws IOException {
    completeFile();
  }

  private void completeFile() throws IOException {
    if (writer == null) {
      return;
    }
    close();
    progress.fileCompleted(file, fileRows);
    index++;
  }

  @Override
  public synchronized void close() throws IOException {
    if (writer != null) {
      try {
        if (csv != null) {
          csv.close();
        } else {
          writer.close();
        }
      } finally {
        writer = null;
        csv = null;
      }
    }
  }
}
