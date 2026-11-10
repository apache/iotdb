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

import org.apache.iotdb.cli.i18n.CliMessages;
import org.apache.iotdb.cli.utils.IoTPrinter;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tool.common.Constants;
import org.apache.iotdb.tool.common.ImportTsFileOperation;
import org.apache.iotdb.tool.tsfile.ImportTsFileScanTool;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractImportData extends AbstractDataTool implements Runnable {

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);
  private static final AtomicBoolean importFailed = new AtomicBoolean();

  @FunctionalInterface
  protected interface DataInsertion {
    void insert() throws IoTDBConnectionException, StatementExecutionException;
  }

  /** Retry connection failures without discarding the caller's batch when insertion fails. */
  protected static void executeWithRetry(DataInsertion insertion, int retryTime) {
    for (int attempt = 0; ; attempt++) {
      try {
        insertion.insert();
        return;
      } catch (IoTDBConnectionException e) {
        if (attempt >= retryTime) {
          throw new IllegalStateException(
              String.format(
                  CliMessages.EXCEPTION_FAILED_TO_INSERT_CSV_BATCH_ARG_159E0269, e.getMessage()),
              e);
        }
      } catch (StatementExecutionException e) {
        throw new IllegalStateException(
            String.format(
                CliMessages.EXCEPTION_FAILED_TO_INSERT_CSV_BATCH_ARG_159E0269, e.getMessage()),
            e);
      }
    }
  }

  public abstract void init()
      throws InterruptedException, IoTDBConnectionException, StatementExecutionException;

  @Override
  public void run() {
    String filePath = "";
    if (Constants.TSFILE_SUFFIXS.equalsIgnoreCase(fileType)) {
      while ((filePath = ImportTsFileScanTool.pollFromQueue()) != null) {
        File file = new File(filePath);
        try {
          if (file.getName().endsWith(Constants.TSFILE_SUFFIXS)) {
            importFromTsFile(file);
          }
        } catch (Exception e) {
          handleUnexpectedError(file, e);
        }
      }
    } else {
      while ((filePath = ImportDataScanTool.pollFromQueue()) != null) {
        File file = new File(filePath);
        try {
          if (file.getName().endsWith(Constants.SQL_SUFFIXS)) {
            importFromSqlFile(file);
          } else {
            importFromCsvFile(file);
          }
        } catch (Exception e) {
          handleUnexpectedError(file, e);
        }
      }
    }
  }

  private static void handleUnexpectedError(File file, Exception e) {
    markFileFailed();
    ioTPrinter.println(
        String.format(
            CliMessages.MESSAGE_ARG_UNEXPECTED_ERROR_OCCURRED_ARG_E7255EE6,
            file.getName(),
            e.getMessage()));
  }

  protected static void markImportFailed() {
    importFailed.set(true);
  }

  protected static void markFileFailed() {
    markImportFailed();
    loadFileFailedNum.increment();
  }

  protected abstract Runnable getAsyncImportRunnable();

  protected class ThreadManager {
    public boolean asyncImportDataFiles() {
      List<Thread> list = new ArrayList<>(threadNum);
      for (int i = 0; i < threadNum; i++) {
        Thread thread = new Thread(getAsyncImportRunnable());
        thread.start();
        list.add(thread);
      }
      list.forEach(
          thread -> {
            try {
              thread.join();
            } catch (InterruptedException e) {
              importFailed.set(true);
              Thread.currentThread().interrupt();
              ioTPrinter.println(
                  String.format(
                      CliMessages.EXCEPTION_IMPORTDATA_THREAD_JOIN_INTERRUPTED_ARG_B94FC04B,
                      Objects.toString(e.getMessage())));
            }
          });
      if (!importFailed.get()) {
        ioTPrinter.println(Constants.IMPORT_COMPLETELY);
      }
      return !importFailed.get();
    }
  }

  public static boolean init(AbstractImportData instance) {
    importFailed.set(false);
    return instance.new ThreadManager().asyncImportDataFiles();
  }

  protected abstract void importFromSqlFile(File file);

  protected abstract void importFromTsFile(File file);

  protected abstract void importFromCsvFile(File file);

  protected void processSuccessFile(String file) {
    loadFileSuccessfulNum.increment();
    if (fileType.equalsIgnoreCase(Constants.TSFILE_SUFFIXS)) {
      try {
        processingFile(file, true);
        processingLoadSuccessfulFileSuccessfulNum.increment();
        ioTPrinter.println(
            String.format(
                CliMessages.LOG_PROCESSED_SUCCESS_FILE_ARG_SUCCESSFULLY_B2CCF2E5,
                Objects.toString(file)));
      } catch (final Exception processSuccessException) {
        importFailed.set(true);
        ioTPrinter.println(
            String.format(
                CliMessages.LOG_FAILED_TO_PROCESS_SUCCESS_FILE_ARG_ARG_8E0E4B9C,
                Objects.toString(file),
                Objects.toString(processSuccessException.getMessage())));
      }
    }
  }

  protected void processingFile(final String file, final boolean isSuccess) throws IOException {
    final String relativePath = ImportTsFileScanTool.getRelativePath(file);
    final Path target = Paths.get(isSuccess ? successDir : failDir).resolve(relativePath);
    final ImportTsFileOperation operation = isSuccess ? successOperation : failOperation;
    operation.processFileGroup(Paths.get(file), target);
  }

  protected void processFailFile(final String filePath, final Exception e) {
    try {
      if (Objects.nonNull(e.getMessage()) && e.getMessage().contains("memory")) {
        ioTPrinter.println(
            String.format(
                CliMessages
                    .LOG_REJECTING_FILE_ARG_DUE_TO_MEMORY_CONSTRAINTS_WILL_RETRY_LATER_AE83A15F,
                Objects.toString(filePath)));
        ImportTsFileScanTool.putToQueue(filePath);
        return;
      }

      loadFileFailedNum.increment();
      importFailed.set(true);
      ioTPrinter.println(
          String.format(
              CliMessages.LOG_FAILED_TO_IMPORT_ARG_FILE_ARG_F085D907,
              Objects.toString(filePath),
              Objects.toString(e.getMessage())));

      try {
        processingFile(filePath, false);
        processingLoadFailedFileSuccessfulNum.increment();
        ioTPrinter.println(
            String.format(
                CliMessages.LOG_PROCESSED_FAIL_FILE_ARG_SUCCESSFULLY_B430F7F0,
                Objects.toString(filePath)));
      } catch (final Exception processFailException) {
        ioTPrinter.println(
            String.format(
                CliMessages.LOG_FAILED_TO_PROCESS_FAIL_FILE_ARG_ARG_D01A0F75,
                Objects.toString(filePath),
                Objects.toString(processFailException.getMessage())));
      }
    } catch (final InterruptedException e1) {
      ioTPrinter.println(
          String.format(
              CliMessages.EXCEPTION_UNEXPECTED_ERROR_OCCURRED_ARG_402FA331,
              Objects.toString(e1.getMessage())));
      Thread.currentThread().interrupt();
    } catch (final Exception e1) {
      ioTPrinter.println(
          String.format(
              CliMessages.EXCEPTION_UNEXPECTED_ERROR_OCCURRED_ARG_402FA331,
              Objects.toString(e1.getMessage())));
    }
  }
}
