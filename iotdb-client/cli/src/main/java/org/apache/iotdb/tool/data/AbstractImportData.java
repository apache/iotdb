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
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
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
    File file = null;
    try {
      if (Constants.TSFILE_SUFFIXS.equalsIgnoreCase(fileType)) {
        while ((filePath = ImportTsFileScanTool.pollFromQueue()) != null) {
          file = new File(filePath);
          if (file.getName().endsWith(Constants.TSFILE_SUFFIXS)) {
            importFromTsFile(file);
          }
        }
      } else {
        while ((filePath = ImportDataScanTool.pollFromQueue()) != null) {
          file = new File(filePath);
          if (file.getName().endsWith(Constants.SQL_SUFFIXS)) {
            importFromSqlFile(file);
          } else {
            importFromCsvFile(file);
          }
        }
      }
    } catch (Exception e) {
      importFailed.set(true);
      loadFileFailedNum.increment();
      ioTPrinter.println(
          String.format(
              CliMessages.MESSAGE_ARG_UNEXPECTED_ERROR_OCCURRED_ARG_E7255EE6,
              file == null ? filePath : file.getName(),
              e.getMessage()));
    }
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
              ioTPrinter.println("ImportData thread join interrupted: " + e.getMessage());
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
        ioTPrinter.println("Processed success file [ " + file + " ] successfully!");
      } catch (final Exception processSuccessException) {
        importFailed.set(true);
        ioTPrinter.println(
            "Failed to process success file [ "
                + file
                + " ]: "
                + processSuccessException.getMessage());
      }
    }
  }

  protected void processingFile(final String file, final boolean isSuccess) throws IOException {
    final String relativePath = ImportTsFileScanTool.getRelativePath(file);
    final Path target = Paths.get(isSuccess ? successDir : failDir).resolve(relativePath);
    final ImportTsFileOperation operation = isSuccess ? successOperation : failOperation;
    final List<String> suffixes = new ArrayList<>();
    suffixes.add("");
    for (String suffix : new String[] {".resource", ".mods"}) {
      if (Files.exists(Paths.get(file + suffix))) {
        suffixes.add(suffix);
      }
    }
    if (operation == ImportTsFileOperation.CP
        || operation == ImportTsFileOperation.HARDLINK
        || operation == ImportTsFileOperation.MV) {
      // Check every destination before moving any file in the TsFile/sidecar group.
      for (String suffix : suffixes) {
        Path destination = Paths.get(target + suffix);
        if (Files.exists(destination, LinkOption.NOFOLLOW_LINKS)) {
          throw new FileAlreadyExistsException(destination.toString());
        }
      }
      Files.createDirectories(target.getParent());
    }
    for (String suffix : suffixes) {
      processFile(
          Paths.get(file + suffix),
          Paths.get(target + suffix),
          operation == ImportTsFileOperation.HARDLINK && !suffix.isEmpty()
              ? ImportTsFileOperation.CP
              : operation);
    }
  }

  private static void processFile(Path source, Path target, ImportTsFileOperation operation)
      throws IOException {
    switch (operation) {
      case DELETE:
        Files.deleteIfExists(source);
        break;
      case CP:
        Files.copy(source, target);
        break;
      case HARDLINK:
        try {
          Files.createLink(target, source);
        } catch (FileAlreadyExistsException e) {
          throw e;
        } catch (IOException | UnsupportedOperationException e) {
          Files.copy(source, target);
        }
        break;
      case MV:
        Files.move(source, target);
        break;
      default:
        break;
    }
  }

  protected void processFailFile(final String filePath, final Exception e) {
    try {
      if (Objects.nonNull(e.getMessage()) && e.getMessage().contains("memory")) {
        ioTPrinter.println(
            "Rejecting file [ " + filePath + " ] due to memory constraints, will retry later.");
        ImportTsFileScanTool.putToQueue(filePath);
        return;
      }

      loadFileFailedNum.increment();
      importFailed.set(true);
      ioTPrinter.println("Failed to import [ " + filePath + " ] file: " + e.getMessage());

      try {
        processingFile(filePath, false);
        processingLoadFailedFileSuccessfulNum.increment();
        ioTPrinter.println("Processed fail file [ " + filePath + " ] successfully!");
      } catch (final Exception processFailException) {
        ioTPrinter.println(
            "Failed to process fail file [ "
                + filePath
                + " ]: "
                + processFailException.getMessage());
      }
    } catch (final InterruptedException e1) {
      ioTPrinter.println("Unexpected error occurred: " + e1.getMessage());
      Thread.currentThread().interrupt();
    } catch (final Exception e1) {
      ioTPrinter.println("Unexpected error occurred: " + e1.getMessage());
    }
  }
}
