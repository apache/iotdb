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

import org.apache.iotdb.cli.utils.IoTPrinter;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tool.common.Constants;
import org.apache.iotdb.tool.tsfile.ImportTsFileScanTool;

import java.io.File;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public abstract class AbstractImportData extends AbstractDataTool implements Runnable {

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);

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
      ioTPrinter.println(
          String.format("[%s] - Unexpected error occurred: " + e.getMessage(), file.getName()));
    }
  }

  protected abstract Runnable getAsyncImportRunnable();

  protected class ThreadManager {
    public void asyncImportDataFiles() {
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
              Thread.currentThread().interrupt();
              ioTPrinter.println("ImportData thread join interrupted: " + e.getMessage());
            }
          });
      ioTPrinter.println("Import completely!");
    }
  }

  public static void init(AbstractImportData instance) {
    instance.new ThreadManager().asyncImportDataFiles();
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
        ioTPrinter.println(
            "Failed to process success file [ "
                + file
                + " ]: "
                + processSuccessException.getMessage());
      }
    }
  }

  protected void processingFile(final String file, final boolean isSuccess) {
    final String relativePath = file.substring(ImportTsFileScanTool.getSourceFullPathLength() + 1);
    final Path sourcePath = Paths.get(file);

    final String target =
        isSuccess
            ? successDir
            : failDir + File.separator + relativePath.replace(File.separator, "_");
    final Path targetPath = Paths.get(target);

    final String RESOURCE = ".resource";
    Path sourceResourcePath = Paths.get(sourcePath + RESOURCE);
    sourceResourcePath = Files.exists(sourceResourcePath) ? sourceResourcePath : null;
    final Path targetResourcePath = Paths.get(target + RESOURCE);

    final String MODS = ".mods";
    Path sourceModsPath = Paths.get(sourcePath + MODS);
    sourceModsPath = Files.exists(sourceModsPath) ? sourceModsPath : null;
    final Path targetModsPath = Paths.get(target + MODS);

    switch (isSuccess ? successOperation : failOperation) {
      case DELETE:
        {
          try {
            Files.deleteIfExists(sourcePath);
            if (null != sourceResourcePath) {
              Files.deleteIfExists(sourceResourcePath);
            }
            if (null != sourceModsPath) {
              Files.deleteIfExists(sourceModsPath);
            }
          } catch (final Exception e) {
            ioTPrinter.println(String.format("Failed to delete file: %s", e.getMessage()));
          }
          break;
        }
      case CP:
        {
          try {
            Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
            if (null != sourceResourcePath) {
              Files.copy(
                  sourceResourcePath, targetResourcePath, StandardCopyOption.REPLACE_EXISTING);
            }
            if (null != sourceModsPath) {
              Files.copy(sourceModsPath, targetModsPath, StandardCopyOption.REPLACE_EXISTING);
            }
          } catch (final Exception e) {
            ioTPrinter.println(String.format("Failed to copy file: %s", e.getMessage()));
          }
          break;
        }
      case HARDLINK:
        {
          try {
            Files.createLink(targetPath, sourcePath);
          } catch (FileAlreadyExistsException e) {
            ioTPrinter.println("Hardlink already exists: " + e.getMessage());
          } catch (final Exception e) {
            try {
              Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
            } catch (final Exception copyException) {
              ioTPrinter.println(
                  String.format("Failed to copy file: %s", copyException.getMessage()));
            }
          }

          try {
            if (null != sourceResourcePath) {
              Files.copy(
                  sourceResourcePath, targetResourcePath, StandardCopyOption.REPLACE_EXISTING);
            }
            if (null != sourceModsPath) {
              Files.copy(sourceModsPath, targetModsPath, StandardCopyOption.REPLACE_EXISTING);
            }
          } catch (final Exception e) {
            ioTPrinter.println(
                String.format("Failed to copy resource or mods file: %s", e.getMessage()));
          }
          break;
        }
      case MV:
        {
          try {
            Files.move(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
            if (null != sourceResourcePath) {
              Files.move(
                  sourceResourcePath, targetResourcePath, StandardCopyOption.REPLACE_EXISTING);
            }
            if (null != sourceModsPath) {
              Files.move(sourceModsPath, targetModsPath, StandardCopyOption.REPLACE_EXISTING);
            }
          } catch (final Exception e) {
            ioTPrinter.println(String.format("Failed to move file: %s", e.getMessage()));
          }
          break;
        }
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
