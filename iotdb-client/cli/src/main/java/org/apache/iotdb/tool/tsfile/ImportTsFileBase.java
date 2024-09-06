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

package org.apache.iotdb.tool.tsfile;

import org.apache.iotdb.cli.utils.IoTPrinter;

import java.io.File;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

public abstract class ImportTsFileBase implements Runnable {

  private static final IoTPrinter IOT_PRINTER = new IoTPrinter(System.out);

  private static final LongAdder loadFileSuccessfulNum = new LongAdder();
  private static final LongAdder loadFileFailedNum = new LongAdder();
  private static final LongAdder processingLoadSuccessfulFileSuccessfulNum = new LongAdder();
  private static final LongAdder processingLoadFailedFileSuccessfulNum = new LongAdder();

  private static String successDir;
  private static ImportTsFile.Operation successOperation;
  private static String failDir;
  private static ImportTsFile.Operation failOperation;

  @Override
  public void run() {
    loadTsFile();
  }

  protected abstract void loadTsFile();

  protected void processFailFile(final String filePath, final Exception e) {
    // Reject because of memory controls, do retry later
    try {
      if (Objects.nonNull(e.getMessage()) && e.getMessage().contains("memory")) {
        IOT_PRINTER.println(
            "Rejecting file [ " + filePath + " ] due to memory constraints, will retry later.");
        ImportTsFileScanTool.putToQueue(filePath);
        return;
      }

      loadFileFailedNum.increment();
      IOT_PRINTER.println("Failed to import [ " + filePath + " ] file: " + e.getMessage());

      try {
        processingFile(filePath, false);
        processingLoadFailedFileSuccessfulNum.increment();
        IOT_PRINTER.println("Processed fail file [ " + filePath + " ] successfully!");
      } catch (final Exception processFailException) {
        IOT_PRINTER.println(
            "Failed to process fail file [ "
                + filePath
                + " ]: "
                + processFailException.getMessage());
      }
    } catch (final InterruptedException e1) {
      IOT_PRINTER.println("Unexpected error occurred: " + e1.getMessage());
      Thread.currentThread().interrupt();
    } catch (final Exception e1) {
      IOT_PRINTER.println("Unexpected error occurred: " + e1.getMessage());
    }
  }

  protected static void processSuccessFile(final String filePath) {
    loadFileSuccessfulNum.increment();
    IOT_PRINTER.println("Imported [ " + filePath + " ] file successfully!");

    try {
      processingFile(filePath, true);
      processingLoadSuccessfulFileSuccessfulNum.increment();
      IOT_PRINTER.println("Processed success file [ " + filePath + " ] successfully!");
    } catch (final Exception processSuccessException) {
      IOT_PRINTER.println(
          "Failed to process success file [ "
              + filePath
              + " ]: "
              + processSuccessException.getMessage());
    }
  }

  public static void processingFile(final String filePath, final boolean isSuccess) {
    final String relativePath =
        filePath.substring(ImportTsFileScanTool.getSourceFullPathLength() + 1);
    final Path sourcePath = Paths.get(filePath);

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
            IOT_PRINTER.println(String.format("Failed to delete file: %s", e.getMessage()));
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
            IOT_PRINTER.println(String.format("Failed to copy file: %s", e.getMessage()));
          }
          break;
        }
      case HARDLINK:
        {
          try {
            Files.createLink(targetPath, sourcePath);
          } catch (FileAlreadyExistsException e) {
            IOT_PRINTER.println("Hardlink already exists: " + e.getMessage());
          } catch (final Exception e) {
            try {
              Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
            } catch (final Exception copyException) {
              IOT_PRINTER.println(
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
            IOT_PRINTER.println(
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
            IOT_PRINTER.println(String.format("Failed to move file: %s", e.getMessage()));
          }
          break;
        }
      default:
        break;
    }
  }

  protected static void printResult(final long startTime) {
    IOT_PRINTER.println(
        "Successfully load "
            + loadFileSuccessfulNum.sum()
            + " tsfile(s) (--on_success operation(s): "
            + processingLoadSuccessfulFileSuccessfulNum.sum()
            + " succeed, "
            + (loadFileSuccessfulNum.sum() - processingLoadSuccessfulFileSuccessfulNum.sum())
            + " failed)");
    IOT_PRINTER.println(
        "Failed to load "
            + loadFileFailedNum.sum()
            + " file(s) (--on_fail operation(s): "
            + processingLoadFailedFileSuccessfulNum.sum()
            + " succeed, "
            + (loadFileFailedNum.sum() - processingLoadFailedFileSuccessfulNum.sum())
            + " failed)");
    IOT_PRINTER.println(
        "Unprocessed "
            + ImportTsFileScanTool.getTsFileQueueSize()
            + " file(s), due to unexpected exceptions");
    IOT_PRINTER.println("For more details, please check the log.");
    IOT_PRINTER.println(
        "Total operation time: " + (System.currentTimeMillis() - startTime) + " ms.");
    IOT_PRINTER.println("Work has been completed!");
  }

  public static void setSuccessAndFailDirAndOperation(
      final String successDir,
      final ImportTsFile.Operation successOperation,
      final String failDir,
      final ImportTsFile.Operation failOperation) {
    ImportTsFileBase.successDir = successDir;
    ImportTsFileBase.successOperation = successOperation;
    ImportTsFileBase.failDir = failDir;
    ImportTsFileBase.failOperation = failOperation;
  }
}
