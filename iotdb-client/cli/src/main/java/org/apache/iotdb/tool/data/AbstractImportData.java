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
import org.apache.iotdb.tool.common.ImportTsFileOperation;
import org.apache.iotdb.tool.tsfile.ImportTsFileScanTool;

import org.apache.tsfile.external.commons.lang3.StringUtils;

import java.io.File;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
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
      final String currentFile = file == null ? "unknown" : file.getName();
      ioTPrinter.println(
          String.format("[%s] - Unexpected error occurred: %s", currentFile, e.getMessage()));
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
      finalizeGlobalObjectDirectory();
      ioTPrinter.println(Constants.IMPORT_COMPLETELY);
    }
  }

  public static void init(AbstractImportData instance) {
    loadFileFailedNum.reset();
    loadFileSuccessfulNum.reset();
    processingLoadFailedFileSuccessfulNum.reset();
    processingLoadSuccessfulFileSuccessfulNum.reset();
    instance.new ThreadManager().asyncImportDataFiles();
  }

  protected abstract void importFromSqlFile(File file);

  protected abstract void importFromTsFile(File file);

  protected abstract void importFromCsvFile(File file);

  /** Escapes content inside a SQL single-quoted string literal (double single-quotes). */
  protected static String escapeLoadTsFileStringLiteralContent(final String s) {
    if (s == null) {
      return "";
    }
    return s.replace("'", "''");
  }

  /**
   * LOAD TSFILE: {@code LOAD 'path' [ WITH ('object-file-path'='...') ]}.
   *
   * <p>Tree and relational grammars both support optional {@code WITH (...)}; only adds {@code
   * object-file-path} when configured.
   */
  protected static String buildLoadTsFileSql(final File file, final String objectFilePaths) {
    final StringBuilder sb =
        new StringBuilder("load '")
            .append(escapeLoadTsFileStringLiteralContent(file.getAbsolutePath()))
            .append("'");
    if (StringUtils.isNotBlank(objectFilePaths)) {
      sb.append(" with ('object-file-path'='")
          .append(escapeLoadTsFileStringLiteralContent(objectFilePaths))
          .append("')");
    }
    return sb.toString();
  }

  protected void processSuccessFile(String file) {
    loadFileSuccessfulNum.increment();
    if (fileType.equalsIgnoreCase(Constants.TSFILE_SUFFIXS)) {
      try {
        processingFile(file, true);
        processPerFileObjectDirectory(file, true);
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
    Path sourcePath = Paths.get(file);

    Path targetPath = isSuccess ? Paths.get(successDir) : Paths.get(failDir);
    String target = targetPath.toString();

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
            Path realTargetPath = targetPath.resolve(sourcePath.getFileName());
            if (Files.notExists(targetPath)) {
              Files.createDirectories(targetPath);
            }
            Files.move(sourcePath, realTargetPath, StandardCopyOption.REPLACE_EXISTING);
            if (sourceResourcePath != null) {
              Files.move(
                  sourceResourcePath,
                  targetPath.resolve(sourceResourcePath.getFileName()),
                  StandardCopyOption.REPLACE_EXISTING);
            }
            if (sourceModsPath != null) {
              Files.move(
                  sourceModsPath,
                  targetPath.resolve(sourceModsPath.getFileName()),
                  StandardCopyOption.REPLACE_EXISTING);
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
        processPerFileObjectDirectory(filePath, false);
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

  private static void finalizeGlobalObjectDirectory() {
    if (!Constants.TSFILE_SUFFIXS.equalsIgnoreCase(fileType)
        || StringUtils.isBlank(AbstractDataTool.objectFilePaths)) {
      return;
    }
    final Path sourceObjectDir = Paths.get(AbstractDataTool.objectFilePaths);
    if (!Files.isDirectory(sourceObjectDir)) {
      return;
    }
    final long failed = loadFileFailedNum.sum();
    final long succeeded = loadFileSuccessfulNum.sum();
    // Global object handling policy:
    // - all succeeded: apply success operation
    // - all failed: apply fail operation
    // - partial success/failed: do nothing
    if (failed > 0 && succeeded > 0) {
      return;
    }
    if (failed == 0 && succeeded == 0) {
      return;
    }
    final boolean allSucceeded = failed == 0;
    final ImportTsFileOperation operation = allSucceeded ? successOperation : failOperation;
    final String targetRoot = allSucceeded ? successDir : failDir;
    final Path targetObjectDir = Paths.get(targetRoot, sourceObjectDir.getFileName().toString());
    try {
      processObjectDirectory(sourceObjectDir, targetObjectDir, operation);
    } catch (Exception e) {
      ioTPrinter.println(
          "Failed to process global object directory [ "
              + sourceObjectDir
              + " ]: "
              + e.getMessage());
    }
  }

  private static void processPerFileObjectDirectory(final String filePath, final boolean isSuccess)
      throws Exception {
    if (!Constants.TSFILE_SUFFIXS.equalsIgnoreCase(fileType)
        || !StringUtils.isBlank(AbstractDataTool.objectFilePaths)) {
      return;
    }
    Path tsFilePath = Paths.get(filePath);
    String fileName = tsFilePath.getFileName().toString();
    String baseName =
        fileName.endsWith("." + Constants.TSFILE_SUFFIXS)
            ? fileName.substring(0, fileName.lastIndexOf("." + Constants.TSFILE_SUFFIXS))
            : fileName;

    Path sourceObjectDir = tsFilePath.resolveSibling(baseName);
    if (!Files.isDirectory(sourceObjectDir)) {
      return;
    }

    Path rootSourceDir = Paths.get(ImportTsFileScanTool.getSourceFullPath());
    Path relativePath = rootSourceDir.relativize(tsFilePath);

    Path objectRelativePath = relativePath.resolveSibling(baseName);

    Path targetRootDir = Paths.get(isSuccess ? successDir : failDir);
    Path targetObjectDir = targetRootDir.resolve(objectRelativePath);
    processObjectDirectory(
        sourceObjectDir, targetObjectDir, isSuccess ? successOperation : failOperation);
  }

  private static void processObjectDirectory(
      final Path sourceDir, final Path targetDir, final ImportTsFileOperation operation)
      throws Exception {
    switch (operation) {
      case NONE:
        return;
      case DELETE:
        deleteDirectoryRecursively(sourceDir);
        return;
      case MV:
        Files.createDirectories(targetDir.getParent());
        Files.move(sourceDir, targetDir, StandardCopyOption.REPLACE_EXISTING);
        return;
      case CP:
      case HARDLINK:
        copyDirectoryRecursively(sourceDir, targetDir);
        return;
      default:
        return;
    }
  }

  private static void copyDirectoryRecursively(final Path sourceDir, final Path targetDir)
      throws Exception {
    Files.walkFileTree(
        sourceDir,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
              throws java.io.IOException {
            final Path relative = sourceDir.relativize(dir);
            Files.createDirectories(targetDir.resolve(relative));
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws java.io.IOException {
            final Path relative = sourceDir.relativize(file);
            Files.copy(file, targetDir.resolve(relative), StandardCopyOption.REPLACE_EXISTING);
            return FileVisitResult.CONTINUE;
          }
        });
  }

  private static void deleteDirectoryRecursively(final Path sourceDir) throws Exception {
    Files.walkFileTree(
        sourceDir,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws java.io.IOException {
            Files.deleteIfExists(file);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, java.io.IOException exc)
              throws java.io.IOException {
            Files.deleteIfExists(dir);
            return FileVisitResult.CONTINUE;
          }
        });
  }
}
