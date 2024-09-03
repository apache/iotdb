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

import org.apache.iotdb.cli.utils.IoTPrinter;

import java.io.File;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

public class IoTDBTsFileScanAndProcessTool {
  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);

  private static final String RESOURCE = ".resource";
  private static final String MODS = ".mods";

  private static final LinkedBlockingQueue<String> tsfileQueue = new LinkedBlockingQueue<>();
  private static final Set<String> tsfileSet = new HashSet<>();
  private static final Set<String> resourceOrModsSet = new HashSet<>();
  private static String sourceFullPath;

  private static String successDir;
  private static ImportTsFile.Operation successOperation;
  private static String failDir;
  private static ImportTsFile.Operation failOperation;

  public static void traverseAndCollectFiles() throws InterruptedException {
    traverseAndCollectFilesBySourceFullPath(new File(sourceFullPath));
  }

  private static void traverseAndCollectFilesBySourceFullPath(final File file)
      throws InterruptedException {
    if (file.isFile()) {
      if (file.getName().endsWith(RESOURCE) || file.getName().endsWith(MODS)) {
        resourceOrModsSet.add(file.getAbsolutePath());
      } else {
        tsfileSet.add(file.getAbsolutePath());
        tsfileQueue.put(file.getAbsolutePath());
      }
    } else if (file.isDirectory()) {
      final File[] files = file.listFiles();
      if (files != null) {
        for (File f : files) {
          traverseAndCollectFilesBySourceFullPath(f);
        }
      }
    }
  }

  public static void addNoResourceOrModsToQueue() throws InterruptedException {
    for (final String filePath : resourceOrModsSet) {
      final String tsfilePath =
          filePath.endsWith(RESOURCE)
              ? filePath.substring(0, filePath.length() - RESOURCE.length())
              : filePath.substring(0, filePath.length() - MODS.length());
      if (!tsfileSet.contains(tsfilePath)) {
        tsfileQueue.put(filePath);
      }
    }
  }

  public static void processingFile(final String filePath, final boolean isSuccess) {
    final String relativePath =
        filePath.substring(IoTDBTsFileScanAndProcessTool.getSourceFullPathLength() + 1);
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

  public static void setSuccessAndFailDirAndOperation(
      final String successDir,
      final ImportTsFile.Operation successOperation,
      final String failDir,
      final ImportTsFile.Operation failOperation) {
    IoTDBTsFileScanAndProcessTool.successDir = successDir;
    IoTDBTsFileScanAndProcessTool.successOperation = successOperation;
    IoTDBTsFileScanAndProcessTool.failDir = failDir;
    IoTDBTsFileScanAndProcessTool.failOperation = failOperation;
  }

  public static boolean isContainModsFile(final String filePath) {
    return IoTDBTsFileScanAndProcessTool.resourceOrModsSet.contains(filePath);
  }

  public static String getFilePath() {
    return IoTDBTsFileScanAndProcessTool.tsfileQueue.poll();
  }

  public static void put(final String filePath) {
    try {
      IoTDBTsFileScanAndProcessTool.tsfileQueue.put(filePath);
    } catch (final InterruptedException e) {
      ioTPrinter.println("add file error");
    }
  }

  public static void setSourceFullPath(final String sourceFullPath) {
    IoTDBTsFileScanAndProcessTool.sourceFullPath = sourceFullPath;
  }

  public static int getSourceFullPathLength() {
    return IoTDBTsFileScanAndProcessTool.sourceFullPath.length();
  }

  public static int getTsFileQueueSize() {
    return IoTDBTsFileScanAndProcessTool.tsfileQueue.size();
  }
}
