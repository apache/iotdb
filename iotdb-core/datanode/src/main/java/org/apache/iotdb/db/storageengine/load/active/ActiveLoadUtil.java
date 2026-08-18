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

package org.apache.iotdb.db.storageengine.load.active;

import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.storageengine.load.disk.ILoadDiskSelector;
import org.apache.iotdb.db.storageengine.rescon.disk.FolderManager;
import org.apache.iotdb.db.storageengine.rescon.disk.strategy.DirectoryStrategyType;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class ActiveLoadUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(ActiveLoadUtil.class);

  private static volatile ILoadDiskSelector loadDiskSelector = updateLoadDiskSelector();

  public static boolean loadTsFileAsyncToActiveDir(
      final List<File> tsFiles,
      final Map<String, String> loadAttributes,
      final boolean isDeleteAfterLoad) {
    if (tsFiles == null || tsFiles.isEmpty()) {
      return true;
    }

    try {
      for (File file : tsFiles) {
        if (!loadTsFilesToActiveDir(loadAttributes, file, isDeleteAfterLoad)) {
          return false;
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Fail to load tsfile to Active dir", e);
      return false;
    }

    return true;
  }

  private static boolean loadTsFilesToActiveDir(
      final Map<String, String> loadAttributes, final File file, final boolean isDeleteAfterLoad)
      throws IOException {
    if (file == null) {
      return true;
    }

    final File targetFilePath;
    try {
      targetFilePath =
          loadDiskSelector.selectTargetDirectory(file.getParentFile(), file.getName(), false, 0);
    } catch (Exception e) {
      LOGGER.warn("Fail to load disk space of file {}", file.getAbsolutePath(), e);
      return false;
    }

    if (targetFilePath == null) {
      LOGGER.warn("Load active listening dir is not set.");
      return false;
    }
    final Map<String, String> attributes = appendCurrentUserIfAbsent(loadAttributes);
    final File targetDir =
        ActiveLoadPathHelper.resolvePipeTransferTargetDir(targetFilePath, attributes);

    transferFilesToActiveDir(
        targetDir,
        Arrays.asList(
            new File(file.getAbsolutePath() + ".resource"),
            new File(file.getAbsolutePath() + ".mods"),
            file),
        isDeleteAfterLoad);
    return true;
  }

  private static Map<String, String> appendCurrentUserIfAbsent(
      final Map<String, String> loadAttributes) {
    final Map<String, String> attributes =
        Objects.nonNull(loadAttributes)
            ? new LinkedHashMap<>(loadAttributes)
            : new LinkedHashMap<>();
    if (!attributes.containsKey(ActiveLoadPathHelper.USER_KEY)) {
      final IClientSession session = SessionManager.getInstance().getCurrSession();
      attributes.put(
          ActiveLoadPathHelper.USER_KEY,
          session == null || session.getUsername() == null
              ? AuthorityChecker.SUPER_USER
              : session.getUsername());
    }
    return attributes;
  }

  public static boolean loadFilesToActiveDir(
      final Map<String, String> loadAttributes,
      final List<String> files,
      final boolean isDeleteAfterLoad)
      throws IOException {
    if (files == null || files.isEmpty()) {
      return true;
    }

    final File targetFilePath;
    try {
      final File file = new File(files.get(0));
      targetFilePath =
          loadDiskSelector.selectTargetDirectory(file.getParentFile(), file.getName(), false, 0);
    } catch (Exception e) {
      LOGGER.warn("Fail to load disk space of file {}", files.get(0), e);
      return false;
    }

    if (targetFilePath == null) {
      LOGGER.warn("Load active listening dir is not set.");
      return false;
    }
    final Map<String, String> attributes = appendCurrentUserIfAbsent(loadAttributes);
    final File targetDir =
        ActiveLoadPathHelper.resolvePipeTransferTargetDir(targetFilePath, attributes);

    final List<File> sourceFiles = new ArrayList<>(files.size());
    for (final String file : files) {
      sourceFiles.add(new File(file));
    }
    sourceFiles.sort(Comparator.comparing(ActiveLoadUtil::isTsFile));
    transferFilesToActiveDir(
        targetDir,
        sourceFiles,
        isDeleteAfterLoad,
        attributes.get(ActiveLoadPathHelper.PIPE_CONVERSION_TASK_ID_KEY));
    return true;
  }

  static void transferFilesToActiveDir(
      final File targetDir, final List<File> sourceFiles, final boolean isDeleteAfterLoad)
      throws IOException {
    transferFilesToActiveDir(targetDir, sourceFiles, isDeleteAfterLoad, null);
  }

  static void transferFilesToActiveDir(
      final File targetDir,
      final List<File> sourceFiles,
      final boolean isDeleteAfterLoad,
      final String conversionTaskId)
      throws IOException {
    final List<File> existingSourceFiles = new ArrayList<>(sourceFiles.size());
    for (final File sourceFile : sourceFiles) {
      if (sourceFile.exists()) {
        existingSourceFiles.add(sourceFile);
      }
    }
    final File transferDir =
        new File(
            targetDir,
            conversionTaskId == null
                ? UUID.randomUUID().toString()
                : ActiveLoadPathHelper.formatPipeTaskTransferDirectoryName(conversionTaskId));

    if (conversionTaskId != null && transferDir.exists()) {
      if (!isExistingTaskComplete(transferDir, sourceFiles)) {
        throw new IOException("Failed to load TsFile to active directory.");
      }
      if (isDeleteAfterLoad) {
        deleteSourceFiles(existingSourceFiles);
      }
      return;
    }

    if (existingSourceFiles.isEmpty()) {
      if (conversionTaskId != null) {
        throw new IOException("Failed to load TsFile to active directory.");
      }
      return;
    }

    final File stagingDir =
        new File(
            targetDir,
            ActiveLoadPathHelper.formatTransferStagingDirectoryName(UUID.randomUUID().toString()));
    try {
      Files.createDirectories(stagingDir.toPath());
      for (final File sourceFile : existingSourceFiles) {
        final File targetFile = new File(stagingDir, sourceFile.getName());
        RetryUtils.retryOnException(
            () -> {
              transferFile(sourceFile, targetFile, isDeleteAfterLoad);
              return null;
            });
      }
      try {
        publishTransferDirectory(stagingDir, transferDir);
      } catch (final IOException e) {
        if (conversionTaskId == null || !isExistingTaskComplete(transferDir, sourceFiles)) {
          throw e;
        }
      }
    } catch (final IOException | RuntimeException e) {
      if (stagingDir.exists()) {
        FileUtils.deleteFileOrDirectoryWithRetry(stagingDir);
      }
      throw e;
    }

    if (stagingDir.exists()) {
      FileUtils.deleteFileOrDirectoryWithRetry(stagingDir);
    }
    if (isDeleteAfterLoad) {
      deleteSourceFiles(existingSourceFiles);
    }
  }

  private static boolean isExistingTaskComplete(
      final File transferDir, final List<File> sourceFiles) {
    if (!transferDir.isDirectory()) {
      return false;
    }
    final File[] targetFiles = transferDir.listFiles(File::isFile);
    if (targetFiles == null || targetFiles.length == 0) {
      return false;
    }

    final List<File> existingSourceFiles =
        sourceFiles.stream().filter(File::isFile).collect(java.util.stream.Collectors.toList());
    if (existingSourceFiles.isEmpty()) {
      return Arrays.stream(targetFiles).anyMatch(ActiveLoadUtil::isTsFile);
    }
    if (targetFiles.length != existingSourceFiles.size()) {
      return false;
    }

    final boolean[] matched = new boolean[targetFiles.length];
    for (final File sourceFile : existingSourceFiles) {
      boolean found = false;
      for (int i = 0; i < targetFiles.length; i++) {
        if (!matched[i]
            && isTsFile(sourceFile) == isTsFile(targetFiles[i])
            && sourceFile.length() == targetFiles[i].length()) {
          matched[i] = true;
          found = true;
          break;
        }
      }
      if (!found) {
        return false;
      }
    }
    return true;
  }

  private static void publishTransferDirectory(final File stagingDir, final File transferDir)
      throws IOException {
    try {
      Files.move(stagingDir.toPath(), transferDir.toPath(), StandardCopyOption.ATOMIC_MOVE);
    } catch (final AtomicMoveNotSupportedException e) {
      Files.move(stagingDir.toPath(), transferDir.toPath());
    }
  }

  private static void transferFile(
      final File sourceFile, final File targetFile, final boolean useHardLink) throws IOException {
    Exception linkException = null;
    if (useHardLink) {
      try {
        Files.createLink(targetFile.toPath(), sourceFile.toPath());
        return;
      } catch (final IOException | UnsupportedOperationException | SecurityException e) {
        linkException = e;
      }
    }

    try {
      Files.copy(
          sourceFile.toPath(),
          targetFile.toPath(),
          StandardCopyOption.REPLACE_EXISTING,
          StandardCopyOption.COPY_ATTRIBUTES);
    } catch (final IOException e) {
      if (linkException != null) {
        e.addSuppressed(linkException);
      }
      throw e;
    }
  }

  private static void deleteSourceFiles(final List<File> sourceFiles) {
    for (final File sourceFile : sourceFiles) {
      try {
        RetryUtils.retryOnException(
            () -> {
              Files.deleteIfExists(sourceFile.toPath());
              return null;
            });
      } catch (final Exception e) {
        LOGGER.warn("Failed to delete file or dir {}", sourceFile, e);
      }
    }
  }

  private static boolean isTsFile(final File file) {
    return file.getName().endsWith(TsFileConstant.TSFILE_SUFFIX);
  }

  public static ILoadDiskSelector updateLoadDiskSelector() {
    final String[] dirs = IoTDBDescriptor.getInstance().getConfig().getLoadActiveListeningDirs();
    FolderManager folderManager = null;
    DiskSpaceInsufficientException exception = null;

    try {
      folderManager =
          new FolderManager(Arrays.asList(dirs), DirectoryStrategyType.SEQUENCE_STRATEGY);
    } catch (DiskSpaceInsufficientException e) {
      // It should be noted that if this exception is not ignored, the entire process may fail to
      // start.
      exception = e;
      LOGGER.warn("Failed to load active listening dirs", e);
    }

    final FolderManager finalFolderManager = folderManager;
    final DiskSpaceInsufficientException finalException = exception;
    ILoadDiskSelector loadDiskSelector =
        ILoadDiskSelector.initDiskSelector(
            IoTDBDescriptor.getInstance().getConfig().getLoadDiskSelectStrategy(),
            dirs,
            (sourceDir, fileName, tierLevel) -> {
              if (finalException != null) {
                throw finalException;
              }
              return new File(finalFolderManager.getNextFolder());
            });

    ActiveLoadUtil.loadDiskSelector = loadDiskSelector;
    return loadDiskSelector;
  }
}
