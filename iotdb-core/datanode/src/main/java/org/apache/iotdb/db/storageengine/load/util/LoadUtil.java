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

package org.apache.iotdb.db.storageengine.load.util;

import org.apache.iotdb.commons.disk.FolderManager;
import org.apache.iotdb.commons.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.commons.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.ModificationFileV1;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.load.active.ActiveLoadPathHelper;
import org.apache.iotdb.db.storageengine.load.disk.ILoadDiskSelector;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
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

public class LoadUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadUtil.class);

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
      LOGGER.warn(StorageEngineMessages.FAIL_TO_LOAD_TSFILE_TO_ACTIVE_DIR, e);
      return false;
    }

    return true;
  }

  public static String getTsFilePath(final String filePathWithResourceOrModsTail) {
    if (filePathWithResourceOrModsTail.endsWith(TsFileResource.RESOURCE_SUFFIX)) {
      return filePathWithResourceOrModsTail.substring(
          0, filePathWithResourceOrModsTail.length() - TsFileResource.RESOURCE_SUFFIX.length());
    }

    if (filePathWithResourceOrModsTail.endsWith(ModificationFileV1.FILE_SUFFIX)) {
      return filePathWithResourceOrModsTail.substring(
          0, filePathWithResourceOrModsTail.length() - ModificationFileV1.FILE_SUFFIX.length());
    }

    if (filePathWithResourceOrModsTail.endsWith(ModificationFile.FILE_SUFFIX)) {
      return filePathWithResourceOrModsTail.substring(
          0, filePathWithResourceOrModsTail.length() - ModificationFile.FILE_SUFFIX.length());
    }

    return filePathWithResourceOrModsTail;
  }

  public static String getTsFileModsV1Path(final String tsFilePath) {
    return tsFilePath + ModificationFileV1.FILE_SUFFIX;
  }

  public static String getTsFileModsV2Path(final String tsFilePath) {
    return tsFilePath + ModificationFile.FILE_SUFFIX;
  }

  public static String getTsFileResourcePath(final String tsFilePath) {
    return tsFilePath + TsFileResource.RESOURCE_SUFFIX;
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
      LOGGER.warn(StorageEngineMessages.FAIL_TO_LOAD_DISK_SPACE, file.getAbsolutePath(), e);
      return false;
    }

    if (targetFilePath == null) {
      LOGGER.warn(StorageEngineMessages.LOAD_ACTIVE_LISTENING_DIR_NOT_SET);
      return false;
    }
    final Map<String, String> attributes = appendCurrentUserIfAbsent(loadAttributes);
    final File targetDir = ActiveLoadPathHelper.resolveTargetDir(targetFilePath, attributes);

    transferFilesToActiveDir(
        targetDir,
        Arrays.asList(
            new File(getTsFileResourcePath(file.getAbsolutePath())),
            new File(getTsFileModsV1Path(file.getAbsolutePath())),
            new File(getTsFileModsV2Path(file.getAbsolutePath())),
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
      LOGGER.warn(StorageEngineMessages.FAIL_TO_LOAD_DISK_SPACE, files.get(0), e);
      return false;
    }

    if (targetFilePath == null) {
      LOGGER.warn(StorageEngineMessages.LOAD_ACTIVE_LISTENING_DIR_NOT_SET);
      return false;
    }
    final Map<String, String> attributes = appendCurrentUserIfAbsent(loadAttributes);
    final File targetDir = ActiveLoadPathHelper.resolveTargetDir(targetFilePath, attributes);

    final List<File> sourceFiles = new ArrayList<>(files.size());
    for (final String file : files) {
      sourceFiles.add(new File(file));
    }
    sourceFiles.sort(Comparator.comparing(LoadUtil::isTsFile));
    transferFilesToActiveDir(targetDir, sourceFiles, isDeleteAfterLoad);
    return true;
  }

  static void transferFilesToActiveDir(
      final File targetDir, final List<File> sourceFiles, final boolean isDeleteAfterLoad)
      throws IOException {
    final List<File> existingSourceFiles = new ArrayList<>(sourceFiles.size());
    for (final File sourceFile : sourceFiles) {
      if (sourceFile.exists()) {
        existingSourceFiles.add(sourceFile);
      }
    }
    if (existingSourceFiles.isEmpty()) {
      return;
    }

    final File transferDir = new File(targetDir, UUID.randomUUID().toString());
    try {
      Files.createDirectories(transferDir.toPath());
      for (final File sourceFile : existingSourceFiles) {
        final File targetFile = new File(transferDir, sourceFile.getName());
        RetryUtils.retryOnException(
            () -> {
              transferFile(sourceFile, targetFile, isDeleteAfterLoad);
              return null;
            });
      }
    } catch (final IOException | RuntimeException e) {
      if (transferDir.exists()) {
        FileUtils.deleteFileOrDirectoryWithRetry(transferDir);
      }
      throw e;
    }

    if (isDeleteAfterLoad) {
      deleteSourceFiles(existingSourceFiles);
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
        LOGGER.warn(StorageEngineMessages.FAILED_TO_DELETE_FILE_OR_DIR, sourceFile, e);
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
      LOGGER.warn(StorageEngineMessages.FAILED_LOAD_ACTIVE_LISTENING_DIRS, e);
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

    LoadUtil.loadDiskSelector = loadDiskSelector;
    return loadDiskSelector;
  }
}
