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

import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.storageengine.load.disk.ILoadDiskSelector;
import org.apache.iotdb.db.storageengine.rescon.disk.FolderManager;
import org.apache.iotdb.db.storageengine.rescon.disk.strategy.DirectoryStrategyType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.apache.iotdb.commons.utils.FileUtils.copyFileWithMD5Check;
import static org.apache.iotdb.commons.utils.FileUtils.moveFileWithMD5Check;

public class ActiveLoadUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(ActiveLoadUtil.class);

  private static volatile ILoadDiskSelector loadDiskSelector = updateLoadDiskSelector();

  public static boolean loadTsFileAsyncToActiveDir(
      final List<File> tsFiles, final String dataBaseName, final boolean isDeleteAfterLoad) {
    if (tsFiles == null || tsFiles.isEmpty()) {
      return true;
    }

    try {
      for (File file : tsFiles) {
        if (!loadTsFilesToActiveDir(dataBaseName, file, isDeleteAfterLoad)) {
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
      final String dataBaseName, final File file, final boolean isDeleteAfterLoad)
      throws IOException {
    if (file == null) {
      return true;
    }

    final File targetFilePath;
    try {
      targetFilePath =
          loadDiskSelector.selectTargetDirectory(file.getParentFile(), file.getName(), false, 0);
    } catch (DiskSpaceInsufficientException e) {
      LOGGER.warn("Fail to load disk space of file {}", file.getAbsolutePath(), e);
      return false;
    }

    if (targetFilePath == null) {
      LOGGER.warn("Load active listening dir is not set.");
      return false;
    }
    final File targetDir;
    if (Objects.nonNull(dataBaseName)) {
      targetDir = new File(targetFilePath, dataBaseName);
    } else {
      targetDir = targetFilePath;
    }

    loadTsFileAsyncToTargetDir(targetDir, file, isDeleteAfterLoad);
    loadTsFileAsyncToTargetDir(
        targetDir, new File(file.getAbsolutePath() + ".resource"), isDeleteAfterLoad);
    loadTsFileAsyncToTargetDir(
        targetDir, new File(file.getAbsolutePath() + ".mods"), isDeleteAfterLoad);
    return true;
  }

  public static boolean loadFilesToActiveDir(
      final String dataBaseName, final List<String> files, final boolean isDeleteAfterLoad)
      throws IOException {
    if (files == null || files.isEmpty()) {
      return true;
    }

    final File targetFilePath;
    try {
      final File file = new File(files.get(0));
      targetFilePath =
          loadDiskSelector.selectTargetDirectory(file.getParentFile(), file.getName(), false, 0);
    } catch (DiskSpaceInsufficientException e) {
      LOGGER.warn("Fail to load disk space of file {}", files.get(0), e);
      return false;
    }

    if (targetFilePath == null) {
      LOGGER.warn("Load active listening dir is not set.");
      return false;
    }
    final File targetDir;
    if (Objects.nonNull(dataBaseName)) {
      targetDir = new File(targetFilePath, dataBaseName);
    } else {
      targetDir = targetFilePath;
    }

    for (final String file : files) {
      loadTsFileAsyncToTargetDir(targetDir, new File(file), isDeleteAfterLoad);
    }
    return true;
  }

  private static void loadTsFileAsyncToTargetDir(
      final File targetDir, final File file, final boolean isDeleteAfterLoad) throws IOException {
    if (!file.exists()) {
      return;
    }
    RetryUtils.retryOnException(
        () -> {
          if (isDeleteAfterLoad) {
            moveFileWithMD5Check(file, targetDir);
          } else {
            copyFileWithMD5Check(file, targetDir);
          }
          return null;
        });
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
