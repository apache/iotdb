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

  private static volatile ILoadDiskSelector loadDiskSelector =
      ILoadDiskSelector.initDiskSelector(
          IoTDBDescriptor.getInstance().getConfig().getLoadDiskSelectStrategy(),
          IoTDBDescriptor.getInstance().getConfig().getLoadActiveListeningDirs());

  private static volatile FolderManager folderManager;

  static {
    try {
      folderManager =
          new FolderManager(
              Arrays.asList(IoTDBDescriptor.getInstance().getConfig().getLoadActiveListeningDirs()),
              DirectoryStrategyType.SEQUENCE_STRATEGY);
    } catch (final DiskSpaceInsufficientException e) {
      LOGGER.error(
          "Fail to create pipe receiver file folders allocation strategy because all disks of folders are full.",
          e);
    }
  }

  public static boolean loadTsFileAsyncToActiveDir(
      final List<File> tsFiles, final String dataBaseName, final boolean isDeleteAfterLoad) {
    if (tsFiles == null || tsFiles.isEmpty()) {
      return true;
    }

    try {
      for (File file : tsFiles) {
        if (!loadTsFilesAsyncToActiveDir(dataBaseName, file, isDeleteAfterLoad)) {
          return false;
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Fail to load tsfile to Active dir", e);
      return false;
    }

    return true;
  }

  public static boolean loadTsFilesAsyncToActiveDir(
      final String dataBaseName, final File file, final boolean isDeleteAfterLoad)
      throws IOException {
    if (file == null) {
      return true;
    }

    final String targetFilePath;
    try {
      targetFilePath = loadDiskSelector.getTargetDir(file, folderManager);
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
      targetDir = new File(targetFilePath);
    }

    loadTsFileAsyncToTargetDir(targetDir, file, isDeleteAfterLoad);
    loadTsFileAsyncToTargetDir(
        targetDir, new File(file.getAbsolutePath() + ".resource"), isDeleteAfterLoad);
    loadTsFileAsyncToTargetDir(
        targetDir, new File(file.getAbsolutePath() + ".mods"), isDeleteAfterLoad);
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
}
