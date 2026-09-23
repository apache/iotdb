/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.load;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.disk.FolderManager;
import org.apache.iotdb.commons.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.commons.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.StorageEngineMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The physical layout of the LOAD staging area, and the deletion of a staged directory.
 *
 * <p>Every DataNode stages the pieces of a LOAD under {@code <load dir>/<database-region>/<load
 * id>/}. This class resolves those directories, hands out the writable ones, and deletes a
 * directory a finished task no longer needs.
 */
final class LoadStagingDirs {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadStagingDirs.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  static final String MESSAGE_DELETE_FAIL = "failed to delete {}.";

  private static final AtomicReference<String[]> LOAD_BASE_DIRS =
      new AtomicReference<>(CONFIG.getLoadTsFileDirs());
  private static final AtomicReference<FolderManager> FOLDER_MANAGER = new AtomicReference<>();

  private LoadStagingDirs() {}

  /**
   * The directories this DataNode stages the files of LOAD tasks in, as currently configured. A
   * scan of the staging area must use this form: the cached array below is only refreshed when the
   * writable directories are rebuilt.
   */
  static String[] configuredBaseDirs() {
    return CONFIG.getLoadTsFileDirs();
  }

  /** The directories this DataNode stages the files of LOAD tasks in. */
  static String[] baseDirs() {
    return LOAD_BASE_DIRS.get();
  }

  /**
   * The path of a staged file as a piece or a payload reference records it: relative to the staging
   * root the file lives under.
   *
   * <p>A DataNode can stage its tasks in several roots, the roots of two nodes need not have the
   * same names, and a restored snapshot may place a task under a different root than the one it was
   * staged in. A reference that names the file by its absolute path therefore stops describing it
   * as soon as it leaves the node that wrote it. Resolving the recorded path under the roots of the
   * node that reads it describes the file wherever it ended up. A file outside every root keeps its
   * absolute path, which both readers still accept.
   */
  static String recordedPath(final File file) {
    final Path path = file.toPath().toAbsolutePath();
    for (final String baseDir : baseDirs()) {
      final Path base = new File(baseDir).toPath().toAbsolutePath();
      if (path.startsWith(base)) {
        return base.relativize(path).toString();
      }
    }
    return file.getAbsolutePath();
  }

  /**
   * @return the index of the staging root that holds this directory, or -1 when no root does
   */
  static int baseDirIndexOf(final File file) {
    final Path path = file.toPath().toAbsolutePath();
    final String[] baseDirs = baseDirs();
    for (int i = 0; i < baseDirs.length; i++) {
      if (path.startsWith(new File(baseDirs[i]).toPath().toAbsolutePath())) {
        return i;
      }
    }
    return -1;
  }

  /** The staging directory of one region, the only place its staged payloads may be read from. */
  static File regionLoadDir(
      final File baseDir, final String databaseName, final String dataRegionIdString) {
    return new File(baseDir, databaseName + IoTDBConstant.FILE_NAME_SEPARATOR + dataRegionIdString);
  }

  /**
   * Deletes a staged directory together with everything inside it. A directory that is still being
   * written to is skipped rather than forced, so a concurrent writer never loses its files.
   */
  static void deleteTaskDir(final File taskDir) {
    // A retained directory still holds the staged TsFiles, so its content is deleted first.
    final File[] children = taskDir.listFiles();
    if (children != null) {
      for (final File child : children) {
        try {
          RetryUtils.retryOnException(
              () -> {
                Files.deleteIfExists(child.toPath());
                return null;
              });
        } catch (final DirectoryNotEmptyException e) {
          LOGGER.info(StorageEngineMessages.TASK_DIR_NOT_EMPTY_SKIP_DELETE, child.getPath());
        } catch (final IOException e) {
          LOGGER.warn(MESSAGE_DELETE_FAIL, child.getPath(), e);
        }
      }
    }
    try {
      RetryUtils.retryOnException(
          () -> {
            Files.deleteIfExists(taskDir.toPath());
            return null;
          });
    } catch (final DirectoryNotEmptyException e) {
      LOGGER.info(StorageEngineMessages.TASK_DIR_NOT_EMPTY_SKIP_DELETE, taskDir.getPath());
    } catch (final IOException e) {
      LOGGER.warn(MESSAGE_DELETE_FAIL, taskDir.getPath(), e);
    }
  }

  static FolderManager folderManager() throws DiskSpaceInsufficientException {
    if (CONFIG.getLoadTsFileDirs() != LOAD_BASE_DIRS.get()) {
      synchronized (FOLDER_MANAGER) {
        if (CONFIG.getLoadTsFileDirs() != LOAD_BASE_DIRS.get()) {
          LOAD_BASE_DIRS.set(CONFIG.getLoadTsFileDirs());
          FOLDER_MANAGER.set(
              new FolderManager(
                  Arrays.asList(LOAD_BASE_DIRS.get()), DirectoryStrategyType.SEQUENCE_STRATEGY));
          return FOLDER_MANAGER.get();
        }
      }
    }

    if (FOLDER_MANAGER.get() == null) {
      synchronized (FOLDER_MANAGER) {
        if (FOLDER_MANAGER.get() == null) {
          FOLDER_MANAGER.set(
              new FolderManager(
                  Arrays.asList(LOAD_BASE_DIRS.get()), DirectoryStrategyType.SEQUENCE_STRATEGY));
          return FOLDER_MANAGER.get();
        }
      }
    }

    return FOLDER_MANAGER.get();
  }
}
