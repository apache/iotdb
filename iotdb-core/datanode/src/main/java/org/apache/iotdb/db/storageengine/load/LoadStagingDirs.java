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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Resolves staging roots, normalizes cross-node payload paths, and handles physical task directory
 * cleanup.
 */
final class LoadStagingDirs {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadStagingDirs.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private static final AtomicReference<StagingDirectoryContext> CONTEXT_REF =
      new AtomicReference<>(new StagingDirectoryContext(CONFIG.getLoadTsFileDirs()));
  private static final Object INIT_LOCK = new Object();

  private LoadStagingDirs() {}

  // -------------------------------------------------------------------------
  // Path Resolution & Introspection
  // -------------------------------------------------------------------------

  /** Returns current configured root directory strings from system descriptor. */
  static String[] configuredBaseDirs() {
    return CONFIG.getLoadTsFileDirs();
  }

  /** Returns active staging root directory strings. */
  static String[] baseDirs() {
    return CONTEXT_REF.get().baseDirs();
  }

  /**
   * Normalizes a staged file path relative to its enclosing staging root for network transport.
   * Falls back to absolute path if file does not reside within any configured base directory.
   */
  static String recordedPath(final File file) {
    Objects.requireNonNull(file, StorageEngineMessages.EXCEPTION_FILE_CANNOT_BE_NULL_29A83D70);
    final Path targetPath = file.toPath().toAbsolutePath().normalize();
    final StagingDirectoryContext context = CONTEXT_REF.get();
    final Path[] basePaths = context.basePaths();

    for (final Path basePath : basePaths) {
      if (targetPath.startsWith(basePath)) {
        return basePath.relativize(targetPath).toString();
      }
    }
    return file.getAbsolutePath();
  }

  /** Resolves index of the staging root containing the target file, or -1 if uncontained. */
  static int baseDirIndexOf(final File file) {
    Objects.requireNonNull(file, StorageEngineMessages.EXCEPTION_FILE_CANNOT_BE_NULL_29A83D70);
    final Path targetPath = file.toPath().toAbsolutePath().normalize();
    final StagingDirectoryContext context = CONTEXT_REF.get();
    final Path[] basePaths = context.basePaths();

    for (int i = 0; i < basePaths.length; i++) {
      if (targetPath.startsWith(basePaths[i])) {
        return i;
      }
    }
    return -1;
  }

  /** Constructs the DataRegion-scoped staging directory. */
  static File regionLoadDir(
      final File baseDir, final String databaseName, final String dataRegionIdString) {
    return new File(baseDir, databaseName + IoTDBConstant.FILE_NAME_SEPARATOR + dataRegionIdString);
  }

  // -------------------------------------------------------------------------
  // Physical Cleanup Operations
  // -------------------------------------------------------------------------

  /** Recursively deletes a task directory and all its contents using safe post-order traversal. */
  static void deleteTaskDir(final File taskDir) {
    if (taskDir == null || !taskDir.exists()) {
      return;
    }

    try {
      Files.walkFileTree(
          taskDir.toPath(),
          new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) {
              deleteWithRetry(file);
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) {
              deleteWithRetry(dir);
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (final IOException e) {
      LOGGER.warn(StorageEngineMessages.LOG_FAILED_TO_DELETE_ARG_3A7BD6FD, taskDir.getPath(), e);
    }
  }

  private static void deleteWithRetry(final Path target) {
    try {
      RetryUtils.retryOnException(
          () -> {
            Files.deleteIfExists(target);
            return null;
          });
    } catch (final DirectoryNotEmptyException e) {
      LOGGER.info(StorageEngineMessages.TASK_DIR_NOT_EMPTY_SKIP_DELETE, target.toString());
    } catch (final IOException e) {
      LOGGER.warn(StorageEngineMessages.LOG_FAILED_TO_DELETE_ARG_3A7BD6FD, target.toString(), e);
    }
  }

  // -------------------------------------------------------------------------
  // Staging Context & FolderManager Lifecycle
  // -------------------------------------------------------------------------

  /** Retrieves or initializes the shared FolderManager with double-checked locking. */
  static FolderManager folderManager() throws DiskSpaceInsufficientException {
    final String[] currentConfiguredDirs = CONFIG.getLoadTsFileDirs();
    StagingDirectoryContext currentContext = CONTEXT_REF.get();

    if (!Arrays.equals(currentConfiguredDirs, currentContext.baseDirs())
        || currentContext.folderManager() == null) {
      synchronized (INIT_LOCK) {
        currentContext = CONTEXT_REF.get();
        if (!Arrays.equals(currentConfiguredDirs, currentContext.baseDirs())
            || currentContext.folderManager() == null) {
          final FolderManager newFolderManager =
              new FolderManager(
                  Arrays.asList(currentConfiguredDirs), DirectoryStrategyType.SEQUENCE_STRATEGY);
          CONTEXT_REF.set(new StagingDirectoryContext(currentConfiguredDirs, newFolderManager));
          return newFolderManager;
        }
      }
    }
    return currentContext.folderManager();
  }

  /**
   * Immutable snapshot holding synchronized base directories, parsed normalized paths, and folder
   * manager.
   */
  private record StagingDirectoryContext(
      String[] baseDirs, Path[] basePaths, FolderManager folderManager) {

    private StagingDirectoryContext(final String[] baseDirs) {
      this(baseDirs, null);
    }

    private StagingDirectoryContext(final String[] baseDirs, final FolderManager folderManager) {
      this(
          baseDirs != null ? baseDirs.clone() : new String[0],
          baseDirs != null
              ? Arrays.stream(baseDirs)
                  .map(dir -> Paths.get(dir).toAbsolutePath().normalize())
                  .toArray(Path[]::new)
              : new Path[0],
          folderManager);
    }
  }
}
