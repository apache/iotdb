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

package org.apache.iotdb.commons.disk;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.disk.strategy.DirectoryStrategy;
import org.apache.iotdb.commons.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.commons.disk.strategy.MaxDiskUsableSpaceFirstStrategy;
import org.apache.iotdb.commons.disk.strategy.MinFolderOccupiedSpaceFirstStrategy;
import org.apache.iotdb.commons.disk.strategy.RandomOnDiskUsableSpaceStrategy;
import org.apache.iotdb.commons.disk.strategy.SequenceStrategy;
import org.apache.iotdb.commons.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.commons.i18n.UtilMessages;
import org.apache.iotdb.commons.utils.JVMCommonUtils;
import org.apache.iotdb.commons.utils.TestOnly;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class FolderManager {
  private static final Logger logger = LoggerFactory.getLogger(FolderManager.class);
  private static final long ALL_FOLDERS_FULL_LOG_INTERVAL_MS = 3600 * 1000L;
  private static final ConcurrentMap<String, AtomicLong> ALL_FOLDERS_FULL_LAST_LOG_TIME_MAP =
      new ConcurrentHashMap<>();

  /** Represents the operational states of a data folder. */
  public enum FolderState {
    /** Indicates the folder is functioning normally with no issues. */
    HEALTHY,
    /** Indicates the folder has operational problems requiring attention. */
    ABNORMAL
  }

  private final List<String> folders;

  /**
   * Map storing the state of each folder (HEALTHY/ABNORMAL). Key: folder path as String Value:
   * corresponding FolderState enum value
   */
  private final Map<String, FolderState> foldersStates = new HashMap<>();

  private final DirectoryStrategy selectStrategy;
  private final String foldersKey;

  public FolderManager(List<String> folders, DirectoryStrategyType type)
      throws DiskSpaceInsufficientException {
    this.folders = folders;
    this.foldersKey = folders.toString();
    folders.forEach(dir -> foldersStates.put(dir, FolderState.HEALTHY));
    switch (type) {
      case SEQUENCE_STRATEGY:
        this.selectStrategy = new SequenceStrategy();
        break;
      case MAX_DISK_USABLE_SPACE_FIRST_STRATEGY:
        this.selectStrategy = new MaxDiskUsableSpaceFirstStrategy();
        break;
      case MIN_FOLDER_OCCUPIED_SPACE_FIRST_STRATEGY:
        this.selectStrategy = new MinFolderOccupiedSpaceFirstStrategy();
        break;
      case RANDOM_ON_DISK_USABLE_SPACE_STRATEGY:
        this.selectStrategy = new RandomOnDiskUsableSpaceStrategy();
        break;
      default:
        throw new RuntimeException();
    }
    try {
      this.selectStrategy.setFolders(folders);
      this.selectStrategy.setFoldersStates(foldersStates);
      resetAllFoldersFullLogTime();
    } catch (DiskSpaceInsufficientException e) {
      handleAllFoldersFull(e);
      throw e;
    }
  }

  public void updateFolderState(String folder, FolderState state) {
    foldersStates.replace(folder, state);
    selectStrategy.updateFolderState(folder, state);
    if (FolderState.HEALTHY.equals(state)) {
      resetAllFoldersFullLogTime();
    }
  }

  public String getNextFolder() throws DiskSpaceInsufficientException {
    try {
      String folder = folders.get(selectStrategy.nextFolderIndex());
      resetAllFoldersFullLogTime();
      return folder;
    } catch (DiskSpaceInsufficientException e) {
      handleAllFoldersFull(e);
      throw e;
    }
  }

  boolean hasHealthyFolder() {
    return folders.stream()
        .anyMatch(
            folder ->
                foldersStates.getOrDefault(folder, FolderState.ABNORMAL) == FolderState.HEALTHY);
  }

  private boolean hasFolderWithAvailableDiskSpace() {
    return folders.stream()
        .anyMatch(
            folder ->
                foldersStates.getOrDefault(folder, FolderState.ABNORMAL) == FolderState.HEALTHY
                    && JVMCommonUtils.hasSpace(folder));
  }

  @FunctionalInterface
  public interface ThrowingFunction<T, R, E extends Exception> {
    R apply(T t) throws E;
  }

  /*
   * Encapsulates the retry logic for folder operations
   * @param folderConsumer The operation to perform on the folder (e.g., creating TsFileWriterManager)
   * @return The result of the operation
   */
  public <T, E extends Exception> T getNextWithRetry(ThrowingFunction<String, T, E> folderConsumer)
      throws DiskSpaceInsufficientException {
    String folder = null;
    while (hasHealthyFolder()) {
      try {
        folder = folders.get(selectStrategy.nextFolderIndex());
      } catch (DiskSpaceInsufficientException e) {
        handleAllFoldersFull(e);
        throw e;
      }
      try {
        T result = folderConsumer.apply(folder);
        resetAllFoldersFullLogTime();
        return result;
      } catch (Exception e) {
        updateFolderState(folder, FolderState.ABNORMAL);
        logger.warn(UtilMessages.FAILED_TO_PROCESS_FOLDER, folder);
      }
    }
    DiskSpaceInsufficientException exception = new DiskSpaceInsufficientException(folders);
    handleAllFoldersFull(exception);
    throw exception;
  }

  private void handleAllFoldersFull(DiskSpaceInsufficientException e) {
    if (!hasFolderWithAvailableDiskSpace()) {
      logAllFoldersFullIfNecessary(e);
      CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
      if (!NodeStatus.ReadOnly.equals(commonConfig.getNodeStatus())) {
        commonConfig.setNodeStatus(NodeStatus.ReadOnly);
      }
      commonConfig.setStatusReason(NodeStatus.DISK_FULL);
    } else {
      logger.warn(UtilMessages.CANNOT_SELECT_FOLDER_BUT_DISK_HAS_SPACE, e);
    }
  }

  void logAllFoldersFullIfNecessary(DiskSpaceInsufficientException e) {
    AtomicLong lastAllFoldersFullLogTime =
        ALL_FOLDERS_FULL_LAST_LOG_TIME_MAP.computeIfAbsent(foldersKey, key -> new AtomicLong(0L));
    long now = System.currentTimeMillis();
    long lastLogTime = lastAllFoldersFullLogTime.get();
    if ((lastLogTime == 0 || now - lastLogTime >= ALL_FOLDERS_FULL_LOG_INTERVAL_MS)
        && lastAllFoldersFullLogTime.compareAndSet(lastLogTime, now)) {
      logger.error(UtilMessages.ALL_FOLDERS_FULL_CHANGE_TO_READ_ONLY, e);
    }
  }

  void resetAllFoldersFullLogTime() {
    ALL_FOLDERS_FULL_LAST_LOG_TIME_MAP.remove(foldersKey);
  }

  @TestOnly
  static void resetAllFoldersFullLogTimes() {
    ALL_FOLDERS_FULL_LAST_LOG_TIME_MAP.clear();
  }

  public List<String> getFolders() {
    return folders;
  }

  public String getFirstFolderOfSameDisk(String pathStr) {
    Path path = Paths.get(pathStr);
    try {
      FileStore fileStore = Files.getFileStore(path);
      for (String folder : folders) {
        if (foldersStates.getOrDefault(folder, FolderState.ABNORMAL) != FolderState.HEALTHY
            || !JVMCommonUtils.hasSpace(folder)) {
          continue;
        }
        Path folderPath = Paths.get(folder);
        FileStore folderFileStore = Files.getFileStore(folderPath);
        if (folderFileStore.equals(fileStore)) {
          return folder;
        }
      }
    } catch (IOException e) {
      logger.warn(UtilMessages.FAILED_TO_READ_FILE_STORE_PATH, pathStr, e);
    }
    return null;
  }
}
