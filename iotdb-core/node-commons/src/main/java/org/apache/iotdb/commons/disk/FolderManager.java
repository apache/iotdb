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
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.disk.strategy.DirectoryStrategy;
import org.apache.iotdb.commons.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.commons.disk.strategy.MaxDiskUsableSpaceFirstStrategy;
import org.apache.iotdb.commons.disk.strategy.MinFolderOccupiedSpaceFirstStrategy;
import org.apache.iotdb.commons.disk.strategy.RandomOnDiskUsableSpaceStrategy;
import org.apache.iotdb.commons.disk.strategy.SequenceStrategy;
import org.apache.iotdb.commons.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.commons.i18n.UtilMessages;
import org.apache.iotdb.commons.log.LoggerPeriodicalLogReducer;
import org.apache.iotdb.commons.utils.JVMCommonUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class FolderManager {
  private static final Logger logger = LoggerFactory.getLogger(FolderManager.class);

  /**
   * Registry of every live {@link FolderManager} instance so the DataNode heartbeat path can ask
   * "is any folder anywhere on this node currently ABNORMAL?" without each subsystem having to push
   * state into a central reporter. Weak references avoid keeping short-lived managers alive (e.g.
   * those created per snapshot/load).
   */
  private static final List<WeakReference<FolderManager>> ALL_INSTANCES =
      new CopyOnWriteArrayList<>();

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

  public FolderManager(List<String> folders, DirectoryStrategyType type)
      throws DiskSpaceInsufficientException {
    this.folders = folders;
    folders.forEach(dir -> foldersStates.put(dir, FolderState.HEALTHY));
    ALL_INSTANCES.add(new WeakReference<>(this));
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
    } catch (DiskSpaceInsufficientException e) {
      changeToReadOnlyIfDiskFull(e);
      throw e;
    }
  }

  public synchronized void updateFolderState(String folder, FolderState state) {
    foldersStates.replace(folder, state);
    selectStrategy.updateFolderState(folder, state);
  }

  public String getNextFolder() throws DiskSpaceInsufficientException {
    try {
      return folders.get(selectStrategy.nextFolderIndex());
    } catch (DiskSpaceInsufficientException e) {
      changeToReadOnlyIfDiskFull(e);
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

  private void changeToReadOnlyIfDiskFull(DiskSpaceInsufficientException e) {
    if (!hasFolderWithAvailableDiskSpace()) {
      if (LoggerPeriodicalLogReducer.shouldLog(UtilMessages.ALL_FOLDERS_FULL_CHANGE_TO_READ_ONLY)) {
        logger.error(UtilMessages.ALL_FOLDERS_FULL_CHANGE_TO_READ_ONLY, e);
      }
      CommonDescriptor.getInstance().getConfig().setNodeStatus(NodeStatus.ReadOnly);
      CommonDescriptor.getInstance().getConfig().setStatusReason(NodeStatus.DISK_FULL);
    } else {
      logger.warn(UtilMessages.CANNOT_SELECT_FOLDER_BUT_DISK_HAS_SPACE, e);
    }
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
        changeToReadOnlyIfDiskFull(e);
        throw e;
      }
      try {
        return folderConsumer.apply(folder);
      } catch (Exception e) {
        updateFolderState(folder, FolderState.ABNORMAL);
        logger.warn(UtilMessages.FAILED_TO_PROCESS_FOLDER, folder);
      }
    }
    throw new DiskSpaceInsufficientException(folders);
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

  /**
   * Walks every live FolderManager instance and reports whether any folder is currently {@link
   * FolderState#ABNORMAL}. Used by the DataNode heartbeat path to derive a {@code
   * NodeStatus.ReadOnly(DiskCrash)} signal from already-observed write failures.
   *
   * <p>Stale (GC'd) weak references are pruned as a side effect.
   */
  public static boolean hasAnyAbnormalFolder() {
    for (WeakReference<FolderManager> reference : ALL_INSTANCES) {
      FolderManager folderManager = reference.get();
      if (folderManager == null) {
        continue;
      }
      synchronized (folderManager) {
        for (FolderState state : folderManager.foldersStates.values()) {
          if (state == FolderState.ABNORMAL) {
            return true;
          }
        }
      }
    }
    ALL_INSTANCES.removeIf(ref -> ref.get() == null);
    return false;
  }
}
