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

package org.apache.iotdb.db.storageengine.rescon.disk;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.storageengine.rescon.disk.strategy.DirectoryStrategy;
import org.apache.iotdb.db.storageengine.rescon.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.db.storageengine.rescon.disk.strategy.MaxDiskUsableSpaceFirstStrategy;
import org.apache.iotdb.db.storageengine.rescon.disk.strategy.MinFolderOccupiedSpaceFirstStrategy;
import org.apache.iotdb.db.storageengine.rescon.disk.strategy.RandomOnDiskUsableSpaceStrategy;
import org.apache.iotdb.db.storageengine.rescon.disk.strategy.SequenceStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FolderManager {
  private static final Logger logger = LoggerFactory.getLogger(FolderManager.class);

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
      logger.error("All folders are full, change system mode to read-only.", e);
      CommonDescriptor.getInstance().getConfig().setNodeStatus(NodeStatus.ReadOnly);
      CommonDescriptor.getInstance().getConfig().setStatusReason(NodeStatus.DISK_FULL);
      throw e;
    }
  }

  public void updateFolderState(String folder, FolderState state) {
    foldersStates.replace(folder, state);
    selectStrategy.updateFolderState(folder, state);
  }

  public String getNextFolder() throws DiskSpaceInsufficientException {
    try {
      return folders.get(selectStrategy.nextFolderIndex());
    } catch (DiskSpaceInsufficientException e) {
      logger.error("All folders are full, change system mode to read-only.", e);
      CommonDescriptor.getInstance().getConfig().setNodeStatus(NodeStatus.ReadOnly);
      CommonDescriptor.getInstance().getConfig().setStatusReason(NodeStatus.DISK_FULL);
      throw e;
    }
  }

  boolean hasHealthyFolder() {
    return folders.stream()
        .anyMatch(
            folder ->
                foldersStates.getOrDefault(folder, FolderState.ABNORMAL) == FolderState.HEALTHY);
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
        logger.error("All folders are full, change system mode to read-only.", e);
        CommonDescriptor.getInstance().getConfig().setNodeStatus(NodeStatus.ReadOnly);
        CommonDescriptor.getInstance().getConfig().setStatusReason(NodeStatus.DISK_FULL);
        throw e;
      }
      try {
        return folderConsumer.apply(folder);
      } catch (Exception e) {
        updateFolderState(folder, FolderState.ABNORMAL);
        logger.warn("Failed to process folder '" + folder);
      }
    }
    throw new DiskSpaceInsufficientException(folders);
  }

  public List<String> getFolders() {
    return folders;
  }
}
