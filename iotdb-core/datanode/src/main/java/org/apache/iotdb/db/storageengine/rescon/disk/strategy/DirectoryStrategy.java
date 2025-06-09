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
package org.apache.iotdb.db.storageengine.rescon.disk.strategy;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.utils.JVMCommonUtils;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.storageengine.rescon.disk.FolderManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.storageengine.rescon.disk.FolderManager.FolderState.HEALTHY;

/**
 * The basic class of all the strategies of multiple directories. If a user wants to define his own
 * strategy, his strategy has to extend this class and implement the abstract method.
 */
public abstract class DirectoryStrategy {

  protected static final Logger LOGGER = LoggerFactory.getLogger(DirectoryStrategy.class);

  /** All the folders of data files, should be init once the subclass is created. */
  List<String> folders = new ArrayList<>();

  /**
   * To init folders. Do not recommend to overwrite. This method guarantees that at least one folder
   * has available space.
   *
   * @param folders the folders from conf
   */
  public void setFolders(List<String> folders) throws DiskSpaceInsufficientException {
    boolean hasSpace = false;
    for (String folder : folders) {
      if (JVMCommonUtils.hasSpace(folder)) {
        hasSpace = true;
        break;
      }
    }
    if (!hasSpace) {
      LOGGER.error("Disk space is insufficient, change system mode to read-only");
      CommonDescriptor.getInstance().getConfig().setNodeStatus(NodeStatus.ReadOnly);
      throw new DiskSpaceInsufficientException(folders);
    }

    this.folders = folders;
  }

  /**
   * Map storing the state of each folder (HEALTHY/ABNORMAL). Key: folder path as String Value:
   * corresponding FolderState enum value
   */
  Map<String, FolderManager.FolderState> foldersStates = new HashMap<>();

  /**
   * Replaces the entire folder states mapping with a new one.
   *
   * @param foldersStates new mapping of folder paths to their states
   */
  public void setFoldersStates(Map<String, FolderManager.FolderState> foldersStates) {
    this.foldersStates = foldersStates;
  }

  /**
   * Updates the state of a specific folder if it exists in the mapping.
   *
   * @param folder path of the folder to update
   * @param state new state to set for the folder
   */
  public void updateFolderState(String folder, FolderManager.FolderState state) {
    foldersStates.replace(folder, state);
  }

  public boolean isUnavailableFolder(String dir) {
    return (foldersStates.getOrDefault(dir, HEALTHY) != HEALTHY);
  }

  /**
   * Choose a folder to allocate. The user should implement this method to define his own strategy.
   *
   * @return the index of folder that will be allocated
   */
  public abstract int nextFolderIndex() throws DiskSpaceInsufficientException;
}
