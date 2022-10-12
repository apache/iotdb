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
package org.apache.iotdb.db.conf.directories;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.conf.directories.strategy.DirectoryStrategy;
import org.apache.iotdb.db.conf.directories.strategy.DirectoryStrategyType;
import org.apache.iotdb.db.conf.directories.strategy.MaxDiskUsableSpaceFirstStrategy;
import org.apache.iotdb.db.conf.directories.strategy.MinFolderOccupiedSpaceFirstStrategy;
import org.apache.iotdb.db.conf.directories.strategy.RandomOnDiskUsableSpaceStrategy;
import org.apache.iotdb.db.conf.directories.strategy.SequenceStrategy;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class FolderManager {
  private static final Logger logger = LoggerFactory.getLogger(FolderManager.class);

  private final List<String> folders;
  private final DirectoryStrategy selectStrategy;

  public FolderManager(List<String> folders, DirectoryStrategyType type)
      throws DiskSpaceInsufficientException {
    this.folders = folders;
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
    } catch (DiskSpaceInsufficientException e) {
      logger.error("All disks of wal folders are full, change system mode to read-only.", e);
      CommonDescriptor.getInstance().getConfig().setNodeStatus(NodeStatus.ReadOnly);
      throw e;
    }
  }

  public String getNextFolder() throws DiskSpaceInsufficientException {
    try {
      return folders.get(selectStrategy.nextFolderIndex());
    } catch (DiskSpaceInsufficientException e) {
      logger.error("All disks of wal folders are full, change system mode to read-only.", e);
      CommonDescriptor.getInstance().getConfig().setNodeStatus(NodeStatus.ReadOnly);
      throw e;
    }
  }
}
