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

package org.apache.iotdb.db.storageengine.dataregion.wal.allocation;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.IWALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALFakeNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;
import org.apache.iotdb.db.storageengine.rescon.disk.FolderManager;
import org.apache.iotdb.db.storageengine.rescon.disk.strategy.DirectoryStrategyType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public abstract class AbstractNodeAllocationStrategy implements NodeAllocationStrategy {
  private static final Logger logger =
      LoggerFactory.getLogger(AbstractNodeAllocationStrategy.class);
  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();

  // manage wal folders
  protected FolderManager folderManager;

  protected AbstractNodeAllocationStrategy() {
    try {
      folderManager =
          new FolderManager(
              Arrays.asList(commonConfig.getWalDirs()), DirectoryStrategyType.SEQUENCE_STRATEGY);
    } catch (DiskSpaceInsufficientException e) {
      // folderManager remains null when disk space is insufficient during initialization
      // It will be lazily initialized later when disk space becomes available
      logger.error(
          "Fail to create wal node allocation strategy because all disks of wal folders are full.",
          e);
    }
  }

  protected synchronized IWALNode createWALNode(String identifier) {
    try {
      // Lazy initialization of folderManager: if it was null during constructor
      // (due to insufficient disk space), try to initialize it now when disk space
      // might have become available
      if (folderManager == null) {
        folderManager =
            new FolderManager(
                Arrays.asList(commonConfig.getWalDirs()), DirectoryStrategyType.SEQUENCE_STRATEGY);
      }
      return folderManager.getNextWithRetry(
          folder -> new WALNode(identifier, folder + File.separator + identifier));
    } catch (DiskSpaceInsufficientException e) {
      logger.error("Fail to create wal node because all disks of wal folders are full.", e);
      return WALFakeNode.getFailureInstance(e);
    } catch (Exception e) {
      logger.warn("Failed to create WAL node after retries for identifier: " + identifier, e);
      return WALFakeNode.getFailureInstance(
          new IOException(
              "Failed to create WAL node after retries for identifier: " + identifier, e));
    }
  }

  protected IWALNode createWALNode(
      String identifier, String folder, long startFileVersion, long startSearchIndex) {
    try {
      return new WALNode(identifier, folder, startFileVersion, startSearchIndex);
    } catch (IOException e) {
      logger.error("Fail to create wal node", e);
      return WALFakeNode.getFailureInstance(e);
    }
  }
}
