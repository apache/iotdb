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
package org.apache.iotdb.db.wal.allocation;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.conf.directories.FolderManager;
import org.apache.iotdb.db.conf.directories.strategy.DirectoryStrategyType;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.wal.node.IWALNode;
import org.apache.iotdb.db.wal.node.WALFakeNode;
import org.apache.iotdb.db.wal.node.WALNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;

public abstract class AbstractNodeAllocationStrategy implements NodeAllocationStrategy {
  private static final Logger logger =
      LoggerFactory.getLogger(AbstractNodeAllocationStrategy.class);
  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();

  /** manage wal folders */
  protected FolderManager folderManager;

  protected AbstractNodeAllocationStrategy() {
    try {
      folderManager =
          new FolderManager(
              Arrays.asList(commonConfig.getWalDirs()), DirectoryStrategyType.SEQUENCE_STRATEGY);
    } catch (DiskSpaceInsufficientException e) {
      logger.error(
          "Fail to create wal node allocation strategy because all disks of wal folders are full.",
          e);
    }
  }

  protected IWALNode createWALNode(String identifier) {
    String folder;
    // get wal folder
    try {
      folder = folderManager.getNextFolder();
    } catch (DiskSpaceInsufficientException e) {
      logger.error("Fail to create wal node because all disks of wal folders are full.", e);
      return WALFakeNode.getFailureInstance(e);
    }
    folder = folder + File.separator + identifier;
    // create new wal node
    return createWALNode(identifier, folder);
  }

  protected IWALNode createWALNode(String identifier, String folder) {
    try {
      return new WALNode(identifier, folder);
    } catch (FileNotFoundException e) {
      logger.error("Fail to create wal node", e);
      return WALFakeNode.getFailureInstance(e);
    }
  }

  protected IWALNode createWALNode(
      String identifier, String folder, long startFileVersion, long startSearchIndex) {
    try {
      return new WALNode(identifier, folder, startFileVersion, startSearchIndex);
    } catch (FileNotFoundException e) {
      logger.error("Fail to create wal node", e);
      return WALFakeNode.getFailureInstance(e);
    }
  }
}
