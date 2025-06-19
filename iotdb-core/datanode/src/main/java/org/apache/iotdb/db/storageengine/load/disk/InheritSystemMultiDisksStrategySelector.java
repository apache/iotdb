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

package org.apache.iotdb.db.storageengine.load.disk;

import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.storageengine.rescon.disk.FolderManager;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;

import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.fileSystem.fsFactory.FSFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class InheritSystemMultiDisksStrategySelector implements ILoadDiskSelector {

  protected final FSFactory fsFactory = FSFactoryProducer.getFSFactory();
  private static final Logger logger =
      LoggerFactory.getLogger(InheritSystemMultiDisksStrategySelector.class);

  public InheritSystemMultiDisksStrategySelector() {
    // empty body
  }

  @Override
  public File getTargetFile(
      File fileToLoad,
      String databaseName,
      String dataRegionId,
      long filePartitionId,
      String tsfileName,
      int tierLevel)
      throws DiskSpaceInsufficientException, LoadFileException {
    // inherit system multi-disks select strategy, see configuration `dn_multi_dir_strategy`
    Exception lastException = null;
    for (int retryTimes = 0; retryTimes <= 1; retryTimes++) {
      String folder = TierManager.getInstance().getNextFolderForTsFile(tierLevel, false);
      try {
        return fsFactory.getFile(
            folder,
            databaseName
                + File.separatorChar
                + dataRegionId
                + File.separatorChar
                + filePartitionId
                + File.separator
                + tsfileName);
      } catch (Exception e) {
        TierManager.getInstance()
            .getFolderManager(tierLevel, false)
            .updateFolderState(folder, FolderManager.FolderState.ABNORMAL);
        lastException = e;
        logger.error(
            "Failed to get target file [{}] for database={}, region={}, partition={}, tier={} at retry {}: {}",
            tsfileName,
            databaseName,
            dataRegionId,
            filePartitionId,
            tierLevel,
            retryTimes,
            e.getMessage(),
            e);
      }
    }
    throw new LoadFileException(
        String.format(
            "Storage allocation failed for %s/%s/%s (tier %d)",
            databaseName, dataRegionId, filePartitionId, tierLevel),
        lastException);
  }
}
