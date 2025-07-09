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

  public File getTargetFile(
      File fileToLoad,
      String databaseName,
      String dataRegionId,
      long filePartitionId,
      String tsfileName,
      int tierLevel)
      throws DiskSpaceInsufficientException, LoadFileException {
    try {
      return TierManager.getInstance()
          .getFolderManager(tierLevel, false)
          .getNextWithRetry(
              folder -> {
                return fsFactory.getFile(
                    folder,
                    databaseName
                        + File.separatorChar
                        + dataRegionId
                        + File.separatorChar
                        + filePartitionId
                        + File.separator
                        + tsfileName);
              });
    } catch (DiskSpaceInsufficientException e) {
      throw e;
    } catch (Exception e) {
      throw new LoadFileException(
          String.format(
              "Storage allocation failed for %s/%s/%s (tier %d)",
              databaseName, dataRegionId, filePartitionId, tierLevel),
          e);
    }
  }
}
