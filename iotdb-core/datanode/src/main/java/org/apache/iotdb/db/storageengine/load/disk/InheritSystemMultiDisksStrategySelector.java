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
import org.apache.iotdb.db.storageengine.rescon.disk.FolderManager;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;

import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.fileSystem.fsFactory.FSFactory;

import java.io.File;

public class InheritSystemMultiDisksStrategySelector implements ILoadDiskSelector {

  protected static final FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  public InheritSystemMultiDisksStrategySelector() {}

  @Override
  public LoadDiskSelectorType getLoadDiskSelectorType() {
    return LoadDiskSelectorType.INHERIT_SYSTEM_MULTI_DISKS_SELECT_STRATEGY;
  }

  @Override
  public File getTargetDir(
      File fileToLoad, TierManager tierManager, String tsfileName, int tierLevel)
      throws DiskSpaceInsufficientException {
    // inherit system multi-disks select strategy, see configuration `dn_multi_dir_strategy`
    return fsFactory.getFile(tierManager.getNextFolderForTsFile(tierLevel, false), tsfileName);
  }

  public String getTargetDir(File fileToLoad, FolderManager folderManager)
      throws DiskSpaceInsufficientException {
    return folderManager.getNextFolder();
  }
}
