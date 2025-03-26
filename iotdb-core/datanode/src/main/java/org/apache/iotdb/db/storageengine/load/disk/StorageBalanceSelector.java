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
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;

import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.fileSystem.fsFactory.FSFactory;

import java.io.File;

public class StorageBalanceSelector implements ILoadDiskSelector {

  protected final FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  public StorageBalanceSelector() {
    // empty body
  }

  @Override
  public File getTargetFile(
      File fileToLoad,
      String databaseName,
      String dataRegionId,
      long filePartitionId,
      String tsfileName)
      throws DiskSpaceInsufficientException {
    return fsFactory.getFile(
        TierManager.getInstance().getNextFolderForTsFile(0, false),
        databaseName
            + File.separatorChar
            + dataRegionId
            + File.separatorChar
            + filePartitionId
            + File.separator
            + tsfileName);
  }
}
