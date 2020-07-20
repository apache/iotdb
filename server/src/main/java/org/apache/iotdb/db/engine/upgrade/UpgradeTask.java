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
package org.apache.iotdb.db.engine.upgrade;

import org.apache.iotdb.db.concurrent.WrappedRunnable;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.service.UpgradeSevice;
import org.apache.iotdb.db.tools.upgrade.TsFileOnlineUpgradeTool;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class UpgradeTask extends WrappedRunnable {

  private TsFileResource upgradeResource;
  private static final Logger logger = LoggerFactory.getLogger(UpgradeTask.class);
  private static final String COMMA_SEPERATOR = ",";


  public UpgradeTask(TsFileResource upgradeResource) {
    this.upgradeResource = upgradeResource;
  }

  @Override
  public void runMayThrow() {
    try {
      List<TsFileResource> upgradedResources = generateUpgradedFiles();
      upgradeResource.writeLock();
      String oldTsfilePath = upgradeResource.getTsFile().getAbsolutePath();
      String oldModificationFilePath = oldTsfilePath + ModificationFile.FILE_SUFFIX;
      try {
        // delete old TsFile and resource
        Files.delete(FSFactoryProducer.getFSFactory().getFile(oldTsfilePath).toPath());
        Files.delete(FSFactoryProducer.getFSFactory()
            .getFile(oldTsfilePath + TsFileResource.RESOURCE_SUFFIX).toPath());
        File modificationFile = FSFactoryProducer.getFSFactory().getFile(oldModificationFilePath);
        // move upgraded TsFiles and modificationFile to their own partition directories
        for (TsFileResource upgradedResource : upgradedResources) {
          File upgradedFile = upgradedResource.getTsFile();
          long partition = upgradedResource.getTimePartition();
          String storageGroupPath = upgradedFile.getParentFile().getParentFile().getParent();
          File partitionDir = FSFactoryProducer.getFSFactory().getFile(storageGroupPath, partition + "");
          if (!partitionDir.exists()) {
            partitionDir.mkdir();
          }
          FSFactoryProducer.getFSFactory().moveFile(upgradedFile,
              FSFactoryProducer.getFSFactory().getFile(partitionDir, upgradedFile.getName()));
          upgradedResource.setFile(
              FSFactoryProducer.getFSFactory().getFile(partitionDir, upgradedFile.getName()));
          // copy mods file to partition directories
          if (modificationFile.exists()) {
            Files.copy(modificationFile.toPath(), 
                FSFactoryProducer.getFSFactory().getFile(partitionDir, upgradedFile.getName()
                    + ModificationFile.FILE_SUFFIX).toPath());
          }
          upgradedResource.serialize();
          // delete tmp partition folder when it is empty
          if (upgradedFile.getParentFile().isDirectory() 
              && upgradedFile.getParentFile().listFiles().length == 0) {
            Files.delete(upgradedFile.getParentFile().toPath());
          }
        }
        // delete old modificationFile 
        if (modificationFile.exists()) {
          Files.delete(modificationFile.toPath());
        }
        // delete upgrade folder when it is empty
        if (upgradeResource.getTsFile().getParentFile().isDirectory()
            && upgradeResource.getTsFile().getParentFile().listFiles().length == 0) {
          Files.delete(upgradeResource.getTsFile().getParentFile().toPath());
        }
        upgradeResource.setUpgradedResources(upgradedResources);
        UpgradeLog.writeUpgradeLogFile(
            oldTsfilePath + COMMA_SEPERATOR + UpgradeCheckStatus.UPGRADE_SUCCESS);
        upgradeResource.getUpgradeTsFileResourceCallBack().call(upgradeResource);
      } finally {
        upgradeResource.writeUnlock();
      }
      UpgradeSevice.setCntUpgradeFileNum(UpgradeSevice.getCntUpgradeFileNum() - 1);
      logger.info("Upgrade completes, file path:{} , the remaining upgraded file num: {}",
          oldTsfilePath, UpgradeSevice.getCntUpgradeFileNum());
    } catch (Exception e) {
      logger.error("meet error when upgrade file:{}", upgradeResource.getTsFile().getAbsolutePath(),
          e);
    }
  }

  private List<TsFileResource> generateUpgradedFiles() throws WriteProcessException {
    upgradeResource.readLock();
    String oldTsfilePath = upgradeResource.getTsFile().getAbsolutePath();
    List<TsFileResource> upgradedResources = new ArrayList<>();
    UpgradeLog.writeUpgradeLogFile(
        oldTsfilePath + COMMA_SEPERATOR + UpgradeCheckStatus.BEGIN_UPGRADE_FILE);
    try {
      TsFileOnlineUpgradeTool.upgradeOneTsfile(oldTsfilePath, upgradedResources);
      UpgradeLog.writeUpgradeLogFile(
          oldTsfilePath + COMMA_SEPERATOR + UpgradeCheckStatus.AFTER_UPGRADE_FILE);
    } catch (IOException e) {
      logger
          .error("generate upgrade file failed, the file to be upgraded:{}", oldTsfilePath, e);
    } finally {
      upgradeResource.readUnlock();
    }
    return upgradedResources;
  }
}
