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
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.service.UpgradeSevice;
import org.apache.iotdb.db.tools.upgrade.UpgradeTool;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
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
      upgradeResource.getWriteQueryLock().readLock().lock();
      String oldTsfilePath = upgradeResource.getFile().getAbsolutePath();
      List<String> upgradedFiles = new ArrayList<>();
      List<TsFileResource> upgradedResources = new ArrayList<>();
      UpgradeLog.writeUpgradeLogFile(
          oldTsfilePath + COMMA_SEPERATOR + UpgradeCheckStatus.BEGIN_UPGRADE_FILE);
      try {
        UpgradeTool.upgradeOneTsfile(oldTsfilePath, upgradedFiles, upgradedResources);
        UpgradeLog.writeUpgradeLogFile(
            oldTsfilePath + COMMA_SEPERATOR + UpgradeCheckStatus.AFTER_UPGRADE_FILE);
      } catch (IOException e) {
        logger
            .error("generate upgrade file failed, the file to be upgraded:{}", oldTsfilePath, e);
      } finally {
        upgradeResource.getWriteQueryLock().readLock().unlock();
      }
      upgradeResource.getWriteQueryLock().writeLock().lock();
      try {
        // delete old TsFile
        upgradeResource.remove();
        // move upgraded TsFiles to their own partition directories
        for (TsFileResource upgradedResource : upgradedResources) {
          File upgradedFile = upgradedResource.getFile();
          long partition = upgradedResource.getTimePartitionWithCheck();
          String storageGroupPath = upgradedFile.getParentFile().getParentFile().getParent();
          File partitionDir = FSFactoryProducer.getFSFactory().getFile(storageGroupPath, partition + "");
          if (!partitionDir.exists()) {
            partitionDir.mkdir();
          }
          FSFactoryProducer.getFSFactory().moveFile(upgradedFile,
              FSFactoryProducer.getFSFactory().getFile(partitionDir, upgradedFile.getName()));
          upgradedResource.setFile(
              FSFactoryProducer.getFSFactory().getFile(partitionDir, upgradedFile.getName()));
          upgradedResource.serialize();
        }
        upgradeResource.setUpgradedResources(upgradedResources);
        UpgradeLog.writeUpgradeLogFile(
            oldTsfilePath + COMMA_SEPERATOR + UpgradeCheckStatus.UPGRADE_SUCCESS);
        upgradeResource.getUpgradeTsFileResourceCallBack().call(upgradeResource);
      } finally {
        upgradeResource.getWriteQueryLock().writeLock().unlock();
      }
      UpgradeSevice.setCntUpgradeFileNum(UpgradeSevice.getCntUpgradeFileNum() - 1);
      logger.info("Upgrade completes, file path:{} , the remaining upgraded file num: {}",
          oldTsfilePath, UpgradeSevice.getCntUpgradeFileNum());
    } catch (Exception e) {
      logger.error("meet error when upgrade file:{}", upgradeResource.getFile().getAbsolutePath(),
          e);
    }
  }
}
