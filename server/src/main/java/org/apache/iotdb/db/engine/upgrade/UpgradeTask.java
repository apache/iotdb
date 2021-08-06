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
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.service.UpgradeSevice;
import org.apache.iotdb.db.tools.upgrade.TsFileOnlineUpgradeTool;
import org.apache.iotdb.db.utils.UpgradeUtils;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;

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

  private FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  public UpgradeTask(TsFileResource upgradeResource) {
    this.upgradeResource = upgradeResource;
  }

  @Override
  public void runMayThrow() {
    try {
      String oldTsfilePath = upgradeResource.getTsFile().getAbsolutePath();
      List<TsFileResource> upgradedResources;
      if (!UpgradeUtils.isUpgradedFileGenerated(upgradeResource.getTsFile().getName())) {
        logger.info("generate upgraded file for {}", upgradeResource.getTsFile());
        upgradedResources = generateUpgradedFiles();
      } else {
        logger.info("find upgraded file for {}", upgradeResource.getTsFile());
        upgradedResources = findUpgradedFiles();
      }
      upgradeResource.setUpgradedResources(upgradedResources);
      upgradeResource.getUpgradeTsFileResourceCallBack().call(upgradeResource);
      UpgradeSevice.getTotalUpgradeFileNum().getAndAdd(-1);
      logger.info(
          "Upgrade completes, file path:{} , the remaining upgraded file num: {}",
          oldTsfilePath,
          UpgradeSevice.getTotalUpgradeFileNum().get());
      if (UpgradeSevice.getTotalUpgradeFileNum().get() == 0) {
        logger.info("Start delete empty tmp folders");
        clearTmpFolders(DirectoryManager.getInstance().getAllSequenceFileFolders());
        clearTmpFolders(DirectoryManager.getInstance().getAllUnSequenceFileFolders());
        UpgradeSevice.getINSTANCE().stop();
        logger.info("All files upgraded successfully! ");
      }
    } catch (Exception e) {
      logger.error(
          "meet error when upgrade file:{}", upgradeResource.getTsFile().getAbsolutePath(), e);
    }
  }

  private List<TsFileResource> generateUpgradedFiles() throws IOException, WriteProcessException {
    upgradeResource.readLock();
    String oldTsfilePath = upgradeResource.getTsFile().getAbsolutePath();
    List<TsFileResource> upgradedResources = new ArrayList<>();
    UpgradeLog.writeUpgradeLogFile(
        oldTsfilePath + COMMA_SEPERATOR + UpgradeCheckStatus.BEGIN_UPGRADE_FILE);
    try {
      TsFileOnlineUpgradeTool.upgradeOneTsFile(upgradeResource, upgradedResources);
      UpgradeLog.writeUpgradeLogFile(
          oldTsfilePath + COMMA_SEPERATOR + UpgradeCheckStatus.AFTER_UPGRADE_FILE);
    } finally {
      upgradeResource.readUnlock();
    }
    return upgradedResources;
  }

  private List<TsFileResource> findUpgradedFiles() throws IOException {
    upgradeResource.readLock();
    List<TsFileResource> upgradedResources = new ArrayList<>();
    String oldTsfilePath = upgradeResource.getTsFile().getAbsolutePath();
    UpgradeLog.writeUpgradeLogFile(
        oldTsfilePath + COMMA_SEPERATOR + UpgradeCheckStatus.BEGIN_UPGRADE_FILE);
    try {
      File upgradeFolder = upgradeResource.getTsFile().getParentFile();
      for (File tempPartitionDir : upgradeFolder.listFiles()) {
        if (tempPartitionDir.isDirectory()
            && fsFactory
                .getFile(
                    tempPartitionDir,
                    upgradeResource.getTsFile().getName() + TsFileResource.RESOURCE_SUFFIX)
                .exists()) {
          TsFileResource resource =
              new TsFileResource(
                  fsFactory.getFile(tempPartitionDir, upgradeResource.getTsFile().getName()));
          resource.deserialize();
          upgradedResources.add(resource);
        }
      }
      UpgradeLog.writeUpgradeLogFile(
          oldTsfilePath + COMMA_SEPERATOR + UpgradeCheckStatus.AFTER_UPGRADE_FILE);
    } finally {
      upgradeResource.readUnlock();
    }
    return upgradedResources;
  }

  private void clearTmpFolders(List<String> folders) {
    for (String baseDir : folders) {
      File fileFolder = fsFactory.getFile(baseDir);
      if (!fileFolder.isDirectory()) {
        continue;
      }
      for (File storageGroup : fileFolder.listFiles()) {
        if (!storageGroup.isDirectory()) {
          continue;
        }
        File virtualStorageGroupDir = fsFactory.getFile(storageGroup, "0");
        File upgradeDir = fsFactory.getFile(virtualStorageGroupDir, "upgrade");

        File[] tmpPartitionDirList = upgradeDir.listFiles();
        for (File tmpPartitionDir : tmpPartitionDirList) {
          if (tmpPartitionDir.isDirectory()) {
            try {
              Files.delete(tmpPartitionDir.toPath());
            } catch (IOException e) {
              logger.error("Delete tmpPartitionDir {} failed", tmpPartitionDir);
            }
          }
        }
        // delete upgrade folder when it is empty
        if (upgradeDir.isDirectory()) {
          try {
            Files.delete(upgradeDir.toPath());
          } catch (IOException e) {
            logger.error("Delete tmpUpgradeDir {} failed", upgradeDir);
          }
        }
      }
    }
  }
}
