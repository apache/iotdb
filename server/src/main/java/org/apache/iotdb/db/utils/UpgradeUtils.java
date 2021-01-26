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
package org.apache.iotdb.db.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.upgrade.UpgradeCheckStatus;
import org.apache.iotdb.db.engine.upgrade.UpgradeLog;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.v2.read.TsFileSequenceReaderForV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class UpgradeUtils {

  private static final Logger logger = LoggerFactory.getLogger(UpgradeUtils.class);
  private static final String COMMA_SEPERATOR = ",";
  private static final ReadWriteLock cntUpgradeFileLock = new ReentrantReadWriteLock();
  private static final ReadWriteLock upgradeLogLock = new ReentrantReadWriteLock();

  private static FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  private static Map<String, Integer> upgradeRecoverMap = new HashMap<>();
  public static ReadWriteLock getCntUpgradeFileLock() {
    return cntUpgradeFileLock;
  }

  public static ReadWriteLock getUpgradeLogLock() {
    return upgradeLogLock;
  }

  /**
   * judge whether a tsfile needs to be upgraded
   */
  public static boolean isNeedUpgrade(TsFileResource tsFileResource) {
    tsFileResource.readLock();
    //case the TsFile's length is equal to 0, the TsFile does not need to be upgraded
    try {
      if (tsFileResource.getTsFile().length() == 0) {
        return false;
      }
    } finally {
      tsFileResource.readUnlock();
    }
    tsFileResource.readLock();
    try (TsFileSequenceReaderForV2 tsFileSequenceReader = new TsFileSequenceReaderForV2(
        tsFileResource.getTsFile().getAbsolutePath())) {
      String versionNumber = tsFileSequenceReader.readVersionNumberV2();
      if (versionNumber.equals(TSFileConfig.VERSION_NUMBER_V2) 
          || versionNumber.equals(TSFileConfig.VERSION_NUMBER_V1)) {
        return true;
      }
    } catch (IOException e) {
      logger.error("meet error when judge whether file needs to be upgraded, the file's path:{}",
          tsFileResource.getTsFile().getAbsolutePath(), e);
    } finally {
      tsFileResource.readUnlock();
    }
    return false;
  }

  public static void moveUpgradedFiles(TsFileResource resource) throws IOException {
    List<TsFileResource> upgradedResources = resource.getUpgradedResources();
    for (TsFileResource upgradedResource : upgradedResources) {
      File upgradedFile = upgradedResource.getTsFile();
      long partition = upgradedResource.getTimePartition();
      String virtualStorageGroupDir = upgradedFile.getParentFile().getParentFile().getParent();
      File partitionDir = fsFactory.getFile(virtualStorageGroupDir, String.valueOf(partition));
      if (!partitionDir.exists()) {
        partitionDir.mkdir();
      }
      // move upgraded TsFile
      if (upgradedFile.exists()) {
        fsFactory.moveFile(upgradedFile,
          fsFactory.getFile(partitionDir, upgradedFile.getName()));
      }
      // get temp resource
      File tempResourceFile = fsFactory
          .getFile(upgradedResource.getTsFile().toPath() + TsFileResource.RESOURCE_SUFFIX);
      // move upgraded mods file
      File newModsFile = fsFactory
          .getFile(upgradedResource.getTsFile().toPath() + ModificationFile.FILE_SUFFIX);
      if (newModsFile.exists()) {
        fsFactory.moveFile(newModsFile,
            fsFactory.getFile(partitionDir, newModsFile.getName()));
      }
      // re-serialize upgraded resource to correct place
      upgradedResource.setFile(
          fsFactory.getFile(partitionDir, upgradedFile.getName()));
      if (fsFactory.getFile(partitionDir, newModsFile.getName()).exists()) {
        upgradedResource.getModFile();
      }
      upgradedResource.setClosed(true);
      upgradedResource.serialize();
      // delete generated temp resource file
      Files.delete(tempResourceFile.toPath());
      // delete tmp partition folder when it is empty
      File tmpPartitionDir = upgradedFile.getParentFile();
      if (tmpPartitionDir.isDirectory() && tmpPartitionDir.listFiles().length == 0) {
        Files.delete(tmpPartitionDir.toPath());
      }
      // delete upgrade folder when it is empty
      File upgradeDir = tmpPartitionDir.getParentFile();
      if (upgradeDir.isDirectory() && upgradeDir.listFiles().length == 0) {
        Files.delete(upgradeDir.toPath());
      }
    }
  }

  public static boolean isUpgradedFileGenerated(String oldFileName) {
    return upgradeRecoverMap.containsKey(oldFileName) 
        && upgradeRecoverMap.get(oldFileName) == UpgradeCheckStatus.AFTER_UPGRADE_FILE
        .getCheckStatusCode();
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static void recoverUpgrade() {
    if (FSFactoryProducer.getFSFactory().getFile(UpgradeLog.getUpgradeLogPath()).exists()) {
      try (BufferedReader upgradeLogReader = new BufferedReader(
          new FileReader(
              FSFactoryProducer.getFSFactory().getFile(UpgradeLog.getUpgradeLogPath())))) {
        String line = null;
        while ((line = upgradeLogReader.readLine()) != null) {
          String oldFileName = line.split(COMMA_SEPERATOR)[0];
          if (upgradeRecoverMap.containsKey(oldFileName)) {
            upgradeRecoverMap.put(oldFileName, upgradeRecoverMap.get(oldFileName) + 1);
          } else {
            upgradeRecoverMap.put(oldFileName, 1);
          }
        }
        for (String key : upgradeRecoverMap.keySet()) {
          if (upgradeRecoverMap.get(key) == UpgradeCheckStatus.BEGIN_UPGRADE_FILE
              .getCheckStatusCode()) {
            // delete generated TsFiles and partition directories
            File upgradeDir = FSFactoryProducer.getFSFactory().getFile(key)
                .getParentFile();
            File[] partitionDirs = upgradeDir.listFiles();
            if (partitionDirs == null) {
              return;
            }
            for (File partitionDir : partitionDirs) {
              if (partitionDir.isDirectory()) {
                File[] generatedFiles = partitionDir.listFiles();
                for (File generatedFile : generatedFiles) {
                  if (generatedFile.getName().equals(FSFactoryProducer.getFSFactory()
                      .getFile(key).getName())) {
                    Files.delete(generatedFile.toPath());
                    Files.deleteIfExists(new File(generatedFile + ModificationFile.FILE_SUFFIX).toPath());
                  }
                }
              }
            }
          }
        }
      } catch (IOException e) {
        logger.error("meet error when recover upgrade process, file path:{}",
            UpgradeLog.getUpgradeLogPath(), e);
      } finally {
        FSFactoryProducer.getFSFactory().getFile(UpgradeLog.getUpgradeLogPath()).delete();
      }
    }
  }
}
