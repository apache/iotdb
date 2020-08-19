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
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.upgrade.UpgradeCheckStatus;
import org.apache.iotdb.db.engine.upgrade.UpgradeLog;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class UpgradeUtils {

  private static final Logger logger = LoggerFactory.getLogger(UpgradeUtils.class);
  private static final String COMMA_SEPERATOR = ",";
  private static final ReadWriteLock cntUpgradeFileLock = new ReentrantReadWriteLock();
  private static final ReadWriteLock upgradeLogLock = new ReentrantReadWriteLock();

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
    if (tsFileResource.getTsFile().length() == 0) {
      return false;
    }
    try (TsFileSequenceReader tsFileSequenceReader = new TsFileSequenceReader(
        tsFileResource.getTsFile().getAbsolutePath())) {
      if (tsFileSequenceReader.readVersionNumber().equals(TSFileConfig.VERSION_NUMBER_V1)) {
        return true;
      }
    } catch (Exception e) {
      logger.error("meet error when judge whether file needs to be upgraded, the file's path:{}",
          tsFileResource.getTsFile().getAbsolutePath(), e);
    } finally {
      tsFileResource.readUnlock();
    }
    return false;
  }

  /**
   * Since one old TsFile may be upgraded to multiple upgraded files, 
   * this method is for getting the name of one of the upgraded file. 
   * 
   * @param upgradeResource TsFile resource to be upgraded
   * @return name of upgraded file
   * 
   */
  public static String getOneUpgradedFileName(TsFileResource upgradeResource)
      throws IOException {
    upgradeResource.deserialize();
    long firstPartitionId = upgradeResource.getTimePartition();
    File oldTsFile = upgradeResource.getTsFile();
    return oldTsFile.getParent()
        + File.separator + firstPartitionId + File.separator+ oldTsFile.getName();
  }

  public static void recoverUpgrade() {
    if (FSFactoryProducer.getFSFactory().getFile(UpgradeLog.getUpgradeLogPath()).exists()) {
      try (BufferedReader upgradeLogReader = new BufferedReader(
          new FileReader(
              FSFactoryProducer.getFSFactory().getFile(UpgradeLog.getUpgradeLogPath())))) {
        Map<String, Integer> upgradeRecoverMap = new HashMap<>();
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
            for (File partitionDir : partitionDirs) {
              if (partitionDir.isDirectory()) {
                File[] generatedFiles = partitionDir.listFiles();
                for (File generatedFile : generatedFiles) {
                  if (generatedFile.getName().equals(FSFactoryProducer.getFSFactory()
                      .getFile(key).getName())) {
                    Files.delete(generatedFile.toPath());
                  }
                }
              }
            }
          } else if (upgradeRecoverMap.get(key) == UpgradeCheckStatus.AFTER_UPGRADE_FILE
              .getCheckStatusCode()) {
            String upgradedFileName = getOneUpgradedFileName(new TsFileResource(
                FSFactoryProducer.getFSFactory().getFile(key)));
            if (FSFactoryProducer.getFSFactory().getFile(key).exists() && FSFactoryProducer
                .getFSFactory().getFile(upgradedFileName).exists()) {
              // if both old tsfile and upgrade file exists, delete the old tsfile and resource
              Files.delete(FSFactoryProducer.getFSFactory().getFile(key).toPath());
              Files.delete(FSFactoryProducer.getFSFactory().getFile(key 
                  + TsFileResource.RESOURCE_SUFFIX).toPath());
            } 
            // move the upgrade files and resources to their own partition directories
            File upgradeDir = FSFactoryProducer.getFSFactory().getFile(key)
                .getParentFile();
            String storageGroupPath = upgradeDir.getParent();
            File[] partitionDirs = upgradeDir.listFiles();
            for (File partitionDir : partitionDirs) {
              if (partitionDir.isDirectory()) {
                String partitionId = partitionDir.getName();
                File destPartitionDir = FSFactoryProducer.getFSFactory().getFile(storageGroupPath, partitionId);
                if (!destPartitionDir.exists()) {
                  destPartitionDir.mkdir();
                }
                File[] generatedFiles = partitionDir.listFiles();
                for (File generatedFile : generatedFiles) {
                  FSFactoryProducer.getFSFactory().moveFile(generatedFile, 
                      FSFactoryProducer.getFSFactory().getFile(destPartitionDir, generatedFile.getName()));
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
