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

  private static final String TMP_STRING = "tmp";
  private static final String UPGRADE_FILE_PREFIX = "upgrade_";
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
    tsFileResource.getWriteQueryLock().readLock().lock();
    try (TsFileSequenceReader tsFileSequenceReader = new TsFileSequenceReader(
        tsFileResource.getFile().getAbsolutePath())) {
      if (tsFileSequenceReader.readVersionNumber().equals(TSFileConfig.OLD_VERSION)) {
        return true;
      }
    } catch (IOException e) {
      logger.error("meet error when judge whether file needs to be upgraded, the file's path:{}",
          tsFileResource.getFile().getAbsolutePath(), e);
    } finally {
      tsFileResource.getWriteQueryLock().readLock().unlock();
    }
    return false;
  }

  public static String getUpgradeFileName(TsFileResource upgradeResource) {
    return upgradeResource.getFile().getParentFile().getParent() + File.separator + TMP_STRING
        + File.separator + UPGRADE_FILE_PREFIX + upgradeResource
        .getFile().getName();
  }

  private static String getUpgradeFileName(String upgradingFileName) {
    File upgradingFile = FSFactoryProducer.getFSFactory().getFile(upgradingFileName);
    return upgradingFile.getParentFile().getParent() + File.separator + TMP_STRING
        + File.separator + UPGRADE_FILE_PREFIX + upgradingFile.getName();
  }

  public static void recoverUpgrade() {
    if (FSFactoryProducer.getFSFactory().getFile(UpgradeLog.getUpgradeLogPath()).exists()) {
      try (BufferedReader upgradeLogReader = new BufferedReader(
          new FileReader(
              FSFactoryProducer.getFSFactory().getFile(UpgradeLog.getUpgradeLogPath())))) {
        Map<String, Integer> upgradeRecoverMap = new HashMap<>();
        String line = null;
        while ((line = upgradeLogReader.readLine()) != null) {
          String upgradeFileName = line.split(COMMA_SEPERATOR)[0];
          if (upgradeRecoverMap.containsKey(upgradeFileName)) {
            upgradeRecoverMap.put(upgradeFileName, upgradeRecoverMap.get(upgradeFileName) + 1);
          } else {
            upgradeRecoverMap.put(upgradeFileName, 1);
          }
        }
        for (String key : upgradeRecoverMap.keySet()) {
          String upgradeFileName = getUpgradeFileName(key);
          if (upgradeRecoverMap.get(key) == UpgradeCheckStatus.BEGIN_UPGRADE_FILE
              .getCheckStatusCode()) {
            if (FSFactoryProducer.getFSFactory().getFile(upgradeFileName).exists()) {
              FSFactoryProducer.getFSFactory().getFile(upgradeFileName).delete();
            }
          } else if (upgradeRecoverMap.get(key) == UpgradeCheckStatus.AFTER_UPGRADE_FILE
              .getCheckStatusCode()) {
            FSFactoryProducer.getFSFactory()
                .moveFile(FSFactoryProducer.getFSFactory().getFile(upgradeFileName),
                    FSFactoryProducer.getFSFactory().getFile(key));
            FSFactoryProducer.getFSFactory().getFile(upgradeFileName).getParentFile()
                .delete();
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
