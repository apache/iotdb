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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.upgrade.UpgradeCheckStatus;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpgradeUtils {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(UpgradeUtils.class);
  private static final String UPGRADE_DIR = "upgrade";
  private static final String UPGRADE_LOG_NAME = "upgrade.txt";
  private static File upgradeLogPath = SystemFileFactory.INSTANCE
      .getFile(SystemFileFactory.INSTANCE.getFile(config.getSystemDir(), UPGRADE_DIR),
          UPGRADE_LOG_NAME);

  private static final ReadWriteLock upgradeLogLock = new ReentrantReadWriteLock();

  public static ReadWriteLock getUpgradeLogLock() {
    return upgradeLogLock;
  }

  public static boolean isNeedUpgrade(TsFileResource tsFileResource) {
    try (TsFileSequenceReader tsFileSequenceReader = new TsFileSequenceReader(
        tsFileResource.getFile().getAbsolutePath())) {
      if (tsFileSequenceReader.readVersionNumber().equals(TSFileConfig.OLD_VERSION)) {
        return true;
      }
    } catch (IOException e) {
      logger.error("meet error when judge whether file needs to be upgraded, the file's path:{}",
          tsFileResource.getFile().getAbsolutePath(), e);
    }
    return false;
  }

  public static boolean createUpgradeLog() {
    try {
      if (!upgradeLogPath.getParentFile().exists()) {
        upgradeLogPath.getParentFile().mkdirs();
      }
      upgradeLogPath.createNewFile();
      return true;
    } catch (IOException e) {
      logger.error("meet error when create upgrade log, file path:{}",
          upgradeLogPath, e);
      return false;
    }
  }

  public static String getUpgradeLogPath() {
    return upgradeLogPath.getAbsolutePath();
  }

  public static void recoverUpgrade() {
    if (FSFactoryProducer.getFSFactory().getFile(getUpgradeLogPath()).exists()) {
      try (BufferedReader upgradeLogReader = new BufferedReader(
          new FileReader(FSFactoryProducer.getFSFactory().getFile(getUpgradeLogPath())))) {
        String line = null;
        while ((line = upgradeLogReader.readLine()) != null) {
          if (Long.parseLong(line.split(",")[2]) == UpgradeCheckStatus.BEGIN_UPGRADE_FILE) {
            if (FSFactoryProducer.getFSFactory().getFile(line.split(",")[1]).exists()) {
              FSFactoryProducer.getFSFactory().getFile(line.split(",")[1]).delete();
            }
          } else if (Long.parseLong(line.split(",")[2]) == UpgradeCheckStatus.AFTER_UPGRADE_FILE) {
            FSFactoryProducer.getFSFactory()
                .moveFile(FSFactoryProducer.getFSFactory().getFile(line.split(",")[1]),
                    FSFactoryProducer.getFSFactory().getFile(line.split(",")[0]));
            FSFactoryProducer.getFSFactory().getFile(line.split(",")[1]).getParentFile().delete();
          }
        }
      } catch (IOException e) {
        logger.error("meet error when recover upgrade process, file path:{}",
            upgradeLogPath, e);
      } finally {
        FSFactoryProducer.getFSFactory().getFile(getUpgradeLogPath()).delete();
      }
    }
  }
}
