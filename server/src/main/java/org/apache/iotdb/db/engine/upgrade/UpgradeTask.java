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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.service.UpgradeSevice;
import org.apache.iotdb.db.utils.UpgradeUtils;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.tool.upgrade.UpgradeTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpgradeTask implements Runnable {

  private final TsFileResource upgradeResource;
  private static final Logger logger = LoggerFactory.getLogger(UpgradeTask.class);
  private static final String DOT_SEPERATOR = ",";

  public UpgradeTask(TsFileResource upgradeResource) {
    this.upgradeResource = upgradeResource;
  }


  private static boolean writeUpgradeLogFile(RandomAccessFile upgradeLog, String content,
      long position) {
    UpgradeUtils.getUpgradeLogLock().writeLock().lock();
    try {
      upgradeLog.seek(position);
      upgradeLog.write(content.getBytes());
      upgradeLog.write(System.getProperty("line.separator").getBytes());
      return true;
    } catch (IOException e) {
      logger.error("write upgrade log file failed, the log file:{}", upgradeLog);
      return false;
    } finally {
      UpgradeUtils.getUpgradeLogLock().writeLock().unlock();
    }
  }

  private static long getUpgradeLogLength(RandomAccessFile upgradeLog) {
    UpgradeUtils.getUpgradeLogLock().readLock().lock();
    try {
      return upgradeLog.length();
    } catch (IOException e) {
      logger.error("read upgrade log file failed, the log file:{}", upgradeLog);
      return 0;
    } finally {
      UpgradeUtils.getUpgradeLogLock().readLock().unlock();
    }
  }

  @Override
  public void run() {
    try (RandomAccessFile upgradeLogFile = new RandomAccessFile(UpgradeUtils.getUpgradeLogPath(),
        "rw")) {
      upgradeResource.getWriteQueryLock().readLock().lock();
      String tsfilePathBefore = upgradeResource.getFile().getAbsolutePath();
      String tsfilePathAfter =
          upgradeResource.getFile().getParentFile().getParent() + File.separator + "tmp"
              + File.separator + "upgrade_" + upgradeResource
              .getFile().getName();

      long upgradePostion = getUpgradeLogLength(upgradeLogFile);
      writeUpgradeLogFile(upgradeLogFile,
          tsfilePathBefore + DOT_SEPERATOR + tsfilePathAfter + DOT_SEPERATOR
              + UpgradeCheckStatus.BEGIN_UPGRADE_FILE, upgradePostion);
      try {
        UpgradeTool.upgradeOneTsfile(tsfilePathBefore, tsfilePathAfter);
        writeUpgradeLogFile(upgradeLogFile,
            tsfilePathBefore + DOT_SEPERATOR + tsfilePathAfter + DOT_SEPERATOR
                + UpgradeCheckStatus.AFTER_UPGRADE_FILE, upgradePostion);
      } catch (IOException e) {
        logger.error("generate upgrade file failed, the file to be upgraded:{}", tsfilePathBefore);
      } finally {
        upgradeResource.getWriteQueryLock().readLock().unlock();
      }
      upgradeResource.getWriteQueryLock().writeLock().lock();
      try {
        FSFactoryProducer.getFSFactory().getFile(tsfilePathBefore).delete();
        FSFactoryProducer.getFSFactory()
            .moveFile(FSFactoryProducer.getFSFactory().getFile(tsfilePathAfter),
                FSFactoryProducer.getFSFactory().getFile(tsfilePathBefore));
        writeUpgradeLogFile(upgradeLogFile,
            tsfilePathBefore + DOT_SEPERATOR + tsfilePathAfter + DOT_SEPERATOR
                + UpgradeCheckStatus.UPGRADE_SUCCESS, upgradePostion);
        FSFactoryProducer.getFSFactory().getFile(tsfilePathAfter).getParentFile().delete();
      } finally {
        upgradeResource.getWriteQueryLock().writeLock().unlock();
      }
      UpgradeSevice.setCntUpgradeFileNum(UpgradeSevice.getCntUpgradeFileNum() - 1);
      logger.info("Upgrade completes, file path:{} , the remaining upgraded file num: {}",
          tsfilePathBefore, UpgradeSevice.getCntUpgradeFileNum());
    } catch (Exception e) {
      logger.error("meet error when upgrade file:{} :", upgradeResource.getFile().getAbsolutePath(),
          e);
    }
  }
}
