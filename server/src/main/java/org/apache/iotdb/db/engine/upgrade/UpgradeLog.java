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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.utils.UpgradeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class UpgradeLog {

  private static final Logger logger = LoggerFactory.getLogger(UpgradeLog.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String UPGRADE_DIR = "upgrade";
  private static final String UPGRADE_LOG_NAME = "upgrade.txt";
  private static BufferedWriter upgradeLogWriter;
  private static File upgradeLogPath =
      SystemFileFactory.INSTANCE.getFile(
          SystemFileFactory.INSTANCE.getFile(config.getSystemDir(), UPGRADE_DIR), UPGRADE_LOG_NAME);

  public static boolean createUpgradeLog() {
    try {
      if (!upgradeLogPath.getParentFile().exists()) {
        upgradeLogPath.getParentFile().mkdirs();
      }
      upgradeLogPath.createNewFile();
      upgradeLogWriter = new BufferedWriter(new FileWriter(getUpgradeLogPath(), true));
      return true;
    } catch (IOException e) {
      logger.error("meet error when create upgrade log, file path:{}", upgradeLogPath, e);
      return false;
    }
  }

  public static String getUpgradeLogPath() {
    return upgradeLogPath.getAbsolutePath();
  }

  public static boolean writeUpgradeLogFile(String content) {
    UpgradeUtils.getUpgradeLogLock().writeLock().lock();
    try {
      upgradeLogWriter.write(content);
      upgradeLogWriter.newLine();
      upgradeLogWriter.flush();
      return true;
    } catch (IOException e) {
      logger.error("write upgrade log file failed, the log file:{}", getUpgradeLogPath(), e);
      return false;
    } finally {
      UpgradeUtils.getUpgradeLogLock().writeLock().unlock();
    }
  }

  public static void closeLogWriter() {
    try {
      if (upgradeLogWriter != null) {
        upgradeLogWriter.close();
      }
    } catch (IOException e) {
      logger.error("close upgrade log file failed, the log file:{}", getUpgradeLogPath(), e);
    }
  }
}
