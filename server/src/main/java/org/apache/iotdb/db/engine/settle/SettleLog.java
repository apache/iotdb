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

package org.apache.iotdb.db.engine.settle;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SettleLog {
  private static final Logger logger = LoggerFactory.getLogger(SettleLog.class);
  public static final String COMMA_SEPERATOR = ",";
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String SETTLE_DIR = "settle";
  private static final String SETTLE_LOG_NAME = "settle.txt";
  private static BufferedWriter settleLogWriter;
  private static File settleLogPath = // the path of upgrade log is "data/system/settle/settle.txt"
      SystemFileFactory.INSTANCE.getFile(
          SystemFileFactory.INSTANCE.getFile(config.getSystemDir(), SETTLE_DIR), SETTLE_LOG_NAME);

  private static final ReadWriteLock settleLogFileLock = new ReentrantReadWriteLock();

  public static boolean createSettleLog() {
    try {
      if (!settleLogPath.getParentFile().exists()) {
        settleLogPath.getParentFile().mkdirs();
      }
      settleLogPath.createNewFile();
      settleLogWriter = new BufferedWriter(new FileWriter(getSettleLogPath(), true));
      return true;
    } catch (IOException e) {
      logger.error("meet error when creating settle log, file path:{}", settleLogPath, e);
      return false;
    }
  }

  public static boolean writeSettleLog(String content) {
    settleLogFileLock.writeLock().lock();
    try {
      settleLogWriter.write(content);
      settleLogWriter.newLine();
      settleLogWriter.flush();
      return true;
    } catch (IOException e) {
      logger.error("write settle log file failed, the log file:{}", getSettleLogPath(), e);
      return false;
    } finally {
      settleLogFileLock.writeLock().unlock();
    }
  }

  public static void closeLogWriter() {
    try {
      if (settleLogWriter != null) {
        settleLogWriter.close();
      }
    } catch (IOException e) {
      logger.error("close upgrade log file failed, the log file:{}", getSettleLogPath(), e);
    }
  }

  public static String getSettleLogPath() { // "data/system/settle/settle.txt"
    return settleLogPath.getAbsolutePath();
  }

  public static void setSettleLogPath(File settleLogPath) {
    SettleLog.settleLogPath = settleLogPath;
  }

  public enum SettleCheckStatus {
    BEGIN_SETTLE_FILE(1),
    AFTER_SETTLE_FILE(2),
    SETTLE_SUCCESS(3);

    private final int checkStatus;

    SettleCheckStatus(int checkStatus) {
      this.checkStatus = checkStatus;
    }

    public int getCheckStatus() {
      return checkStatus;
    }

    @Override
    public String toString() {
      return String.valueOf(checkStatus);
    }
  }
}
