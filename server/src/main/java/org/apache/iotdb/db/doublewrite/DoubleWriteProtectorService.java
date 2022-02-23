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
package org.apache.iotdb.db.doublewrite;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.doublewrite.log.DoubleWriteLogWriter;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.session.pool.SessionPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/** DoubleWriteProtectorService manages the creation of log files and the scheduling of protector */
public class DoubleWriteProtectorService implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DoubleWriteProtectorService.class);

  private final Lock firstLogLock;
  private final Condition firstLogCondition;
  private final Lock logWriterLock;

  private static final String logFileDir = "data" + File.separator + "doublewrite";
  private static final String logFileName = "DBLog";
  private int logFileID;
  private File logFile;
  private DoubleWriteLogWriter logWriter;
  private final int maxLogFileSize;
  private final int maxLogFileCount;
  private final int maxWaitingTime;

  private final SessionPool doubleWriteSessionPool;
  private final ExecutorService protectorThreadPool;

  public DoubleWriteProtectorService(
      Lock firstLogLock, Condition firstLogCondition, SessionPool doubleWriteSessionPool) {
    this.firstLogLock = firstLogLock;
    this.firstLogCondition = firstLogCondition;
    this.logFileID = 0;
    this.doubleWriteSessionPool = doubleWriteSessionPool;
    this.logWriterLock = new ReentrantLock();

    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    maxLogFileSize = config.getDoubleWriteMaxLogSize();
    maxLogFileCount = config.getDoubleWriteProtectorMaxPoolSize();
    maxWaitingTime = config.getDoubleWriteProtectorMaxWaitingTime() * 60 * 1000;
    protectorThreadPool =
        new ThreadPoolExecutor(
            config.getDoubleWriteProtectorCorePoolSize(),
            config.getDoubleWriteProtectorMaxPoolSize(),
            config.getDoubleWriteProtectorKeepAliveTime(),
            TimeUnit.HOURS,
            new ArrayBlockingQueue<>(config.getDoubleWriteProtectorMaxPoolSize()),
            Executors.defaultThreadFactory(),
            new ThreadPoolExecutor.DiscardOldestPolicy());
  }

  @Override
  public void run() {
    // re-transmit remain data when recover
    for (int ID = 0; ID < maxLogFileCount; ID++) {
      File file =
          SystemFileFactory.INSTANCE.getFile(logFileDir + File.separator + logFileName + ID);
      if (file.exists()) {

        protectorThreadPool.execute(
            new DoubleWriteProtector(
                SystemFileFactory.INSTANCE.getFile(logFileDir + File.separator + logFileName + ID),
                doubleWriteSessionPool));
      }
    }

    firstLogLock.lock();
    // create first DoubleWriteLog
    createNewLogFile();
    firstLogCondition.signal();
    firstLogLock.unlock();

    long lastStartTime = System.currentTimeMillis();
    while (true) {
      // switch the DoubleWriteLog if it's too large or existing too long
      if (logFile.length() > maxLogFileSize
          || System.currentTimeMillis() - lastStartTime > maxWaitingTime) {
        switchLogFile();
        lastStartTime = System.currentTimeMillis();
      }

      // check switch condition per minute
      try {
        TimeUnit.MINUTES.sleep(1);
      } catch (InterruptedException e) {
        LOGGER.error("DoubleWriteProtectorService been interrupted", e);
      }
    }
  }

  /** Notify that createNewLogFile is not concurrency safe */
  private void createNewLogFile() {
    // generate new log file ID
    int slotID = -1;
    boolean slotExist = false;
    for (int offset = 1; offset < maxLogFileCount; offset++) {
      slotID = (logFileID + offset) % maxLogFileCount;
      File slot =
          SystemFileFactory.INSTANCE.getFile(logFileDir + File.separator + logFileName + slotID);
      if (!slot.exists()) {
        slotExist = true;
        break;
      }
    }
    if (!slotExist) {
      // random delete the oldest DoubleWriteLogFile
      slotID = new Random().nextInt(maxLogFileCount);
      File slot =
          SystemFileFactory.INSTANCE.getFile(logFileDir + File.separator + logFileName + slotID);
      while (true) {
        if (slot.delete()) {
          LOGGER.info("DoubleWriteProtectorService delete DoubleWriteLogFile {}.", slot.getName());
          break;
        } else {
          LOGGER.error("DoubleWriteProtectorService can't delete {}. Retrying", slot.getName());
        }
      }
    }

    // create log directory
    File directory = SystemFileFactory.INSTANCE.getFile(logFileDir);
    if (!directory.exists()) {
      if (directory.mkdirs()) {
        LOGGER.info("DoubleWriteProtectorService create double write log folder {}.", logFileDir);
      } else {
        LOGGER.warn(
            "DoubleWriteProtectorService create double write log folder {} failed.", logFileDir);
      }
    }

    // create new log file and new log writer
    logFileID = slotID;
    logFile =
        SystemFileFactory.INSTANCE.getFile(logFileDir + File.separator + logFileName + slotID);
    try {
      if (logFile.createNewFile()) {
        logWriterLock.lock();
        if (logWriter != null) {
          logWriter.close();
        }
        logWriter = new DoubleWriteLogWriter(logFile);
        logWriterLock.unlock();

        LOGGER.info(
            "DoubleWriteProtectorService create double write log file {}.", logFile.getPath());
      } else {
        LOGGER.error(
            "DoubleWriteProtectorService create double write log file {} failed.",
            logFile.getPath());
      }
    } catch (IOException e) {
      LOGGER.error("DoubleWriteProtectorService can't create new log file", e);
    }
  }

  private void switchLogFile() {
    // create a protector for last DoubleWriteLog
    protectorThreadPool.execute(
        new DoubleWriteProtector(
            new File(logFileDir + File.separator + logFileName + logFileID),
            doubleWriteSessionPool));
    // create a new DoubleWriteLog
    createNewLogFile();
  }

  /**
   * Note that DoubleWriteLogWriter in DoubleWriteProtectorService is not concurrency save. Make
   * sure that there is at most one DoubleWriteTask uses this DoubleWriteLogWriter.
   */
  public DoubleWriteLogWriter acquireLogWriter() {
    logWriterLock.lock();
    return logWriter;
  }

  public void releaseLogWriter() {
    logWriterLock.unlock();
  }
}
