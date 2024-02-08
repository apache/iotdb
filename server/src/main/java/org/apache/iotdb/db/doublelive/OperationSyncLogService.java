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
package org.apache.iotdb.db.doublelive;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.writelog.io.LogWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class OperationSyncLogService implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(OperationSyncLogService.class);

  private static final String LOG_FILE_DIR =
      IoTDBDescriptor.getInstance().getConfig().getOperationSyncLogDir();
  private static final long LOG_FILE_VALIDITY =
      IoTDBDescriptor.getInstance().getConfig().getOperationSyncLogValidity() * 1000L;
  private static final int MAX_LOG_FILE_NUM =
      IoTDBDescriptor.getInstance().getConfig().getOperationSyncLogNum();
  private static final long MAX_LOG_FILE_SIZE =
      IoTDBDescriptor.getInstance().getConfig().getOperationSyncMaxLogSize();

  private static long currentLogFileSize = 0;

  private final OperationSyncProtector protector;
  private final Lock logWriterLock;
  private final String logFileName;
  private int logFileID;
  private long logFileCreateTime;
  private File logFile;
  private LogWriter logWriter;

  public OperationSyncLogService(String logFileName, OperationSyncProtector protector) {
    this.logFileName = logFileName;
    this.protector = protector;

    this.logWriterLock = new ReentrantLock();
    this.logFile = null;
    this.logWriter = null;

    File logDir = new File(LOG_FILE_DIR);
    if (!logDir.exists()) {
      if (!logDir.mkdirs()) {
        LOGGER.error("Can't make OperationSyncLog file dir: {}", logDir.getAbsolutePath());
      }
    }
  }

  @Override
  public void run() {
    // Check if there exists remnant logs
    List<Integer> logFileIDList = new ArrayList<>();
    for (int ID = 0; ID < MAX_LOG_FILE_NUM; ID++) {
      File file =
          SystemFileFactory.INSTANCE.getFile(LOG_FILE_DIR + File.separator + logFileName + ID);
      if (file.exists()) {
        logFileIDList.add(ID);
      }
    }

    int firstID = 0;
    if (logFileIDList.size() > 0) {
      // Re-transmit the remnant logs
      for (int i = 0; i < logFileIDList.size() - 1; i++) {
        if (logFileIDList.get(i + 1) - logFileIDList.get(i) > 1) {
          firstID = i + 1;
          break;
        }
      }

      for (int i = firstID; i < logFileIDList.size(); i++) {
        protector.registerLogFile(
            LOG_FILE_DIR + File.separator + logFileName + logFileIDList.get(i));
      }
      for (int i = 0; i < firstID; i++) {
        protector.registerLogFile(
            LOG_FILE_DIR + File.separator + logFileName + logFileIDList.get(i));
      }

      int nextID;
      if (firstID == 0) {
        nextID = logFileIDList.get(logFileIDList.size() - 1) + 1;
      } else {
        nextID = logFileIDList.get(firstID - 1) + 1;
      }
      logFileID = nextID % MAX_LOG_FILE_NUM;
    } else {
      logFileID = 0;
    }

    while (true) {
      // Check the validity of logFile
      logWriterLock.lock();
      try {
        if (logWriter != null
            && System.currentTimeMillis() - logFileCreateTime > LOG_FILE_VALIDITY) {
          // Submit logFile when it's expired
          submitLogFile();
        }
      } finally {
        logWriterLock.unlock();
      }

      try {
        // Sleep 10s before next check
        TimeUnit.SECONDS.sleep(10);
      } catch (InterruptedException e) {
        LOGGER.error("OperationSyncLogService been interrupted", e);
      }
    }
  }

  private void submitLogFile() {
    try {
      logWriter.force();
    } catch (IOException e) {
      LOGGER.error("Can't force logWrite", e);
    }
    incLogFileSize(logFile.length());

    for (int retry = 0; retry < 5; retry++) {
      try {
        logWriter.close();
      } catch (IOException e) {
        LOGGER.warn("Can't close OperationSyncLog: {}, retrying...", logFile.getAbsolutePath());
        try {
          // Sleep 1s and retry
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException ignored) {
          // Ignore and retry
        }
        continue;
      }

      LOGGER.info("OperationSyncLog: {} is expired and closed", logFile.getAbsolutePath());
      break;
    }

    protector.registerLogFile(
        LOG_FILE_DIR
            + File.separator
            + logFileName
            + (logFileID - 1 + MAX_LOG_FILE_NUM) % MAX_LOG_FILE_NUM);

    logWriter = null;
    logFile = null;
  }

  private void createLogFile() {
    logFile =
        SystemFileFactory.INSTANCE.getFile(LOG_FILE_DIR + File.separator + logFileName + logFileID);
    while (true) {
      try {
        if (logFile.createNewFile()) {
          logFileCreateTime = System.currentTimeMillis();
          logWriter = new LogWriter(logFile, false);
          LOGGER.info("Create OperationSyncLog: {}", logFile.getAbsolutePath());
          break;
        }
      } catch (IOException e) {
        LOGGER.warn("Can't create OperationSyncLog: {}, retrying...", logFile.getAbsolutePath());
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException ignored) {
          // Ignore and retry
        }
      }
    }
    logFileID = (logFileID + 1) % MAX_LOG_FILE_NUM;
  }

  public static synchronized void incLogFileSize(long size) {
    currentLogFileSize = currentLogFileSize + size;
  }

  public void acquireLogWriter() {
    logWriterLock.lock();
  }

  public void write(ByteBuffer buffer) throws IOException {
    if (currentLogFileSize < MAX_LOG_FILE_SIZE) {
      if (logWriter == null) {
        // Create logFile when there are no valid
        createLogFile();
      }
      logWriter.write(buffer);
    } else {
      LOGGER.error("The OperationSyncLog is full, new PhysicalPlans will be discarded.");
    }
  }

  public void releaseLogWriter() {
    logWriterLock.unlock();
  }
}
