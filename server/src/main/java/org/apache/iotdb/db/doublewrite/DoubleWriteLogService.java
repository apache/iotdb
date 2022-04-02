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

import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.writelog.io.LogWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DoubleWriteLogService implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(DoubleWriteLogService.class);

  private static final String logFileDir;
  private static final int logFileValidity;
  private static final int maxLogFileNum;

  private final DoubleWriteProtector protector;
  private final Lock logWriterLock;
  private final String logFileName;
  private int logFileID;
  private long logFileCreateTime;
  private File logFile;
  private LogWriter logWriter;

  public DoubleWriteLogService(String logFileName, DoubleWriteProtector protector) {
    this.logFileName = logFileName;
    this.protector = protector;

    this.logWriterLock = new ReentrantLock();
    this.logFile = null;
    this.logWriter = null;
  }

  @Override
  public void run() {
    // Check if there exists remnant logs
    List<Integer> logFileIDList = new ArrayList<>();
    for (int ID = 0; ID < maxLogFileNum; ID++) {
      File file =
        SystemFileFactory.INSTANCE.getFile(logFileDir + File.separator + logFileName + ID);
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
        protector.registerLogFile(logFileDir + File.separator + logFileName + logFileIDList.get(i));
      }
      for (int i = 0; i < firstID; i++) {
        protector.registerLogFile(logFileDir + File.separator + logFileName + logFileIDList.get(i));
      }

      int nextID;
      if (firstID == 0) {
        nextID = logFileIDList.get(logFileIDList.size() - 1) + 1;
      } else {
        nextID = logFileIDList.get(firstID - 1) + 1;
      }
      logFileID = nextID % maxLogFileNum;
    } else {
      logFileID = 0;
    }

    while (true) {
      // Check the validity of logFile
      logWriterLock.lock();
      if (logWriter != null && System.currentTimeMillis() - logFileCreateTime > logFileValidity) {
        // Submit logFile when it's expired
        submitLogFile();
      }
      logWriterLock.unlock();

      try {
        // Sleep 10s before next check
        TimeUnit.SECONDS.sleep(10);
      } catch (InterruptedException e) {
        LOGGER.error("DoubleWriteLogService been interrupted", e);
      }
    }
  }

  private void submitLogFile() {
    for (int retry = 0; retry < 5; retry++) {
      try {
        logWriter.close();
      } catch (IOException e) {
        LOGGER.warn("Can't close DoubleWriteLog: {}, retrying...", logFile.getAbsolutePath());
        try {
          // Sleep 1s and retry
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException ignored) {
          // Ignore and retry
        }
        continue;
      }

      LOGGER.info("DoubleWriteLog: {} is expired and closed", logFile.getAbsolutePath());
      break;
    }

    protector.registerLogFile(logFileDir + File.separator + logFileName + (logFileID - 1 + maxLogFileNum) % maxLogFileNum);

    logWriter = null;
    logFile = null;
  }

  private void createLogFile() {
    logFile = SystemFileFactory.INSTANCE.getFile(logFileDir + File.separator + logFileName + logFileID);
    while (true) {
      try {
        if (logFile.createNewFile()) {
          logFileCreateTime = System.currentTimeMillis();
          logWriter = new LogWriter(logFile, false);
          LOGGER.info("Create DoubleWriteLog: {}", logFile.getAbsolutePath());
          break;
        }
      } catch (IOException e) {
        LOGGER.warn("Can't create DoubleWriteLog: {}, retrying...", logFile.getAbsolutePath());
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException ignored) {
          // Ignore and retry
        }
      }
    }
    logFileID = (logFileID + 1) % maxLogFileNum;
  }

  public LogWriter acquireLogWriter() {
    logWriterLock.lock();
    if (logWriter == null) {
      // Create logFile when there are no valid
      createLogFile();
    }
    return logWriter;
  }

  public void releaseLogWriter() {
    logWriterLock.unlock();
  }
}
