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
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.writelog.io.SingleFileLogReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class OperationSyncProtector implements Runnable {

  protected static final Logger LOGGER = LoggerFactory.getLogger(OperationSyncProtector.class);
  protected static final int LOG_FILE_VALIDITY =
      IoTDBDescriptor.getInstance().getConfig().getOperationSyncLogValidity();

  // For transmit log files
  protected final Lock logFileListLock;
  protected List<String> registeredLogFiles;
  protected List<String> processingLogFiles;

  // For serialize PhysicalPlan
  private static final int MAX_PHYSICALPLAN_SIZE = 16 * 1024 * 1024;
  protected final ByteArrayOutputStream protectorByteStream;
  protected final DataOutputStream protectorDeserializeStream;

  // Working state
  protected volatile boolean isProtectorAtWork;

  protected OperationSyncProtector() {
    logFileListLock = new ReentrantLock();
    registeredLogFiles = new ArrayList<>();

    protectorByteStream = new ByteArrayOutputStream(MAX_PHYSICALPLAN_SIZE);
    protectorDeserializeStream = new DataOutputStream(protectorByteStream);

    isProtectorAtWork = false;
  }

  protected void registerLogFile(String logFile) {
    logFileListLock.lock();
    try {
      registeredLogFiles.add(logFile);
    } finally {
      logFileListLock.unlock();
    }
  }

  protected void wrapLogFiles() {
    processingLogFiles = new ArrayList<>(registeredLogFiles);
    registeredLogFiles = new ArrayList<>();
  }

  @Override
  public void run() {
    while (true) {
      while (true) {
        // Wrap and transmit all OperationSyncLogs
        logFileListLock.lock();
        try {
          if (registeredLogFiles.size() > 0) {
            isProtectorAtWork = true;
            wrapLogFiles();
          } else {
            isProtectorAtWork = false;
            break;
          }
        } finally {
          logFileListLock.unlock();
        }
        if (isProtectorAtWork) {
          transmitLogFiles();
        }
      }

      try {
        // Sleep a while before next check
        TimeUnit.SECONDS.sleep(LOG_FILE_VALIDITY);
      } catch (InterruptedException e) {
        LOGGER.warn("OperationSyncProtector been interrupted", e);
      }
    }
  }

  protected void transmitLogFiles() {
    preCheck();
    for (String logFileName : processingLogFiles) {
      File logFile = SystemFileFactory.INSTANCE.getFile(logFileName);
      SingleFileLogReader logReader;
      try {
        logReader = new SingleFileLogReader(logFile);
      } catch (FileNotFoundException e) {
        LOGGER.error(
            "OperationSyncProtector can't open OperationSyncLog: {}, discarded",
            logFile.getAbsolutePath(),
            e);
        continue;
      }
      LOGGER.info("begin trans " + logFileName);
      while (logReader.hasNext()) {
        // read and re-serialize the PhysicalPlan
        PhysicalPlan nextPlan = logReader.next();
        try {
          nextPlan.serialize(protectorDeserializeStream);
        } catch (IOException e) {
          LOGGER.error("OperationSyncProtector can't serialize PhysicalPlan", e);
          continue;
        }
        ByteBuffer nextBuffer = ByteBuffer.wrap(protectorByteStream.toByteArray());
        protectorByteStream.reset();
        transmitPhysicalPlan(nextBuffer, nextPlan);
      }
      LOGGER.info("end trans " + logFileName);
      logReader.close();
      try {
        // sleep one second then delete OperationSyncLog
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        LOGGER.warn("OperationSyncProtector is interrupted", e);
      }

      OperationSyncLogService.incLogFileSize(-logFile.length());

      boolean deleted = false;
      for (int retryCnt = 0; retryCnt < 5; retryCnt++) {
        if (logFile.delete()) {
          deleted = true;
          LOGGER.info("OperationSyncLog: {} is deleted.", logFile.getAbsolutePath());
          break;
        } else {
          LOGGER.warn("Delete OperationSyncLog: {} failed. Retrying", logFile.getAbsolutePath());
        }
      }
      if (!deleted) {
        OperationSyncLogService.incLogFileSize(logFile.length());
        LOGGER.error("Couldn't delete OperationSyncLog: {}", logFile.getAbsolutePath());
      }
    }
  }

  protected abstract void preCheck();

  protected abstract void transmitPhysicalPlan(ByteBuffer planBuffer, PhysicalPlan physicalPlan);
}
