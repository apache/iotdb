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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class DoubleWriteProtector implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(DoubleWriteProtector.class);
  private static final int logFileValidity;

  // For transmit log files
  protected final Lock logFileListLock;
  protected List<String> registeredLogFiles;
  protected List<String> processingLogFiles;

  // For serialize PhysicalPlan
  private static final int MAX_PHYSICALPLAN_SIZE = 16 * 1024 * 1024;
  protected final ByteArrayOutputStream protectorByteStream;
  protected final DataOutputStream protectorSerializeStream;

  // Working state
  protected final Lock atWorkLock;
  protected boolean isProtectorAtWork;

  protected DoubleWriteProtector() {
    logFileListLock = new ReentrantLock();
    registeredLogFiles = new ArrayList<>();

    protectorByteStream = new ByteArrayOutputStream(MAX_PHYSICALPLAN_SIZE);
    protectorSerializeStream = new DataOutputStream(protectorByteStream);

    atWorkLock = new ReentrantLock();
    isProtectorAtWork = false;
  }

  protected void registerLogFile(String logFile) {
    logFileListLock.lock();
    registeredLogFiles.add(logFile);
    logFileListLock.unlock();
  }

  protected void wrapLogFiles() {
    processingLogFiles = new ArrayList<>(registeredLogFiles);
    registeredLogFiles = new ArrayList<>();
  }

  @Override
  public void run() {
    while (true) {
      boolean startWork = false;
      atWorkLock.lock();
      if (!isProtectorAtWork) {
        logFileListLock.lock();
        if (registeredLogFiles.size() > 0) {
          isProtectorAtWork = true;
          startWork = true;
          wrapLogFiles();
        }
        logFileListLock.unlock();
      }
      atWorkLock.unlock();

      if (startWork) {
        transmitLogFiles();
      }

      try {
        // Sleep a while before next check
        TimeUnit.SECONDS.sleep(logFileValidity);
      } catch (InterruptedException e) {
        LOGGER.warn("DoubleWriteProtector been interrupted", e);
      }
    }
  }

  protected abstract void transmitLogFiles();
}
