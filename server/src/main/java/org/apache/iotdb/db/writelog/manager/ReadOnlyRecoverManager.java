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
package org.apache.iotdb.db.writelog.manager;

import org.apache.iotdb.db.common.RetryCounter;
import org.apache.iotdb.db.common.RetryCounterFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.RecoverReadOnlyException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** read only mode recover default: 10 min once, retry 3 times */
public class ReadOnlyRecoverManager {
  private static final Logger logger = LoggerFactory.getLogger(ReadOnlyRecoverManager.class);

  private static final ReadOnlyRecoverManager INSTANCE = new ReadOnlyRecoverManager();

  static {
    ReadOnlyRecoverManager.getInstance().registerRecover(new DefaultRecoverCallBack());
  }

  private final List<RecoverCallBack> recoverCallBacks = new ArrayList<>();

  private RetryCounterFactory retryCounterFactory;

  private volatile boolean isRecovering = false;

  private AtomicBoolean isCloseFailed = new AtomicBoolean(false);

  private ReadOnlyRecoverManager() {
    this.retryCounterFactory =
        new RetryCounterFactory(
            new RetryCounter.RetryConfig()
                .setMaxAttempts(
                    IoTDBDescriptor.getInstance().getConfig().getReadOnlyRecoverRetryAttempts())
                .setSleepInterval(
                    IoTDBDescriptor.getInstance()
                        .getConfig()
                        .getReadOnlyRecoverRetrySleepInterval()));
  }

  public static ReadOnlyRecoverManager getInstance() {
    return INSTANCE;
  }

  public synchronized void executeRecover() {
    if (isRecovering) {
      logger.info("Read-Only mode is recovering");
    }
    isRecovering = true;

    new Thread(
            () -> {
              RetryCounter retryCounter = retryCounterFactory.create();
              while (true) {
                try {
                  isCloseFailed.set(false);
                  for (RecoverCallBack recoverCallBack : recoverCallBacks) {
                    logger.info("Start read-only mode recovering");
                    recoverCallBack.callBack();
                  }
                  if (isCloseFailed.get()) {
                    throw new RecoverReadOnlyException("recovering is failed");
                  }
                  logger.info("recover read-write mode.");
                  IoTDBDescriptor.getInstance().getConfig().setReadOnly(false);
                  isRecovering = false;
                  break;
                } catch (Exception e) {
                  retryOrThrow(retryCounter, e);
                }
              }
            })
        .start();
  }

  private void retryOrThrow(RetryCounter retryCounter, Exception e) {
    if (retryCounter.shouldRetry()) {
      try {
        logger.warn("Failed to recover read-only mode, retry again", e);
        retryCounter.sleepToNextRetry();
      } catch (InterruptedException ex) {
        // ignore
        logger.warn("Sleep Interrupted:", e);
      }
      return;
    }
    clear();
    throw new RecoverReadOnlyException(e);
  }

  public void registerRecover(RecoverCallBack recoverCallBack) {
    if (!recoverCallBacks.contains(recoverCallBack)) {
      recoverCallBacks.add(recoverCallBack);
    }
  }

  public void setClosingFailed(boolean bool) {
    this.isCloseFailed.set(bool);
  }

  private void clear() {
    this.isCloseFailed.set(false);
    this.isRecovering = false;
  }

  interface RecoverCallBack {
    void callBack();
  }

  static class DefaultRecoverCallBack implements RecoverCallBack {
    @Override
    public void callBack() {
      StorageEngine.getInstance().syncCloseAllProcessor();
    }
  }
}
