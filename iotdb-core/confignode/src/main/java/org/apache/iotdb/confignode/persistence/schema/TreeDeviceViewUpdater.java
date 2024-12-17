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

package org.apache.iotdb.confignode.persistence.schema;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class TreeDeviceViewUpdater {

  private static final Logger LOGGER = LoggerFactory.getLogger(TreeDeviceViewUpdater.class);

  private static final long UPDATE_INTERVAL_SECONDS =
      ConfigNodeDescriptor.getInstance().getConf().getTreeDeviceViewUpdateIntervalInMs();

  private static final ScheduledExecutorService DEVICE_VIEW_UPDATER_EXECUTOR =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.TREE_DEVICE_VIEW_UPDATER.getName());

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition condition = lock.newCondition();
  private volatile boolean skipNextSleep = false;
  private volatile boolean allowSubmitListen = false;

  public void notifyUpdate() {
    if (lock.tryLock()) {
      try {
        condition.signalAll();
      } finally {
        lock.unlock();
      }
    } else {
      skipNextSleep = true;
    }
  }

  private void execute() {
    lock.lock();
    try {
      if (!skipNextSleep) {
        condition.await(UPDATE_INTERVAL_SECONDS, TimeUnit.MILLISECONDS);
      }
      skipNextSleep = false;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("Interrupted when waiting for the next device view update: {}", e.getMessage());
    } finally {
      lock.unlock();

      if (allowSubmitListen) {
        DEVICE_VIEW_UPDATER_EXECUTOR.submit(this::execute);
      }
    }
  }

  public void start() {
    allowSubmitListen = true;
    DEVICE_VIEW_UPDATER_EXECUTOR.submit(this::execute);

    LOGGER.info("Tree device view updater is started successfully.");
  }

  public void stop() {
    allowSubmitListen = false;
    DEVICE_VIEW_UPDATER_EXECUTOR.shutdown();

    LOGGER.info("Tree device view updater is stopped successfully.");
  }
}
