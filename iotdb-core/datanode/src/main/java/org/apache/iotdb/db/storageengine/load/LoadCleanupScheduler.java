/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.load;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Background cleanup daemon for abandoned LOAD tasks. A plain hash map with lazy expiry replaces
 * the old {@code PriorityBlockingQueue}: {@link #registerOrRefresh} is an O(1) {@code compute},
 * eviction removes exactly the expired entry and never scans the queue, and no monitor is held
 * while the eviction action (which closes writers and may block) runs.
 */
final class LoadCleanupScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadCleanupScheduler.class);
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private static final class TaskState {
    private volatile long expireTime;
    private volatile boolean isRunning;
  }

  private final ConcurrentHashMap<String, TaskState> tasks = new ConcurrentHashMap<>();
  private final Consumer<String> evictionAction;
  private final long delayInMs;

  LoadCleanupScheduler(long timeoutSeconds, Consumer<String> evictionAction) {
    this.evictionAction = evictionAction;
    this.delayInMs = timeoutSeconds * 1000L;
  }

  /** Registers the task or refreshes its expiry, without touching any other entry. */
  void registerOrRefresh(String uuid) {
    tasks.compute(
        uuid,
        (id, state) -> {
          if (state == null) {
            state = new TaskState();
          }
          state.expireTime = System.currentTimeMillis() + delayInMs;
          return state;
        });
  }

  void markRunning(String uuid) {
    final TaskState state = tasks.get(uuid);
    if (state != null) {
      state.isRunning = true;
      state.expireTime = System.currentTimeMillis() + delayInMs;
    }
  }

  void markIdle(String uuid) {
    final TaskState state = tasks.get(uuid);
    if (state != null) {
      state.isRunning = false;
      state.expireTime = System.currentTimeMillis() + delayInMs;
    }
  }

  void remove(String uuid) {
    tasks.remove(uuid);
  }

  void start() {
    PipeDataNodeAgent.runtime()
        .registerPeriodicalJob(
            "LoadTsFileManager#cleanupTasks",
            this::sweep,
            CONFIG.getLoadCleanupTaskExecutionDelayTimeSeconds() >> 2);
  }

  void shutdown() {
    tasks.clear();
  }

  private void sweep() {
    for (Map.Entry<String, TaskState> entry : tasks.entrySet()) {
      final String uuid = entry.getKey();
      final TaskState state = entry.getValue();
      if (state.isRunning) {
        // A live LOAD must never be evicted; defer it like the old queue re-schedule.
        state.expireTime = System.currentTimeMillis() + delayInMs;
        continue;
      }
      if (state.expireTime > System.currentTimeMillis()) {
        continue;
      }
      final TaskState removed = tasks.remove(uuid);
      if (removed == null) {
        continue;
      }
      LOGGER.info(StorageEngineMessages.LOAD_CLEANUP_TASK_STARTS, uuid);
      try {
        // Run outside the map iteration and any monitor: the eviction closes writers and may block.
        evictionAction.accept(uuid);
      } catch (Exception e) {
        LOGGER.warn(StorageEngineMessages.LOAD_CLEANUP_TASK_ERROR, uuid, e);
      }
    }
  }
}
