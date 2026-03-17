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

package org.apache.iotdb.commons.consensus.iotv2.consistency;

import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Background scheduler for periodic consistency checks across all consensus groups managed by this
 * DataNode. Integrates with IoTV2GlobalComponentContainer's background task service.
 *
 * <p>Features:
 *
 * <ul>
 *   <li>Configurable check interval (default: 1 hour)
 *   <li>Rate limiting to prevent resource saturation
 *   <li>Per-region tracking to avoid overlapping checks
 *   <li>SyncLag pre-check gate (only checks regions with completed replication)
 * </ul>
 */
public class ConsistencyCheckScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsistencyCheckScheduler.class);

  private static final long DEFAULT_CHECK_INTERVAL_MS = 3_600_000L; // 1 hour
  private static final long DEFAULT_INITIAL_DELAY_MS = 300_000L; // 5 minutes

  /** Callback interface for triggering consistency checks. */
  public interface ConsistencyCheckTrigger {
    /**
     * Trigger a consistency check for a specific consensus group.
     *
     * @param consensusGroupId the group to check
     * @return true if the check was successfully initiated
     */
    boolean triggerCheck(String consensusGroupId);

    /**
     * Check if replication is complete for a consensus group.
     *
     * @param consensusGroupId the group to check
     * @return true if syncLag <= 0
     */
    boolean isReplicationComplete(String consensusGroupId);
  }

  private final ScheduledExecutorService executorService;
  private final ConsistencyCheckTrigger trigger;
  private final long checkIntervalMs;
  private final long initialDelayMs;
  private final AtomicBoolean running;
  private ScheduledFuture<?> scheduledFuture;
  private final ConcurrentHashMap<String, Long> lastCheckTimes;
  private final ConcurrentHashMap<String, Boolean> activeChecks;

  public ConsistencyCheckScheduler(
      ScheduledExecutorService executorService, ConsistencyCheckTrigger trigger) {
    this(executorService, trigger, DEFAULT_CHECK_INTERVAL_MS, DEFAULT_INITIAL_DELAY_MS);
  }

  public ConsistencyCheckScheduler(
      ScheduledExecutorService executorService,
      ConsistencyCheckTrigger trigger,
      long checkIntervalMs,
      long initialDelayMs) {
    this.executorService = executorService;
    this.trigger = trigger;
    this.checkIntervalMs = checkIntervalMs;
    this.initialDelayMs = initialDelayMs;
    this.running = new AtomicBoolean(false);
    this.lastCheckTimes = new ConcurrentHashMap<>();
    this.activeChecks = new ConcurrentHashMap<>();
  }

  /** Start the periodic scheduling. */
  public void start() {
    if (!running.compareAndSet(false, true)) {
      LOGGER.warn("ConsistencyCheckScheduler already running");
      return;
    }
    scheduledFuture =
        ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
            executorService,
            this::runChecks,
            initialDelayMs,
            checkIntervalMs,
            TimeUnit.MILLISECONDS);
    LOGGER.info(
        "ConsistencyCheckScheduler started: initialDelay={}ms, interval={}ms",
        initialDelayMs,
        checkIntervalMs);
  }

  /** Stop the scheduler gracefully. */
  public void stop() {
    if (!running.compareAndSet(true, false)) {
      return;
    }
    if (scheduledFuture != null) {
      scheduledFuture.cancel(false);
    }
    LOGGER.info("ConsistencyCheckScheduler stopped");
  }

  /** Register a consensus group for periodic checking. */
  public void registerGroup(String consensusGroupId) {
    lastCheckTimes.putIfAbsent(consensusGroupId, 0L);
    activeChecks.putIfAbsent(consensusGroupId, false);
    LOGGER.debug("Registered consensus group {} for consistency checks", consensusGroupId);
  }

  /** Unregister a consensus group. */
  public void unregisterGroup(String consensusGroupId) {
    lastCheckTimes.remove(consensusGroupId);
    activeChecks.remove(consensusGroupId);
  }

  /** Trigger an immediate check for a specific group (e.g., from CLI). */
  public boolean triggerManualCheck(String consensusGroupId) {
    if (activeChecks.getOrDefault(consensusGroupId, false)) {
      LOGGER.info("Check already active for group {}, skipping", consensusGroupId);
      return false;
    }
    return checkGroup(consensusGroupId);
  }

  private void runChecks() {
    if (!running.get()) {
      return;
    }

    long now = System.currentTimeMillis();
    for (Map.Entry<String, Long> entry : lastCheckTimes.entrySet()) {
      String groupId = entry.getKey();
      long lastCheck = entry.getValue();

      // Rate limiting: skip if checked recently
      if (now - lastCheck < checkIntervalMs) {
        continue;
      }

      // Skip if a check is already active
      if (activeChecks.getOrDefault(groupId, false)) {
        continue;
      }

      checkGroup(groupId);
    }
  }

  private boolean checkGroup(String consensusGroupId) {
    try {
      // SyncLag pre-check
      if (!trigger.isReplicationComplete(consensusGroupId)) {
        LOGGER.debug(
            "Skipping consistency check for group {}: replication in progress", consensusGroupId);
        return false;
      }

      activeChecks.put(consensusGroupId, true);
      lastCheckTimes.put(consensusGroupId, System.currentTimeMillis());

      boolean triggered = trigger.triggerCheck(consensusGroupId);
      if (triggered) {
        LOGGER.info("Triggered consistency check for group {}", consensusGroupId);
      }
      return triggered;
    } catch (Exception e) {
      LOGGER.error(
          "Error triggering consistency check for group {}: {}",
          consensusGroupId,
          e.getMessage(),
          e);
      return false;
    } finally {
      activeChecks.put(consensusGroupId, false);
    }
  }

  public boolean isRunning() {
    return running.get();
  }

  public long getCheckIntervalMs() {
    return checkIntervalMs;
  }

  public int getRegisteredGroupCount() {
    return lastCheckTimes.size();
  }
}
