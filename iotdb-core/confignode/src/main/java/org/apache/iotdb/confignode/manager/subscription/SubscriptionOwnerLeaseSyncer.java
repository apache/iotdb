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

package org.apache.iotdb.confignode.manager.subscription;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.confignode.i18n.ManagerMessages;
import org.apache.iotdb.confignode.manager.ConfigManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Dedicated, fine-grained subscription owner-lease heartbeat, independent from the node heartbeat.
 *
 * <p>Each tick pushes relative-duration owner leases to all DataNodes (see {@link
 * SubscriptionCoordinator#pushTopicOwnerLeasesToDataNodes()}). A DataNode that stops receiving
 * pushes (e.g. partitioned from this ConfigNode) lets its local lease expire and fences the owner
 * (fail-closed). The interval must stay well below the configured lease duration so transient
 * misses do not spuriously fence a healthy owner.
 */
public class SubscriptionOwnerLeaseSyncer {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionOwnerLeaseSyncer.class);

  // Note: candidate for promotion to SubscriptionConfig; invariant must hold: lease-duration >>
  // interval * tolerated-misses.
  private static final long OWNER_LEASE_HEARTBEAT_INTERVAL_SECONDS = 5;

  /** The owner-lease heartbeat interval {@code H}, used by the transfer admission-wait bound. */
  public static long getHeartbeatIntervalMs() {
    return OWNER_LEASE_HEARTBEAT_INTERVAL_SECONDS * 1000L;
  }

  private static final ScheduledExecutorService LEASE_EXECUTOR =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("SubscriptionOwnerLeaseSyncer");

  private final ConfigManager configManager;

  private Future<?> leaseSyncFuture;

  SubscriptionOwnerLeaseSyncer(final ConfigManager configManager) {
    this.configManager = configManager;
  }

  public synchronized void start() {
    if (leaseSyncFuture == null) {
      leaseSyncFuture =
          ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
              LEASE_EXECUTOR,
              this::sync,
              OWNER_LEASE_HEARTBEAT_INTERVAL_SECONDS,
              OWNER_LEASE_HEARTBEAT_INTERVAL_SECONDS,
              TimeUnit.SECONDS);
      LOGGER.info(
          ManagerMessages.MESSAGE_SUBSCRIPTIONOWNERLEASESYNCER_IS_STARTED_SUCCESSFULLY_09CA6848);
    }
  }

  private void sync() {
    try {
      configManager
          .getSubscriptionManager()
          .getSubscriptionCoordinator()
          .pushTopicOwnerLeasesToDataNodes();
    } catch (final Exception e) {
      LOGGER.warn(
          ManagerMessages
              .MESSAGE_FAILED_TO_PUSH_SUBSCRIPTION_TOPIC_OWNER_LEASES_TO_DATANODES_EBFBA668,
          e);
    }
  }

  public synchronized void stop() {
    if (leaseSyncFuture != null) {
      leaseSyncFuture.cancel(false);
      leaseSyncFuture = null;
      LOGGER.info(
          ManagerMessages.MESSAGE_SUBSCRIPTIONOWNERLEASESYNCER_IS_STOPPED_SUCCESSFULLY_11442F29);
    }
  }
}
