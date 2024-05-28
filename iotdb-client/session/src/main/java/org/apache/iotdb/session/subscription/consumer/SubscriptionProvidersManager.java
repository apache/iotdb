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

package org.apache.iotdb.session.subscription.consumer;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

final class SubscriptionProvidersManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionProvidersManager.class);

  private final Map<Set<TEndPoint>, SubscriptionProviders> endPointsToSubscriptionProviders =
      new HashMap<>();

  private final Map<SubscriptionProviders, Set<SubscriptionConsumer>>
      subscriptionProvidersToConsumers = new HashMap<>();

  public synchronized SubscriptionProviders bindSubscriptionProviders(
      final SubscriptionConsumer consumer, final Set<TEndPoint> initialEndpoints)
      throws SubscriptionException {
    if (Objects.isNull(endPointsToSubscriptionProviders.get(initialEndpoints))) {
      // construct subscription providers
      final SubscriptionProviders providers = new SubscriptionProviders(initialEndpoints);
      endPointsToSubscriptionProviders.put(initialEndpoints, providers);
      subscriptionProvidersToConsumers
          .computeIfAbsent(providers, k -> new HashSet<>())
          .add(consumer);

      // open subscription providers
      providers.acquireWriteLock();
      try {
        providers.openProviders(consumer); // throw SubscriptionException
      } finally {
        providers.releaseWriteLock();
      }

      // launch heartbeat worker
      {
        final ScheduledFuture<?>[] future = new ScheduledFuture<?>[1];
        future[0] =
            SubscriptionExecutorService.submitHeartbeatWorker(
                () -> {
                  if (consumer.isClosed()) {
                    if (Objects.nonNull(future[0])) {
                      future[0].cancel(false);
                    }
                    return;
                  }
                  providers.heartbeat(consumer);
                },
                consumer.getHeartbeatIntervalMs());
      }

      // launch endpoints syncer
      {
        final ScheduledFuture<?>[] future = new ScheduledFuture<?>[1];
        future[0] =
            SubscriptionExecutorService.submitEndpointsSyncer(
                () -> {
                  if (consumer.isClosed()) {
                    if (Objects.nonNull(future[0])) {
                      future[0].cancel(false);
                    }
                    return;
                  }
                  providers.sync(consumer);
                },
                consumer.getEndpointsSyncIntervalMs());
      }
    }

    return endPointsToSubscriptionProviders.get(initialEndpoints);
  }

  public synchronized void unbindSubscriptionProviders(
      final SubscriptionConsumer consumer, final SubscriptionProviders providers) {
    final Set<SubscriptionConsumer> consumers = subscriptionProvidersToConsumers.get(providers);
    if (Objects.isNull(consumers)) {
      LOGGER.warn(
          "SubscriptionProviders not existed when unbinding providers {} for consumer {}",
          providers,
          consumer);
      return;
    }
    if (!consumers.contains(consumer)) {
      LOGGER.warn(
          "SubscriptionConsumer not existed when unbinding providers {} for consumer {}",
          providers,
          consumer);
      return;
    }

    consumers.remove(consumer);
    if (consumers.isEmpty()) {
      // close subscription providers
      providers.acquireWriteLock();
      try {
        providers.closeProviders();
      } finally {
        providers.releaseWriteLock();
      }

      subscriptionProvidersToConsumers.remove(providers);
    }
  }

  //////////////////////////// singleton ////////////////////////////

  private static class SubscriptionProvidersManagerHolder {

    private static final SubscriptionProvidersManager INSTANCE = new SubscriptionProvidersManager();

    private SubscriptionProvidersManagerHolder() {
      // empty constructor
    }
  }

  public static SubscriptionProvidersManager getInstance() {
    return SubscriptionProvidersManager.SubscriptionProvidersManagerHolder.INSTANCE;
  }

  private SubscriptionProvidersManager() {
    // empty constructor
  }
}
