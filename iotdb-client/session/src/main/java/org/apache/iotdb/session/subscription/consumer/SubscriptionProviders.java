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
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionConnectionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

final class SubscriptionProviders {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionProviders.class);

  private final SortedMap<Integer, SubscriptionProvider> subscriptionProviders =
      new ConcurrentSkipListMap<>();
  private int nextDataNodeId = -1;

  private final ReentrantReadWriteLock subscriptionProvidersLock = new ReentrantReadWriteLock(true);

  private final Set<TEndPoint> initialEndpoints;

  SubscriptionProviders(final Set<TEndPoint> initialEndpoints) {
    this.initialEndpoints = initialEndpoints;
  }

  /////////////////////////////// lock ///////////////////////////////

  void acquireReadLock() {
    subscriptionProvidersLock.readLock().lock();
  }

  void releaseReadLock() {
    subscriptionProvidersLock.readLock().unlock();
  }

  void acquireWriteLock() {
    subscriptionProvidersLock.writeLock().lock();
  }

  void releaseWriteLock() {
    subscriptionProvidersLock.writeLock().unlock();
  }

  /////////////////////////////// CRUD ///////////////////////////////

  /** Caller should ensure that the method is called in the lock {@link #acquireWriteLock()}. */
  void openProviders(final SubscriptionConsumer consumer) throws SubscriptionException {
    // close stale providers
    closeProviders();

    for (final TEndPoint endPoint : initialEndpoints) {
      final SubscriptionProvider defaultProvider;
      final int defaultDataNodeId;

      try {
        defaultProvider = consumer.constructProviderAndHandshake(endPoint);
      } catch (final Exception e) {
        LOGGER.warn("Failed to create connection with {}", endPoint, e);
        continue; // try next endpoint
      }
      defaultDataNodeId = defaultProvider.getDataNodeId();
      addProvider(defaultDataNodeId, defaultProvider);

      final Map<Integer, TEndPoint> allEndPoints;
      try {
        allEndPoints = defaultProvider.getSessionConnection().fetchAllEndPoints();
      } catch (final Exception e) {
        LOGGER.warn("Failed to fetch all endpoints from {}, will retry later...", endPoint, e);
        break; // retry later
      }

      for (final Map.Entry<Integer, TEndPoint> entry : allEndPoints.entrySet()) {
        if (defaultDataNodeId == entry.getKey()) {
          continue;
        }

        final SubscriptionProvider provider;
        try {
          provider = consumer.constructProviderAndHandshake(entry.getValue());
        } catch (final Exception e) {
          LOGGER.warn(
              "Failed to create connection with {}, will retry later...", entry.getValue(), e);
          continue; // retry later
        }
        addProvider(entry.getKey(), provider);
      }

      break;
    }

    if (hasNoAvailableProviders()) {
      throw new SubscriptionConnectionException(
          String.format(
              "Cluster has no available subscription providers to connect with initial endpoints %s",
              initialEndpoints));
    }

    nextDataNodeId = subscriptionProviders.firstKey();
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireWriteLock()}. */
  void closeProviders() {
    for (final SubscriptionProvider provider : getAllProviders()) {
      try {
        provider.close();
      } catch (final Exception ignored) {
      }
    }
    subscriptionProviders.clear();
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireWriteLock()}. */
  void addProvider(final int dataNodeId, final SubscriptionProvider provider) {
    // the subscription provider is opened
    LOGGER.info("add new subscription provider {}", provider);
    subscriptionProviders.put(dataNodeId, provider);
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireWriteLock()}. */
  void closeAndRemoveProvider(final int dataNodeId)
      throws SubscriptionException, IoTDBConnectionException {
    if (!containsProvider(dataNodeId)) {
      return;
    }
    final SubscriptionProvider provider = subscriptionProviders.get(dataNodeId);
    try {
      provider.close();
    } finally {
      LOGGER.info("close and remove stale subscription provider {}", provider);
      subscriptionProviders.remove(dataNodeId);
    }
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  boolean hasNoProviders() {
    return subscriptionProviders.isEmpty();
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  List<SubscriptionProvider> getAllProviders() {
    return new ArrayList<>(subscriptionProviders.values());
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  SubscriptionProvider getProvider(final int dataNodeId) {
    return containsProvider(dataNodeId) ? subscriptionProviders.get(dataNodeId) : null;
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  boolean hasNoAvailableProviders() {
    return subscriptionProviders.values().stream().noneMatch(SubscriptionProvider::isAvailable);
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  boolean containsProvider(final int dataNodeId) {
    return subscriptionProviders.containsKey(dataNodeId);
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  List<SubscriptionProvider> getAllAvailableProviders() {
    return subscriptionProviders.values().stream()
        .filter(SubscriptionProvider::isAvailable)
        .collect(Collectors.toList());
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  void updateNextDataNodeId() {
    final SortedMap<Integer, SubscriptionProvider> subProviders =
        subscriptionProviders.tailMap(nextDataNodeId + 1);
    nextDataNodeId =
        subProviders.isEmpty() ? subscriptionProviders.firstKey() : subProviders.firstKey();
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  SubscriptionProvider getNextAvailableProvider() {
    if (hasNoAvailableProviders()) {
      return null;
    }

    SubscriptionProvider provider;
    provider = getProvider(nextDataNodeId);
    while (Objects.isNull(provider) || !provider.isAvailable()) {
      updateNextDataNodeId();
      provider = getProvider(nextDataNodeId);
    }
    updateNextDataNodeId();

    return provider;
  }

  /////////////////////////////// heartbeat ///////////////////////////////

  void heartbeat(final SubscriptionConsumer consumer) {
    if (consumer.isClosed()) {
      return;
    }

    acquireWriteLock();
    try {
      heartbeatInternal(consumer);
    } finally {
      releaseWriteLock();
    }
  }

  private void heartbeatInternal(final SubscriptionConsumer consumer) {
    for (final SubscriptionProvider provider : getAllProviders()) {
      try {
        consumer.subscribedTopics = provider.heartbeat();
        provider.setAvailable();
      } catch (final Exception e) {
        LOGGER.warn(
            "something unexpected happened when sending heartbeat to subscription provider {}, set subscription provider unavailable",
            provider,
            e);
        provider.setUnavailable();
      }
    }
  }

  /////////////////////////////// sync endpoints ///////////////////////////////

  void sync(final SubscriptionConsumer consumer) {
    if (consumer.isClosed()) {
      return;
    }

    acquireWriteLock();
    try {
      syncInternal(consumer);
    } finally {
      releaseWriteLock();
    }
  }

  private void syncInternal(final SubscriptionConsumer consumer) {
    if (hasNoAvailableProviders()) {
      try {
        openProviders(consumer);
      } catch (final Exception e) {
        LOGGER.warn("something unexpected happened when syncing subscription endpoints...", e);
        return;
      }
    }

    final Map<Integer, TEndPoint> allEndPoints;
    try {
      allEndPoints = consumer.fetchAllEndPointsWithRedirection();
    } catch (final Exception e) {
      LOGGER.warn("Failed to fetch all endpoints, will retry later...", e);
      return; // retry later
    }

    // add new providers or handshake existing providers
    for (final Map.Entry<Integer, TEndPoint> entry : allEndPoints.entrySet()) {
      final SubscriptionProvider provider = getProvider(entry.getKey());
      if (Objects.isNull(provider)) {
        // new provider
        final TEndPoint endPoint = entry.getValue();
        final SubscriptionProvider newProvider;
        try {
          newProvider = consumer.constructProviderAndHandshake(endPoint);
        } catch (final Exception e) {
          LOGGER.warn(
              "Failed to create connection with endpoint {}, will retry later...", endPoint, e);
          continue; // retry later
        }
        addProvider(entry.getKey(), newProvider);
      } else {
        // existing provider
        try {
          consumer.subscribedTopics = provider.heartbeat();
          provider.setAvailable();
        } catch (final Exception e) {
          LOGGER.warn(
              "something unexpected happened when sending heartbeat to subscription provider {}, set subscription provider unavailable",
              provider,
              e);
          provider.setUnavailable();
        }
        // close and remove unavailable provider (reset the connection as much as possible)
        if (!provider.isAvailable()) {
          try {
            closeAndRemoveProvider(entry.getKey());
          } catch (final Exception e) {
            LOGGER.warn(
                "Exception occurred when closing and removing subscription provider with data node id {}",
                entry.getKey(),
                e);
          }
        }
      }
    }

    // close and remove stale providers
    for (final SubscriptionProvider provider : getAllProviders()) {
      final int dataNodeId = provider.getDataNodeId();
      if (!allEndPoints.containsKey(dataNodeId)) {
        try {
          closeAndRemoveProvider(dataNodeId);
        } catch (final Exception e) {
          LOGGER.warn(
              "Exception occurred when closing and removing subscription provider with data node id {}",
              dataNodeId,
              e);
        }
      }
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("SubscriptionProviders{");
    for (final Map.Entry<Integer, SubscriptionProvider> entry : subscriptionProviders.entrySet()) {
      sb.append(entry.getValue().toString()).append(", ");
    }
    if (!subscriptionProviders.isEmpty()) {
      sb.delete(sb.length() - 2, sb.length());
    }
    sb.append("}");
    return sb.toString();
  }
}
