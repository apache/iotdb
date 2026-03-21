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

package org.apache.iotdb.session.subscription.consumer.base;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionConnectionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeHeartbeatResp;

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

final class AbstractSubscriptionProviders {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSubscriptionProviders.class);

  private final SortedMap<Integer, AbstractSubscriptionProvider> subscriptionProviders =
      new ConcurrentSkipListMap<>();
  private int nextDataNodeId = -1;

  private final ReentrantReadWriteLock subscriptionProvidersLock = new ReentrantReadWriteLock(true);

  private final Set<TEndPoint> initialEndpoints;

  AbstractSubscriptionProviders(final Set<TEndPoint> initialEndpoints) {
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
  void openProviders(final AbstractSubscriptionConsumer consumer) throws SubscriptionException {
    // close stale providers
    closeProviders();

    for (final TEndPoint endPoint : initialEndpoints) {
      final AbstractSubscriptionProvider defaultProvider;
      final int defaultDataNodeId;

      try {
        defaultProvider = consumer.constructProviderAndHandshake(endPoint);
      } catch (final Exception e) {
        LOGGER.warn(
            "{} failed to create connection with {} because of {}", consumer, endPoint, e, e);
        continue; // try next endpoint
      }
      defaultDataNodeId = defaultProvider.getDataNodeId();
      addProvider(defaultDataNodeId, defaultProvider);

      final Map<Integer, TEndPoint> allEndPoints;
      try {
        allEndPoints = defaultProvider.heartbeat().getEndPoints();
      } catch (final Exception e) {
        LOGGER.warn(
            "{} failed to fetch all endpoints from {} because of {}", consumer, endPoint, e, e);
        break;
      }

      for (final Map.Entry<Integer, TEndPoint> entry : allEndPoints.entrySet()) {
        if (defaultDataNodeId == entry.getKey()) {
          continue;
        }

        final AbstractSubscriptionProvider provider;
        try {
          provider = consumer.constructProviderAndHandshake(entry.getValue());
        } catch (final Exception e) {
          LOGGER.warn(
              "{} failed to create connection with {} because of {}",
              consumer,
              entry.getValue(),
              e,
              e);
          continue;
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
    for (final AbstractSubscriptionProvider provider : getAllProviders()) {
      try {
        provider.close();
      } catch (final Exception e) {
        LOGGER.warn("Failed to close subscription provider {} because of {}", provider, e, e);
      }
    }
    subscriptionProviders.clear();
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireWriteLock()}. */
  void addProvider(final int dataNodeId, final AbstractSubscriptionProvider provider) {
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
    final AbstractSubscriptionProvider provider = subscriptionProviders.get(dataNodeId);
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
  List<AbstractSubscriptionProvider> getAllProviders() {
    return new ArrayList<>(subscriptionProviders.values());
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  AbstractSubscriptionProvider getProvider(final int dataNodeId) {
    return containsProvider(dataNodeId) ? subscriptionProviders.get(dataNodeId) : null;
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  boolean hasNoAvailableProviders() {
    return subscriptionProviders.values().stream()
        .noneMatch(AbstractSubscriptionProvider::isAvailable);
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  boolean containsProvider(final int dataNodeId) {
    return subscriptionProviders.containsKey(dataNodeId);
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  List<AbstractSubscriptionProvider> getAllAvailableProviders() {
    return subscriptionProviders.values().stream()
        .filter(AbstractSubscriptionProvider::isAvailable)
        .collect(Collectors.toList());
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  void updateNextDataNodeId() {
    final SortedMap<Integer, AbstractSubscriptionProvider> subProviders =
        subscriptionProviders.tailMap(nextDataNodeId + 1);
    nextDataNodeId =
        subProviders.isEmpty() ? subscriptionProviders.firstKey() : subProviders.firstKey();
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  AbstractSubscriptionProvider getNextAvailableProvider() {
    if (hasNoAvailableProviders()) {
      return null;
    }

    AbstractSubscriptionProvider provider;
    provider = getProvider(nextDataNodeId);
    while (Objects.isNull(provider) || !provider.isAvailable()) {
      updateNextDataNodeId();
      provider = getProvider(nextDataNodeId);
    }
    updateNextDataNodeId();

    return provider;
  }

  /////////////////////////////// heartbeat ///////////////////////////////

  void heartbeat(final AbstractSubscriptionConsumer consumer) {
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

  private void heartbeatInternal(final AbstractSubscriptionConsumer consumer) {
    for (final AbstractSubscriptionProvider provider : getAllProviders()) {
      try {
        final PipeSubscribeHeartbeatResp resp = provider.heartbeat();
        // update subscribed topics
        consumer.subscribedTopics = resp.getTopics();
        // unsubscribe completed topics
        for (final String topicName : resp.getTopicNamesToUnsubscribe()) {
          LOGGER.info(
              "Termination occurred when SubscriptionConsumer {} polling topics, unsubscribe topic {} automatically",
              consumer.coreReportMessage(),
              topicName);
          consumer.unsubscribe(topicName);
        }
        provider.setAvailable();
      } catch (final Exception e) {
        LOGGER.warn(
            "{} failed to sending heartbeat to subscription provider {} because of {}, set subscription provider unavailable",
            consumer,
            provider,
            e,
            e);
        provider.setUnavailable();
      }
    }
  }

  /////////////////////////////// sync endpoints ///////////////////////////////

  void sync(final AbstractSubscriptionConsumer consumer) {
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

  private void syncInternal(final AbstractSubscriptionConsumer consumer) {
    if (hasNoAvailableProviders()) {
      try {
        openProviders(consumer);
      } catch (final Exception e) {
        LOGGER.warn("Failed to open providers for consumer {} because of {}", consumer, e, e);
        return;
      }
    }

    final Map<Integer, TEndPoint> allEndPoints;
    try {
      allEndPoints = consumer.fetchAllEndPointsWithRedirection();
    } catch (final Exception e) {
      LOGGER.warn("Failed to fetch all endpoints for consumer {} because of {}", consumer, e, e);
      return;
    }

    // add new providers or handshake existing providers
    for (final Map.Entry<Integer, TEndPoint> entry : allEndPoints.entrySet()) {
      final AbstractSubscriptionProvider provider = getProvider(entry.getKey());
      if (Objects.isNull(provider)) {
        // new provider
        final TEndPoint endPoint = entry.getValue();
        final AbstractSubscriptionProvider newProvider;
        try {
          newProvider = consumer.constructProviderAndHandshake(endPoint);
        } catch (final Exception e) {
          LOGGER.warn(
              "{} failed to create connection with {} because of {}", consumer, endPoint, e, e);
          continue;
        }
        addProvider(entry.getKey(), newProvider);
      } else {
        // existing provider
        try {
          consumer.subscribedTopics = provider.heartbeat().getTopics();
          provider.setAvailable();
        } catch (final Exception e) {
          LOGGER.warn(
              "{} failed to sending heartbeat to subscription provider {} because of {}, set subscription provider unavailable",
              consumer,
              provider,
              e,
              e);
          provider.setUnavailable();
        }
        // close and remove unavailable provider (reset the connection as much as possible)
        if (!provider.isAvailable()) {
          try {
            closeAndRemoveProvider(entry.getKey());
          } catch (final Exception e) {
            LOGGER.warn(
                "Exception occurred when {} closing and removing subscription provider {} because of {}",
                consumer,
                provider,
                e,
                e);
          }
        }
      }
    }

    // close and remove stale providers
    for (final AbstractSubscriptionProvider provider : getAllProviders()) {
      final int dataNodeId = provider.getDataNodeId();
      if (!allEndPoints.containsKey(dataNodeId)) {
        try {
          closeAndRemoveProvider(dataNodeId);
        } catch (final Exception e) {
          LOGGER.warn(
              "Exception occurred when {} closing and removing subscription provider {} because of {}",
              consumer,
              provider,
              e,
              e);
        }
      }
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("SubscriptionProviders{");
    for (final Map.Entry<Integer, AbstractSubscriptionProvider> entry :
        subscriptionProviders.entrySet()) {
      sb.append(entry.getValue().toString()).append(", ");
    }
    if (!subscriptionProviders.isEmpty()) {
      sb.delete(sb.length() - 2, sb.length());
    }
    sb.append("}");
    return sb.toString();
  }
}
