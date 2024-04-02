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

package org.apache.iotdb.session.subscription;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.session.util.SessionUtils;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public abstract class SubscriptionConsumer implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionConsumer.class);

  private static final IoTDBConnectionException NO_PROVIDERS_EXCEPTION =
      new IoTDBConnectionException("Cluster has no available subscription providers to connect");

  private final List<TEndPoint> initialEndpoints;

  private final String username;
  private final String password;

  private final String consumerId;
  private final String consumerGroupId;

  private final SortedMap<Integer, SubscriptionProvider> subscriptionProviders =
      new ConcurrentSkipListMap<>();
  private final ReentrantReadWriteLock subscriptionProvidersLock = new ReentrantReadWriteLock(true);

  private static final long HEARTBEAT_INTERVAL = 5000; // unit: ms
  private ScheduledExecutorService heartbeatWorkerExecutor;

  private static final long ENDPOINTS_SYNC_INTERVAL = 30000; // unit: ms
  private ScheduledExecutorService endpointsSyncerExecutor;

  private final AtomicBoolean isClosed = new AtomicBoolean(true);

  public String getConsumerId() {
    return consumerId;
  }

  public String getConsumerGroupId() {
    return consumerGroupId;
  }

  /////////////////////////////// ctor ///////////////////////////////

  protected SubscriptionConsumer(Builder builder) {
    this.initialEndpoints = new ArrayList<>();
    // From org.apache.iotdb.session.Session.getNodeUrls
    // Priority is given to `host:port` over `nodeUrls`.
    if (Objects.nonNull(builder.host)) {
      initialEndpoints.add(new TEndPoint(builder.host, builder.port));
    } else {
      initialEndpoints.addAll(SessionUtils.parseSeedNodeUrls(builder.nodeUrls));
    }

    this.username = builder.username;
    this.password = builder.password;

    this.consumerId = builder.consumerId;
    this.consumerGroupId = builder.consumerGroupId;
  }

  protected SubscriptionConsumer(Builder builder, Properties config) {
    this(
        builder
            .host(
                (String) config.getOrDefault(ConsumerConstant.HOST_KEY, SessionConfig.DEFAULT_HOST))
            .port(
                (Integer)
                    config.getOrDefault(ConsumerConstant.PORT_KEY, SessionConfig.DEFAULT_PORT))
            .nodeUrls((List<String>) config.get(ConsumerConstant.NODE_URLS_KEY))
            .username(
                (String)
                    config.getOrDefault(ConsumerConstant.USERNAME_KEY, SessionConfig.DEFAULT_USER))
            .password(
                (String)
                    config.getOrDefault(
                        ConsumerConstant.PASSWORD_KEY, SessionConfig.DEFAULT_PASSWORD))
            .consumerId((String) config.get(ConsumerConstant.CONSUMER_ID_KEY))
            .consumerGroupId((String) config.get(ConsumerConstant.CONSUMER_GROUP_ID_KEY)));
  }

  /////////////////////////////// open & close ///////////////////////////////

  public synchronized void open()
      throws TException, IoTDBConnectionException, IOException, StatementExecutionException {
    if (!isClosed.get()) {
      return;
    }

    // open subscription providers
    acquireWriteLock();
    try {
      openProviders(); // throw IoTDBConnectionException
    } finally {
      releaseWriteLock();
    }

    // launch heartbeat worker
    launchHeartbeatWorker();

    // launch endpoints syncer
    launchEndpointsSyncer();

    isClosed.set(false);
  }

  @Override
  public synchronized void close() throws IoTDBConnectionException {
    if (isClosed.get()) {
      return;
    }

    try {
      // shutdown endpoints syncer
      shutdownEndpointsSyncer();

      // shutdown heartbeat worker
      shutdownHeartbeatWorker();

      // close subscription providers
      acquireWriteLock();
      try {
        closeProviders();
      } finally {
        releaseWriteLock();
      }
    } finally {
      isClosed.set(true);
    }
  }

  boolean isClosed() {
    return isClosed.get();
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

  /////////////////////////////// subscribe & unsubscribe ///////////////////////////////

  public void subscribe(String topicName)
      throws TException, IOException, StatementExecutionException, IoTDBConnectionException {
    subscribe(Collections.singleton(topicName));
  }

  public void subscribe(String... topicNames)
      throws TException, IOException, StatementExecutionException, IoTDBConnectionException {
    subscribe(new HashSet<>(Arrays.asList(topicNames)));
  }

  public void subscribe(Set<String> topicNames)
      throws TException, IOException, StatementExecutionException, IoTDBConnectionException {
    acquireReadLock();
    try {
      subscribeWithRedirection(topicNames);
    } finally {
      releaseReadLock();
    }
  }

  public void unsubscribe(String topicName)
      throws TException, IOException, StatementExecutionException, IoTDBConnectionException {
    unsubscribe(Collections.singleton(topicName));
  }

  public void unsubscribe(String... topicNames)
      throws TException, IOException, StatementExecutionException, IoTDBConnectionException {
    unsubscribe(new HashSet<>(Arrays.asList(topicNames)));
  }

  public void unsubscribe(Set<String> topicNames)
      throws TException, IOException, StatementExecutionException, IoTDBConnectionException {
    acquireReadLock();
    try {
      unsubscribeWithRedirection(topicNames);
    } finally {
      releaseReadLock();
    }
  }

  /////////////////////////////// heartbeat ///////////////////////////////

  @SuppressWarnings("unsafeThreadSchedule")
  private void launchHeartbeatWorker() {
    heartbeatWorkerExecutor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t =
                  new Thread(
                      Thread.currentThread().getThreadGroup(), r, "ConsumerHeartbeatWorker", 0);
              if (!t.isDaemon()) {
                t.setDaemon(true);
              }
              if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
              }
              return t;
            });
    heartbeatWorkerExecutor.scheduleAtFixedRate(
        new ConsumerHeartbeatWorker(this), 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);
  }

  private void shutdownHeartbeatWorker() {
    heartbeatWorkerExecutor.shutdown();
    heartbeatWorkerExecutor = null;
  }

  /////////////////////////////// endpoints syncer ///////////////////////////////

  @SuppressWarnings("unsafeThreadSchedule")
  private void launchEndpointsSyncer() {
    endpointsSyncerExecutor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t =
                  new Thread(
                      Thread.currentThread().getThreadGroup(), r, "SubscriptionEndpointsSyncer", 0);
              if (!t.isDaemon()) {
                t.setDaemon(true);
              }
              if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
              }
              return t;
            });
    endpointsSyncerExecutor.scheduleAtFixedRate(
        new SubscriptionEndpointsSyncer(this), 0, ENDPOINTS_SYNC_INTERVAL, TimeUnit.MILLISECONDS);
  }

  private void shutdownEndpointsSyncer() {
    endpointsSyncerExecutor.shutdown();
    endpointsSyncerExecutor = null;
  }

  /////////////////////////////// subscription provider ///////////////////////////////

  SubscriptionProvider constructProvider(final TEndPoint endPoint) {
    return new SubscriptionProvider(
        endPoint, this.username, this.password, this.consumerId, this.consumerGroupId);
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireWriteLock()}. */
  void openProviders() throws IoTDBConnectionException {
    // close stale providers
    closeProviders();

    for (final TEndPoint endPoint : initialEndpoints) {
      final SubscriptionProvider defaultProvider;
      final int defaultDataNodeId;

      try {
        defaultProvider = constructProvider(endPoint);
        defaultDataNodeId = defaultProvider.handshake();
      } catch (final Exception e) {
        LOGGER.warn("Failed to create connection with {}, exception: {}", endPoint, e.getMessage());
        continue; // try next endpoint
      }
      addProvider(defaultDataNodeId, defaultProvider);

      final Map<Integer, TEndPoint> allEndPoints;
      try {
        allEndPoints = defaultProvider.getSessionConnection().fetchAllEndPoints();
      } catch (final Exception e) {
        LOGGER.warn(
            "Failed to fetch all endpoints from {}, exception: {}, will retry later...",
            endPoint,
            e.getMessage());
        break; // retry later
      }

      for (final Map.Entry<Integer, TEndPoint> entry : allEndPoints.entrySet()) {
        if (defaultDataNodeId == entry.getKey()) {
          continue;
        }

        final SubscriptionProvider provider;
        try {
          provider = constructProvider(entry.getValue());
          provider.handshake();
        } catch (final Exception e) {
          LOGGER.warn(
              "Failed to create connection with {}, exception: {}, will retry later...",
              entry.getValue(),
              e.getMessage());
          continue; // retry later
        }
        addProvider(entry.getKey(), provider);
      }

      break;
    }

    if (hasNoProviders()) {
      throw NO_PROVIDERS_EXCEPTION;
    }
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireWriteLock()}. */
  private void closeProviders() throws IoTDBConnectionException {
    for (final SubscriptionProvider provider : getAllProviders()) {
      provider.close();
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
  void closeAndRemoveProvider(final int dataNodeId) throws IoTDBConnectionException {
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
  boolean containsProvider(final int dataNodeId) {
    return subscriptionProviders.containsKey(dataNodeId);
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  List<SubscriptionProvider> getAvailableProviders() {
    return subscriptionProviders.values().stream()
        .filter(SubscriptionProvider::isAvailable)
        .collect(Collectors.toList());
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  List<SubscriptionProvider> getAllProviders() {
    return new ArrayList<>(subscriptionProviders.values());
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  SubscriptionProvider getProvider(final int dataNodeId) {
    if (!containsProvider(dataNodeId)) {
      return null;
    }
    final SubscriptionProvider provider = subscriptionProviders.get(dataNodeId);
    if (!provider.isAvailable()) {
      return null;
    }
    return provider;
  }

  /////////////////////////////// redirection ///////////////////////////////

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  public void subscribeWithRedirection(final Set<String> topicNames)
      throws IoTDBConnectionException {
    for (final SubscriptionProvider provider : getAvailableProviders()) {
      try {
        provider.getSessionConnection().subscribe(topicNames);
        return;
      } catch (final Exception e) {
        LOGGER.warn(
            "Failed to subscribe topics {} from subscription provider {}, exception: {}, try next subscription provider...",
            topicNames,
            provider,
            e.getMessage());
      }
    }
    throw NO_PROVIDERS_EXCEPTION;
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  public void unsubscribeWithRedirection(final Set<String> topicNames)
      throws IoTDBConnectionException {
    for (final SubscriptionProvider provider : getAvailableProviders()) {
      try {
        provider.getSessionConnection().unsubscribe(topicNames);
        return;
      } catch (final Exception e) {
        LOGGER.warn(
            "Failed to unsubscribe topics {} from subscription provider {}, exception: {}, try next subscription provider...",
            topicNames,
            provider,
            e.getMessage());
      }
    }
    throw NO_PROVIDERS_EXCEPTION;
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  public Map<Integer, TEndPoint> fetchAllEndPointsWithRedirection()
      throws IoTDBConnectionException {
    Map<Integer, TEndPoint> endPoints = null;
    for (final SubscriptionProvider provider : getAvailableProviders()) {
      try {
        endPoints = provider.getSessionConnection().fetchAllEndPoints();
      } catch (final Exception e) {
        LOGGER.warn(
            "Failed to fetch all endpoints from subscription provider {}, exception: {}, try next subscription provider...",
            provider,
            e.getMessage());
      }
    }
    if (Objects.isNull(endPoints)) {
      throw NO_PROVIDERS_EXCEPTION;
    }
    return endPoints;
  }

  /////////////////////////////// builder ///////////////////////////////

  public abstract static class Builder {

    protected String host = SessionConfig.DEFAULT_HOST;
    protected int port = SessionConfig.DEFAULT_PORT;
    protected List<String> nodeUrls = null;

    protected String username = SessionConfig.DEFAULT_USER;
    protected String password = SessionConfig.DEFAULT_PASSWORD;

    protected String consumerId;
    protected String consumerGroupId;

    public Builder host(String host) {
      this.host = host;
      return this;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder nodeUrls(List<String> nodeUrls) {
      this.nodeUrls = nodeUrls;
      return this;
    }

    public Builder username(String username) {
      this.username = username;
      return this;
    }

    public Builder password(String password) {
      this.password = password;
      return this;
    }

    public Builder consumerId(String consumerId) {
      this.consumerId = consumerId;
      return this;
    }

    public Builder consumerGroupId(String consumerGroupId) {
      this.consumerGroupId = consumerGroupId;
      return this;
    }

    public abstract SubscriptionPullConsumer buildPullConsumer();

    public abstract SubscriptionPushConsumer buildPushConsumer();
  }
}
