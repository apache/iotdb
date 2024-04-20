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

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class SubscriptionPullConsumer extends SubscriptionConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionPullConsumer.class);

  private final boolean autoCommit;
  private final long autoCommitIntervalMs;

  private ScheduledExecutorService autoCommitWorkerExecutor;
  private SortedMap<Long, Set<SubscriptionMessage>> uncommittedMessages;

  private final AtomicBoolean isClosed = new AtomicBoolean(true);

  /////////////////////////////// ctor ///////////////////////////////

  public SubscriptionPullConsumer(SubscriptionPullConsumer.Builder builder) {
    super(builder);

    this.autoCommit = builder.autoCommit;
    this.autoCommitIntervalMs = builder.autoCommitIntervalMs;
  }

  public SubscriptionPullConsumer(Properties properties) {
    this(
        properties,
        (Boolean)
            properties.getOrDefault(
                ConsumerConstant.AUTO_COMMIT_KEY, ConsumerConstant.AUTO_COMMIT_DEFAULT_VALUE),
        (Long)
            properties.getOrDefault(
                ConsumerConstant.AUTO_COMMIT_INTERVAL_MS_KEY,
                ConsumerConstant.AUTO_COMMIT_INTERVAL_MS_DEFAULT_VALUE));
  }

  private SubscriptionPullConsumer(
      Properties properties, boolean autoCommit, long autoCommitIntervalMs) {
    super(
        new Builder().autoCommit(autoCommit).autoCommitIntervalMs(autoCommitIntervalMs),
        properties);

    this.autoCommit = autoCommit;
    this.autoCommitIntervalMs = autoCommitIntervalMs;
  }

  /////////////////////////////// open & close ///////////////////////////////

  public synchronized void open()
      throws TException, IoTDBConnectionException, IOException, StatementExecutionException {
    if (!isClosed.get()) {
      return;
    }

    super.open();

    if (autoCommit) {
      launchAutoCommitWorker();
    }

    isClosed.set(false);
  }

  @Override
  public synchronized void close() throws IoTDBConnectionException {
    if (isClosed.get()) {
      return;
    }

    try {
      if (autoCommit) {
        // shutdown auto commit worker
        shutdownAutoCommitWorker();

        // commit all uncommitted messages
        commitAllUncommittedMessages();
      }
      super.close();
    } finally {
      isClosed.set(true);
    }
  }

  /////////////////////////////// poll & commit ///////////////////////////////

  public List<SubscriptionMessage> poll(Duration timeoutMs)
      throws TException, IOException, StatementExecutionException {
    return poll(Collections.emptySet(), timeoutMs.toMillis());
  }

  public List<SubscriptionMessage> poll(long timeoutMs)
      throws TException, IOException, StatementExecutionException {
    return poll(Collections.emptySet(), timeoutMs);
  }

  public List<SubscriptionMessage> poll(Set<String> topicNames, Duration timeoutMs)
      throws TException, IOException, StatementExecutionException {
    return poll(topicNames, timeoutMs.toMillis());
  }

  public List<SubscriptionMessage> poll(Set<String> topicNames, long timeoutMs)
      throws TException, IOException, StatementExecutionException {
    List<SubscriptionMessage> messages = super.poll(topicNames, timeoutMs);

    if (autoCommit) {
      long currentTimestamp = System.currentTimeMillis();
      long index = currentTimestamp / autoCommitIntervalMs;
      if (currentTimestamp % autoCommitIntervalMs == 0) {
        index -= 1;
      }
      uncommittedMessages
          .computeIfAbsent(index, o -> new ConcurrentSkipListSet<>())
          .addAll(messages);
    }

    return messages;
  }

  public void commitSync(SubscriptionMessage message)
      throws TException, IOException, StatementExecutionException, IoTDBConnectionException {
    super.commitSync(Collections.singletonList(message));
  }

  public void commitSync(Iterable<SubscriptionMessage> messages)
      throws TException, IOException, StatementExecutionException, IoTDBConnectionException {
    super.commitSync(messages);
  }

  public void commitAsync(SubscriptionMessage message) {
    super.commitAsync(Collections.singletonList(message));
  }

  public void commitAsync(Iterable<SubscriptionMessage> messages) {
    super.commitAsync(messages);
  }

  public void commitAsync(SubscriptionMessage message, AsyncCommitCallback callback) {
    super.commitAsync(Collections.singletonList(message), callback);
  }

  public void commitAsync(Iterable<SubscriptionMessage> messages, AsyncCommitCallback callback) {
    super.commitAsync(messages, callback);
  }

  /////////////////////////////// auto commit ///////////////////////////////

  @SuppressWarnings("unsafeThreadSchedule")
  private void launchAutoCommitWorker() {
    uncommittedMessages = new ConcurrentSkipListMap<>();
    autoCommitWorkerExecutor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t =
                  new Thread(
                      Thread.currentThread().getThreadGroup(),
                      r,
                      "PullConsumerAutoCommitWorker",
                      0);
              if (!t.isDaemon()) {
                t.setDaemon(true);
              }
              if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
              }
              return t;
            });
    autoCommitWorkerExecutor.scheduleAtFixedRate(
        new PullConsumerAutoCommitWorker(this), 0, autoCommitIntervalMs, TimeUnit.MILLISECONDS);
  }

  private void shutdownAutoCommitWorker() {
    autoCommitWorkerExecutor.shutdown();
    autoCommitWorkerExecutor = null;
  }

  private void commitAllUncommittedMessages() {
    for (Map.Entry<Long, Set<SubscriptionMessage>> entry : uncommittedMessages.entrySet()) {
      try {
        commitSync(entry.getValue());
        uncommittedMessages.remove(entry.getKey());
      } catch (final Exception e) {
        LOGGER.warn("something unexpected happened when commit messages during close", e);
      }
    }
  }

  @Override
  boolean isClosed() {
    return isClosed.get();
  }

  long getAutoCommitIntervalMs() {
    return autoCommitIntervalMs;
  }

  SortedMap<Long, Set<SubscriptionMessage>> getUncommittedMessages() {
    return uncommittedMessages;
  }

  /////////////////////////////// builder ///////////////////////////////

  public static class Builder extends SubscriptionConsumer.Builder {

    private boolean autoCommit = ConsumerConstant.AUTO_COMMIT_DEFAULT_VALUE;
    private long autoCommitIntervalMs = ConsumerConstant.AUTO_COMMIT_INTERVAL_MS_DEFAULT_VALUE;

    public Builder host(String host) {
      super.host(host);
      return this;
    }

    public Builder port(int port) {
      super.port(port);
      return this;
    }

    public Builder nodeUrls(List<String> nodeUrls) {
      super.nodeUrls(nodeUrls);
      return this;
    }

    public Builder username(String username) {
      super.username(username);
      return this;
    }

    public Builder password(String password) {
      super.password(password);
      return this;
    }

    public Builder consumerId(String consumerId) {
      super.consumerId(consumerId);
      return this;
    }

    public Builder consumerGroupId(String consumerGroupId) {
      super.consumerGroupId(consumerGroupId);
      return this;
    }

    public Builder heartbeatIntervalMs(long heartbeatIntervalMs) {
      super.heartbeatIntervalMs(heartbeatIntervalMs);
      return this;
    }

    public Builder endpointsSyncIntervalMs(long endpointsSyncIntervalMs) {
      super.endpointsSyncIntervalMs(endpointsSyncIntervalMs);
      return this;
    }

    public Builder autoCommit(boolean autoCommit) {
      this.autoCommit = autoCommit;
      return this;
    }

    public Builder autoCommitIntervalMs(long autoCommitIntervalMs) {
      this.autoCommitIntervalMs =
          Math.max(autoCommitIntervalMs, ConsumerConstant.AUTO_COMMIT_INTERVAL_MS_MIN_VALUE);
      return this;
    }

    @Override
    public SubscriptionPullConsumer buildPullConsumer() {
      return new SubscriptionPullConsumer(this);
    }

    @Override
    public SubscriptionPushConsumer buildPushConsumer() {
      throw new SubscriptionException(
          "SubscriptionPullConsumer.Builder do not support build push consumer.");
    }
  }
}
