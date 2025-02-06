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

import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.session.subscription.consumer.AsyncCommitCallback;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.util.CollectionUtils;
import org.apache.iotdb.session.subscription.util.IdentifierUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * The {@link AbstractSubscriptionPullConsumer} corresponds to the pull consumption mode in the
 * message queue.
 *
 * <p>User code needs to actively call the data retrieval logic, i.e., the {@link #poll} method.
 *
 * <p>Auto-commit for consumption progress can be configured in {@link #autoCommit}.
 *
 * <p>NOTE: It is not recommended to use the {@link #poll} method with the same consumer in a
 * multithreaded environment. Instead, it is advised to increase the number of consumers to improve
 * data retrieval parallelism.
 */
public abstract class AbstractSubscriptionPullConsumer extends AbstractSubscriptionConsumer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractSubscriptionPullConsumer.class);

  private final boolean autoCommit;
  private final long autoCommitIntervalMs;

  private SortedMap<Long, Set<SubscriptionMessage>> uncommittedMessages;

  private final AtomicBoolean isClosed = new AtomicBoolean(true);

  @Override
  boolean isClosed() {
    return isClosed.get();
  }

  /////////////////////////////// ctor ///////////////////////////////

  protected AbstractSubscriptionPullConsumer(
      final AbstractSubscriptionPullConsumerBuilder builder) {
    super(builder);

    this.autoCommit = builder.autoCommit;
    this.autoCommitIntervalMs = builder.autoCommitIntervalMs;
  }

  public AbstractSubscriptionPullConsumer(final Properties properties) {
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

  protected AbstractSubscriptionPullConsumer(
      final Properties properties, final boolean autoCommit, final long autoCommitIntervalMs) {
    super(
        new AbstractSubscriptionPullConsumerBuilder()
            .autoCommit(autoCommit)
            .autoCommitIntervalMs(autoCommitIntervalMs),
        properties);

    this.autoCommit = autoCommit;
    this.autoCommitIntervalMs =
        Math.max(autoCommitIntervalMs, ConsumerConstant.AUTO_COMMIT_INTERVAL_MS_MIN_VALUE);
  }

  /////////////////////////////// open & close ///////////////////////////////

  @Override
  protected synchronized void open() throws SubscriptionException {
    if (!isClosed.get()) {
      return;
    }

    super.open();

    // set isClosed to false before submitting workers
    isClosed.set(false);

    // submit auto poll worker if enabling auto commit
    if (autoCommit) {
      uncommittedMessages = new ConcurrentSkipListMap<>();
      submitAutoCommitWorker();
    }
  }

  @Override
  public synchronized void close() {
    if (isClosed.get()) {
      return;
    }

    if (autoCommit) {
      // commit all uncommitted messages
      commitAllUncommittedMessages();
    }

    super.close();
    isClosed.set(true);
  }

  /////////////////////////////// poll & commit ///////////////////////////////

  protected List<SubscriptionMessage> poll(final Duration timeout) throws SubscriptionException {
    return poll(Collections.emptySet(), timeout.toMillis());
  }

  protected List<SubscriptionMessage> poll(final long timeoutMs) throws SubscriptionException {
    return poll(Collections.emptySet(), timeoutMs);
  }

  protected List<SubscriptionMessage> poll(final Set<String> topicNames, final Duration timeout)
      throws SubscriptionException {
    return poll(topicNames, timeout.toMillis());
  }

  protected List<SubscriptionMessage> poll(final Set<String> topicNames, final long timeoutMs)
      throws SubscriptionException {
    // parse topic names from external source
    Set<String> parsedTopicNames =
        topicNames.stream()
            .map(IdentifierUtils::checkAndParseIdentifier)
            .collect(Collectors.toSet());

    if (!parsedTopicNames.isEmpty()) {
      // filter unsubscribed topics
      parsedTopicNames.stream()
          .filter(topicName -> !subscribedTopics.containsKey(topicName))
          .forEach(
              topicName ->
                  LOGGER.warn(
                      "SubscriptionPullConsumer {} does not subscribe to topic {}",
                      this,
                      topicName));
    } else {
      parsedTopicNames = subscribedTopics.keySet();
    }

    if (parsedTopicNames.isEmpty()) {
      return Collections.emptyList();
    }

    final List<SubscriptionMessage> messages = multiplePoll(parsedTopicNames, timeoutMs);
    if (messages.isEmpty()) {
      LOGGER.info(
          "SubscriptionPullConsumer {} poll empty message from topics {} after {} millisecond(s)",
          this,
          CollectionUtils.getLimitedString(parsedTopicNames, 32),
          timeoutMs);
      return messages;
    }

    // add to uncommitted messages
    if (autoCommit) {
      final long currentTimestamp = System.currentTimeMillis();
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

  /////////////////////////////// commit ///////////////////////////////

  protected void commitSync(final SubscriptionMessage message) throws SubscriptionException {
    super.ack(Collections.singletonList(message));
  }

  protected void commitSync(final Iterable<SubscriptionMessage> messages)
      throws SubscriptionException {
    super.ack(messages);
  }

  protected CompletableFuture<Void> commitAsync(final SubscriptionMessage message) {
    return super.commitAsync(Collections.singletonList(message));
  }

  protected CompletableFuture<Void> commitAsync(final Iterable<SubscriptionMessage> messages) {
    return super.commitAsync(messages);
  }

  protected void commitAsync(
      final SubscriptionMessage message, final AsyncCommitCallback callback) {
    super.commitAsync(Collections.singletonList(message), callback);
  }

  protected void commitAsync(
      final Iterable<SubscriptionMessage> messages, final AsyncCommitCallback callback) {
    super.commitAsync(messages, callback);
  }

  /////////////////////////////// auto commit ///////////////////////////////

  private void submitAutoCommitWorker() {
    final ScheduledFuture<?>[] future = new ScheduledFuture<?>[1];
    future[0] =
        SubscriptionExecutorServiceManager.submitAutoCommitWorker(
            () -> {
              if (isClosed()) {
                if (Objects.nonNull(future[0])) {
                  future[0].cancel(false);
                  LOGGER.info("SubscriptionPullConsumer {} cancel auto commit worker", this);
                }
                return;
              }
              new AutoCommitWorker().run();
            },
            autoCommitIntervalMs);
    LOGGER.info("SubscriptionPullConsumer {} submit auto commit worker", this);
  }

  private class AutoCommitWorker implements Runnable {
    @Override
    public void run() {
      if (isClosed()) {
        return;
      }

      final long currentTimestamp = System.currentTimeMillis();
      long index = currentTimestamp / autoCommitIntervalMs;
      if (currentTimestamp % autoCommitIntervalMs == 0) {
        index -= 1;
      }

      for (final Map.Entry<Long, Set<SubscriptionMessage>> entry :
          uncommittedMessages.headMap(index).entrySet()) {
        try {
          ack(entry.getValue());
          uncommittedMessages.remove(entry.getKey());
        } catch (final Exception e) {
          LOGGER.warn("something unexpected happened when auto commit messages...", e);
        }
      }
    }
  }

  private void commitAllUncommittedMessages() {
    for (final Map.Entry<Long, Set<SubscriptionMessage>> entry : uncommittedMessages.entrySet()) {
      try {
        ack(entry.getValue());
        uncommittedMessages.remove(entry.getKey());
      } catch (final Exception e) {
        LOGGER.warn("something unexpected happened when commit messages during close", e);
      }
    }
  }

  /////////////////////////////// stringify ///////////////////////////////

  @Override
  public String toString() {
    return "SubscriptionPullConsumer" + this.coreReportMessage();
  }

  @Override
  protected Map<String, String> coreReportMessage() {
    final Map<String, String> coreReportMessage = super.coreReportMessage();
    coreReportMessage.put("autoCommit", String.valueOf(autoCommit));
    return coreReportMessage;
  }

  @Override
  protected Map<String, String> allReportMessage() {
    final Map<String, String> allReportMessage = super.allReportMessage();
    allReportMessage.put("autoCommit", String.valueOf(autoCommit));
    allReportMessage.put("autoCommitIntervalMs", String.valueOf(autoCommitIntervalMs));
    if (autoCommit) {
      allReportMessage.put("uncommittedMessages", uncommittedMessages.toString());
    }
    return allReportMessage;
  }
}
