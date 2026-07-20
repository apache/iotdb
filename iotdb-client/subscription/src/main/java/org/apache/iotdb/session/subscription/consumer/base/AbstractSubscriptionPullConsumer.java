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
import org.apache.iotdb.rpc.subscription.exception.SubscriptionParameterNotValidException;
import org.apache.iotdb.rpc.subscription.i18n.SubscriptionMessages;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.TopicProgress;
import org.apache.iotdb.session.subscription.consumer.AsyncCommitCallback;
import org.apache.iotdb.session.subscription.payload.PollResult;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessageType;
import org.apache.iotdb.session.subscription.util.CollectionUtils;
import org.apache.iotdb.session.subscription.util.IdentifierUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
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

  private final List<SubscriptionMessageProcessor> processors = new ArrayList<>();

  private final Set<SubscriptionCommitContext> processorBufferedCommitContexts =
      ConcurrentHashMap.newKeySet();

  private final Queue<SubscriptionMessage> pendingDrainedMessages = new ConcurrentLinkedQueue<>();

  private SortedMap<Long, Set<SubscriptionCommitContext>> uncommittedCommitContexts;

  private final EmptyPollLogThrottler emptyPollLogThrottler = new EmptyPollLogThrottler();

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

    // set isClosed to false before submitting workers
    isClosed.set(false);
    try {
      super.open();
    } catch (final SubscriptionException e) {
      isClosed.set(true);
      throw e;
    }
    emptyPollLogThrottler.reset();

    // submit auto poll worker if enabling auto commit
    if (autoCommit) {
      uncommittedCommitContexts = new ConcurrentSkipListMap<>();
      submitAutoCommitWorker();
    }
  }

  @Override
  public synchronized void close() {
    if (isClosed.get()) {
      return;
    }

    if (!processors.isEmpty()) {
      if (autoCommit) {
        final List<SubscriptionMessage> drainedMessages = drainProcessorPipeline();
        if (!drainedMessages.isEmpty()) {
          try {
            commitSync(drainedMessages);
          } catch (final SubscriptionException e) {
            LOGGER.warn(
                SubscriptionMessages.LOG_FAILED_COMMIT_DRAINED_PROCESSOR_MESSAGES_CLOSE_4264DB35,
                e);
          }
        }
      } else {
        final List<SubscriptionMessage> drainedMessages = drainProcessorPipeline();
        if (!drainedMessages.isEmpty()) {
          pendingDrainedMessages.addAll(drainedMessages);
        }
        ensureNoManualBufferedMessagesOnClose();
      }
    }

    if (autoCommit && !pendingDrainedMessages.isEmpty()) {
      final List<SubscriptionMessage> drainedMessages = drainPendingDrainedMessages();
      if (!drainedMessages.isEmpty()) {
        try {
          commitSync(drainedMessages);
        } catch (final SubscriptionException e) {
          LOGGER.warn(
              SubscriptionMessages
                  .LOG_FAILED_COMMIT_PENDING_DRAINED_PROCESSOR_MESSAGES_CLOSE_644B5DDD,
              e);
        }
      }
    }

    if (!autoCommit) {
      ensureNoManualBufferedMessagesOnClose();
    }

    if (autoCommit) {
      // commit all uncommitted messages
      commitAllUncommittedMessages();
    }

    isClosed.set(true);
    super.close();
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
                      SubscriptionMessages
                          .LOG_SUBSCRIPTIONPULLCONSUMER_ARG_DOES_NOT_SUBSCRIBE_TOPIC_ARG_F40BE4D1,
                      this,
                      topicName));
    } else {
      parsedTopicNames = subscribedTopics.keySet();
    }

    if (parsedTopicNames.isEmpty()) {
      return Collections.emptyList();
    }

    final List<SubscriptionMessage> messages = multiplePoll(parsedTopicNames, timeoutMs);
    if (messages.isEmpty() && processors.isEmpty()) {
      final OptionalLong consecutiveEmptyPollCount =
          emptyPollLogThrottler.markEmptyPollAndMaybeGetCount();
      if (consecutiveEmptyPollCount.isPresent()) {
        LOGGER.info(
            SubscriptionMessages.PULL_CONSUMER_POLL_EMPTY_MESSAGE,
            this,
            CollectionUtils.getLimitedString(parsedTopicNames, 32),
            timeoutMs,
            consecutiveEmptyPollCount.getAsLong());
      }
      return messages;
    }

    // Apply processor chain if configured
    List<SubscriptionMessage> processed = messages;
    if (!processors.isEmpty()) {
      for (final SubscriptionMessageProcessor processor : processors) {
        processed = processor.process(processed);
      }
      refreshProcessorBufferedCommitContexts();
    }

    processed = filterUserVisibleMessages(processed);

    if (processed.isEmpty()) {
      return processed;
    }

    emptyPollLogThrottler.reset();
    trackAutoCommitMessages(processed);

    return processed;
  }

  protected List<SubscriptionMessage> drainBufferedMessages() throws SubscriptionException {
    if (isClosed()) {
      final String errorMessage =
          String.format(
              "%s is not yet open, please open the subscription consumer before draining buffered messages.",
              this);
      LOGGER.error(errorMessage);
      throw new SubscriptionException(errorMessage);
    }

    final List<SubscriptionMessage> drainedMessages = drainPendingDrainedMessages();
    if (!drainedMessages.isEmpty()) {
      trackAutoCommitMessages(drainedMessages);
      return drainedMessages;
    }

    if (processors.isEmpty()) {
      return Collections.emptyList();
    }

    drainedMessages.addAll(drainProcessorPipeline());
    trackAutoCommitMessages(drainedMessages);
    return drainedMessages;
  }

  /////////////////////////////// processor ///////////////////////////////

  /**
   * Adds a message processor to the pipeline. Processors are applied in order on each poll() call.
   *
   * @param processor the processor to add
   */
  protected AbstractSubscriptionPullConsumer addProcessor(
      final SubscriptionMessageProcessor processor) {
    processors.add(processor);
    return this;
  }

  @Override
  List<SubscriptionCommitContext> getProcessorBufferedCommitContexts(final int dataNodeId) {
    if (processorBufferedCommitContexts.isEmpty()) {
      return Collections.emptyList();
    }

    final List<SubscriptionCommitContext> result = new ArrayList<>();
    for (final SubscriptionCommitContext commitContext : processorBufferedCommitContexts) {
      if (Objects.nonNull(commitContext) && commitContext.getDataNodeId() == dataNodeId) {
        result.add(commitContext);
      }
    }
    return result;
  }

  private void refreshProcessorBufferedCommitContexts() {
    processorBufferedCommitContexts.clear();
    for (final SubscriptionMessageProcessor processor : processors) {
      final List<SubscriptionCommitContext> bufferedCommitContexts =
          processor.getBufferedCommitContexts();
      if (Objects.isNull(bufferedCommitContexts)) {
        continue;
      }
      for (final SubscriptionCommitContext commitContext : bufferedCommitContexts) {
        if (Objects.nonNull(commitContext) && commitContext.isCommittable()) {
          processorBufferedCommitContexts.add(commitContext);
        }
      }
    }
  }

  private List<SubscriptionMessage> drainProcessorPipeline() {
    List<SubscriptionMessage> drainedMessages = Collections.emptyList();
    for (final SubscriptionMessageProcessor processor : processors) {
      if (!drainedMessages.isEmpty()) {
        drainedMessages = processor.process(drainedMessages);
        if (Objects.isNull(drainedMessages)) {
          drainedMessages = Collections.emptyList();
        }
      }

      final List<SubscriptionMessage> flushedMessages = processor.flush();
      drainedMessages = appendMessages(drainedMessages, flushedMessages);
    }

    refreshProcessorBufferedCommitContexts();
    return filterUserVisibleMessages(drainedMessages);
  }

  private static List<SubscriptionMessage> appendMessages(
      final List<SubscriptionMessage> baseMessages,
      final List<SubscriptionMessage> appendedMessages) {
    if (Objects.isNull(appendedMessages) || appendedMessages.isEmpty()) {
      return baseMessages;
    }
    if (baseMessages.isEmpty()) {
      return appendedMessages;
    }

    final List<SubscriptionMessage> mergedMessages =
        new ArrayList<>(baseMessages.size() + appendedMessages.size());
    mergedMessages.addAll(baseMessages);
    mergedMessages.addAll(appendedMessages);
    return mergedMessages;
  }

  private List<SubscriptionMessage> drainPendingDrainedMessages() {
    final List<SubscriptionMessage> drainedMessages = new ArrayList<>();
    SubscriptionMessage message;
    while (Objects.nonNull(message = pendingDrainedMessages.poll())) {
      drainedMessages.add(message);
    }
    return drainedMessages;
  }

  private boolean hasProcessorBufferedMessages() {
    if (!processorBufferedCommitContexts.isEmpty()) {
      return true;
    }

    for (final SubscriptionMessageProcessor processor : processors) {
      if (processor.getBufferedCount() > 0) {
        return true;
      }
    }
    return false;
  }

  private void ensureNoManualBufferedMessagesOnClose() throws SubscriptionException {
    if (pendingDrainedMessages.isEmpty() && !hasProcessorBufferedMessages()) {
      return;
    }

    final String errorMessage =
        String.format(
            "SubscriptionPullConsumer %s still has processor-buffered or drained messages when closing in manual-commit mode. Call drainBufferedMessages() and commit the returned messages before close.",
            this);
    LOGGER.warn(errorMessage);
    throw new SubscriptionException(errorMessage);
  }

  private void ensureTopicScopedProcessorResetSupported(final String topicName)
      throws SubscriptionException {
    if (processors.isEmpty() || subscribedTopics.size() <= 1) {
      return;
    }

    for (final SubscriptionMessageProcessor processor : processors) {
      if (!processor.supportsTopicScopedReset()) {
        throw new SubscriptionParameterNotValidException(
            String.format(
                SubscriptionMessages
                    .EXCEPTION_SUBSCRIPTIONPULLCONSUMER_ARG_CANNOT_SEEK_TOPIC_ARG_SUBSCRIBED_MULTIPLE_TOPICS_BECAUSE_B99BCABC,
                this,
                topicName,
                processor.getClass().getName()));
      }
    }
  }

  private void resetProcessors(final String topicName) {
    for (final SubscriptionMessageProcessor processor : processors) {
      if (processor.supportsTopicScopedReset()) {
        processor.reset(topicName);
      } else {
        processor.reset();
      }
    }
    refreshProcessorBufferedCommitContexts();
  }

  private void clearUncommittedCommitContexts(final String topicName) {
    for (final Map.Entry<Long, Set<SubscriptionCommitContext>> entry :
        uncommittedCommitContexts.entrySet()) {
      entry
          .getValue()
          .removeIf(
              commitContext ->
                  Objects.nonNull(commitContext)
                      && Objects.equals(topicName, commitContext.getTopicName()));
      if (entry.getValue().isEmpty()) {
        uncommittedCommitContexts.remove(entry.getKey());
      }
    }
  }

  /**
   * Polls with processor metadata. Returns a {@link PollResult} containing the messages, the total
   * number of buffered messages across all processors, and the current watermark.
   */
  protected PollResult pollWithInfo(final long timeoutMs) throws SubscriptionException {
    final List<SubscriptionMessage> messages = poll(timeoutMs);
    int totalBuffered = 0;
    long watermark = -1;
    for (final SubscriptionMessageProcessor processor : processors) {
      totalBuffered += processor.getBufferedCount();
      if (processor instanceof WatermarkProcessor) {
        watermark = ((WatermarkProcessor) processor).getWatermark();
      }
    }
    return new PollResult(messages, totalBuffered, watermark);
  }

  protected PollResult pollWithInfo(final Set<String> topicNames, final long timeoutMs)
      throws SubscriptionException {
    final List<SubscriptionMessage> messages = poll(topicNames, timeoutMs);
    int totalBuffered = 0;
    long watermark = -1;
    for (final SubscriptionMessageProcessor processor : processors) {
      totalBuffered += processor.getBufferedCount();
      if (processor instanceof WatermarkProcessor) {
        watermark = ((WatermarkProcessor) processor).getWatermark();
      }
    }
    return new PollResult(messages, totalBuffered, watermark);
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

  private List<SubscriptionMessage> filterUserVisibleMessages(
      final List<SubscriptionMessage> messages) {
    if (messages.isEmpty()) {
      return messages;
    }

    final List<SubscriptionMessage> result = new ArrayList<>(messages.size());
    for (final SubscriptionMessage message : messages) {
      if (message.getMessageType() == SubscriptionMessageType.WATERMARK.getType()) {
        final long timestamp = message.getWatermarkTimestamp();
        if (timestamp > latestWatermarkTimestamp) {
          latestWatermarkTimestamp = timestamp;
        }
        continue;
      }
      result.add(message);
    }
    return result;
  }

  private void trackAutoCommitMessages(final List<SubscriptionMessage> messages) {
    if (!autoCommit || messages.isEmpty()) {
      return;
    }

    final long currentTimestamp = System.currentTimeMillis();
    long index = currentTimestamp / autoCommitIntervalMs;
    if (currentTimestamp % autoCommitIntervalMs == 0) {
      index -= 1;
    }
    uncommittedCommitContexts
        .computeIfAbsent(index, o -> new ConcurrentSkipListSet<>())
        .addAll(
            messages.stream()
                .map(SubscriptionMessage::getCommitContext)
                .collect(Collectors.toList()));
  }

  /////////////////////////////// seek ///////////////////////////////

  /**
   * Clears uncommitted auto-commit messages after seek to prevent stale acks from committing events
   * that belonged to the pre-seek position.
   */
  @Override
  public void seekToBeginning(final String topicName) throws SubscriptionException {
    final String parsedTopicName = IdentifierUtils.checkAndParseIdentifier(topicName);
    ensureTopicScopedProcessorResetSupported(parsedTopicName);
    super.seekToBeginning(parsedTopicName);
    resetProcessors(parsedTopicName);
    if (autoCommit) {
      clearUncommittedCommitContexts(parsedTopicName);
    }
  }

  @Override
  public void seekToEnd(final String topicName) throws SubscriptionException {
    final String parsedTopicName = IdentifierUtils.checkAndParseIdentifier(topicName);
    ensureTopicScopedProcessorResetSupported(parsedTopicName);
    super.seekToEnd(parsedTopicName);
    resetProcessors(parsedTopicName);
    if (autoCommit) {
      clearUncommittedCommitContexts(parsedTopicName);
    }
  }

  @Override
  public void seek(final String topicName, final TopicProgress topicProgress)
      throws SubscriptionException {
    final String parsedTopicName = IdentifierUtils.checkAndParseIdentifier(topicName);
    ensureTopicScopedProcessorResetSupported(parsedTopicName);
    super.seek(parsedTopicName, topicProgress);
    resetProcessors(parsedTopicName);
    if (autoCommit) {
      clearUncommittedCommitContexts(parsedTopicName);
    }
  }

  @Override
  public void seekAfter(final String topicName, final TopicProgress topicProgress)
      throws SubscriptionException {
    final String parsedTopicName = IdentifierUtils.checkAndParseIdentifier(topicName);
    ensureTopicScopedProcessorResetSupported(parsedTopicName);
    super.seekAfter(parsedTopicName, topicProgress);
    resetProcessors(parsedTopicName);
    if (autoCommit) {
      clearUncommittedCommitContexts(parsedTopicName);
    }
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
                  LOGGER.info(SubscriptionMessages.PULL_CONSUMER_CANCEL_AUTO_COMMIT, this);
                }
                return;
              }
              new AutoCommitWorker().run();
            },
            autoCommitIntervalMs);
    LOGGER.info(SubscriptionMessages.PULL_CONSUMER_SUBMIT_AUTO_COMMIT, this);
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

      for (final Map.Entry<Long, Set<SubscriptionCommitContext>> entry :
          uncommittedCommitContexts.headMap(index).entrySet()) {
        try {
          final Set<SubscriptionCommitContext> removableCommitContexts =
              ackCommitContextsWithPartialProgress(entry.getValue());
          if (removableCommitContexts.isEmpty()) {
            continue;
          }
          if (removableCommitContexts.size() == entry.getValue().size()) {
            uncommittedCommitContexts.remove(entry.getKey());
            continue;
          }
          entry.getValue().removeAll(removableCommitContexts);
          if (entry.getValue().isEmpty()) {
            uncommittedCommitContexts.remove(entry.getKey());
          }
        } catch (final Exception e) {
          LOGGER.warn(SubscriptionMessages.AUTO_COMMIT_UNEXPECTED, e);
        }
      }
    }
  }

  private void commitAllUncommittedMessages() {
    for (final Map.Entry<Long, Set<SubscriptionCommitContext>> entry :
        uncommittedCommitContexts.entrySet()) {
      try {
        final Set<SubscriptionCommitContext> removableCommitContexts =
            ackCommitContextsWithPartialProgress(entry.getValue());
        if (removableCommitContexts.isEmpty()) {
          continue;
        }
        if (removableCommitContexts.size() == entry.getValue().size()) {
          uncommittedCommitContexts.remove(entry.getKey());
          continue;
        }
        entry.getValue().removeAll(removableCommitContexts);
        if (entry.getValue().isEmpty()) {
          uncommittedCommitContexts.remove(entry.getKey());
        }
      } catch (final Exception e) {
        LOGGER.warn(SubscriptionMessages.COMMIT_DURING_CLOSE_UNEXPECTED, e);
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
      allReportMessage.put("uncommittedCommitContexts", uncommittedCommitContexts.toString());
    }
    return allReportMessage;
  }
}
