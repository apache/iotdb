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
import org.apache.iotdb.rpc.subscription.SubscriptionException;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.payload.EnrichedTablets;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SubscriptionPullConsumer extends SubscriptionConsumer implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionPullConsumer.class);

  private final boolean autoCommit;
  private final int autoCommitInterval;

  private ScheduledExecutorService workerExecutor;
  private SortedMap<Long, List<SubscriptionMessage>> uncommittedMessages;

  private boolean isClosed = true;

  /////////////////////////////// ctor ///////////////////////////////

  public SubscriptionPullConsumer(SubscriptionPullConsumer.Builder builder) {
    super(builder);

    this.autoCommit = builder.autoCommit;
    this.autoCommitInterval = builder.autoCommitInterval;
  }

  public SubscriptionPullConsumer(Properties config) {
    this(
        config,
        (Boolean)
            config.getOrDefault(
                ConsumerConstant.AUTO_COMMIT_KEY, ConsumerConstant.AUTO_COMMIT_DEFAULT_VALUE),
        (Integer)
            config.getOrDefault(
                ConsumerConstant.AUTO_COMMIT_INTERVAL_KEY,
                ConsumerConstant.AUTO_COMMIT_INTERVAL_DEFAULT_VALUE));
  }

  private SubscriptionPullConsumer(Properties config, boolean autoCommit, int autoCommitInterval) {
    super(new Builder().autoCommit(autoCommit).autoCommitInterval(autoCommitInterval), config);

    this.autoCommit = autoCommit;
    this.autoCommitInterval = autoCommitInterval;
  }

  /////////////////////////////// APIs ///////////////////////////////

  public void open()
      throws TException, IoTDBConnectionException, IOException, StatementExecutionException {
    super.open();

    if (autoCommit) {
      launchAutoCommitWorker();
    }

    isClosed = false;
  }

  @Override
  public void close() throws IoTDBConnectionException {
    if (isClosed) {
      return;
    }

    try {
      if (autoCommit) {
        workerExecutor.shutdown();
        workerExecutor = null;
        // commit all uncommitted messages
        commitAllUncommittedMessages();
      }
      super.close();
    } finally {
      isClosed = true;
    }
  }

  public boolean isClosed() {
    return isClosed;
  }

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
    // TODO: network timeout
    List<EnrichedTablets> enrichedTabletsList = new ArrayList<>();
    for (SubscriptionSessionConnection connection : getSessionConnections()) {
      enrichedTabletsList.addAll(connection.poll(topicNames, timeoutMs));
    }

    List<SubscriptionMessage> messages =
        enrichedTabletsList.stream()
            .map(
                (enrichedTablets) ->
                    new SubscriptionMessage(new SubscriptionSessionDataSet(enrichedTablets)))
            .collect(Collectors.toList());
    if (autoCommit) {
      long currentTimestamp = System.currentTimeMillis();
      long index = currentTimestamp / autoCommitInterval;
      if (currentTimestamp % autoCommitInterval == 0) {
        index -= 1;
      }
      // TODO: use thread-safe list
      uncommittedMessages.computeIfAbsent(index, o -> new ArrayList<>()).addAll(messages);
    }
    return messages;
  }

  public void commitSync(SubscriptionMessage message)
      throws TException, IOException, StatementExecutionException {
    commitSync(Collections.singletonList(message));
  }

  public void commitSync(List<SubscriptionMessage> messages)
      throws TException, IOException, StatementExecutionException {
    Map<Integer, Map<String, List<String>>> dataNodeIdToTopicNameToSubscriptionCommitIds =
        new HashMap<>();
    for (SubscriptionMessage message : messages) {
      dataNodeIdToTopicNameToSubscriptionCommitIds
          .computeIfAbsent(message.getDataNodeIdFromSubscriptionCommitId(), (id) -> new HashMap<>())
          .computeIfAbsent(message.getTopicName(), (topicName) -> new ArrayList<>())
          .add(message.getSubscriptionCommitId());
    }
    for (Map.Entry<Integer, Map<String, List<String>>> entry :
        dataNodeIdToTopicNameToSubscriptionCommitIds.entrySet()) {
      commitSyncInternal(entry.getKey(), entry.getValue());
    }
  }

  /////////////////////////////// utility ///////////////////////////////

  private void commitSyncInternal(
      int dataNodeId, Map<String, List<String>> topicNameToSubscriptionCommitIds)
      throws TException, IOException, StatementExecutionException {
    getSessionConnection(dataNodeId).commitSync(topicNameToSubscriptionCommitIds);
  }

  /////////////////////////////// auto commit ///////////////////////////////

  @SuppressWarnings("unsafeThreadSchedule")
  private void launchAutoCommitWorker() {
    uncommittedMessages = new ConcurrentSkipListMap<>();
    workerExecutor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t =
                  new Thread(
                      Thread.currentThread().getThreadGroup(),
                      r,
                      "SubscriptionAutoCommitWorker",
                      0);
              if (!t.isDaemon()) {
                t.setDaemon(true);
              }
              if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
              }
              return t;
            });
    workerExecutor.scheduleAtFixedRate(this, 0, autoCommitInterval, TimeUnit.MILLISECONDS);
  }

  private void commitAllUncommittedMessages() {
    for (Map.Entry<Long, List<SubscriptionMessage>> entry : uncommittedMessages.entrySet()) {
      try {
        commitSync(entry.getValue());
        uncommittedMessages.remove(entry.getKey());
      } catch (TException | IOException | StatementExecutionException e) {
        LOGGER.warn("something unexpected happened when commit messages during close", e);
      }
    }
  }

  @Override
  public void run() {
    if (isClosed()) {
      return;
    }

    long currentTimestamp = System.currentTimeMillis();
    long index = currentTimestamp / autoCommitInterval;
    if (currentTimestamp % autoCommitInterval == 0) {
      index -= 1;
    }

    for (Map.Entry<Long, List<SubscriptionMessage>> entry :
        uncommittedMessages.headMap(index).entrySet()) {
      try {
        commitSync(entry.getValue());
        uncommittedMessages.remove(entry.getKey());
      } catch (TException | IOException | StatementExecutionException e) {
        LOGGER.warn("something unexpected happened when auto commit messages...", e);
      }
    }
  }

  /////////////////////////////// builder ///////////////////////////////

  public static class Builder extends SubscriptionConsumer.Builder {

    private boolean autoCommit = ConsumerConstant.AUTO_COMMIT_DEFAULT_VALUE;
    private int autoCommitInterval = ConsumerConstant.AUTO_COMMIT_INTERVAL_DEFAULT_VALUE;

    public Builder autoCommit(boolean autoCommit) {
      this.autoCommit = autoCommit;
      return this;
    }

    public Builder autoCommitInterval(int autoCommitInterval) {
      // TODO: set min interval
      this.autoCommitInterval = autoCommitInterval;
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
