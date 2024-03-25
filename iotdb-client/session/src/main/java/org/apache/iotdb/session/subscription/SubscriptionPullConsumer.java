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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SubscriptionPullConsumer extends SubscriptionConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionPullConsumer.class);

  private final boolean autoCommit;
  private final int autoCommitInterval;

  private ScheduledExecutorService workerExecutor;
  private ConcurrentLinkedQueue<SubscriptionMessage> uncommittedMessages;

  private boolean isClosed = true;

  /////////////////////////////// ctor ///////////////////////////////

  public SubscriptionPullConsumer(SubscriptionPullConsumer.Builder builder)
      throws IoTDBConnectionException, TException, IOException, StatementExecutionException {
    super(builder);

    this.autoCommit = builder.autoCommit;
    this.autoCommitInterval = builder.autoCommitInterval;

    if (autoCommit) {
      this.uncommittedMessages = new ConcurrentLinkedQueue<>();
      launchAutoCommitWorker();
    }

    isClosed = false;
  }

  public SubscriptionPullConsumer(Properties config)
      throws TException, IoTDBConnectionException, IOException, StatementExecutionException {
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

  private SubscriptionPullConsumer(Properties config, boolean autoCommit, int autoCommitInterval)
      throws TException, IoTDBConnectionException, IOException, StatementExecutionException {
    super(new Builder().autoCommit(autoCommit).autoCommitInterval(autoCommitInterval), config);

    this.autoCommit = autoCommit;
    this.autoCommitInterval = autoCommitInterval;

    if (autoCommit) {
      this.uncommittedMessages = new ConcurrentLinkedQueue<>();
      launchAutoCommitWorker();
    }

    isClosed = false;
  }

  /////////////////////////////// APIs ///////////////////////////////

  public List<SubscriptionMessage> poll(Duration timeout)
      throws TException, IOException, StatementExecutionException {
    // TODO: timeout
    List<EnrichedTablets> enrichedTabletsList = new ArrayList<>();
    for (SubscriptionSessionConnection connection : getSessionConnections()) {
      // TODO: specify the topics to poll
      enrichedTabletsList.addAll(connection.poll(Collections.emptySet()));
    }

    List<SubscriptionMessage> messages =
        enrichedTabletsList.stream()
            .map(
                (enrichedTablets) ->
                    new SubscriptionMessage(new SubscriptionSessionDataSet(enrichedTablets)))
            .collect(Collectors.toList());
    if (autoCommit) {
      messages.forEach((message) -> uncommittedMessages.offer(message));
    }
    return messages;
  }

  public void commitSync(List<SubscriptionMessage> messages)
      throws TException, IOException, StatementExecutionException {
    checkBeforeCommit();

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

  @Override
  public void close() throws IoTDBConnectionException {
    if (isClosed) {
      return;
    }

    try {
      if (autoCommit) {
        workerExecutor.shutdown();
        workerExecutor = null;
        // TODO: commit all uncommitted message
      }
      super.close();
    } finally {
      isClosed = true;
    }
  }

  public boolean isClosed() {
    return isClosed;
  }

  /////////////////////////////// utility ///////////////////////////////

  private void commitSyncInternal(
      int dataNodeId, Map<String, List<String>> topicNameToSubscriptionCommitIds)
      throws TException, IOException, StatementExecutionException {
    getSessionConnection(dataNodeId).commitSync(topicNameToSubscriptionCommitIds);
  }

  private void checkBeforeCommit() {
    if (autoCommit) {
      LOGGER.warn(
          "The 'auto-commit' parameter is set to true, so manual commits are not required...");
    }
  }

  private void launchAutoCommitWorker() {
    this.workerExecutor =
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
    workerExecutor.scheduleWithFixedDelay(
        new SubscriptionAutoCommitWorker(this), 0, autoCommitInterval, TimeUnit.MILLISECONDS);
  }

  ConcurrentLinkedQueue<SubscriptionMessage> getUncommittedMessages() {
    return uncommittedMessages;
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
      this.autoCommitInterval = autoCommitInterval;
      return this;
    }

    @Override
    public SubscriptionPullConsumer buildPullConsumer()
        throws IoTDBConnectionException, TException, IOException, StatementExecutionException {
      return new SubscriptionPullConsumer(this);
    }

    @Override
    public SubscriptionPushConsumer buildPushConsumer() {
      throw new SubscriptionException(
          "SubscriptionPullConsumer.Builder do not support build push consumer.");
    }
  }
}
