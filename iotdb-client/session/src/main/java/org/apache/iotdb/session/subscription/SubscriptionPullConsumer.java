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
import org.apache.iotdb.rpc.subscription.payload.EnrichedTablets;

import org.apache.thrift.TException;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class SubscriptionPullConsumer extends SubscriptionConsumer {

  private final boolean autoCommit;
  private final int autoCommitInterval;

  /////////////////////////////// ctor ///////////////////////////////

  public SubscriptionPullConsumer(SubscriptionPullConsumer.Builder builder)
      throws IoTDBConnectionException, TException, IOException, StatementExecutionException {
    super(builder);

    this.autoCommit = builder.autoCommit;
    this.autoCommitInterval = builder.autoCommitInterval;
  }

  public SubscriptionPullConsumer(Properties config)
      throws TException, IoTDBConnectionException, IOException, StatementExecutionException {
    this(new Builder(), config);
  }

  public SubscriptionPullConsumer(Builder builder, Properties config)
      throws TException, IoTDBConnectionException, IOException, StatementExecutionException {
    super(
        builder
            .autoCommit(
                (Boolean)
                    config.getOrDefault(
                        ConsumerConstant.AUTO_COMMIT_KEY,
                        ConsumerConstant.AUTO_COMMIT_DEFAULT_VALUE))
            .autoCommitInterval(
                (Integer)
                    config.getOrDefault(
                        ConsumerConstant.AUTO_COMMIT_INTERVAL_KEY,
                        ConsumerConstant.AUTO_COMMIT_INTERVAL_DEFAULT_VALUE)),
        config);

    this.autoCommit =
        (Boolean)
            config.getOrDefault(
                ConsumerConstant.AUTO_COMMIT_KEY, ConsumerConstant.AUTO_COMMIT_DEFAULT_VALUE);
    this.autoCommitInterval =
        (Integer)
            config.getOrDefault(
                ConsumerConstant.AUTO_COMMIT_INTERVAL_KEY,
                ConsumerConstant.AUTO_COMMIT_INTERVAL_DEFAULT_VALUE);
  }

  /////////////////////////////// APIs ///////////////////////////////

  public List<SubscriptionMessage> poll(Duration timeout)
      throws TException, IOException, StatementExecutionException {
    // TODO: timeout
    // TODO: specify the topics to poll
    List<EnrichedTablets> enrichedTabletsList = new ArrayList<>();
    for (SubscriptionSessionConnection connection : getSessionConnections()) {
      enrichedTabletsList.addAll(connection.poll(Collections.emptySet()));
    }
    return enrichedTabletsList.stream()
        .map(
            (enrichedTablets) ->
                new SubscriptionMessage(new SubscriptionSessionDataSet(enrichedTablets)))
        .collect(Collectors.toList());
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
      int dataNodeId =
          EnrichedTablets.parseDataNodeIdFromSubscriptionCommitId(
              message.getSubscriptionCommitId());
      dataNodeIdToTopicNameToSubscriptionCommitIds
          .computeIfAbsent(dataNodeId, (id) -> new HashMap<>())
          .computeIfAbsent(message.getTopic(), (topic) -> new ArrayList<>())
          .add(message.getSubscriptionCommitId());
    }
    for (Map.Entry<Integer, Map<String, List<String>>> entry :
        dataNodeIdToTopicNameToSubscriptionCommitIds.entrySet()) {
      commitSync(entry.getKey(), entry.getValue());
    }
  }

  /////////////////////////////// utility ///////////////////////////////

  private void commitSync(
      int dataNodeId, Map<String, List<String>> topicNameToSubscriptionCommitIds)
      throws TException, IOException, StatementExecutionException {
    getSessionConnection(dataNodeId).commit(topicNameToSubscriptionCommitIds);
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
      throw new UnsupportedOperationException(
          "SubscriptionPullConsumer.Builder do not support build push consumer.");
    }
  }
}
