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

package org.apache.iotdb;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.subscription.ISubscriptionTreeSession;
import org.apache.iotdb.session.subscription.SubscriptionTreeSessionBuilder;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTreePullConsumer;
import org.apache.iotdb.session.subscription.consumer.tree.SubscriptionTreePullConsumerBuilder;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionRecordHandler;

import org.apache.tsfile.read.query.dataset.ResultSet;

import java.util.List;
import java.util.Properties;

public class ConsensusSubscriptionSessionExample {

  private static final String HOST = "127.0.0.1";
  private static final int PORT = 6667;
  private static final String USER = "root";
  private static final String PASSWORD = "root";

  private static final long POLL_TIMEOUT_MS = 1_000L;
  private static final int MAX_POLL_ROUNDS = 20;
  private static final int EXPECTED_ROWS = 5;

  public static void main(final String[] args) throws Exception {
    final long runId = System.currentTimeMillis();
    final String database = "root.db_consensus_example_" + runId;
    final String device = database + ".d0";
    final String topic = "topic_consensus_example_" + runId;
    final String consumerGroup = "cg_consensus_example_" + runId;
    final String consumerId = "consumer_consensus_example_" + runId;

    System.out.println("=== Consensus Subscription Tree Example ===");
    System.out.println("database = " + database);
    System.out.println("topic = " + topic);
    System.out.println("consumerGroup = " + consumerGroup);

    prepareBootstrapData(database, device);
    createConsensusTopic(topic, database + ".**");

    try (final ISubscriptionTreePullConsumer consumer =
        new SubscriptionTreePullConsumerBuilder()
            .host(HOST)
            .port(PORT)
            .username(USER)
            .password(PASSWORD)
            .consumerId(consumerId)
            .consumerGroupId(consumerGroup)
            .autoCommit(false)
            .build()) {
      consumer.open();
      consumer.subscribe(topic);

      writeRealtimeDataAfterSubscribe(device);
      pollAndCommit(consumer, EXPECTED_ROWS);
      consumer.unsubscribe(topic);
    } finally {
      dropTopic(topic);
    }
  }

  private static void prepareBootstrapData(final String database, final String device)
      throws Exception {
    try (final ISession session = openSession()) {
      session.executeNonQueryStatement("CREATE DATABASE " + database);
      session.executeNonQueryStatement(
          "CREATE TIMESERIES "
              + device
              + ".s1 with datatype=INT64, encoding=RLE, compressor=SNAPPY");
      session.executeNonQueryStatement(
          String.format("insert into %s(time, s1) values (%d, %d)", device, 0L, 0L));
      session.executeNonQueryStatement("flush");
    }
  }

  private static void createConsensusTopic(final String topicName, final String path)
      throws Exception {
    try (final ISubscriptionTreeSession session =
        new SubscriptionTreeSessionBuilder()
            .host(HOST)
            .port(PORT)
            .username(USER)
            .password(PASSWORD)
            .build()) {
      session.open();

      final Properties config = new Properties();
      config.put(TopicConstant.MODE_KEY, TopicConstant.MODE_CONSENSUS_VALUE);
      config.put(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_RECORD_HANDLER_VALUE);
      config.put(TopicConstant.PATH_KEY, path);
      config.put(TopicConstant.ORDER_MODE_KEY, TopicConstant.ORDER_MODE_PER_WRITER_VALUE);
      session.createTopicIfNotExists(topicName, config);
    }
  }

  private static void writeRealtimeDataAfterSubscribe(final String device) throws Exception {
    try (final ISession session = openSession()) {
      for (int i = 1; i <= EXPECTED_ROWS; i++) {
        session.executeNonQueryStatement(
            String.format("insert into %s(time, s1) values (%d, %d)", device, i, i * 10L));
      }
      session.executeNonQueryStatement("flush");
    }
  }

  private static void pollAndCommit(
      final ISubscriptionTreePullConsumer consumer, final int expectedRows) throws Exception {
    int totalRows = 0;
    int consecutiveEmptyPolls = 0;

    for (int round = 1; round <= MAX_POLL_ROUNDS; round++) {
      final List<SubscriptionMessage> messages = consumer.poll(POLL_TIMEOUT_MS);
      if (messages.isEmpty()) {
        consecutiveEmptyPolls++;
        if (totalRows >= expectedRows && consecutiveEmptyPolls >= 3) {
          break;
        }
        continue;
      }

      consecutiveEmptyPolls = 0;

      for (final SubscriptionMessage message : messages) {
        for (final ResultSet resultSet : message.getResultSets()) {
          final SubscriptionRecordHandler.SubscriptionResultSet subscriptionResultSet =
              (SubscriptionRecordHandler.SubscriptionResultSet) resultSet;
          System.out.println("Columns = " + subscriptionResultSet.getColumnNames());
          while (subscriptionResultSet.hasNext()) {
            System.out.println(subscriptionResultSet.nextRecord());
            totalRows++;
          }
        }
      }

      consumer.commitSync(messages);
      System.out.println("poll round " + round + ", totalRows = " + totalRows);
    }

    if (totalRows != expectedRows) {
      throw new IllegalStateException(
          "Expected "
              + expectedRows
              + " realtime rows, but consumed "
              + totalRows
              + ". Please check whether consensus subscription is enabled on the server.");
    }
  }

  private static void dropTopic(final String topicName) throws Exception {
    try (final ISubscriptionTreeSession session =
        new SubscriptionTreeSessionBuilder()
            .host(HOST)
            .port(PORT)
            .username(USER)
            .password(PASSWORD)
            .build()) {
      session.open();
      session.dropTopicIfExists(topicName);
    }
  }

  private static ISession openSession() throws Exception {
    final ISession session =
        new Session.Builder()
            .host(HOST)
            .port(PORT)
            .username(USER)
            .password(PASSWORD)
            .version(Version.V_1_0)
            .build();
    session.open(false);
    return session;
  }
}
