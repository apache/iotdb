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

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.TableSessionBuilder;
import org.apache.iotdb.session.subscription.ISubscriptionTableSession;
import org.apache.iotdb.session.subscription.SubscriptionTableSessionBuilder;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumerBuilder;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionRecordHandler;

import org.apache.tsfile.read.query.dataset.ResultSet;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class ConsensusTableModelSubscriptionSessionExample {

  private static final String HOST = "127.0.0.1";
  private static final int PORT = 6667;
  private static final String USER = "root";
  private static final String PASSWORD = "root";

  private static final long POLL_TIMEOUT_MS = 1_000L;
  private static final int MAX_POLL_ROUNDS = 20;
  private static final int EXPECTED_ROWS = 5;

  public static void main(final String[] args) throws Exception {
    final long runId = System.currentTimeMillis();
    final String database = "db_consensus_example_" + runId;
    final String table = "events";
    final String topic = "topic_consensus_table_example_" + runId;
    final String consumerGroup = "cg_consensus_table_example_" + runId;
    final String consumerId = "consumer_consensus_table_example_" + runId;

    System.out.println("=== Consensus Subscription Table Example ===");
    System.out.println("database = " + database);
    System.out.println("table = " + table);
    System.out.println("topic = " + topic);
    System.out.println("consumerGroup = " + consumerGroup);

    prepareBootstrapData(database, table);
    createConsensusTopic(topic, database, table);

    try (final ISubscriptionTablePullConsumer consumer =
        new SubscriptionTablePullConsumerBuilder()
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

      writeRealtimeDataAfterSubscribe(database, table);
      pollAndCommit(consumer, EXPECTED_ROWS);
      consumer.unsubscribe(topic);
    } finally {
      dropTopic(topic);
    }
  }

  private static void prepareBootstrapData(final String database, final String table)
      throws Exception {
    try (final ITableSession session = openTableSession()) {
      session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS " + database);
      session.executeNonQueryStatement("USE " + database);
      session.executeNonQueryStatement(
          "CREATE TABLE " + table + "(tag1 STRING TAG, s1 INT64 FIELD)");
      session.executeNonQueryStatement(
          "insert into " + table + "(tag1, s1, time) values ('bootstrap', 0, 0)");
      session.executeNonQueryStatement("flush");
    }
  }

  private static void createConsensusTopic(
      final String topicName, final String database, final String table) throws Exception {
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder()
            .host(HOST)
            .port(PORT)
            .username(USER)
            .password(PASSWORD)
            .build()) {
      final Properties config = new Properties();
      config.put(TopicConstant.MODE_KEY, TopicConstant.MODE_CONSENSUS_VALUE);
      config.put(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_RECORD_HANDLER_VALUE);
      config.put(TopicConstant.DATABASE_KEY, database);
      config.put(TopicConstant.TABLE_KEY, table);
      config.put(TopicConstant.ORDER_MODE_KEY, TopicConstant.ORDER_MODE_PER_WRITER_VALUE);
      session.createTopicIfNotExists(topicName, config);
    }
  }

  private static void writeRealtimeDataAfterSubscribe(final String database, final String table)
      throws Exception {
    try (final ITableSession session = openTableSession()) {
      session.executeNonQueryStatement("USE " + database);
      for (int i = 1; i <= EXPECTED_ROWS; i++) {
        session.executeNonQueryStatement(
            String.format(
                "insert into %s(tag1, s1, time) values ('device_%d', %d, %d)",
                table, i, i * 10L, i));
      }
      session.executeNonQueryStatement("flush");
    }
  }

  private static void pollAndCommit(
      final ISubscriptionTablePullConsumer consumer, final int expectedRows) throws Exception {
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
          System.out.println(
              "database = "
                  + subscriptionResultSet.getDatabaseName()
                  + ", table = "
                  + subscriptionResultSet.getTableName());
          System.out.println("Columns = " + subscriptionResultSet.getColumnNames());
          System.out.println("Types = " + subscriptionResultSet.getColumnTypes());
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
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder()
            .host(HOST)
            .port(PORT)
            .username(USER)
            .password(PASSWORD)
            .build()) {
      session.dropTopicIfExists(topicName);
    }
  }

  private static ITableSession openTableSession() throws Exception {
    return new TableSessionBuilder()
        .nodeUrls(Collections.singletonList(HOST + ":" + PORT))
        .username(USER)
        .password(PASSWORD)
        .build();
  }
}
