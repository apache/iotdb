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

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.subscription.SubscriptionMessage;
import org.apache.iotdb.session.subscription.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.apache.iotdb.session.subscription.SubscriptionSessionDataSet;
import org.apache.iotdb.session.subscription.SubscriptionSessionDataSets;
import org.apache.iotdb.session.subscription.SubscriptionTsFileReader;

import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class SubscriptionSessionExample {

  private static Session session;

  private static final String HOST = "127.0.0.1";
  private static final int PORT = 6667;

  private static final String TOPIC_1 = "topic1";
  private static final String TOPIC_2 = "`topic2`";

  private static void prepareData() throws Exception {
    // Open session
    session =
        new Session.Builder()
            .host(HOST)
            .port(PORT)
            .username("root")
            .password("root")
            .version(Version.V_1_0)
            .build();
    session.open(false);

    // Insert some historical data
    final long currentTime = System.currentTimeMillis();
    for (int i = 0; i < 100; ++i) {
      session.executeNonQueryStatement(
          String.format("insert into root.db.d1(time, s1, s2) values (%s, 1, 2)", i));
      session.executeNonQueryStatement(
          String.format("insert into root.db.d2(time, s3, s4) values (%s, 3, 4)", currentTime + i));
      session.executeNonQueryStatement(
          String.format("insert into root.sg.d3(time, s5) values (%s, 5)", currentTime + 2 * i));
    }
    session.executeNonQueryStatement("flush");

    // Close session
    session.close();
    session = null;
  }

  private static void dataQuery() throws Exception {
    // Open session
    session =
        new Session.Builder()
            .host(HOST)
            .port(PORT)
            .username("root")
            .password("root")
            .version(Version.V_1_0)
            .build();
    session.open(false);

    // Query
    final SessionDataSet dataSet = session.executeQueryStatement("select ** from root.**");
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }

    // Close session
    session.close();
    session = null;
  }

  private static void createTopics() throws Exception {
    // Create topics
    try (final SubscriptionSession subscriptionSession = new SubscriptionSession(HOST, PORT)) {
      subscriptionSession.open();
      subscriptionSession.createTopic(TOPIC_1);
      final Properties config = new Properties();
      config.put(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_TS_FILE_READER_VALUE);
      subscriptionSession.createTopic(TOPIC_2, config);
    }
  }

  private static void subscriptionExample1() throws Exception {
    int retryCount = 0;
    // Subscription: property-style ctor
    final Properties config = new Properties();
    config.put(ConsumerConstant.CONSUMER_ID_KEY, "c1");
    config.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "cg1");
    final SubscriptionPullConsumer consumer1 = new SubscriptionPullConsumer(config);
    consumer1.open();
    consumer1.subscribe(TOPIC_1);
    while (true) {
      Thread.sleep(1000); // Wait for some time
      final List<SubscriptionMessage> messages = consumer1.poll(Duration.ofMillis(10000));
      if (messages.isEmpty()) {
        retryCount++;
        if (retryCount >= 5) {
          break;
        }
      }
      for (final SubscriptionMessage message : messages) {
        final SubscriptionSessionDataSets dataSets =
            (SubscriptionSessionDataSets) message.getPayload();
        for (final SubscriptionSessionDataSet dataSet : dataSets) {
          System.out.println(dataSet.getColumnNames());
          System.out.println(dataSet.getColumnTypes());
          while (dataSet.hasNext()) {
            System.out.println(dataSet.next());
          }
        }
      }
      // Auto commit
    }

    // Show topics and subscriptions
    try (final SubscriptionSession subscriptionSession = new SubscriptionSession(HOST, PORT)) {
      subscriptionSession.open();
      subscriptionSession.getTopics().forEach((System.out::println));
      subscriptionSession.getSubscriptions().forEach((System.out::println));
    }

    consumer1.unsubscribe(TOPIC_1);
    consumer1.close();
  }

  private static void subscriptionExample2() throws Exception {
    int retryCount = 0;
    // Subscription: builder-style ctor
    try (final SubscriptionPullConsumer consumer2 =
        new SubscriptionPullConsumer.Builder()
            .consumerId("c2")
            .consumerGroupId("cg2")
            .autoCommit(false)
            .buildPullConsumer()) {
      consumer2.open();
      consumer2.subscribe(TOPIC_2);
      while (true) {
        Thread.sleep(1000); // wait some time
        final List<SubscriptionMessage> messages =
            consumer2.poll(Collections.singleton(TOPIC_2), Duration.ofMillis(10000));
        if (messages.isEmpty()) {
          retryCount++;
          if (retryCount >= 5) {
            break;
          }
        }
        for (final SubscriptionMessage message : messages) {
          final SubscriptionTsFileReader reader = (SubscriptionTsFileReader) message.getPayload();
          try (final TsFileReader tsFileReader = reader.open()) {
            final Path path = new Path("root.db.d1", "s1", true);
            final QueryDataSet dataSet =
                tsFileReader.query(QueryExpression.create(Collections.singletonList(path), null));
            while (dataSet.hasNext()) {
              System.out.println(dataSet.next());
            }
          }
        }
        consumer2.commitSync(messages);
      }
      consumer2.unsubscribe(TOPIC_2);
    }
  }

  public static void main(final String[] args) throws Exception {
    prepareData();
    dataQuery();
    createTopics();
    subscriptionExample1();
    subscriptionExample2();
  }
}
