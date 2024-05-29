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
import org.apache.iotdb.session.subscription.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionSessionDataSet;

import org.apache.tsfile.read.TsFileReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.LockSupport;

public class SubscriptionSessionExample {

  private static Session session;

  private static final String HOST = "127.0.0.1";
  private static final int PORT = 6667;

  private static final String TOPIC_1 = "topic1";
  private static final String TOPIC_2 = "`topic2`";

  public static final long SLEEP_NS = 1_000_000_000L;
  public static final long POLL_TIMEOUT_MS = 10_000L;
  private static final int MAX_RETRY_TIMES = 3;

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
    for (int i = 0; i < 10000; ++i) {
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

  private static void subscriptionExample1() throws Exception {
    // Create topics
    try (final SubscriptionSession subscriptionSession = new SubscriptionSession(HOST, PORT)) {
      subscriptionSession.open();
      subscriptionSession.createTopic(TOPIC_1);
    }

    int retryCount = 0;
    // Subscription: property-style ctor
    final Properties config = new Properties();
    config.put(ConsumerConstant.CONSUMER_ID_KEY, "c1");
    config.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "cg1");
    final SubscriptionPullConsumer consumer1 = new SubscriptionPullConsumer(config);
    consumer1.open();
    consumer1.subscribe(TOPIC_1);
    while (true) {
      LockSupport.parkNanos(SLEEP_NS); // wait some time
      final List<SubscriptionMessage> messages = consumer1.poll(POLL_TIMEOUT_MS);
      if (messages.isEmpty()) {
        retryCount++;
        if (retryCount >= MAX_RETRY_TIMES) {
          break;
        }
      }
      for (final SubscriptionMessage message : messages) {
        for (final SubscriptionSessionDataSet dataSet : message.getSessionDataSetsHandler()) {
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
    try (final SubscriptionSession subscriptionSession = new SubscriptionSession(HOST, PORT)) {
      subscriptionSession.open();
      final Properties config = new Properties();
      config.put(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE);
      subscriptionSession.createTopic(TOPIC_2, config);
    }

    final List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < 8; ++i) {
      final int idx = i;
      final Thread thread =
          new Thread(
              () -> {
                int retryCount = 0;
                // Subscription: builder-style ctor
                try (final SubscriptionPullConsumer consumer2 =
                    new SubscriptionPullConsumer.Builder()
                        .consumerId("c" + idx)
                        .consumerGroupId("cg2")
                        .autoCommit(false)
                        .buildPullConsumer()) {
                  consumer2.open();
                  consumer2.subscribe(TOPIC_2);
                  while (true) {
                    LockSupport.parkNanos(SLEEP_NS); // wait some time
                    final List<SubscriptionMessage> messages =
                        consumer2.poll(Collections.singleton(TOPIC_2), POLL_TIMEOUT_MS);
                    if (messages.isEmpty()) {
                      retryCount++;
                      if (retryCount >= MAX_RETRY_TIMES) {
                        break;
                      }
                    }
                    for (final SubscriptionMessage message : messages) {
                      try (final TsFileReader reader = message.getTsFileHandler().openReader()) {
                        // do something...
                      }
                    }
                    consumer2.commitSync(messages);
                  }
                  consumer2.unsubscribe(TOPIC_2);
                } catch (final IOException e) {
                  throw new RuntimeException(e);
                }
              });
      thread.start();
      threads.add(thread);
    }

    for (final Thread thread : threads) {
      thread.join();
    }
  }

  public static void main(final String[] args) throws Exception {
    prepareData();
    dataQuery();
    subscriptionExample1();
    subscriptionExample2();
  }
}
