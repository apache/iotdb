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
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.apache.iotdb.session.subscription.consumer.AckStrategy;
import org.apache.iotdb.session.subscription.consumer.ConsumeResult;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPushConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionSessionDataSet;
import org.apache.iotdb.session.subscription.payload.SubscriptionTsFileHandler;

import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public class SubscriptionSessionExample {

  private static Session session;

  private static final String HOST = "127.0.0.1";
  private static final int PORT = 6667;

  private static final String TOPIC_1 = "topic1";
  private static final String TOPIC_2 = "`'topic2'`";
  private static final String TOPIC_3 = "`\"topic3\"`";
  private static final String TOPIC_4 = "`\"top \\.i.c4\"`";

  private static final long SLEEP_NS = 1_000_000_000L;
  private static final long POLL_TIMEOUT_MS = 10_000L;
  private static final int MAX_RETRY_TIMES = 3;
  private static final int PARALLELISM = 8;
  private static final long CURRENT_TIME = System.currentTimeMillis();

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
    for (int i = 0; i < 100; ++i) {
      session.executeNonQueryStatement(
          String.format("insert into root.db.d1(time, s1, s2) values (%s, 1, 2)", i));
      session.executeNonQueryStatement(
          String.format(
              "insert into root.db.d2(time, s1, s2) values (%s, 3, 4)", CURRENT_TIME + i));
      session.executeNonQueryStatement(
          String.format("insert into root.sg.d3(time, s1) values (%s, 5)", CURRENT_TIME + 2 * i));
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

  /** single pull consumer subscribe topic with path and time range */
  private static void dataSubscription1() throws Exception {
    // Create topics
    try (final SubscriptionSession subscriptionSession = new SubscriptionSession(HOST, PORT)) {
      subscriptionSession.open();
      final Properties config = new Properties();
      config.put(TopicConstant.PATH_KEY, "root.db.d1.s1");
      config.put(TopicConstant.START_TIME_KEY, 25);
      config.put(TopicConstant.END_TIME_KEY, 75);
      subscriptionSession.createTopic(TOPIC_1, config);
    }

    int retryCount = 0;
    // Subscription: property-style ctor
    final Properties config = new Properties();
    config.put(ConsumerConstant.CONSUMER_ID_KEY, "c1");
    config.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "cg1");
    try (SubscriptionPullConsumer consumer1 = new SubscriptionPullConsumer(config)) {
      consumer1.open();
      consumer1.subscribe(TOPIC_1);
      while (true) {
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
    }
  }

  /** multi pull consumer subscribe topic with tsfile format */
  private static void dataSubscription2() throws Exception {
    try (final SubscriptionSession subscriptionSession = new SubscriptionSession(HOST, PORT)) {
      subscriptionSession.open();
      final Properties config = new Properties();
      config.put(TopicConstant.START_TIME_KEY, CURRENT_TIME + 33);
      config.put(TopicConstant.END_TIME_KEY, CURRENT_TIME + 66);
      config.put(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE);
      subscriptionSession.createTopic(TOPIC_2, config);
    }

    final List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < PARALLELISM; ++i) {
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
                        final QueryDataSet dataSet =
                            reader.query(
                                QueryExpression.create(
                                    Arrays.asList(
                                        new Path("root.db.d2", "s2", true),
                                        new Path("root.sg.d3", "s1", true)),
                                    null));
                        while (dataSet.hasNext()) {
                          System.out.println(dataSet.next());
                        }
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

  /** multi push consumer subscribe topic with tsfile format and snapshot mode */
  private static void dataSubscription3() throws Exception {
    try (final SubscriptionSession subscriptionSession = new SubscriptionSession(HOST, PORT)) {
      subscriptionSession.open();
      final Properties config = new Properties();
      config.put(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE);
      config.put(TopicConstant.MODE_KEY, TopicConstant.MODE_SNAPSHOT_VALUE);
      subscriptionSession.createTopic(TOPIC_3, config);
    }

    final List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < PARALLELISM; ++i) {
      final int idx = i;
      final Thread thread =
          new Thread(
              () -> {
                // Subscription: builder-style ctor
                try (final SubscriptionPushConsumer consumer3 =
                    new SubscriptionPushConsumer.Builder()
                        .consumerId("c" + idx)
                        .consumerGroupId("cg3")
                        .ackStrategy(AckStrategy.AFTER_CONSUME)
                        .consumeListener(
                            message -> {
                              // do something for SubscriptionTsFileHandler
                              System.out.println(
                                  message.getTsFileHandler().getFile().getAbsolutePath());
                              return ConsumeResult.SUCCESS;
                            })
                        .buildPushConsumer()) {
                  consumer3.open();
                  consumer3.subscribe(TOPIC_3);
                  while (!consumer3.allSnapshotTopicMessagesHaveBeenConsumed()) {
                    LockSupport.parkNanos(SLEEP_NS); // wait some time
                  }
                }
              });
      thread.start();
      threads.add(thread);
    }

    for (final Thread thread : threads) {
      thread.join();
    }
  }

  /** multi pull consumer subscribe topic with tsfile format and snapshot mode */
  private static void dataSubscription4() throws Exception {
    try (final SubscriptionSession subscriptionSession = new SubscriptionSession(HOST, PORT)) {
      subscriptionSession.open();
      final Properties config = new Properties();
      config.put(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE);
      config.put(TopicConstant.MODE_KEY, TopicConstant.MODE_SNAPSHOT_VALUE);
      subscriptionSession.createTopic(TOPIC_4, config);
    }

    final AtomicLong counter = new AtomicLong();
    final List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < PARALLELISM; ++i) {
      final int idx = i;
      final Thread thread =
          new Thread(
              () -> {
                // Subscription: builder-style ctor
                try (final SubscriptionPullConsumer consumer4 =
                    new SubscriptionPullConsumer.Builder()
                        .consumerId("c" + idx)
                        .consumerGroupId("cg4")
                        .autoCommit(true)
                        .fileSaveFsync(true)
                        .buildPullConsumer()) {
                  consumer4.open();
                  consumer4.subscribe(TOPIC_4);
                  while (!consumer4.allSnapshotTopicMessagesHaveBeenConsumed()) {
                    for (final SubscriptionMessage message : consumer4.poll(POLL_TIMEOUT_MS)) {
                      final SubscriptionTsFileHandler handler = message.getTsFileHandler();
                      handler.moveFile(
                          Paths.get(System.getProperty("user.dir"), "exported-tsfiles")
                              .resolve(
                                  URLEncoder.encode(TOPIC_4)
                                      + "-"
                                      + counter.getAndIncrement()
                                      + ".tsfile"));
                    }
                  }
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
    // dataQuery();
    // dataSubscription1();
    // dataSubscription2();
    // dataSubscription3();
    dataSubscription4();
  }
}
