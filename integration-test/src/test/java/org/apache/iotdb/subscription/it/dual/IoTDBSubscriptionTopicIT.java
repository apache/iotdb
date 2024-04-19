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

package org.apache.iotdb.subscription.it.dual;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2Subscription;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.subscription.SubscriptionMessage;
import org.apache.iotdb.session.subscription.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.apache.iotdb.session.subscription.SubscriptionSessionDataSets;

import org.apache.tsfile.write.record.Tablet;
import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2Subscription.class})
public class IoTDBSubscriptionTopicIT extends AbstractSubscriptionDualIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSubscriptionTopicIT.class);

  @Test
  public void testTopicPathSubscription() throws Exception {
    // Insert some historical data on sender
    try (final ISession session = senderEnv.getSessionConnection()) {
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s) values (%s, 1)", i));
        session.executeNonQueryStatement(
            String.format("insert into root.db.d2(time, s) values (%s, 1)", i));
        session.executeNonQueryStatement(
            String.format("insert into root.db.d3(time, t) values (%s, 1)", i));
        session.executeNonQueryStatement(
            String.format("insert into root.db.t1(time, s1) values (%s, 1)", i));
      }
      session.executeNonQueryStatement("flush");
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Create topic on sender
    final String topicName = "topic1";
    final String host = senderEnv.getIP();
    final int port = Integer.parseInt(senderEnv.getPort());
    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      final Properties config = new Properties();
      config.put(TopicConstant.PATH_KEY, "root.db.*.s");
      session.createTopic(topicName, config);
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Subscribe on sender and insert on receiver
    final AtomicBoolean isClosed = new AtomicBoolean(false);
    final Thread thread =
        new Thread(
            () -> {
              try (final SubscriptionPullConsumer consumer =
                      new SubscriptionPullConsumer.Builder()
                          .host(host)
                          .port(port)
                          .consumerId("c1")
                          .consumerGroupId("cg1")
                          .autoCommit(false)
                          .buildPullConsumer();
                  final ISession session = receiverEnv.getSessionConnection()) {
                consumer.open();
                consumer.subscribe(topicName);
                while (!isClosed.get()) {
                  try {
                    Thread.sleep(1000); // wait some time
                  } catch (final InterruptedException e) {
                    break;
                  }
                  final List<SubscriptionMessage> messages =
                      consumer.poll(Duration.ofMillis(10000));
                  if (messages.isEmpty()) {
                    continue;
                  }
                  for (final SubscriptionMessage message : messages) {
                    final SubscriptionSessionDataSets payload =
                        (SubscriptionSessionDataSets) message.getPayload();
                    for (final Iterator<Tablet> it = payload.tabletIterator(); it.hasNext(); ) {
                      final Tablet tablet = it.next();
                      session.insertTablet(tablet);
                    }
                  }
                  consumer.commitSync(messages);
                }
                consumer.unsubscribe(topicName);
              } catch (final Exception e) {
                e.printStackTrace();
                // Avoid fail
              } finally {
                LOGGER.info("consumer exiting...");
              }
            });
    thread.start();

    // Check data on receiver
    try {
      try (final Connection connection = receiverEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        // Keep retrying if there are execution failures
        Awaitility.await()
            .pollDelay(1, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .atMost(120, TimeUnit.SECONDS)
            .untilAsserted(
                () ->
                    TestUtils.assertSingleResultSetEqual(
                        TestUtils.executeQueryWithRetry(statement, "select count(*) from root.**"),
                        new HashMap<String, String>() {
                          {
                            put("count(root.db.d1.s)", "100");
                            put("count(root.db.d2.s)", "100");
                          }
                        }));
      }
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      isClosed.set(true);
      thread.join();
    }
  }

  @Test
  public void testTopicTimeSubscription() throws Exception {
    // Insert some historical data on sender
    final long currentTime = System.currentTimeMillis();
    try (final ISession session = senderEnv.getSessionConnection()) {
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s) values (%s, 1)", i));
        session.executeNonQueryStatement(
            String.format("insert into root.db.d2(time, s) values (%s, 1)", currentTime + i));
      }
      session.executeNonQueryStatement("flush");
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Create topic on sender
    final String topicName = "topic2";
    final String host = senderEnv.getIP();
    final int port = Integer.parseInt(senderEnv.getPort());
    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      final Properties config = new Properties();
      config.put(TopicConstant.START_TIME_KEY, currentTime);
      session.createTopic(topicName, config);
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Subscribe on sender and insert on receiver
    final AtomicBoolean isClosed = new AtomicBoolean(false);
    final Thread thread =
        new Thread(
            () -> {
              try (final SubscriptionPullConsumer consumer =
                      new SubscriptionPullConsumer.Builder()
                          .host(host)
                          .port(port)
                          .consumerId("c1")
                          .consumerGroupId("cg1")
                          .autoCommit(false)
                          .buildPullConsumer();
                  final ISession session = receiverEnv.getSessionConnection()) {
                consumer.open();
                consumer.subscribe(topicName);
                while (!isClosed.get()) {
                  try {
                    Thread.sleep(1000); // wait some time
                  } catch (final InterruptedException e) {
                    break;
                  }
                  final List<SubscriptionMessage> messages =
                      consumer.poll(Duration.ofMillis(10000));
                  if (messages.isEmpty()) {
                    continue;
                  }
                  for (final SubscriptionMessage message : messages) {
                    final SubscriptionSessionDataSets payload =
                        (SubscriptionSessionDataSets) message.getPayload();
                    for (final Iterator<Tablet> it = payload.tabletIterator(); it.hasNext(); ) {
                      final Tablet tablet = it.next();
                      session.insertTablet(tablet);
                    }
                  }
                  consumer.commitSync(messages);
                }
                consumer.unsubscribe(topicName);
              } catch (final Exception e) {
                e.printStackTrace();
                // Avoid failure
              } finally {
                LOGGER.info("consumer exiting...");
              }
            });
    thread.start();

    // Check data on receiver
    try {
      try (final Connection connection = receiverEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        // Keep retrying if there are execution failures
        Awaitility.await()
            .pollDelay(1, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .atMost(120, TimeUnit.SECONDS)
            .untilAsserted(
                () ->
                    TestUtils.assertSingleResultSetEqual(
                        TestUtils.executeQueryWithRetry(statement, "select count(*) from root.**"),
                        new HashMap<String, String>() {
                          {
                            put("count(root.db.d2.s)", "100");
                          }
                        }));
      }
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      isClosed.set(true);
      thread.join();
    }
  }

  @Test
  public void testTopicProcessorSubscription() throws Exception {
    // Insert some history data on sender
    try (final ISession session = senderEnv.getSessionConnection()) {
      session.executeNonQueryStatement(
          "insert into root.db.d1 (time, at1) values (1000, 1), (1500, 2), (2000, 3), (2500, 4), (3000, 5)");
      session.executeNonQueryStatement("flush");
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Create topic
    final String topicName = "topic3";
    final String host = senderEnv.getIP();
    final int port = Integer.parseInt(senderEnv.getPort());
    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      final Properties config = new Properties();
      config.put("processor", "tumbling-time-sampling-processor");
      config.put("processor.tumbling-time.interval-seconds", "1");
      config.put("processor.down-sampling.split-file", "true");
      session.createTopic(topicName, config);
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Subscribe on sender and insert on receiver
    final AtomicBoolean isClosed = new AtomicBoolean(false);
    final Thread thread =
        new Thread(
            () -> {
              try (final SubscriptionPullConsumer consumer =
                      new SubscriptionPullConsumer.Builder()
                          .host(host)
                          .port(port)
                          .consumerId("c1")
                          .consumerGroupId("cg1")
                          .autoCommit(false)
                          .buildPullConsumer();
                  final ISession session = receiverEnv.getSessionConnection()) {
                consumer.open();
                consumer.subscribe(topicName);
                while (!isClosed.get()) {
                  try {
                    Thread.sleep(1000); // wait some time
                  } catch (final InterruptedException e) {
                    break;
                  }
                  final List<SubscriptionMessage> messages =
                      consumer.poll(Duration.ofMillis(10000));
                  if (messages.isEmpty()) {
                    continue;
                  }
                  for (final SubscriptionMessage message : messages) {
                    final SubscriptionSessionDataSets payload =
                        (SubscriptionSessionDataSets) message.getPayload();
                    for (final Iterator<Tablet> it = payload.tabletIterator(); it.hasNext(); ) {
                      final Tablet tablet = it.next();
                      session.insertTablet(tablet);
                    }
                  }
                  consumer.commitSync(messages);
                }
                consumer.unsubscribe(topicName);
              } catch (final Exception e) {
                e.printStackTrace();
                // Avoid failure
              } finally {
                LOGGER.info("consumer exiting...");
              }
            });
    thread.start();

    // Check data on receiver
    final Set<String> expectedResSet = new HashSet<>();
    expectedResSet.add("1000,1.0,");
    expectedResSet.add("2000,3.0,");
    expectedResSet.add("3000,5.0,");
    try {
      try (final Connection connection = receiverEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        // Keep retrying if there are execution failures
        Awaitility.await()
            .pollDelay(1, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .atMost(120, TimeUnit.SECONDS)
            .untilAsserted(
                () ->
                    TestUtils.assertResultSetEqual(
                        TestUtils.executeQueryWithRetry(statement, "select * from root.**"),
                        "Time,root.db.d1.at1,",
                        expectedResSet));
      }
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      isClosed.set(true);
      thread.join();
    }
  }

  @Test
  public void testTopicNameWithBackQuote() throws Exception {
    // Insert some historical data on sender
    try (final ISession session = senderEnv.getSessionConnection()) {
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s) values (%s, 1)", i));
      }
      for (int i = 100; i < 200; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s) values (%s, 1)", i));
      }
      for (int i = 200; i < 300; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s) values (%s, 1)", i));
      }
      session.executeNonQueryStatement("flush");
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Create topic on sender
    final String topic1 = "`topic1`";
    final String topic2 = "`'topic2'`";
    final String topic3 = "`\"topic3\"`";
    final String host = senderEnv.getIP();
    final int port = Integer.parseInt(senderEnv.getPort());
    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      {
        final Properties config = new Properties();
        config.put(TopicConstant.START_TIME_KEY, 0);
        config.put(TopicConstant.END_TIME_KEY, 99);
        session.createTopic(topic1, config);
      }
      {
        final Properties config = new Properties();
        config.put(TopicConstant.START_TIME_KEY, 100);
        config.put(TopicConstant.END_TIME_KEY, 199);
        session.createTopic(topic2, config);
      }
      {
        final Properties config = new Properties();
        config.put(TopicConstant.START_TIME_KEY, 200);
        config.put(TopicConstant.END_TIME_KEY, 299);
        session.createTopic(topic3, config);
      }
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Subscribe on sender and insert on receiver
    final Set<String> topics = new HashSet<>();
    topics.add(topic1);
    topics.add(topic2);
    topics.add(topic3);
    final AtomicBoolean isClosed = new AtomicBoolean(false);
    final Thread thread =
        new Thread(
            () -> {
              try (final SubscriptionPullConsumer consumer =
                      new SubscriptionPullConsumer.Builder()
                          .host(host)
                          .port(port)
                          .consumerId("c1")
                          .consumerGroupId("cg1")
                          .autoCommit(false)
                          .buildPullConsumer();
                  final ISession session = receiverEnv.getSessionConnection()) {
                consumer.open();
                consumer.subscribe(topics);
                while (!isClosed.get()) {
                  try {
                    Thread.sleep(1000); // wait some time
                  } catch (final InterruptedException e) {
                    break;
                  }
                  final List<SubscriptionMessage> messages =
                      consumer.poll(topics, Duration.ofMillis(10000));
                  if (messages.isEmpty()) {
                    continue;
                  }
                  for (final SubscriptionMessage message : messages) {
                    final SubscriptionSessionDataSets payload =
                        (SubscriptionSessionDataSets) message.getPayload();
                    for (final Iterator<Tablet> it = payload.tabletIterator(); it.hasNext(); ) {
                      final Tablet tablet = it.next();
                      session.insertTablet(tablet);
                    }
                  }
                  consumer.commitSync(messages);
                }
                consumer.unsubscribe(topics);
              } catch (final Exception e) {
                e.printStackTrace();
                // Avoid failure
              } finally {
                LOGGER.info("consumer exiting...");
              }
            });
    thread.start();

    // Check data on receiver
    try {
      try (final Connection connection = receiverEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        // Keep retrying if there are execution failures
        Awaitility.await()
            .pollDelay(1, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .atMost(120, TimeUnit.SECONDS)
            .untilAsserted(
                () ->
                    TestUtils.assertSingleResultSetEqual(
                        TestUtils.executeQueryWithRetry(statement, "select count(*) from root.**"),
                        new HashMap<String, String>() {
                          {
                            put("count(root.db.d1.s)", "300");
                          }
                        }));
      }
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      isClosed.set(true);
      thread.join();
    }
  }
}
