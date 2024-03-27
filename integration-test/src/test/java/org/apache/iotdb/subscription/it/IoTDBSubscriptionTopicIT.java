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

package org.apache.iotdb.subscription.it;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT3;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.subscription.SubscriptionMessage;
import org.apache.iotdb.session.subscription.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.apache.iotdb.session.subscription.SubscriptionSessionDataSets;
import org.apache.iotdb.tsfile.write.record.Tablet;

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
@Category({MultiClusterIT3.class})
public class IoTDBSubscriptionTopicIT extends AbstractSubscriptionDualIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSubscriptionTopicIT.class);

  @Test
  public void testTopicPathSubscription() throws Exception {
    // insert some history data on sender
    try (ISession session = senderEnv.getSessionConnection()) {
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
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // create topic on sender
    final String host = senderEnv.getIP();
    final int port = Integer.parseInt(senderEnv.getPort());
    try (SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      Properties config = new Properties();
      config.put(TopicConstant.PATH_KEY, "root.db.*.s");
      session.createTopic("topic1", config);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // subscribe on sender and insert on receiver
    AtomicBoolean isClosed = new AtomicBoolean(false);
    Thread thread =
        new Thread(
            () -> {
              try (SubscriptionPullConsumer consumer =
                      new SubscriptionPullConsumer.Builder()
                          .host(host)
                          .port(port)
                          .consumerId("c1")
                          .consumerGroupId("cg1")
                          .autoCommit(false)
                          .buildPullConsumer();
                  ISession session = receiverEnv.getSessionConnection()) {
                consumer.open();
                consumer.subscribe("topic1");
                while (!isClosed.get()) {
                  try {
                    Thread.sleep(1000); // wait some time
                  } catch (InterruptedException e) {
                    break;
                  }
                  List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(10000));
                  if (messages.isEmpty()) {
                    continue;
                  }
                  for (SubscriptionMessage message : messages) {
                    SubscriptionSessionDataSets payload =
                        (SubscriptionSessionDataSets) message.getPayload();
                    for (Iterator<Tablet> it = payload.tabletIterator(); it.hasNext(); ) {
                      Tablet tablet = it.next();
                      session.insertTablet(tablet);
                    }
                  }
                  consumer.commitSync(messages);
                }
                consumer.unsubscribe("topic1");
                LOGGER.info(
                    "consumer {} (group {}) exiting...",
                    consumer.getConsumerId(),
                    consumer.getConsumerGroupId());
              } catch (Exception e) {
                e.printStackTrace();
                // avoid fail
              }
            });
    thread.start();

    // check data on receiver
    try {
      try (Connection connection = receiverEnv.getConnection();
          Statement statement = connection.createStatement()) {
        // Keep retrying if there are execution failures
        Awaitility.await()
            .atMost(100, TimeUnit.SECONDS)
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
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      isClosed.set(true);
      thread.join();
    }
  }

  @Test
  public void testTopicTimeSubscription() throws Exception {
    // insert some history data on sender
    long currentTime = System.currentTimeMillis();
    try (ISession session = senderEnv.getSessionConnection()) {
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s) values (%s, 1)", i));
        session.executeNonQueryStatement(
            String.format("insert into root.db.d2(time, s) values (%s, 1)", currentTime + i));
      }
      session.executeNonQueryStatement("flush");
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // create topic on sender
    final String host = senderEnv.getIP();
    final int port = Integer.parseInt(senderEnv.getPort());
    try (SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      Properties config = new Properties();
      config.put(TopicConstant.START_TIME_KEY, currentTime);
      session.createTopic("topic1", config);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // subscribe on sender and insert on receiver
    AtomicBoolean isClosed = new AtomicBoolean(false);
    Thread thread =
        new Thread(
            () -> {
              try (SubscriptionPullConsumer consumer =
                      new SubscriptionPullConsumer.Builder()
                          .host(host)
                          .port(port)
                          .consumerId("c1")
                          .consumerGroupId("cg1")
                          .autoCommit(false)
                          .buildPullConsumer();
                  ISession session = receiverEnv.getSessionConnection()) {
                consumer.open();
                consumer.subscribe("topic1");
                while (!isClosed.get()) {
                  try {
                    Thread.sleep(1000); // wait some time
                  } catch (InterruptedException e) {
                    break;
                  }
                  List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(10000));
                  if (messages.isEmpty()) {
                    continue;
                  }
                  for (SubscriptionMessage message : messages) {
                    SubscriptionSessionDataSets payload =
                        (SubscriptionSessionDataSets) message.getPayload();
                    for (Iterator<Tablet> it = payload.tabletIterator(); it.hasNext(); ) {
                      Tablet tablet = it.next();
                      session.insertTablet(tablet);
                    }
                  }
                  consumer.commitSync(messages);
                }
                consumer.unsubscribe("topic1");
                LOGGER.info(
                    "consumer {} (group {}) exiting...",
                    consumer.getConsumerId(),
                    consumer.getConsumerGroupId());
              } catch (Exception e) {
                e.printStackTrace();
                // avoid fail
              }
            });
    thread.start();

    // check data on receiver
    try {
      try (Connection connection = receiverEnv.getConnection();
          Statement statement = connection.createStatement()) {
        // Keep retrying if there are execution failures
        Awaitility.await()
            .atMost(100, TimeUnit.SECONDS)
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
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      isClosed.set(true);
      thread.join();
    }
  }

  @Test
  public void testTopicProcessorSubscription() throws Exception {
    // insert some history data on sender
    try (ISession session = senderEnv.getSessionConnection()) {
      session.executeNonQueryStatement(
          "insert into root.db.d1 (time, at1) values (1000, 1), (1500, 2), (2000, 3), (2500, 4), (3000, 5)");
      session.executeNonQueryStatement("flush");
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // create topic
    final String host = senderEnv.getIP();
    final int port = Integer.parseInt(senderEnv.getPort());
    try (SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      Properties config = new Properties();
      config.put("processor", "tumbling-time-sampling-processor");
      config.put("processor.tumbling-time.interval-seconds", "1");
      config.put("processor.down-sampling.split-file", "true");
      session.createTopic("topic1", config);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // subscribe on sender and insert on receiver
    AtomicBoolean isClosed = new AtomicBoolean(false);
    Thread thread =
        new Thread(
            () -> {
              try (SubscriptionPullConsumer consumer =
                      new SubscriptionPullConsumer.Builder()
                          .host(host)
                          .port(port)
                          .consumerId("c1")
                          .consumerGroupId("cg1")
                          .autoCommit(false)
                          .buildPullConsumer();
                  ISession session = receiverEnv.getSessionConnection()) {
                consumer.open();
                consumer.subscribe("topic1");
                while (!isClosed.get()) {
                  try {
                    Thread.sleep(1000); // wait some time
                  } catch (InterruptedException e) {
                    break;
                  }
                  List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(10000));
                  if (messages.isEmpty()) {
                    continue;
                  }
                  for (SubscriptionMessage message : messages) {
                    SubscriptionSessionDataSets payload =
                        (SubscriptionSessionDataSets) message.getPayload();
                    for (Iterator<Tablet> it = payload.tabletIterator(); it.hasNext(); ) {
                      Tablet tablet = it.next();
                      session.insertTablet(tablet);
                    }
                  }
                  consumer.commitSync(messages);
                }
                consumer.unsubscribe("topic1");
                LOGGER.info(
                    "consumer {} (group {}) exiting...",
                    consumer.getConsumerId(),
                    consumer.getConsumerGroupId());
              } catch (Exception e) {
                e.printStackTrace();
                // avoid fail
              }
            });
    thread.start();

    // check data on receiver
    Set<String> expectedResSet = new HashSet<>();
    expectedResSet.add("1000,1.0,");
    expectedResSet.add("2000,3.0,");
    expectedResSet.add("3000,5.0,");
    try {
      try (Connection connection = receiverEnv.getConnection();
          Statement statement = connection.createStatement()) {
        // Keep retrying if there are execution failures
        Awaitility.await()
            .atMost(100, TimeUnit.SECONDS)
            .untilAsserted(
                () ->
                    TestUtils.assertResultSetEqual(
                        TestUtils.executeQueryWithRetry(statement, "select * from root.**"),
                        "Time,root.db.d1.at1,",
                        expectedResSet));
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      isClosed.set(true);
      thread.join();
    }
  }
}
