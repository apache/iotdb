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

package org.apache.iotdb.subscription.it.local;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.session.subscription.AckStrategy;
import org.apache.iotdb.session.subscription.AsyncCommitCallback;
import org.apache.iotdb.session.subscription.ConsumeResult;
import org.apache.iotdb.session.subscription.SubscriptionMessage;
import org.apache.iotdb.session.subscription.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.SubscriptionPushConsumer;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.apache.iotdb.session.subscription.SubscriptionSessionDataSet;
import org.apache.iotdb.session.subscription.SubscriptionSessionDataSets;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBSubscriptionBasicIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSubscriptionBasicIT.class);

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testBasicPullConsumer() throws Exception {
    // Insert some historical data
    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
      }
      session.executeNonQueryStatement("flush");
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Create topic
    final String topicName = "topic1";
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());
    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      session.createTopic(topicName);
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Subscription
    final AtomicInteger rowCount = new AtomicInteger();
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
                      .buildPullConsumer()) {
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
                    for (final SubscriptionSessionDataSet dataSet : payload) {
                      while (dataSet.hasNext()) {
                        dataSet.next();
                        rowCount.addAndGet(1);
                      }
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

    // Check row count
    try {
      // Keep retrying if there are execution failures
      Awaitility.await()
          .pollDelay(1, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .atMost(120, TimeUnit.SECONDS)
          .untilAsserted(() -> Assert.assertEquals(100, rowCount.get()));
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      isClosed.set(true);
      thread.join();
    }
  }

  @Test
  public void testBasicPullConsumerWithCommitAsync() throws Exception {
    // Insert some historical data
    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
      }
      session.executeNonQueryStatement("flush");
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Create topic
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());
    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      session.createTopic("topic1");
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Subscription
    final AtomicInteger rowCount = new AtomicInteger();
    final AtomicInteger commitSuccessCount = new AtomicInteger();
    final AtomicInteger lastCommitSuccessCount = new AtomicInteger();
    final AtomicInteger commitFailureCount = new AtomicInteger();
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
                      .buildPullConsumer()) {
                consumer.open();
                consumer.subscribe("topic1");
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
                    int rowCountInOneMessage = 0;
                    for (final SubscriptionSessionDataSet dataSet : payload) {
                      while (dataSet.hasNext()) {
                        dataSet.next();
                        rowCount.addAndGet(1);
                        rowCountInOneMessage++;
                      }
                    }
                    LOGGER.info(rowCountInOneMessage + " rows in message");
                  }
                  consumer.commitAsync(
                      messages,
                      new AsyncCommitCallback() {
                        @Override
                        public void onComplete() {
                          commitSuccessCount.incrementAndGet();
                          LOGGER.info("commit success, messages size: {}", messages.size());
                        }

                        @Override
                        public void onFailure(Throwable e) {
                          commitFailureCount.incrementAndGet();
                        }
                      });
                }
                consumer.unsubscribe("topic1");
              } catch (final Exception e) {
                e.printStackTrace();
                // avoid fail
              } finally {
                LOGGER.info("consumer exiting...");
              }
            });
    thread.start();

    // Check row count
    try {
      // Keep retrying if there are execution failures
      Awaitility.await()
          .pollDelay(1, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .atMost(120, TimeUnit.SECONDS)
          .untilAsserted(() -> Assert.assertEquals(100, rowCount.get()));
      Assert.assertTrue(commitSuccessCount.get() > lastCommitSuccessCount.get());
      Assert.assertEquals(0, commitFailureCount.get());
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    lastCommitSuccessCount.set(commitSuccessCount.get());

    // Insert more data, the pull consumer is also running, so the data may be pulled more than
    // once.
    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (int i = 100; i < 200; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
      }
      session.executeNonQueryStatement("flush");
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Check row count
    try {
      // Keep retrying if there are execution failures
      Awaitility.await()
          .pollDelay(1, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .atMost(120, TimeUnit.SECONDS)
          .untilAsserted(() -> Assert.assertEquals(200, rowCount.get()));
      Assert.assertTrue(commitSuccessCount.get() > lastCommitSuccessCount.get());
      Assert.assertEquals(0, commitFailureCount.get());
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      isClosed.set(true);
      thread.join();
    }
  }

  @Test
  public void testBasicPushConsumer() {
    final AtomicInteger onReceiveCount = new AtomicInteger(0);
    final AtomicInteger lastOnReceiveCount = new AtomicInteger(0);
    final AtomicInteger rowCount = new AtomicInteger(0);

    // Insert some historical data
    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (int i = 0; i < 10; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
      }
      session.executeNonQueryStatement("flush");
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Create topic
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());
    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      session.createTopic("topic1");
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Subscription
    try (final SubscriptionPushConsumer consumer =
        new SubscriptionPushConsumer.Builder()
            .host(host)
            .port(port)
            .consumerId("c1")
            .consumerGroupId("cg1")
            .ackStrategy(AckStrategy.BEFORE_CONSUME)
            .consumeListener(
                message -> {
                  onReceiveCount.getAndIncrement();
                  SubscriptionSessionDataSets dataSets =
                      (SubscriptionSessionDataSets) message.getPayload();
                  dataSets
                      .tabletIterator()
                      .forEachRemaining(tablet -> rowCount.addAndGet(tablet.rowSize));
                  return ConsumeResult.SUCCESS;
                })
            .buildPushConsumer()) {

      consumer.open();
      consumer.subscribe("topic1");

      // The push consumer should automatically poll 10 rows of data by 1 onReceive()
      Awaitility.await()
          .pollDelay(1, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .atMost(10, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                Assert.assertEquals(10, rowCount.get());
                Assert.assertTrue(onReceiveCount.get() > lastOnReceiveCount.get());
              });

      lastOnReceiveCount.set(onReceiveCount.get());

      // Insert more rows and check if the push consumer can automatically poll the new data
      try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
        for (int i = 10; i < 20; ++i) {
          session.executeNonQueryStatement(
              String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
        }
        session.executeNonQueryStatement("flush");
      }

      Awaitility.await()
          .pollDelay(1, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .atMost(10, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                Assert.assertEquals(20, rowCount.get());
                Assert.assertTrue(onReceiveCount.get() > lastOnReceiveCount.get());
              });

      lastOnReceiveCount.set(onReceiveCount.get());

      try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
        for (int i = 20; i < 30; ++i) {
          session.executeNonQueryStatement(
              String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
        }
        session.executeNonQueryStatement("flush");
      }

      Awaitility.await()
          .pollDelay(1, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .atMost(10, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                Assert.assertEquals(30, rowCount.get());
                Assert.assertTrue(onReceiveCount.get() > lastOnReceiveCount.get());
              });

      consumer.unsubscribe("topic1");

    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
