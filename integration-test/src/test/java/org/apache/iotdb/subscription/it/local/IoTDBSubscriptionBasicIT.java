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
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.apache.iotdb.session.subscription.consumer.AckStrategy;
import org.apache.iotdb.session.subscription.consumer.AsyncCommitCallback;
import org.apache.iotdb.session.subscription.consumer.ConsumeResult;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPushConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionSessionDataSet;
import org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant;

import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import static org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant.AWAIT;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBSubscriptionBasicIT extends AbstractSubscriptionLocalIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSubscriptionBasicIT.class);

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
                consumer.subscribe(topicName);
                while (!isClosed.get()) {
                  LockSupport.parkNanos(IoTDBSubscriptionITConstant.SLEEP_NS); // wait some time
                  final List<SubscriptionMessage> messages =
                      consumer.poll(Duration.ofMillis(10000));
                  if (messages.isEmpty()) {
                    continue;
                  }
                  for (final SubscriptionMessage message : messages) {
                    int rowCountInOneMessage = 0;
                    for (final SubscriptionSessionDataSet dataSet :
                        message.getSessionDataSetsHandler()) {
                      while (dataSet.hasNext()) {
                        dataSet.next();
                        rowCount.addAndGet(1);
                        rowCountInOneMessage++;
                      }
                    }
                    LOGGER.info("{} rows in message", rowCountInOneMessage);
                  }
                  consumer.commitAsync(
                      messages,
                      new AsyncCommitCallback() {
                        @Override
                        public void onComplete() {
                          commitSuccessCount.incrementAndGet();
                          LOGGER.info(
                              "async commit success, commit contexts: {}",
                              messages.stream()
                                  .map(SubscriptionMessage::getCommitContext)
                                  .collect(Collectors.toList()));
                        }

                        @Override
                        public void onFailure(final Throwable e) {
                          commitFailureCount.incrementAndGet();
                          LOGGER.info(
                              "async commit failed, commit contexts: {}",
                              messages.stream()
                                  .map(SubscriptionMessage::getCommitContext)
                                  .collect(Collectors.toList()),
                              e);
                        }
                      });
                }
                consumer.unsubscribe(topicName);
              } catch (final Exception e) {
                e.printStackTrace();
                // avoid fail
              } finally {
                LOGGER.info("consumer exiting...");
              }
            },
            String.format("%s - consumer", testName.getMethodName()));
    thread.start();

    // Check row count
    try {
      // Keep retrying if there are execution failures
      AWAIT.untilAsserted(
          () -> {
            Assert.assertEquals(100, rowCount.get());
            Assert.assertTrue(commitSuccessCount.get() > lastCommitSuccessCount.get());
            Assert.assertEquals(0, commitFailureCount.get());
          });
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
      AWAIT.untilAsserted(
          () -> {
            Assert.assertEquals(200, rowCount.get());
            Assert.assertTrue(commitSuccessCount.get() > lastCommitSuccessCount.get());
            Assert.assertEquals(0, commitFailureCount.get());
          });
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
    final String topicName = "topic2";
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
                  message
                      .getSessionDataSetsHandler()
                      .tabletIterator()
                      .forEachRemaining(tablet -> rowCount.addAndGet(tablet.rowSize));
                  return ConsumeResult.SUCCESS;
                })
            .buildPushConsumer()) {

      consumer.open();
      consumer.subscribe(topicName);

      // The push consumer should automatically poll 10 rows of data by 1 onReceive()
      AWAIT.untilAsserted(
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

      AWAIT.untilAsserted(
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

      AWAIT.untilAsserted(
          () -> {
            Assert.assertEquals(30, rowCount.get());
            Assert.assertTrue(onReceiveCount.get() > lastOnReceiveCount.get());
          });

      consumer.unsubscribe(topicName);
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testPollUnsubscribedTopics() throws Exception {
    // Insert some historical data
    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
      }
      for (int i = 100; i < 200; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
      }
      session.executeNonQueryStatement("flush");
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Create topic
    final String topicName3 = "topic3";
    final String topicName4 = "topic4";
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());
    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      {
        final Properties properties = new Properties();
        properties.put(TopicConstant.END_TIME_KEY, 99);
        session.createTopic(topicName3, properties);
      }
      {
        final Properties properties = new Properties();
        properties.put(TopicConstant.START_TIME_KEY, 100);
        session.createTopic(topicName4, properties);
      }
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Subscription
    final AtomicInteger rowCount = new AtomicInteger();
    final AtomicLong timestampSum = new AtomicLong();
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
                consumer.subscribe(topicName4); // only subscribe topic4
                while (!isClosed.get()) {
                  LockSupport.parkNanos(IoTDBSubscriptionITConstant.SLEEP_NS); // wait some time
                  final List<SubscriptionMessage> messages =
                      consumer.poll(IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS);
                  for (final SubscriptionMessage message : messages) {
                    for (final SubscriptionSessionDataSet dataSet :
                        message.getSessionDataSetsHandler()) {
                      while (dataSet.hasNext()) {
                        timestampSum.getAndAdd(dataSet.next().getTimestamp());
                        rowCount.addAndGet(1);
                      }
                    }
                  }
                  consumer.commitSync(messages);
                }
                // automatically unsubscribe topics when closing
              } catch (final Exception e) {
                e.printStackTrace();
                // Avoid failure
              } finally {
                LOGGER.info("consumer exiting...");
              }
            },
            String.format("%s - consumer", testName.getMethodName()));
    thread.start();

    // Check row count
    try {
      // Keep retrying if there are execution failures
      AWAIT.untilAsserted(
          () -> {
            Assert.assertEquals(100, rowCount.get());
            Assert.assertEquals((100 + 199) * 100 / 2, timestampSum.get());
          });
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      isClosed.set(true);
      thread.join();
    }
  }

  @Test
  public void testTsFileDeduplication() {
    // Insert some historical data
    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
      }
      // DO NOT FLUSH HERE
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Create topic
    final String topicName = "topic5";
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());
    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      final Properties config = new Properties();
      config.put(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE);
      session.createTopic(topicName, config);
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Subscription
    final AtomicInteger onReceiveCount = new AtomicInteger();
    final AtomicInteger rowCount = new AtomicInteger();
    try (final SubscriptionPushConsumer consumer =
        new SubscriptionPushConsumer.Builder()
            .host(host)
            .port(port)
            .consumerId("c1")
            .consumerGroupId("cg1")
            .ackStrategy(AckStrategy.AFTER_CONSUME)
            .consumeListener(
                message -> {
                  onReceiveCount.getAndIncrement();
                  try (final TsFileReader tsFileReader = message.getTsFileHandler().openReader()) {
                    final QueryDataSet dataSet =
                        tsFileReader.query(
                            QueryExpression.create(
                                Collections.singletonList(new Path("root.db.d1", "s1", true)),
                                null));
                    while (dataSet.hasNext()) {
                      dataSet.next();
                      rowCount.addAndGet(1);
                    }
                  } catch (final IOException e) {
                    throw new RuntimeException(e);
                  }
                  return ConsumeResult.SUCCESS;
                })
            .buildPushConsumer()) {

      consumer.open();
      consumer.subscribe(topicName);

      AWAIT.untilAsserted(
          () -> {
            Assert.assertEquals(100, rowCount.get());
            Assert.assertEquals(1, onReceiveCount.get()); // exactly one tsfile
          });
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
