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

import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2Subscription;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessageType;
import org.apache.iotdb.session.subscription.payload.SubscriptionSessionDataSet;
import org.apache.iotdb.session.subscription.payload.SubscriptionTsFileHandler;
import org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant;

import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.write.record.Tablet;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant.AWAIT;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2Subscription.class})
public class IoTDBSubscriptionTopicIT extends AbstractSubscriptionDualIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSubscriptionTopicIT.class);

  @Override
  protected void setUpConfig() {
    super.setUpConfig();

    // Shorten heartbeat and sync interval to avoid timeout of snapshot mode test
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setPipeHeartbeatIntervalSecondsForCollectingPipeMeta(30);
    senderEnv.getConfig().getCommonConfig().setPipeMetaSyncerInitialSyncDelayMinutes(1);
    senderEnv.getConfig().getCommonConfig().setPipeMetaSyncerSyncIntervalMinutes(1);
  }

  @Test
  public void testTabletTopicWithPath() throws Exception {
    testTopicWithPathTemplate(TopicConstant.FORMAT_SESSION_DATA_SETS_HANDLER_VALUE);
  }

  @Test
  public void testTsFileTopicWithPath() throws Exception {
    testTopicWithPathTemplate(TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE);
  }

  private void testTopicWithPathTemplate(final String topicFormat) throws Exception {
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
      config.put(TopicConstant.FORMAT_KEY, topicFormat);
      config.put(TopicConstant.PATH_KEY, "root.db.*.s");
      session.createTopic(topicName, config);
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertTopicCount(1);

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
                  LockSupport.parkNanos(IoTDBSubscriptionITConstant.SLEEP_NS); // wait some time
                  final List<SubscriptionMessage> messages =
                      consumer.poll(IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS);
                  insertData(messages, session);
                  consumer.commitSync(messages);
                }
                consumer.unsubscribe(topicName);
              } catch (final Exception e) {
                e.printStackTrace();
                // Avoid fail
              } finally {
                LOGGER.info("consumer exiting...");
              }
            },
            String.format("%s - consumer", testName.getMethodName()));
    thread.start();

    // Check data on receiver
    try {
      try (final Connection connection = receiverEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        // Keep retrying if there are execution failures
        AWAIT.untilAsserted(
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
  public void testTabletTopicWithTime() throws Exception {
    testTopicWithTimeTemplate(TopicConstant.FORMAT_SESSION_DATA_SETS_HANDLER_VALUE);
  }

  @Test
  public void testTsFileTopicWithTime() throws Exception {
    testTopicWithTimeTemplate(TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE);
  }

  private void testTopicWithTimeTemplate(final String topicFormat) throws Exception {
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
      config.put(TopicConstant.FORMAT_KEY, topicFormat);
      config.put(TopicConstant.START_TIME_KEY, currentTime);
      session.createTopic(topicName, config);
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertTopicCount(1);

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
                  LockSupport.parkNanos(IoTDBSubscriptionITConstant.SLEEP_NS); // wait some time
                  final List<SubscriptionMessage> messages =
                      consumer.poll(IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS);
                  insertData(messages, session);
                  consumer.commitSync(messages);
                }
                consumer.unsubscribe(topicName);
              } catch (final Exception e) {
                e.printStackTrace();
                // Avoid failure
              } finally {
                LOGGER.info("consumer exiting...");
              }
            },
            String.format("%s - consumer", testName.getMethodName()));
    thread.start();

    // Check data on receiver
    try {
      try (final Connection connection = receiverEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        // Keep retrying if there are execution failures
        AWAIT.untilAsserted(
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
  public void testTabletTopicWithProcessor() throws Exception {
    testTopicWithProcessorTemplate(TopicConstant.FORMAT_SESSION_DATA_SETS_HANDLER_VALUE);
  }

  @Test
  public void testTsFileTopicWithProcessor() throws Exception {
    testTopicWithProcessorTemplate(TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE);
  }

  private void testTopicWithProcessorTemplate(final String topicFormat) throws Exception {
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
      config.put(TopicConstant.FORMAT_KEY, topicFormat);
      config.put("processor", "tumbling-time-sampling-processor");
      config.put("processor.tumbling-time.interval-seconds", "1");
      config.put("processor.down-sampling.split-file", "true");
      session.createTopic(topicName, config);
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertTopicCount(1);

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
                  LockSupport.parkNanos(IoTDBSubscriptionITConstant.SLEEP_NS); // wait some time
                  final List<SubscriptionMessage> messages =
                      consumer.poll(IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS);
                  insertData(messages, session);
                  consumer.commitSync(messages);
                }
                consumer.unsubscribe(topicName);
              } catch (final Exception e) {
                e.printStackTrace();
                // Avoid failure
              } finally {
                LOGGER.info("consumer exiting...");
              }
            },
            String.format("%s - consumer", testName.getMethodName()));
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
        AWAIT.untilAsserted(
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
    final String topic1 = "`topic4`";
    final String topic2 = "`'topic5'`";
    final String topic3 = "`\"topic6\"`";
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
    assertTopicCount(3);

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
                  LockSupport.parkNanos(IoTDBSubscriptionITConstant.SLEEP_NS); // wait some time
                  final List<SubscriptionMessage> messages =
                      consumer.poll(IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS);
                  for (final SubscriptionMessage message : messages) {
                    for (final Iterator<Tablet> it =
                            message.getSessionDataSetsHandler().tabletIterator();
                        it.hasNext(); ) {
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
            },
            String.format("%s - consumer", testName.getMethodName()));
    thread.start();

    // Check data on receiver
    try {
      try (final Connection connection = receiverEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        // Keep retrying if there are execution failures
        AWAIT.untilAsserted(
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

  @Test
  public void testTopicWithInvalidTimeConfig() throws Exception {
    final String host = senderEnv.getIP();
    final int port = Integer.parseInt(senderEnv.getPort());

    // Scenario 1: invalid time
    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      final Properties properties = new Properties();
      properties.put(TopicConstant.START_TIME_KEY, "2024-01-32");
      properties.put(TopicConstant.END_TIME_KEY, TopicConstant.NOW_TIME_VALUE);
      session.createTopic("topic7", properties);
      fail();
    } catch (final Exception ignored) {
    }
    assertTopicCount(0);

    // Scenario 2: test when 'start-time' is greater than 'end-time'
    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      final Properties properties = new Properties();
      properties.put(TopicConstant.START_TIME_KEY, "2001.01.01T08:00:00");
      properties.put(TopicConstant.END_TIME_KEY, "2000.01.01T08:00:00");
      session.createTopic("topic8", properties);
      fail();
    } catch (final Exception ignored) {
    }
    assertTopicCount(0);
  }

  @Test
  public void testTabletTopicWithSnapshotMode() throws Exception {
    testTopicWithSnapshotModeTemplate(TopicConstant.FORMAT_SESSION_DATA_SETS_HANDLER_VALUE);
  }

  @Test
  public void testTsFileTopicWithSnapshotMode() throws Exception {
    testTopicWithSnapshotModeTemplate(TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE);
  }

  private void testTopicWithSnapshotModeTemplate(final String topicFormat) throws Exception {
    // Insert some historical data before subscription
    try (final ISession session = senderEnv.getSessionConnection()) {
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
    final String topicName = "topic9";
    final String host = senderEnv.getIP();
    final int port = Integer.parseInt(senderEnv.getPort());
    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      final Properties config = new Properties();
      config.put(TopicConstant.FORMAT_KEY, topicFormat);
      config.put(TopicConstant.MODE_KEY, TopicConstant.MODE_SNAPSHOT_VALUE);
      session.createTopic(topicName, config);
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertTopicCount(1);

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

                // Insert some data after subscription
                try (final ISession session = senderEnv.getSessionConnection()) {
                  for (int i = 100; i < 200; ++i) {
                    session.executeNonQueryStatement(
                        String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
                  }
                  session.executeNonQueryStatement("flush");
                } catch (final Exception e) {
                  e.printStackTrace();
                  fail(e.getMessage());
                }

                while (!isClosed.get()) {
                  LockSupport.parkNanos(IoTDBSubscriptionITConstant.SLEEP_NS); // wait some time
                  final List<SubscriptionMessage> messages =
                      consumer.poll(IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS);
                  for (final SubscriptionMessage message : messages) {
                    final short messageType = message.getMessageType();
                    if (!SubscriptionMessageType.isValidatedMessageType(messageType)) {
                      LOGGER.warn("unexpected message type: {}", messageType);
                      continue;
                    }
                    switch (SubscriptionMessageType.valueOf(messageType)) {
                      case SESSION_DATA_SETS_HANDLER:
                        for (final SubscriptionSessionDataSet dataSet :
                            message.getSessionDataSetsHandler()) {
                          while (dataSet.hasNext()) {
                            dataSet.next();
                            rowCount.addAndGet(1);
                          }
                        }
                        break;
                      case TS_FILE_HANDLER:
                        try (final TsFileReader tsFileReader =
                            message.getTsFileHandler().openReader()) {
                          final Path path = new Path("root.db.d1", "s1", true);
                          final QueryDataSet dataSet =
                              tsFileReader.query(
                                  QueryExpression.create(Collections.singletonList(path), null));
                          while (dataSet.hasNext()) {
                            dataSet.next();
                            rowCount.addAndGet(1);
                          }
                        }
                        break;
                      default:
                        LOGGER.warn("unexpected message type: {}", messageType);
                        break;
                    }
                  }
                  consumer.commitSync(messages);
                }
                // Exiting the loop represents passing the awaitility test, at this point the result
                // of 'show subscription' is empty, so there is no need to explicitly unsubscribe.
              } catch (final Exception e) {
                e.printStackTrace();
                // Avoid failure
              } finally {
                LOGGER.info("consumer exiting...");
              }
            },
            String.format("%s - consumer", testName.getMethodName()));
    thread.start();

    try {
      // Keep retrying if there are execution failures
      AWAIT.untilAsserted(
          () -> {
            // Check row count
            Assert.assertEquals(100, rowCount.get());
            // Check empty subscription
            try (final SyncConfigNodeIServiceClient client =
                (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
              final TShowSubscriptionResp showSubscriptionResp =
                  client.showSubscription(new TShowSubscriptionReq());
              Assert.assertEquals(
                  RpcUtils.SUCCESS_STATUS.getCode(), showSubscriptionResp.status.getCode());
              Assert.assertNotNull(showSubscriptionResp.subscriptionInfoList);
              Assert.assertEquals(0, showSubscriptionResp.subscriptionInfoList.size());
            }
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
  public void testTabletTopicWithLooseRange() throws Exception {
    testTopicWithLooseRangeTemplate(TopicConstant.FORMAT_SESSION_DATA_SETS_HANDLER_VALUE);
  }

  @Test
  public void testTsFileTopicWithLooseRange() throws Exception {
    testTopicWithLooseRangeTemplate(TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE);
  }

  private void testTopicWithLooseRangeTemplate(final String topicFormat) throws Exception {
    // Insert some historical data on sender
    try (final ISession session = senderEnv.getSessionConnection()) {
      session.executeNonQueryStatement(
          "insert into root.db.d1 (time, at1, at2) values (1000, 1, 2), (2000, 3, 4)");
      session.executeNonQueryStatement(
          "insert into root.db1.d1 (time, at1, at2) values (1000, 1, 2), (2000, 3, 4)");
      session.executeNonQueryStatement(
          "insert into root.db.d1 (time, at1, at2) values (3000, 1, 2), (4000, 3, 4)");
      session.executeNonQueryStatement("flush");
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Create topic
    final String topicName = "topic10";
    final String host = senderEnv.getIP();
    final int port = Integer.parseInt(senderEnv.getPort());
    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      final Properties config = new Properties();
      config.put(TopicConstant.FORMAT_KEY, topicFormat);
      config.put(TopicConstant.LOOSE_RANGE_KEY, TopicConstant.LOOSE_RANGE_ALL_VALUE);
      config.put(TopicConstant.PATH_KEY, "root.db.d1.at1");
      config.put(TopicConstant.START_TIME_KEY, "1500");
      config.put(TopicConstant.END_TIME_KEY, "2500");
      session.createTopic(topicName, config);
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertTopicCount(1);

    final AtomicBoolean dataPrepared = new AtomicBoolean(false);
    final AtomicBoolean topicSubscribed = new AtomicBoolean(false);
    final AtomicBoolean finished = new AtomicBoolean(false);
    final List<Thread> threads = new ArrayList<>();

    // Subscribe on sender
    threads.add(
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
                topicSubscribed.set(true);
                while (!finished.get()) {
                  LockSupport.parkNanos(IoTDBSubscriptionITConstant.SLEEP_NS); // wait some time
                  if (dataPrepared.get()) {
                    final List<SubscriptionMessage> messages =
                        consumer.poll(IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS);
                    insertData(messages, session);
                    consumer.commitSync(messages);
                  }
                }
                consumer.unsubscribe(topicName);
              } catch (final Exception e) {
                e.printStackTrace();
                // Avoid failure
              } finally {
                LOGGER.info("consumer exiting...");
              }
            },
            String.format("%s - consumer", testName.getMethodName())));

    // Insert some realtime data on sender
    threads.add(
        new Thread(
            () -> {
              while (!topicSubscribed.get()) {
                LockSupport.parkNanos(IoTDBSubscriptionITConstant.SLEEP_NS); // wait some time
              }
              try (final ISession session = senderEnv.getSessionConnection()) {
                session.executeNonQueryStatement(
                    "insert into root.db.d1 (time, at1, at2) values (1001, 1, 2), (2001, 3, 4)");
                session.executeNonQueryStatement(
                    "insert into root.db1.d1 (time, at1, at2) values (1001, 1, 2), (2001, 3, 4)");
                session.executeNonQueryStatement(
                    "insert into root.db.d1 (time, at1, at2) values (3001, 1, 2), (4001, 3, 4)");
                session.executeNonQueryStatement("flush");
              } catch (final Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
              }
              dataPrepared.set(true);
            },
            String.format("%s - data inserter", testName.getMethodName())));

    for (final Thread thread : threads) {
      thread.start();
    }

    try (final Connection connection = receiverEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      // Keep retrying if there are execution failures
      AWAIT.untilAsserted(
          () ->
              TestUtils.assertSingleResultSetEqual(
                  TestUtils.executeQueryWithRetry(
                      statement,
                      "select count(at1) from root.db.d1 where time >= 1500 and time <= 2500"),
                  new HashMap<String, String>() {
                    {
                      put("count(root.db.d1.at1)", "2");
                    }
                  }));
    }

    finished.set(true);
    for (final Thread thread : threads) {
      thread.join();
    }
  }

  /////////////////////////////// utility ///////////////////////////////

  private void assertTopicCount(final int count) throws Exception {
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowTopicInfo> showTopicResult =
          client.showTopic(new TShowTopicReq()).topicInfoList;
      Assert.assertEquals(count, showTopicResult.size());
    }
  }

  private void insertData(final List<SubscriptionMessage> messages, final ISession session)
      throws Exception {
    for (final SubscriptionMessage message : messages) {
      final short messageType = message.getMessageType();
      if (!SubscriptionMessageType.isValidatedMessageType(messageType)) {
        LOGGER.warn("unexpected message type: {}", messageType);
        continue;
      }
      switch (SubscriptionMessageType.valueOf(messageType)) {
        case SESSION_DATA_SETS_HANDLER:
          for (final Iterator<Tablet> it = message.getSessionDataSetsHandler().tabletIterator();
              it.hasNext(); ) {
            final Tablet tablet = it.next();
            session.insertTablet(tablet);
          }
          break;
        case TS_FILE_HANDLER:
          final SubscriptionTsFileHandler tsFileHandler = message.getTsFileHandler();
          session.executeNonQueryStatement(
              String.format("load '%s'", tsFileHandler.getFile().getAbsolutePath()));
          break;
        default:
          LOGGER.warn("unexpected message type: {}", messageType);
          break;
      }
    }
  }
}
