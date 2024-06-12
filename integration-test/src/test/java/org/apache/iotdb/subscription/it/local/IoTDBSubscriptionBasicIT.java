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
import org.apache.iotdb.session.subscription.payload.SubscriptionFileHandler;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionSessionDataSet;
import org.apache.iotdb.session.subscription.payload.SubscriptionTsFileHandler;
import org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
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
  public void testSubscribeBooleanData() throws Exception {
    testBasicSubscribeTablets(TSDataType.BOOLEAN, "true", true);
  }

  @Test
  public void testSubscribeIntData() throws Exception {
    testBasicSubscribeTablets(TSDataType.INT32, "1", 1);
  }

  @Test
  public void testSubscribeLongData() throws Exception {
    testBasicSubscribeTablets(TSDataType.INT64, "1", 1L);
  }

  @Test
  public void testSubscribeFloatData() throws Exception {
    testBasicSubscribeTablets(TSDataType.FLOAT, "1.0", 1.0F);
  }

  @Test
  public void testSubscribeDoubleData() throws Exception {
    testBasicSubscribeTablets(TSDataType.DOUBLE, "1.0", 1.0);
  }

  @Test
  public void testSubscribeTextData() throws Exception {
    testBasicSubscribeTablets(TSDataType.TEXT, "'a'", new Binary("a", TSFileConfig.STRING_CHARSET));
  }

  @Test
  public void testSubscribeTimestampData() throws Exception {
    testBasicSubscribeTablets(TSDataType.TIMESTAMP, "123", 123L);
  }

  @Test
  public void testSubscribeDateData() throws Exception {
    testBasicSubscribeTablets(TSDataType.DATE, "'2011-03-01'", LocalDate.of(2011, 3, 1));
  }

  @Test
  public void testSubscribeBlobData() throws Exception {
    testBasicSubscribeTablets(
        TSDataType.BLOB, "X'f013'", new Binary(new byte[] {(byte) 0xf0, 0x13}));
  }

  @Test
  public void testSubscribeStringData() throws Exception {
    testBasicSubscribeTablets(
        TSDataType.STRING, "'a'", new Binary("a", TSFileConfig.STRING_CHARSET));
  }

  private void testBasicSubscribeTablets(
      final TSDataType type, final String valueStr, final Object expectedData) throws Exception {
    // Insert some historical data
    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement(
          String.format("create timeseries root.db.d1.s1 %s", type.toString()));
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s1) values (%s, %s)", i, valueStr));
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
                  LockSupport.parkNanos(IoTDBSubscriptionITConstant.SLEEP_NS); // wait some time
                  final List<SubscriptionMessage> messages =
                      consumer.poll(IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS);
                  for (final SubscriptionMessage message : messages) {
                    for (final SubscriptionSessionDataSet dataSet :
                        message.getSessionDataSetsHandler()) {
                      while (dataSet.hasNext()) {
                        final RowRecord record = dataSet.next();
                        Assert.assertEquals(type.toString(), dataSet.getColumnTypes().get(1));
                        Assert.assertEquals(type, record.getFields().get(0).getDataType());
                        Assert.assertEquals(expectedData, getValue(type, dataSet.getTablet()));
                        Assert.assertEquals(
                            expectedData, record.getFields().get(0).getObjectValue(type));
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
            },
            String.format("%s - consumer", testName.getMethodName()));
    thread.start();

    // Check row count
    try {
      // Keep retrying if there are execution failures
      AWAIT.untilAsserted(() -> Assert.assertEquals(100, rowCount.get()));
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      isClosed.set(true);
      thread.join();
    }
  }

  private Object getValue(final TSDataType type, final Tablet tablet) {
    switch (type) {
      case BOOLEAN:
        return ((boolean[]) tablet.values[0])[0];
      case INT32:
        return ((int[]) tablet.values[0])[0];
      case INT64:
      case TIMESTAMP:
        return ((long[]) tablet.values[0])[0];
      case FLOAT:
        return ((float[]) tablet.values[0])[0];
      case DOUBLE:
        return ((double[]) tablet.values[0])[0];
      case TEXT:
      case BLOB:
      case STRING:
        return ((Binary[]) tablet.values[0])[0];
      case DATE:
        return ((LocalDate[]) tablet.values[0])[0];
      default:
        return null;
    }
  }

  @Test
  public void testBasicSubscribeTsFile() throws Exception {
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
      final Properties config = new Properties();
      config.put(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE);
      session.createTopic(topicName, config);
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Record file handlers
    final List<SubscriptionFileHandler> fileHandlers = new ArrayList<>();

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
                      .fileSaveDir(System.getProperty("java.io.tmpdir")) // hack for license check
                      .buildPullConsumer()) {
                consumer.open();
                consumer.subscribe(topicName);
                while (!isClosed.get()) {
                  LockSupport.parkNanos(IoTDBSubscriptionITConstant.SLEEP_NS); // wait some time
                  final List<SubscriptionMessage> messages =
                      consumer.poll(IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS);
                  for (final SubscriptionMessage message : messages) {
                    final SubscriptionTsFileHandler tsFileHandler = message.getTsFileHandler();
                    fileHandlers.add(tsFileHandler);
                    try (final TsFileReader tsFileReader = tsFileHandler.openReader()) {
                      final Path path = new Path("root.db.d1", "s1", true);
                      final QueryDataSet dataSet =
                          tsFileReader.query(
                              QueryExpression.create(Collections.singletonList(path), null));
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
            },
            String.format("%s - consumer", testName.getMethodName()));
    thread.start();

    // Check row count
    try {
      // Keep retrying if there are execution failures
      AWAIT.untilAsserted(() -> Assert.assertEquals(100, rowCount.get()));
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      isClosed.set(true);
      thread.join();
    }

    // Do something for file handlers
    Assert.assertFalse(fileHandlers.isEmpty());
    final SubscriptionFileHandler fileHandler = fileHandlers.get(0);
    final java.nio.file.Path filePath = fileHandler.getPath();
    Assert.assertTrue(Files.exists(filePath));

    // Copy file
    java.nio.file.Path tmpFilePath;
    tmpFilePath = fileHandler.copyFile(Files.createTempFile(null, null).toAbsolutePath());
    Assert.assertTrue(Files.exists(filePath));
    Assert.assertTrue(Files.exists(tmpFilePath));

    // Move file
    tmpFilePath = fileHandler.moveFile(Files.createTempFile(null, null).toAbsolutePath());
    Assert.assertFalse(Files.exists(filePath));
    Assert.assertTrue(Files.exists(tmpFilePath));

    // Delete file
    Assert.assertThrows(NoSuchFileException.class, fileHandler::deleteFile);
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
      AWAIT.untilAsserted(() -> Assert.assertEquals(100, rowCount.get()));
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
      AWAIT.untilAsserted(() -> Assert.assertEquals(200, rowCount.get()));
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
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());
    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      {
        final Properties properties = new Properties();
        properties.put(TopicConstant.END_TIME_KEY, 99);
        session.createTopic("topic1", properties);
      }
      {
        final Properties properties = new Properties();
        properties.put(TopicConstant.START_TIME_KEY, 100);
        session.createTopic("topic2", properties);
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
                consumer.subscribe("topic2"); // only subscribe topic2
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
}
