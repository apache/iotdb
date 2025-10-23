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

package org.apache.iotdb.subscription.it.dual.tablemodel;

import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2SubscriptionTableArchVerification;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.subscription.ISubscriptionTableSession;
import org.apache.iotdb.session.subscription.SubscriptionTableSessionBuilder;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumerBuilder;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessageType;
import org.apache.iotdb.session.subscription.payload.SubscriptionSessionDataSet;
import org.apache.iotdb.session.subscription.payload.SubscriptionTsFileHandler;
import org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant;
import org.apache.iotdb.subscription.it.dual.AbstractSubscriptionDualIT;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import static org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant.AWAIT;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionTableArchVerification.class})
public class IoTDBSubscriptionTopicIT extends AbstractSubscriptionDualIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSubscriptionTopicIT.class);

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  protected void setUpConfig() {
    super.setUpConfig();

    // Shorten heartbeat and sync interval to avoid timeout of snapshot mode test
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setPipeHeartbeatIntervalSecondsForCollectingPipeMeta(30);
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setPipeMetaSyncerInitialSyncDelayMinutes(1)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setPipeMetaSyncerSyncIntervalMinutes(1)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);
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
    TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test1");
    TableModelUtils.createDataBaseAndTable(senderEnv, "test2", "test2");
    TableModelUtils.createDataBaseAndTable(senderEnv, "foo", "foo");

    TableModelUtils.createDataBaseAndTable(receiverEnv, "test1", "test1");
    TableModelUtils.createDataBaseAndTable(receiverEnv, "test2", "test2");
    TableModelUtils.createDataBaseAndTable(receiverEnv, "foo", "foo");

    // Insert some historical data on sender
    TableModelUtils.insertData("test1", "test1", 0, 10, senderEnv);
    TableModelUtils.insertData("test2", "test2", 0, 10, senderEnv);
    TableModelUtils.insertData("foo", "foo", 0, 10, senderEnv);

    // Create topic on sender
    final String topicName = "topic1";
    final String host = senderEnv.getIP();
    final int port = Integer.parseInt(senderEnv.getPort());
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder().host(host).port(port).build()) {
      final Properties config = new Properties();
      config.put(TopicConstant.FORMAT_KEY, topicFormat);
      config.put(TopicConstant.DATABASE_KEY, "test.*");
      config.put(TopicConstant.TABLE_KEY, "test.*");
      session.createTopic(topicName, config);
    }
    assertTopicCount(1);

    // Subscribe on sender and insert on receiver
    final AtomicBoolean isClosed = new AtomicBoolean(false);
    final Thread thread =
        new Thread(
            () -> {
              try (final ISubscriptionTablePullConsumer consumer =
                      new SubscriptionTablePullConsumerBuilder()
                          .host(host)
                          .port(port)
                          .consumerId("c1")
                          .consumerGroupId("cg1")
                          .autoCommit(false)
                          .build();
                  final ITableSession session = receiverEnv.getTableSessionConnection()) {
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
            String.format("%s - consumer", testName.getDisplayName()));
    thread.start();

    // Check data on receiver
    try {
      final Consumer<String> handleFailure =
          o -> {
            TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
            TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
          };
      // Keep retrying if there are execution failures
      AWAIT.untilAsserted(
          () -> {
            TableModelUtils.assertData("test1", "test1", 0, 10, receiverEnv, handleFailure);
            TableModelUtils.assertData("test2", "test2", 0, 10, receiverEnv, handleFailure);
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
  public void testTabletTopicWithTime() throws Exception {
    testTopicWithTimeTemplate(TopicConstant.FORMAT_SESSION_DATA_SETS_HANDLER_VALUE);
  }

  @Test
  public void testTsFileTopicWithTime() throws Exception {
    testTopicWithTimeTemplate(TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE);
  }

  private void testTopicWithTimeTemplate(final String topicFormat) throws Exception {
    TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test1");
    TableModelUtils.createDataBaseAndTable(receiverEnv, "test1", "test1");

    // Insert some historical data on sender
    TableModelUtils.insertData("test1", "test1", 0, 100, senderEnv);

    // Create topic on sender
    final String topicName = "topic2";
    final String host = senderEnv.getIP();
    final int port = Integer.parseInt(senderEnv.getPort());
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder().host(host).port(port).build()) {
      final Properties config = new Properties();
      config.put(TopicConstant.FORMAT_KEY, topicFormat);
      config.put(TopicConstant.START_TIME_KEY, 25);
      config.put(TopicConstant.END_TIME_KEY, 75);
      session.createTopic(topicName, config);
    }
    assertTopicCount(1);

    // Subscribe on sender and insert on receiver
    final AtomicBoolean isClosed = new AtomicBoolean(false);
    final Thread thread =
        new Thread(
            () -> {
              try (final ISubscriptionTablePullConsumer consumer =
                      new SubscriptionTablePullConsumerBuilder()
                          .host(host)
                          .port(port)
                          .consumerId("c1")
                          .consumerGroupId("cg1")
                          .autoCommit(false)
                          .build();
                  final ITableSession session = receiverEnv.getTableSessionConnection()) {
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
            String.format("%s - consumer", testName.getDisplayName()));
    thread.start();

    // Check data on receiver
    try {
      final Consumer<String> handleFailure =
          o -> {
            TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
            TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
          };
      // Keep retrying if there are execution failures
      AWAIT.untilAsserted(
          () -> TableModelUtils.assertData("test1", "test1", 25, 76, receiverEnv, handleFailure));
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      isClosed.set(true);
      thread.join();
    }
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
    TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test1");
    TableModelUtils.createDataBaseAndTable(receiverEnv, "test1", "test1");

    // Insert some historical data on sender
    TableModelUtils.insertData("test1", "test1", 0, 100, senderEnv);

    // Create topic
    final String topicName = "topic3";
    final String host = senderEnv.getIP();
    final int port = Integer.parseInt(senderEnv.getPort());
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder().host(host).port(port).build()) {
      final Properties config = new Properties();
      config.put(TopicConstant.FORMAT_KEY, topicFormat);
      config.put(TopicConstant.MODE_KEY, TopicConstant.MODE_SNAPSHOT_VALUE);
      session.createTopic(topicName, config);
    }
    assertTopicCount(1);

    // Subscription
    final AtomicBoolean isClosed = new AtomicBoolean(false);
    final Thread thread =
        new Thread(
            () -> {
              try (final ISubscriptionTablePullConsumer consumer =
                      new SubscriptionTablePullConsumerBuilder()
                          .host(host)
                          .port(port)
                          .consumerId("c1")
                          .consumerGroupId("cg1")
                          .autoCommit(false)
                          .build();
                  final ITableSession session = receiverEnv.getTableSessionConnection()) {
                consumer.open();
                consumer.subscribe(topicName);

                // Insert some realtime data on sender
                TableModelUtils.insertData("test1", "test1", 100, 200, senderEnv);

                while (!isClosed.get()) {
                  LockSupport.parkNanos(IoTDBSubscriptionITConstant.SLEEP_NS); // wait some time
                  final List<SubscriptionMessage> messages =
                      consumer.poll(IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS);
                  insertData(messages, session);
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
            String.format("%s - consumer", testName.getDisplayName()));
    thread.start();

    try {
      final Consumer<String> handleFailure =
          o -> {
            TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
            TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
          };
      // Keep retrying if there are execution failures
      AWAIT.untilAsserted(
          () -> {
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
            // Check data
            TableModelUtils.assertData("test1", "test1", 0, 100, receiverEnv, handleFailure);
          });
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      isClosed.set(true);
      thread.join();
    }
  }

  /////////////////////////////// utility ///////////////////////////////

  private void assertTopicCount(final int count) throws Exception {
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowTopicInfo> showTopicResult =
          client.showTopic(new TShowTopicReq().setIsTableModel(true)).topicInfoList;
      Assert.assertEquals(count, showTopicResult.size());
    }
  }

  private void insertData(final List<SubscriptionMessage> messages, final ITableSession session)
      throws Exception {
    for (final SubscriptionMessage message : messages) {
      final short messageType = message.getMessageType();
      if (!SubscriptionMessageType.isValidatedMessageType(messageType)) {
        LOGGER.warn("unexpected message type: {}", messageType);
        continue;
      }
      switch (SubscriptionMessageType.valueOf(messageType)) {
        case SESSION_DATA_SETS_HANDLER:
          for (final SubscriptionSessionDataSet dataSet : message.getSessionDataSetsHandler()) {
            session.executeNonQueryStatement(
                "use " + Objects.requireNonNull(dataSet.getDatabaseName()));
            session.insert(dataSet.getTablet());
          }
          break;
        case TS_FILE_HANDLER:
          final SubscriptionTsFileHandler tsFileHandler = message.getTsFileHandler();
          session.executeNonQueryStatement(
              "use " + Objects.requireNonNull(tsFileHandler.getDatabaseName()));
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
