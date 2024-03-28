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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2Subscription;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.subscription.SubscriptionMessage;
import org.apache.iotdb.session.subscription.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.SubscriptionSessionDataSet;
import org.apache.iotdb.session.subscription.SubscriptionSessionDataSets;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2Subscription.class})
public class IoTDBSubscriptionConsumerGroupIT extends AbstractSubscriptionDualIT {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBSubscriptionConsumerGroupIT.class);

  // --------------------------- //
  // Scenario 1: Historical Data //
  // --------------------------- //

  @Test
  public void test3C1CGSubscribeOneTopicHistoricalData() throws Exception {
    final long currentTime = System.currentTimeMillis();

    // history data
    insertData(currentTime);

    createTopics(currentTime);
    createPipes(currentTime);
    final List<SubscriptionPullConsumer> consumers = new ArrayList<>();
    consumers.add(createConsumerAndSubscribeTopics("c1", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c2", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c3", "cg1", "topic1"));

    pollMessagesAndCheck(
        consumers,
        new HashMap<String, String>() {
          {
            put("count(root.cg1.topic1.s)", "100");
            put("count(root.topic1.s)", "100");
            put("count(root.topic2.s)", "100");
          }
        });
  }

  @Test
  public void test3C3CGSubscribeOneTopicHistoricalData() throws Exception {
    final long currentTime = System.currentTimeMillis();

    // history data
    insertData(currentTime);

    createTopics(currentTime);
    createPipes(currentTime);
    final List<SubscriptionPullConsumer> consumers = new ArrayList<>();
    consumers.add(createConsumerAndSubscribeTopics("c1", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c2", "cg2", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c3", "cg3", "topic1"));

    pollMessagesAndCheck(
        consumers,
        new HashMap<String, String>() {
          {
            put("count(root.cg1.topic1.s)", "100");
            put("count(root.cg2.topic1.s)", "100");
            put("count(root.cg3.topic1.s)", "100");
            put("count(root.topic1.s)", "100");
            put("count(root.topic2.s)", "100");
          }
        });
  }

  @Test
  public void test3C1CGSubscribeTwoTopicHistoricalData() throws Exception {
    final long currentTime = System.currentTimeMillis();

    // history data
    insertData(currentTime);

    createTopics(currentTime);
    createPipes(currentTime);
    final List<SubscriptionPullConsumer> consumers = new ArrayList<>();
    consumers.add(createConsumerAndSubscribeTopics("c1", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c2", "cg1", "topic1", "topic2"));
    consumers.add(createConsumerAndSubscribeTopics("c3", "cg1", "topic2"));

    pollMessagesAndCheck(
        consumers,
        new HashMap<String, String>() {
          {
            put("count(root.cg1.topic1.s)", "100");
            put("count(root.cg1.topic2.s)", "100");
            put("count(root.topic1.s)", "100");
            put("count(root.topic2.s)", "100");
          }
        });
  }

  @Test
  public void test3C3CGSubscribeTwoTopicHistoricalData() throws Exception {
    final long currentTime = System.currentTimeMillis();

    // history data
    insertData(currentTime);

    createTopics(currentTime);
    createPipes(currentTime);
    final List<SubscriptionPullConsumer> consumers = new ArrayList<>();
    consumers.add(createConsumerAndSubscribeTopics("c1", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c2", "cg2", "topic1", "topic2"));
    consumers.add(createConsumerAndSubscribeTopics("c3", "cg3", "topic2"));

    pollMessagesAndCheck(
        consumers,
        new HashMap<String, String>() {
          {
            put("count(root.cg1.topic1.s)", "100");
            put("count(root.cg2.topic1.s)", "100");
            put("count(root.cg2.topic2.s)", "100");
            put("count(root.cg3.topic2.s)", "100");
            put("count(root.topic1.s)", "100");
            put("count(root.topic2.s)", "100");
          }
        });
  }

  @Test
  public void test4C2CGSubscribeTwoTopicHistoricalData() throws Exception {
    final long currentTime = System.currentTimeMillis();

    // history data
    insertData(currentTime);

    createTopics(currentTime);
    createPipes(currentTime);
    final List<SubscriptionPullConsumer> consumers = new ArrayList<>();
    consumers.add(createConsumerAndSubscribeTopics("c1", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c2", "cg2", "topic1", "topic2"));
    consumers.add(createConsumerAndSubscribeTopics("c3", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c4", "cg2", "topic2"));

    pollMessagesAndCheck(
        consumers,
        new HashMap<String, String>() {
          {
            put("count(root.cg1.topic1.s)", "100");
            put("count(root.cg2.topic1.s)", "100");
            put("count(root.cg2.topic2.s)", "100");
            put("count(root.topic1.s)", "100");
            put("count(root.topic2.s)", "100");
          }
        });
  }

  // --------------------------- //
  // Scenario 2: Realtime Data //
  // --------------------------- //

  @Test
  public void test3C1CGSubscribeOneTopicRealtimeData() throws Exception {
    final long currentTime = System.currentTimeMillis();

    createTopics(currentTime);
    createPipes(currentTime);
    final List<SubscriptionPullConsumer> consumers = new ArrayList<>();
    consumers.add(createConsumerAndSubscribeTopics("c1", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c2", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c3", "cg1", "topic1"));

    // realtime data
    insertData(currentTime);

    pollMessagesAndCheck(
        consumers,
        new HashMap<String, String>() {
          {
            put("count(root.cg1.topic1.s)", "100");
            put("count(root.topic1.s)", "100");
            put("count(root.topic2.s)", "100");
          }
        });
  }

  @Test
  public void test3C3CGSubscribeOneTopicRealtimeData() throws Exception {
    final long currentTime = System.currentTimeMillis();

    createTopics(currentTime);
    createPipes(currentTime);
    final List<SubscriptionPullConsumer> consumers = new ArrayList<>();
    consumers.add(createConsumerAndSubscribeTopics("c1", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c2", "cg2", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c3", "cg3", "topic1"));

    // Realtime data
    insertData(currentTime);

    pollMessagesAndCheck(
        consumers,
        new HashMap<String, String>() {
          {
            put("count(root.cg1.topic1.s)", "100");
            put("count(root.cg2.topic1.s)", "100");
            put("count(root.cg3.topic1.s)", "100");
            put("count(root.topic1.s)", "100");
            put("count(root.topic2.s)", "100");
          }
        });
  }

  @Test
  public void test3C1CGSubscribeTwoTopicRealtimeData() throws Exception {
    final long currentTime = System.currentTimeMillis();

    createTopics(currentTime);
    createPipes(currentTime);
    final List<SubscriptionPullConsumer> consumers = new ArrayList<>();
    consumers.add(createConsumerAndSubscribeTopics("c1", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c2", "cg1", "topic1", "topic2"));
    consumers.add(createConsumerAndSubscribeTopics("c3", "cg1", "topic2"));

    // Realtime data
    insertData(currentTime);

    pollMessagesAndCheck(
        consumers,
        new HashMap<String, String>() {
          {
            put("count(root.cg1.topic1.s)", "100");
            put("count(root.cg1.topic2.s)", "100");
            put("count(root.topic1.s)", "100");
            put("count(root.topic2.s)", "100");
          }
        });
  }

  @Test
  public void test3C3CGSubscribeTwoTopicRealtimeData() throws Exception {
    final long currentTime = System.currentTimeMillis();

    createTopics(currentTime);
    createPipes(currentTime);
    final List<SubscriptionPullConsumer> consumers = new ArrayList<>();
    consumers.add(createConsumerAndSubscribeTopics("c1", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c2", "cg2", "topic1", "topic2"));
    consumers.add(createConsumerAndSubscribeTopics("c3", "cg3", "topic2"));

    // Realtime data
    insertData(currentTime);

    pollMessagesAndCheck(
        consumers,
        new HashMap<String, String>() {
          {
            put("count(root.cg1.topic1.s)", "100");
            put("count(root.cg2.topic1.s)", "100");
            put("count(root.cg2.topic2.s)", "100");
            put("count(root.cg3.topic2.s)", "100");
            put("count(root.topic1.s)", "100");
            put("count(root.topic2.s)", "100");
          }
        });
  }

  @Test
  public void test4C2CGSubscribeTwoTopicRealtimeData() throws Exception {
    final long currentTime = System.currentTimeMillis();

    createTopics(currentTime);
    createPipes(currentTime);
    final List<SubscriptionPullConsumer> consumers = new ArrayList<>();
    consumers.add(createConsumerAndSubscribeTopics("c1", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c2", "cg2", "topic1", "topic2"));
    consumers.add(createConsumerAndSubscribeTopics("c3", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c4", "cg2", "topic2"));

    // realtime data
    insertData(currentTime);

    pollMessagesAndCheck(
        consumers,
        new HashMap<String, String>() {
          {
            put("count(root.cg1.topic1.s)", "100");
            put("count(root.cg2.topic1.s)", "100");
            put("count(root.cg2.topic2.s)", "100");
            put("count(root.topic1.s)", "100");
            put("count(root.topic2.s)", "100");
          }
        });
  }

  /////////////////////////////// utility ///////////////////////////////

  private void createTopics(long currentTime) {
    // Create topics on sender
    try (final ISession session = senderEnv.getSessionConnection()) {
      session.executeNonQueryStatement(
          String.format("create topic topic1 with ('end-time'='%s')", currentTime - 1));
      session.executeNonQueryStatement(
          String.format("create topic topic2 with ('start-time'='%s')", currentTime));
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void insertData(long currentTime) {
    // Insert some data on sender
    try (ISession session = senderEnv.getSessionConnection()) {
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.topic1(time, s) values (%s, 1)", i)); // topic1
        session.executeNonQueryStatement(
            String.format(
                "insert into root.topic2(time, s) values (%s, 1)", currentTime + i)); // topic2
      }
      session.executeNonQueryStatement("flush");
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void createPipes(long currentTime) {
    // For sync reference
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverEnv.getIP());
      connectorAttributes.put("connector.port", receiverEnv.getPort());

      extractorAttributes.put("inclusion", "data.insert");
      extractorAttributes.put("inclusion.exclusion", "data.delete");
      extractorAttributes.put("end-time", String.valueOf(currentTime - 1));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("sync_topic1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverEnv.getIP());
      connectorAttributes.put("connector.port", receiverEnv.getPort());

      extractorAttributes.put("inclusion", "data.insert");
      extractorAttributes.put("inclusion.exclusion", "data.delete");
      extractorAttributes.put("start-time", String.valueOf(currentTime));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("sync_topic2", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private SubscriptionPullConsumer createConsumerAndSubscribeTopics(
      String consumerId, String consumerGroupId, String... topicNames) throws Exception {
    final SubscriptionPullConsumer consumer =
        new SubscriptionPullConsumer.Builder()
            .host(senderEnv.getIP())
            .port(Integer.parseInt(senderEnv.getPort()))
            .consumerId(consumerId)
            .consumerGroupId(consumerGroupId)
            .autoCommit(false)
            .buildPullConsumer();
    consumer.open();
    consumer.subscribe(topicNames);
    return consumer;
  }

  private void pollMessagesAndCheck(
      List<SubscriptionPullConsumer> consumers, Map<String, String> expectedHeaderWithResult)
      throws Exception {
    final AtomicBoolean isClosed = new AtomicBoolean(false);
    final AtomicBoolean receiverCrashed = new AtomicBoolean(false);

    final List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < consumers.size(); ++i) {
      final int index = i;
      final String consumerId = consumers.get(index).getConsumerId();
      final String consumerGroupId = consumers.get(index).getConsumerGroupId();
      Thread t =
          new Thread(
              () -> {
                try (final SubscriptionPullConsumer consumer = consumers.get(index)) {
                  while (!isClosed.get()) {
                    try {
                      Thread.sleep(1000); // wait some time
                    } catch (InterruptedException e) {
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
                        final List<String> columnNameList = dataSet.getColumnNames();
                        while (dataSet.hasNext()) {
                          final RowRecord record = dataSet.next();
                          if (!insertRowRecordEnrichedByConsumerGroupId(
                              columnNameList, record, consumerGroupId)) {
                            receiverCrashed.set(true);
                            throw new RuntimeException("detect receiver crashed");
                          }
                        }
                      }
                    }
                    consumer.commitSync(messages);
                  }
                  // No need to unsubscribe
                } catch (Exception e) {
                  e.printStackTrace();
                  // Avoid failure
                } finally {
                  LOGGER.info("consumer {} (group {}) exiting...", consumerId, consumerGroupId);
                }
              },
              String.format("%s_%s", consumerGroupId, consumerId));
      t.start();
      threads.add(t);
    }

    // Check data on receiver
    try {
      try (final Connection connection = receiverEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        // Keep retrying if there are execution failures
        Awaitility.await()
            .pollDelay(1, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .atMost(180, TimeUnit.SECONDS)
            .untilAsserted(
                () -> {
                  if (receiverCrashed.get()) {
                    LOGGER.info("detect receiver crashed, skipping this test...");
                    return;
                  }
                  TestUtils.assertSingleResultSetEqual(
                      TestUtils.executeQueryWithRetry(statement, "select count(*) from root.**"),
                      expectedHeaderWithResult);
                });
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      isClosed.set(true);
      for (Thread thread : threads) {
        thread.join();
      }
    }
  }

  /** @return false -> receiver crashed */
  private boolean insertRowRecordEnrichedByConsumerGroupId(
      List<String> columnNameList, RowRecord record, String consumerGroupId) throws Exception {
    if (columnNameList.size() != 2) {
      LOGGER.warn("unexpected column name list: {}", columnNameList);
      throw new Exception("unexpected column name list");
    }
    final String columnName = columnNameList.get(1);
    if ("root.topic1.s".equals(columnName)) {
      final String sql =
          String.format(
              "insert into root.%s.topic1(time, s) values (%s, 1)",
              consumerGroupId, record.getTimestamp());
      return TestUtils.tryExecuteNonQueryWithRetry(receiverEnv, sql);
    } else if ("root.topic2.s".equals(columnName)) {
      final String sql =
          String.format(
              "insert into root.%s.topic2(time, s) values (%s, 1)",
              consumerGroupId, record.getTimestamp());
      return TestUtils.tryExecuteNonQueryWithRetry(receiverEnv, sql);
    } else {
      LOGGER.warn("unexpected column name: {}", columnName);
      throw new Exception("unexpected column name list");
    }
  }
}
