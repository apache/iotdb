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
import org.apache.iotdb.itbase.category.MultiClusterIT3;
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
@Category({MultiClusterIT3.class})
public class IoTDBSubscriptionConsumerGroupIT extends AbstractSubscriptionDualIT {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBSubscriptionConsumerGroupIT.class);

  @Test
  public void test3C1CGSubscribeOneTopic() throws Exception {
    long currentTime = System.currentTimeMillis();
    prepareData(currentTime);
    createTopics(currentTime);
    createPipes(currentTime);
    List<SubscriptionPullConsumer> consumers = new ArrayList<>();
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
  public void test3C3CGSubscribeOneTopic() throws Exception {
    long currentTime = System.currentTimeMillis();
    prepareData(currentTime);
    createTopics(currentTime);
    createPipes(currentTime);
    List<SubscriptionPullConsumer> consumers = new ArrayList<>();
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
  public void test3C1CGSubscribeTwoTopic() throws Exception {
    long currentTime = System.currentTimeMillis();
    prepareData(currentTime);
    createTopics(currentTime);
    createPipes(currentTime);
    List<SubscriptionPullConsumer> consumers = new ArrayList<>();
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
  public void test3C3CGSubscribeTwoTopic() throws Exception {
    long currentTime = System.currentTimeMillis();
    prepareData(currentTime);
    createTopics(currentTime);
    createPipes(currentTime);
    List<SubscriptionPullConsumer> consumers = new ArrayList<>();
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
  public void test4C2CGSubscribeTwoTopic() throws Exception {
    long currentTime = System.currentTimeMillis();
    prepareData(currentTime);
    createTopics(currentTime);
    createPipes(currentTime);
    List<SubscriptionPullConsumer> consumers = new ArrayList<>();
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

  @Test
  public void test3C3CGSubscribeTwoTopicRealTime() throws Exception {
    long currentTime = System.currentTimeMillis();
    createTopics(currentTime);
    List<SubscriptionPullConsumer> consumers = new ArrayList<>();
    consumers.add(createConsumerAndSubscribeTopics("c1", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c2", "cg2", "topic1", "topic2"));
    consumers.add(createConsumerAndSubscribeTopics("c3", "cg3", "topic2"));
    createPipes(currentTime);
    Thread.sleep(5000);
    prepareData(currentTime);
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
  public void test4C2CGSubscribeTwoTopicRealTime() throws Exception {
    long currentTime = System.currentTimeMillis();
    createTopics(currentTime);
    List<SubscriptionPullConsumer> consumers = new ArrayList<>();
    consumers.add(createConsumerAndSubscribeTopics("c1", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c2", "cg2", "topic1", "topic2"));
    consumers.add(createConsumerAndSubscribeTopics("c3", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c4", "cg2", "topic2"));
    createPipes(currentTime);
    Thread.sleep(5000);
    prepareData(currentTime);
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

  @Test
  public void test3C3CGSubscribeNaiveTopicRealTime() throws Exception {
    createNaiveTopic();
    List<SubscriptionPullConsumer> consumers = new ArrayList<>();
    consumers.add(createConsumerAndSubscribeTopics("c1", "cg1", "topic3"));
    consumers.add(createConsumerAndSubscribeTopics("c2", "cg2", "topic3"));
    consumers.add(createConsumerAndSubscribeTopics("c3", "cg3", "topic3"));
    createNaivePipe();
    Thread.sleep(5000);
    prepareNaiveData();
    pollMessagesAndCheck(
        consumers,
        new HashMap<String, String>() {
          {
            put("count(root.cg1.topic3.s)", "100");
            put("count(root.cg2.topic3.s)", "100");
            put("count(root.cg3.topic3.s)", "100");
            put("count(root.topic3.s)", "100");
          }
        });
  }

  private void createTopics(long currentTime) {
    // create topics on sender
    try (ISession session = senderEnv.getSessionConnection()) {
      session.executeNonQueryStatement(
          String.format("create topic topic1 with ('end-time'='%s')", currentTime - 1));
      session.executeNonQueryStatement(
          String.format("create topic topic2 with ('start-time'='%s')", currentTime));
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void createNaiveTopic() {
    // create topics on sender
    try (ISession session = senderEnv.getSessionConnection()) {
      session.executeNonQueryStatement("create topic topic3");
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void prepareData(long currentTime) {
    // insert some history data on sender
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

  private void prepareNaiveData() {
    // insert some history data on sender
    try (ISession session = senderEnv.getSessionConnection()) {
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.topic3(time, s) values (%s, 1)", i)); // topic3
      }
      session.executeNonQueryStatement("flush");
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void createPipes(long currentTime) {
    // for sync reference
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverEnv.getIP());
      connectorAttributes.put("connector.port", receiverEnv.getPort());

      extractorAttributes.put("end-time", String.valueOf(currentTime - 1));

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("sync_topic1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverEnv.getIP());
      connectorAttributes.put("connector.port", receiverEnv.getPort());

      extractorAttributes.put("start-time", String.valueOf(currentTime));

      TSStatus status =
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

  private void createNaivePipe() {
    // for sync reference
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverEnv.getIP());
      connectorAttributes.put("connector.port", receiverEnv.getPort());

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("sync_topic3", connectorAttributes)
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
    SubscriptionPullConsumer consumer =
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
    AtomicBoolean isClosed = new AtomicBoolean(false);
    AtomicBoolean receiverCrashed = new AtomicBoolean(false);
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < consumers.size(); ++i) {
      final int index = i;
      String consumerId = consumers.get(index).getConsumerId();
      String consumerGroupId = consumers.get(index).getConsumerGroupId();
      Thread t =
          new Thread(
              () -> {
                try (SubscriptionPullConsumer consumer = consumers.get(index)) {
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
                      for (SubscriptionSessionDataSet dataSet : payload) {
                        List<String> columnNameList = dataSet.getColumnNames();
                        while (dataSet.hasNext()) {
                          RowRecord record = dataSet.next();
                          if (!insertRowRecordEnrichByConsumerGroupId(
                              columnNameList, record, consumerGroupId)) {
                            receiverCrashed.set(true);
                            throw new RuntimeException("detect receiver crashed");
                          }
                        }
                      }
                    }
                    consumer.commitSync(messages);
                  }
                  // no need to unsubscribe
                } catch (Exception e) {
                  e.printStackTrace();
                  // avoid fail
                } finally {
                  LOGGER.info("consumer {} (group {}) exiting...", consumerId, consumerGroupId);
                }
              },
              String.format("%s_%s", consumerGroupId, consumerId));
      t.start();
      threads.add(t);
    }

    // check data on receiver
    try {
      try (Connection connection = receiverEnv.getConnection();
          Statement statement = connection.createStatement()) {
        // Keep retrying if there are execution failures
        Awaitility.await()
            .pollDelay(1, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .atMost(100, TimeUnit.SECONDS)
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
  private boolean insertRowRecordEnrichByConsumerGroupId(
      List<String> columnNameList, RowRecord record, String consumerGroupId) throws Exception {
    if (columnNameList.size() != 2) {
      LOGGER.warn("unexpected column name list: {}", columnNameList);
      throw new Exception("unexpected column name list");
    }
    String columnName = columnNameList.get(1);
    if ("root.topic1.s".equals(columnName)) {
      String sql =
          String.format(
              "insert into root.%s.topic1(time, s) values (%s, 1)",
              consumerGroupId, record.getTimestamp());
      // REMOVE ME: for debug
      LOGGER.info(sql);
      return TestUtils.tryExecuteNonQueryWithRetry(receiverEnv, sql);
    } else if ("root.topic2.s".equals(columnName)) {
      String sql =
          String.format(
              "insert into root.%s.topic2(time, s) values (%s, 1)",
              consumerGroupId, record.getTimestamp());
      // REMOVE ME: for debug
      LOGGER.info(sql);
      return TestUtils.tryExecuteNonQueryWithRetry(receiverEnv, sql);
    } else if ("root.topic3.s".equals(columnName)) {
      String sql =
          String.format(
              "insert into root.%s.topic3(time, s) values (%s, 1)",
              consumerGroupId, record.getTimestamp());
      // REMOVE ME: for debug
      LOGGER.info(sql);
      return TestUtils.tryExecuteNonQueryWithRetry(receiverEnv, sql);
    } else {
      LOGGER.warn("unexpected column name: {}", columnName);
      throw new Exception("unexpected column name list");
    }
  }
}
