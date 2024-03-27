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
import org.apache.iotdb.itbase.category.MultiClusterIT2;
import org.apache.iotdb.session.subscription.SubscriptionMessage;
import org.apache.iotdb.session.subscription.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.SubscriptionSessionDataSet;
import org.apache.iotdb.session.subscription.SubscriptionSessionDataSets;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2.class})
public class IoTDBSubscriptionConsumerGroupIT extends AbstractSubscriptionDualIT {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBSubscriptionConsumerGroupIT.class);

  @Test
  public void test3C1CGSubscribeOneTopic() throws Exception {
    long currentTime = createTopics();
    List<SubscriptionPullConsumer> consumers = new ArrayList<>();
    consumers.add(createConsumerAndSubscribeTopics("c1", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c2", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c3", "cg1", "topic1"));
    testMultiConsumersSubscribeMultiTopicsTemplate(
        currentTime,
        consumers,
        new HashMap<String, String>() {
          {
            put("count(root.topic1.s.cg1)", "100");
          }
        });
  }

  @Test
  public void test3C3CGSubscribeOneTopic() throws Exception {
    long currentTime = createTopics();
    List<SubscriptionPullConsumer> consumers = new ArrayList<>();
    consumers.add(createConsumerAndSubscribeTopics("c1", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c2", "cg2", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c3", "cg3", "topic1"));
    testMultiConsumersSubscribeMultiTopicsTemplate(
        currentTime,
        consumers,
        new HashMap<String, String>() {
          {
            put("count(root.topic1.s.cg1)", "100");
            put("count(root.topic1.s.cg2)", "100");
            put("count(root.topic1.s.cg3)", "100");
          }
        });
  }

  @Test
  public void test3C1CGSubscribeTwoTopic() throws Exception {
    long currentTime = createTopics();
    List<SubscriptionPullConsumer> consumers = new ArrayList<>();
    consumers.add(createConsumerAndSubscribeTopics("c1", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c2", "cg1", "topic1", "topic2"));
    consumers.add(createConsumerAndSubscribeTopics("c3", "cg1", "topic2"));
    testMultiConsumersSubscribeMultiTopicsTemplate(
        currentTime,
        consumers,
        new HashMap<String, String>() {
          {
            put("count(root.topic1.s.cg1)", "100");
            put("count(root.topic2.s.cg1)", "100");
          }
        });
  }

  @Test
  public void test3C3CGSubscribeTwoTopic() throws Exception {
    long currentTime = createTopics();
    List<SubscriptionPullConsumer> consumers = new ArrayList<>();
    consumers.add(createConsumerAndSubscribeTopics("c1", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c2", "cg2", "topic1", "topic2"));
    consumers.add(createConsumerAndSubscribeTopics("c3", "cg3", "topic2"));
    testMultiConsumersSubscribeMultiTopicsTemplate(
        currentTime,
        consumers,
        new HashMap<String, String>() {
          {
            put("count(root.topic1.s.cg1)", "100");
            put("count(root.topic1.s.cg2)", "100");
            put("count(root.topic2.s.cg2)", "100");
            put("count(root.topic2.s.cg3)", "100");
          }
        });
  }

  @Test
  public void test4C2CGSubscribeTwoTopic() throws Exception {
    long currentTime = createTopics();
    List<SubscriptionPullConsumer> consumers = new ArrayList<>();
    consumers.add(createConsumerAndSubscribeTopics("c1", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c2", "cg2", "topic1", "topic2"));
    consumers.add(createConsumerAndSubscribeTopics("c3", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c4", "cg2", "topic2"));
    testMultiConsumersSubscribeMultiTopicsTemplate(
        currentTime,
        consumers,
        new HashMap<String, String>() {
          {
            put("count(root.topic1.s.cg1)", "100");
            put("count(root.topic1.s.cg2)", "100");
            put("count(root.topic2.s.cg2)", "100");
          }
        });
  }

  private long createTopics() {
    // create topics on sender
    long currentTime = System.currentTimeMillis();
    try (ISession session = senderEnv.getSessionConnection()) {
      session.executeNonQueryStatement(
          String.format("create topic topic1 with ('end-time'='%s')", currentTime - 1));
      session.executeNonQueryStatement(
          String.format("create topic topic2 with ('start-time'='%s')", currentTime));
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    return currentTime;
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

  private void testMultiConsumersSubscribeMultiTopicsTemplate(
      long currentTime,
      List<SubscriptionPullConsumer> consumers,
      Map<String, String> expectedHeaderWithResult)
      throws Exception {
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

    AtomicBoolean isClosed = new AtomicBoolean(false);
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < consumers.size(); ++i) {
      final int index = i;
      Thread t =
          new Thread(
              () -> {
                try (SubscriptionPullConsumer consumer = consumers.get(index);
                    ISession session = receiverEnv.getSessionConnection()) {
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
                          insertRowRecordEnrichByConsumerGroupId(
                              session, columnNameList, record, consumer.getConsumerGroupId());
                        }
                      }
                    }
                    consumer.commitSync(messages);
                  }
                  // no need to unsubscribe
                  LOGGER.info(
                      "consumer {} (group {}) exiting...",
                      consumer.getConsumerId(),
                      consumer.getConsumerGroupId());
                } catch (Exception e) {
                  e.printStackTrace();
                  // avoid fail
                }
              });
      t.start();
      threads.add(t);
    }

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
                        expectedHeaderWithResult));
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

  private void insertRowRecordEnrichByConsumerGroupId(
      ISession session, List<String> columnNameList, RowRecord record, String consumerGroupId)
      throws Exception {
    if (columnNameList.size() != 2) {
      LOGGER.warn("unexpected column name list: {}", columnNameList);
      throw new Exception("unexpected column name list");
    }
    String columnName = columnNameList.get(1);
    // REMOVE ME: for debug
    LOGGER.info(
        "insert {}.{} {} {}",
        columnName,
        consumerGroupId,
        record.getTimestamp(),
        record.getFields().get(0).getFloatV());
    session.insertRecord(
        columnName,
        record.getTimestamp(),
        Collections.singletonList(consumerGroupId),
        Collections.singletonList(TSDataType.FLOAT),
        Collections.singletonList(record.getFields().get(0).getFloatV()));
  }
}
