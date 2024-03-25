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

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.ISessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.session.subscription.SubscriptionMessage;
import org.apache.iotdb.session.subscription.SubscriptionPullConsumer;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSubscriptionConsumerGroupIT {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBSubscriptionConsumerGroupIT.class);

  private static final int BASE = 233;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private void createTopics() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement("create topic topic1 with ('start-time'='now')");
      session.executeNonQueryStatement("create topic topic2 with ('end-time'='now')");
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  private void testMultiConsumersSubscribeMultiTopicsTemplate(
      List<SubscriptionPullConsumer> consumers, int factor) throws Exception {
    ConcurrentHashMap<String, ConcurrentHashMap<Long, Long>> consumerGroupIdToTimestamps =
        new ConcurrentHashMap<>();
    long currentTime = System.currentTimeMillis();
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (int i = 0; i < BASE; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s) values (%s, 1)", i));
      }
      for (int i = 0; i < BASE; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d2(time, s) values (%s, 1)", currentTime + i));
      }
      session.executeNonQueryStatement("flush");
    } catch (Exception e) {
      fail(e.getMessage());
    }

    List<Thread> threads = new ArrayList<>();
    for (SubscriptionPullConsumer consumer : consumers) {
      Thread t =
          new Thread(
              () -> {
                try {
                  while (true) {
                    Thread.sleep(1000); // wait some time
                    List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(100));
                    if (messages.isEmpty()) {
                      break;
                    }
                    for (SubscriptionMessage message : messages) {
                      ISessionDataSet dataSet = message.getPayload();
                      while (dataSet.hasNext()) {
                        long time = dataSet.next().getTimestamp();
                        consumerGroupIdToTimestamps
                            .computeIfAbsent(
                                consumer.getConsumerGroupId(),
                                (consumerGroupId) -> new ConcurrentHashMap<>())
                            .put(time, time);
                      }
                    }
                    consumer.commitSync(messages);
                  }
                  consumer.close();
                } catch (Exception e) {
                  fail(e.getMessage());
                }
              });
      t.start();
      threads.add(t);
    }

    for (Thread thread : threads) {
      thread.join();
    }

    Assert.assertEquals(
        BASE * factor,
        consumerGroupIdToTimestamps.values().stream().mapToInt(Map::size).reduce(0, Integer::sum));
  }

  private SubscriptionPullConsumer createConsumerAndSubscribeTopics(
      String consumerId, String consumerGroupId, String... topicNames) throws Exception {
    SubscriptionPullConsumer consumer =
        new SubscriptionPullConsumer.Builder()
            .host(EnvFactory.getEnv().getIP())
            .port(Integer.parseInt(EnvFactory.getEnv().getPort()))
            .consumerId(consumerId)
            .consumerGroupId(consumerGroupId)
            .buildPullConsumer();
    consumer.subscribe(topicNames);
    return consumer;
  }

  @Test
  public void test3C1CGSubscribeOneTopic() throws Exception {
    createTopics();
    List<SubscriptionPullConsumer> consumers = new ArrayList<>();
    consumers.add(createConsumerAndSubscribeTopics("c1", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c2", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c3", "cg1", "topic1"));
    testMultiConsumersSubscribeMultiTopicsTemplate(consumers, 1);
  }

  @Test
  public void test3C3CGSubscribeOneTopic() throws Exception {
    createTopics();
    List<SubscriptionPullConsumer> consumers = new ArrayList<>();
    consumers.add(createConsumerAndSubscribeTopics("c1", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c2", "cg2", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c3", "cg3", "topic1"));
    testMultiConsumersSubscribeMultiTopicsTemplate(consumers, 3);
  }

  @Test
  public void test3C1CGSubscribeTwoTopic() throws Exception {
    createTopics();
    List<SubscriptionPullConsumer> consumers = new ArrayList<>();
    consumers.add(createConsumerAndSubscribeTopics("c1", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c2", "cg1", "topic1", "topic2"));
    consumers.add(createConsumerAndSubscribeTopics("c3", "cg1", "topic2"));
    testMultiConsumersSubscribeMultiTopicsTemplate(consumers, 2);
  }

  @Test
  public void test3C3CGSubscribeTwoTopic() throws Exception {
    createTopics();
    List<SubscriptionPullConsumer> consumers = new ArrayList<>();
    consumers.add(createConsumerAndSubscribeTopics("c1", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c2", "cg2", "topic1", "topic2"));
    consumers.add(createConsumerAndSubscribeTopics("c3", "cg3", "topic2"));
    testMultiConsumersSubscribeMultiTopicsTemplate(consumers, 4);
  }

  @Test
  public void test4C2CGSubscribeTwoTopic() throws Exception {
    createTopics();
    List<SubscriptionPullConsumer> consumers = new ArrayList<>();
    consumers.add(createConsumerAndSubscribeTopics("c1", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c2", "cg2", "topic1", "topic2"));
    consumers.add(createConsumerAndSubscribeTopics("c3", "cg1", "topic1"));
    consumers.add(createConsumerAndSubscribeTopics("c4", "cg2", "topic2"));
    testMultiConsumersSubscribeMultiTopicsTemplate(consumers, 3);
  }
}
