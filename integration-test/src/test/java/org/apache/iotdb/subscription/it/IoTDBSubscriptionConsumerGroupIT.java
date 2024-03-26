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
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.subscription.payload.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.payload.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.payload.response.EnrichedTablets;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.Tablet;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
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

  private void testMultiConsumersSubscribeMultiTopicsTemplate(
      List<Pair<ConsumerConfig, Set<String>>> consumerConfigs, int factor) throws Exception {
    ConcurrentHashMap<String, ConcurrentHashMap<Long, Long>> consumerGroupIdToTimestamps =
        new ConcurrentHashMap<>();

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement("create topic topic1 with ('start-time'='now')");
      session.executeNonQueryStatement("create topic topic2 with ('end-time'='now')");
    } catch (Exception e) {
      fail(e.getMessage());
    }

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
      session.executeNonQueryStatement("flush");
    } catch (Exception e) {
      fail(e.getMessage());
    }

    List<Thread> threads = new ArrayList<>();
    for (Pair<ConsumerConfig, Set<String>> consumerConfig : consumerConfigs) {
      Thread t =
          new Thread(
              () -> {
                try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
                  session.createConsumer(consumerConfig.left);
                  session.subscribe(consumerConfig.right);

                  List<EnrichedTablets> enrichedTabletsList;
                  while (true) {
                    Thread.sleep(1000); // wait some time
                    enrichedTabletsList = session.poll(consumerConfig.right);
                    if (enrichedTabletsList.isEmpty()) {
                      break;
                    }
                    Map<String, List<String>> topicNameToSubscriptionCommitIds = new HashMap<>();
                    for (EnrichedTablets enrichedTablets : enrichedTabletsList) {
                      for (Tablet tablet : enrichedTablets.getTablets()) {
                        for (Long time : tablet.timestamps) {
                          consumerGroupIdToTimestamps
                              .computeIfAbsent(
                                  consumerConfig.left.getConsumerGroupId(),
                                  (consumerGroupId) -> new ConcurrentHashMap<>())
                              .put(time, time);
                        }
                      }
                      topicNameToSubscriptionCommitIds
                          .computeIfAbsent(
                              enrichedTablets.getTopicName(), (topicName) -> new ArrayList<>())
                          .add(enrichedTablets.getSubscriptionCommitId());
                    }
                    session.commit(topicNameToSubscriptionCommitIds);
                  }
                  session.unsubscribe(consumerConfig.right);
                  session.dropConsumer();
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

  @Test
  public void test3C1CGSubscribeOneTopic() throws Exception {
    List<Pair<ConsumerConfig, Set<String>>> consumerConfigs = new ArrayList<>();
    consumerConfigs.add(
        new Pair<>(
            new ConsumerConfig(
                new HashMap<String, String>() {
                  {
                    put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "cg1");
                    put(ConsumerConstant.CONSUMER_ID_KEY, "c1");
                  }
                }),
            new HashSet<>(Collections.singletonList("topic1"))));
    consumerConfigs.add(
        new Pair<>(
            new ConsumerConfig(
                new HashMap<String, String>() {
                  {
                    put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "cg1");
                    put(ConsumerConstant.CONSUMER_ID_KEY, "c2");
                  }
                }),
            new HashSet<>(Collections.singletonList("topic1"))));
    consumerConfigs.add(
        new Pair<>(
            new ConsumerConfig(
                new HashMap<String, String>() {
                  {
                    put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "cg1");
                    put(ConsumerConstant.CONSUMER_ID_KEY, "c3");
                  }
                }),
            new HashSet<>(Collections.singletonList("topic1"))));
    testMultiConsumersSubscribeMultiTopicsTemplate(consumerConfigs, 1);
  }

  @Test
  public void test3C3CGSubscribeOneTopic() throws Exception {
    List<Pair<ConsumerConfig, Set<String>>> consumerConfigs = new ArrayList<>();
    consumerConfigs.add(
        new Pair<>(
            new ConsumerConfig(
                new HashMap<String, String>() {
                  {
                    put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "cg1");
                    put(ConsumerConstant.CONSUMER_ID_KEY, "c1");
                  }
                }),
            new HashSet<>(Collections.singletonList("topic1"))));
    consumerConfigs.add(
        new Pair<>(
            new ConsumerConfig(
                new HashMap<String, String>() {
                  {
                    put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "cg2");
                    put(ConsumerConstant.CONSUMER_ID_KEY, "c2");
                  }
                }),
            new HashSet<>(Collections.singletonList("topic1"))));
    consumerConfigs.add(
        new Pair<>(
            new ConsumerConfig(
                new HashMap<String, String>() {
                  {
                    put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "cg3");
                    put(ConsumerConstant.CONSUMER_ID_KEY, "c3");
                  }
                }),
            new HashSet<>(Collections.singletonList("topic1"))));
    testMultiConsumersSubscribeMultiTopicsTemplate(consumerConfigs, 3);
  }

  @Test
  public void test3C1CGSubscribeTwoTopic() throws Exception {
    List<Pair<ConsumerConfig, Set<String>>> consumerConfigs = new ArrayList<>();
    consumerConfigs.add(
        new Pair<>(
            new ConsumerConfig(
                new HashMap<String, String>() {
                  {
                    put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "cg1");
                    put(ConsumerConstant.CONSUMER_ID_KEY, "c1");
                  }
                }),
            new HashSet<>(Collections.singletonList("topic1"))));
    consumerConfigs.add(
        new Pair<>(
            new ConsumerConfig(
                new HashMap<String, String>() {
                  {
                    put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "cg1");
                    put(ConsumerConstant.CONSUMER_ID_KEY, "c2");
                  }
                }),
            new HashSet<>(Arrays.asList("topic1", "topic2"))));
    consumerConfigs.add(
        new Pair<>(
            new ConsumerConfig(
                new HashMap<String, String>() {
                  {
                    put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "cg1");
                    put(ConsumerConstant.CONSUMER_ID_KEY, "c3");
                  }
                }),
            new HashSet<>(Collections.singletonList("topic2"))));
    testMultiConsumersSubscribeMultiTopicsTemplate(consumerConfigs, 2);
  }

  @Test
  public void test3C3CGSubscribeTwoTopic() throws Exception {
    List<Pair<ConsumerConfig, Set<String>>> consumerConfigs = new ArrayList<>();
    consumerConfigs.add(
        new Pair<>(
            new ConsumerConfig(
                new HashMap<String, String>() {
                  {
                    put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "cg1");
                    put(ConsumerConstant.CONSUMER_ID_KEY, "c1");
                  }
                }),
            new HashSet<>(Collections.singletonList("topic1"))));
    consumerConfigs.add(
        new Pair<>(
            new ConsumerConfig(
                new HashMap<String, String>() {
                  {
                    put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "cg2");
                    put(ConsumerConstant.CONSUMER_ID_KEY, "c2");
                  }
                }),
            new HashSet<>(Arrays.asList("topic1", "topic2"))));
    consumerConfigs.add(
        new Pair<>(
            new ConsumerConfig(
                new HashMap<String, String>() {
                  {
                    put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "cg3");
                    put(ConsumerConstant.CONSUMER_ID_KEY, "c3");
                  }
                }),
            new HashSet<>(Collections.singletonList("topic2"))));
    testMultiConsumersSubscribeMultiTopicsTemplate(consumerConfigs, 4);
  }

  @Test
  public void test4C2CGSubscribeTwoTopic() throws Exception {
    List<Pair<ConsumerConfig, Set<String>>> consumerConfigs = new ArrayList<>();
    consumerConfigs.add(
        new Pair<>(
            new ConsumerConfig(
                new HashMap<String, String>() {
                  {
                    put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "cg1");
                    put(ConsumerConstant.CONSUMER_ID_KEY, "c1");
                  }
                }),
            new HashSet<>(Collections.singletonList("topic1"))));
    consumerConfigs.add(
        new Pair<>(
            new ConsumerConfig(
                new HashMap<String, String>() {
                  {
                    put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "cg2");
                    put(ConsumerConstant.CONSUMER_ID_KEY, "c2");
                  }
                }),
            new HashSet<>(Arrays.asList("topic1", "topic2"))));
    consumerConfigs.add(
        new Pair<>(
            new ConsumerConfig(
                new HashMap<String, String>() {
                  {
                    put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "cg1");
                    put(ConsumerConstant.CONSUMER_ID_KEY, "c3");
                  }
                }),
            new HashSet<>(Collections.singletonList("topic1"))));
    consumerConfigs.add(
        new Pair<>(
            new ConsumerConfig(
                new HashMap<String, String>() {
                  {
                    put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "cg2");
                    put(ConsumerConstant.CONSUMER_ID_KEY, "c4");
                  }
                }),
            new HashSet<>(Collections.singletonList("topic2"))));
    testMultiConsumersSubscribeMultiTopicsTemplate(consumerConfigs, 3);
  }
}
