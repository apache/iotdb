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
import org.apache.iotdb.rpc.subscription.payload.request.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.payload.response.EnrichedTablets;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBSubscriptionSimpleIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSubscriptionSimpleIT.class);

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void basicSubscriptionTest() {
    // insert some history data
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
      }
      session.executeNonQueryStatement("flush");
    } catch (Exception e) {
      fail(e.getMessage());
    }

    int count = 0;

    // subscription
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.createConsumer(new ConsumerConfig("cg1", "c1"));
      session.subscribe(Collections.singletonList("topic1"));
      // TODO: manually create pipe
      session.executeNonQueryStatement(
          "create pipe topic1_cg1 with source ('source'='iotdb-source', 'inclusion'='data', 'inclusion.exclusion'='deletion') with sink ('sink'='subscription-sink', 'topic'='topic1', 'consumer-group'='cg1')");

      List<EnrichedTablets> enrichedTabletsList;
      while (true) {
        Thread.sleep(1000); // wait some time
        enrichedTabletsList = session.poll(Collections.singletonList("topic1"));
        if (enrichedTabletsList.isEmpty()) {
          break;
        }
        Map<String, List<String>> topicNameToSubscriptionCommitIds = new HashMap<>();
        for (EnrichedTablets enrichedTablets : enrichedTabletsList) {
          for (Tablet tablet : enrichedTablets.getTablets()) {
            count += tablet.rowSize;
          }
          topicNameToSubscriptionCommitIds
              .computeIfAbsent(enrichedTablets.getTopicName(), (topicName) -> new ArrayList<>())
              .addAll(enrichedTablets.getSubscriptionCommitIds());
        }
        session.commit(topicNameToSubscriptionCommitIds);
      }
      session.unsubscribe(Collections.singletonList("topic1"));
      session.dropConsumer();
    } catch (Exception e) {
      fail(e.getMessage());
    }

    Assert.assertEquals(count, 100);
  }

  @Test
  public void multiConsumersSubscriptionTest() throws Exception {
    ConcurrentHashMap<Long, Long> timestamps = new ConcurrentHashMap<>();

    List<Thread> threads = new ArrayList<>();
    Thread t =
        new Thread(
            () -> {
              try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
                for (int i = 0; i < 100; ++i) {
                  session.executeNonQueryStatement(
                      String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
                }
                session.executeNonQueryStatement("flush");
              } catch (Exception e) {
                fail(e.getMessage());
              }
            });
    t.start();
    threads.add(t);

    for (int i = 0; i < 3; ++i) {
      final int idx = i;
      t =
          new Thread(
              () -> {
                try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
                  session.createConsumer(new ConsumerConfig("cg1", String.format("c%s", idx)));
                  session.subscribe(Collections.singletonList("topic1"));
                  if (idx == 0) {
                    // TODO: manually create pipe
                    session.executeNonQueryStatement(
                        "create pipe topic1_cg1 with source ('source'='iotdb-source', 'inclusion'='data', 'inclusion.exclusion'='deletion') with sink ('sink'='subscription-sink', 'topic'='topic1', 'consumer-group'='cg1')");
                  }

                  List<EnrichedTablets> enrichedTabletsList;
                  while (true) {
                    Thread.sleep(1000); // wait some time
                    enrichedTabletsList = session.poll(Collections.singletonList("topic1"));
                    if (enrichedTabletsList.isEmpty()) {
                      break;
                    }
                    Map<String, List<String>> topicNameToSubscriptionCommitIds = new HashMap<>();
                    for (EnrichedTablets enrichedTablets : enrichedTabletsList) {
                      for (Tablet tablet : enrichedTablets.getTablets()) {
                        for (Long time : tablet.timestamps) {
                          timestamps.put(time, time);
                        }
                      }
                      topicNameToSubscriptionCommitIds
                          .computeIfAbsent(
                              enrichedTablets.getTopicName(), (topicName) -> new ArrayList<>())
                          .addAll(enrichedTablets.getSubscriptionCommitIds());
                    }
                    session.commit(topicNameToSubscriptionCommitIds);
                  }
                  session.unsubscribe(Collections.singletonList("topic1"));
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

    Assert.assertEquals(timestamps.size(), 100);
  }
}
