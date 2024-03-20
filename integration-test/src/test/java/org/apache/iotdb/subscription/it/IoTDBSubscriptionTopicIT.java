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

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBSubscriptionTopicIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSubscriptionTopicIT.class);

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testTopicPathSubscription() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement("create topic topic1 with ('path'='root.db.*.s')");
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // insert some history data
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s) values (%s, 1)", i));
      }
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d2(time, s) values (%s, 1)", i));
      }
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d3(time, t) values (%s, 1)", i));
      }
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.t1(time, s1) values (%s, 1)", i));
      }
      session.executeNonQueryStatement("flush");
    } catch (Exception e) {
      fail(e.getMessage());
    }

    int count = 0;

    // subscription
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      Map<String, String> consumerAttributes = new HashMap<>();
      consumerAttributes.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "cg1");
      consumerAttributes.put(ConsumerConstant.CONSUMER_ID_KEY, "c1");

      session.createConsumer(new ConsumerConfig(consumerAttributes));
      session.subscribe(Collections.singleton("topic1"));

      List<EnrichedTablets> enrichedTabletsList;
      while (true) {
        Thread.sleep(1000); // wait some time
        enrichedTabletsList = session.poll(Collections.singleton("topic1"));
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
              .add(enrichedTablets.getSubscriptionCommitId());
        }
        session.commit(topicNameToSubscriptionCommitIds);
      }
      session.unsubscribe(Collections.singleton("topic1"));
      session.dropConsumer();
    } catch (Exception e) {
      fail(e.getMessage());
    }

    Assert.assertEquals(200, count);
  }

  @Test
  public void testTopicTimeSubscription() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement("create topic topic1 with ('start-time'='now')");
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // insert some history data
    long currentTime = System.currentTimeMillis();
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s) values (%s, 1)", i));
      }
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d2(time, s) values (%s, 1)", currentTime + i));
      }
      session.executeNonQueryStatement("flush");
    } catch (Exception e) {
      fail(e.getMessage());
    }

    int count = 0;

    // subscription
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      Map<String, String> consumerAttributes = new HashMap<>();
      consumerAttributes.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "cg1");
      consumerAttributes.put(ConsumerConstant.CONSUMER_ID_KEY, "c1");

      session.createConsumer(new ConsumerConfig(consumerAttributes));
      session.subscribe(Collections.singleton("topic1"));

      List<EnrichedTablets> enrichedTabletsList;
      while (true) {
        Thread.sleep(1000); // wait some time
        enrichedTabletsList = session.poll(Collections.singleton("topic1"));
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
              .add(enrichedTablets.getSubscriptionCommitId());
        }
        session.commit(topicNameToSubscriptionCommitIds);
      }
      session.unsubscribe(Collections.singleton("topic1"));
      session.dropConsumer();
    } catch (Exception e) {
      fail(e.getMessage());
    }

    Assert.assertEquals(100, count);
  }

  @Test
  public void testTopicProcessorSubscription() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement(
          "create topic topic1 with ('processor'='tumbling-time-sampling-processor', 'processor.tumbling-time.interval-seconds'='1', 'processor.down-sampling.split-file'='true')");
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // insert some history data
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement(
          "insert into root.db.d1 (time, at1) values (1000, 1), (1500, 2), (2000, 3), (2500, 4), (3000, 5)");
      session.executeNonQueryStatement("flush");
    } catch (Exception e) {
      fail(e.getMessage());
    }

    int count = 0;

    // subscription
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      Map<String, String> consumerAttributes = new HashMap<>();
      consumerAttributes.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "cg1");
      consumerAttributes.put(ConsumerConstant.CONSUMER_ID_KEY, "c1");

      session.createConsumer(new ConsumerConfig(consumerAttributes));
      session.subscribe(Collections.singleton("topic1"));

      List<EnrichedTablets> enrichedTabletsList;
      while (true) {
        Thread.sleep(1000); // wait some time
        enrichedTabletsList = session.poll(Collections.singleton("topic1"));
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
              .add(enrichedTablets.getSubscriptionCommitId());
        }
        session.commit(topicNameToSubscriptionCommitIds);
      }
      session.unsubscribe(Collections.singleton("topic1"));
      session.dropConsumer();
    } catch (Exception e) {
      fail(e.getMessage());
    }

    Assert.assertEquals(3, count);
  }
}
