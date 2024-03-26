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

import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicResp;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.RpcUtils;
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
public class IoTDBSubscriptionBasicIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSubscriptionBasicIT.class);

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testSimpleSubscription() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement("create topic topic1");
    } catch (Exception e) {
      fail(e.getMessage());
    }

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
  public void testRestartSubscription() throws Exception {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement("create topic topic1");
    } catch (Exception e) {
      fail(e.getMessage());
    }

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
      // we do not unsubscribe and drop consumer
    } catch (Exception e) {
      fail(e.getMessage());
    }

    Assert.assertEquals(100, count);

    // restart cluster
    TestUtils.restartCluster(EnvFactory.getEnv());

    // show topics and subscriptions
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      TShowTopicResp showTopicResp = client.showTopic(new TShowTopicReq());
      Assert.assertEquals(RpcUtils.SUCCESS_STATUS.getCode(), showTopicResp.status.getCode());
      Assert.assertNotNull(showTopicResp.topicInfoList);
      Assert.assertEquals(1, showTopicResp.topicInfoList.size());

      TShowSubscriptionResp showSubscriptionResp =
          client.showSubscription(new TShowSubscriptionReq());
      Assert.assertEquals(RpcUtils.SUCCESS_STATUS.getCode(), showSubscriptionResp.status.getCode());
      Assert.assertNotNull(showSubscriptionResp.subscriptionInfoList);
      Assert.assertEquals(1, showSubscriptionResp.subscriptionInfoList.size());
    }

    // insert some history data again
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (int i = 100; i < 200; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
      }
      session.executeNonQueryStatement("flush");
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // handshake and poll again
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      Map<String, String> consumerAttributes = new HashMap<>();
      consumerAttributes.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "cg1");
      consumerAttributes.put(ConsumerConstant.CONSUMER_ID_KEY, "c1");

      // no-op just for handshake
      session.createConsumer(new ConsumerConfig(consumerAttributes));

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
        session.unsubscribe(Collections.singleton("topic1"));
        session.dropConsumer();
      }
    }

    Assert.assertEquals(200, count);
  }
}
