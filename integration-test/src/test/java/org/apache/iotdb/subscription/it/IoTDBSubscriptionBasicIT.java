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
import org.apache.iotdb.isession.ISessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.RpcUtils;
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
import java.util.List;

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
    try (SubscriptionPullConsumer consumer =
        new SubscriptionPullConsumer.Builder()
            .host(EnvFactory.getEnv().getIP())
            .port(Integer.parseInt(EnvFactory.getEnv().getPort()))
            .consumerId("c1")
            .consumerGroupId("cg1")
            .buildPullConsumer()) {
      consumer.createTopic("topic1");
      consumer.subscribe("topic1");
      while (true) {
        Thread.sleep(1000); // wait some time
        List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(100));
        if (messages.isEmpty()) {
          break;
        }
        for (SubscriptionMessage message : messages) {
          ISessionDataSet dataSet = message.getPayload();
          while (dataSet.hasNext()) {
            dataSet.next();
            count += 1;
          }
        }
        consumer.commitSync(messages);
        consumer.unsubscribe("topic1");
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    Assert.assertEquals(100, count);
  }

  @Test
  public void testRestartSubscription() throws Exception {
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
    try {
      SubscriptionPullConsumer consumer =
          new SubscriptionPullConsumer.Builder()
              .host(EnvFactory.getEnv().getIP())
              .port(Integer.parseInt(EnvFactory.getEnv().getPort()))
              .consumerId("c1")
              .consumerGroupId("cg1")
              .buildPullConsumer();
      consumer.createTopic("topic1");
      consumer.subscribe("topic1");
      while (true) {
        Thread.sleep(1000); // wait some time
        List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(100));
        if (messages.isEmpty()) {
          break;
        }
        for (SubscriptionMessage message : messages) {
          ISessionDataSet dataSet = message.getPayload();
          while (dataSet.hasNext()) {
            dataSet.next();
            count += 1;
          }
        }
        consumer.commitSync(messages);
        // we do not unsubscribe topic and close consumer here
      }
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

    // subscription again
    try (SubscriptionPullConsumer consumer =
        new SubscriptionPullConsumer.Builder()
            .host(EnvFactory.getEnv().getIP())
            .port(Integer.parseInt(EnvFactory.getEnv().getPort()))
            .consumerId("c1")
            .consumerGroupId("cg1")
            .buildPullConsumer()) {
      while (true) {
        Thread.sleep(1000); // wait some time
        List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(100));
        if (messages.isEmpty()) {
          break;
        }
        for (SubscriptionMessage message : messages) {
          ISessionDataSet dataSet = message.getPayload();
          while (dataSet.hasNext()) {
            dataSet.next();
            count += 1;
          }
        }
        consumer.commitSync(messages);
        consumer.unsubscribe("topic1");
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    Assert.assertEquals(200, count);
  }
}
