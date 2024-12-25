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

package org.apache.iotdb.subscription.it.cluster;

import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.env.AbstractEnv;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionSessionDataSet;
import org.apache.iotdb.subscription.it.AbstractSubscriptionIT;
import org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant;

import org.apache.tsfile.utils.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant.AWAIT;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBSubscriptionRestartIT extends AbstractSubscriptionIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSubscriptionRestartIT.class);

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(3)
        .setDataReplicationFactor(2);

    EnvFactory.getEnv().initClusterEnvironment(3, 3);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();

    super.tearDown();
  }

  @Test
  public void testSubscriptionAfterRestartCluster() throws Exception {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    // Create topic
    final String topicName = "topic1";
    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      session.createTopic(topicName);
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Subscription
    final SubscriptionPullConsumer consumer1;
    final SubscriptionPullConsumer consumer2;
    try {
      consumer1 =
          new SubscriptionPullConsumer.Builder()
              .host(host)
              .port(port)
              .consumerId("c1")
              .consumerGroupId("cg1")
              .autoCommit(true)
              .heartbeatIntervalMs(1000) // narrow heartbeat interval
              .endpointsSyncIntervalMs(5000) // narrow endpoints sync interval
              .buildPullConsumer();
      consumer1.open();
      consumer1.subscribe(topicName);

      consumer2 =
          new SubscriptionPullConsumer.Builder()
              .host(host)
              .port(port)
              .consumerId("c2")
              .consumerGroupId("cg2")
              .autoCommit(true)
              .heartbeatIntervalMs(1000) // narrow heartbeat interval
              .endpointsSyncIntervalMs(5000) // narrow endpoints sync interval
              .buildPullConsumer();
      consumer2.open();
      consumer2.subscribe(topicName);
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
      return;
    }

    // Restart cluster
    try {
      TestUtils.restartCluster(EnvFactory.getEnv());
    } catch (final Throwable e) {
      e.printStackTrace();
      // Avoid failure
      return;
    }

    // Show topics and subscriptions
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      final TShowTopicResp showTopicResp = client.showTopic(new TShowTopicReq());
      Assert.assertEquals(RpcUtils.SUCCESS_STATUS.getCode(), showTopicResp.status.getCode());
      Assert.assertNotNull(showTopicResp.topicInfoList);
      Assert.assertEquals(1, showTopicResp.topicInfoList.size());

      final TShowSubscriptionResp showSubscriptionResp =
          client.showSubscription(new TShowSubscriptionReq());
      Assert.assertEquals(RpcUtils.SUCCESS_STATUS.getCode(), showSubscriptionResp.status.getCode());
      Assert.assertNotNull(showSubscriptionResp.subscriptionInfoList);
      Assert.assertEquals(2, showSubscriptionResp.subscriptionInfoList.size());
    }

    // Insert some historical data
    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
      }
      session.executeNonQueryStatement("flush");
    } catch (final Throwable e) {
      e.printStackTrace();
      // Avoid failure
      return;
    }

    // Subscription again
    final Map<Pair<Long, String>, Long> timestamps = new ConcurrentHashMap<>();
    final AtomicBoolean isClosed = new AtomicBoolean(false);
    final List<Thread> threads = new ArrayList<>();
    threads.add(
        new Thread(
            () -> {
              try (final SubscriptionPullConsumer consumerRef1 = consumer1) {
                while (!isClosed.get()) {
                  LockSupport.parkNanos(IoTDBSubscriptionITConstant.SLEEP_NS); // wait some time
                  final List<SubscriptionMessage> messages;
                  try {
                    messages = consumerRef1.poll(IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS);
                  } catch (final Exception e) {
                    e.printStackTrace();
                    // Avoid failure
                    continue;
                  }
                  for (final SubscriptionMessage message : messages) {
                    for (final SubscriptionSessionDataSet dataSet :
                        message.getSessionDataSetsHandler()) {
                      while (dataSet.hasNext()) {
                        final long timestamp = dataSet.next().getTimestamp();
                        timestamps.put(new Pair<>(timestamp, consumerRef1.toString()), timestamp);
                      }
                    }
                  }
                  // Auto commit
                }
                consumerRef1.unsubscribe(topicName);
              } catch (final Exception e) {
                e.printStackTrace();
                // Avoid failure
              } finally {
                LOGGER.info("consumer exiting...");
              }
            },
            String.format("%s - %s", testName.getDisplayName(), consumer1)));
    threads.add(
        new Thread(
            () -> {
              try (final SubscriptionPullConsumer consumerRef2 = consumer2) {
                while (!isClosed.get()) {
                  LockSupport.parkNanos(IoTDBSubscriptionITConstant.SLEEP_NS); // wait some time
                  final List<SubscriptionMessage> messages;
                  try {
                    messages = consumerRef2.poll(IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS);
                  } catch (final Exception e) {
                    e.printStackTrace();
                    // Avoid failure
                    continue;
                  }
                  for (final SubscriptionMessage message : messages) {
                    for (final SubscriptionSessionDataSet dataSet :
                        message.getSessionDataSetsHandler()) {
                      while (dataSet.hasNext()) {
                        final long timestamp = dataSet.next().getTimestamp();
                        timestamps.put(new Pair<>(timestamp, consumerRef2.toString()), timestamp);
                      }
                    }
                  }
                  // Auto commit
                }
                consumerRef2.unsubscribe(topicName);
              } catch (final Exception e) {
                e.printStackTrace();
                // Avoid failure
              } finally {
                LOGGER.info("consumer exiting...");
              }
            },
            String.format("%s - %s", testName.getDisplayName(), consumer2)));
    for (final Thread thread : threads) {
      thread.start();
    }

    // Check timestamps size
    try {
      // Keep retrying if there are execution failures
      AWAIT.untilAsserted(() -> Assert.assertEquals(200, timestamps.size()));
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      isClosed.set(true);
      for (final Thread thread : threads) {
        thread.join();
      }
    }
  }

  @Test
  public void testSubscriptionAfterRestartDataNode() throws Exception {
    // Fetch ip and port from DN 0
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    // Create topic
    final String topicName = "topic2";
    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      session.createTopic(topicName);
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Subscription
    final SubscriptionPullConsumer consumer;
    try {
      consumer =
          new SubscriptionPullConsumer.Builder()
              .host(host)
              .port(port)
              .consumerId("c1")
              .consumerGroupId("cg1")
              .autoCommit(true)
              .heartbeatIntervalMs(1000) // narrow heartbeat interval
              .endpointsSyncIntervalMs(5000) // narrow endpoints sync interval
              .buildPullConsumer();
      consumer.open();
      consumer.subscribe(topicName);
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
      return;
    }

    // Insert some historical data
    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
      }
      session.executeNonQueryStatement("flush");
    } catch (final Throwable e) {
      e.printStackTrace();
      // Avoid failure
      return;
    }

    // Shutdown DN 1 & DN 2
    try {
      Thread.sleep(10000); // wait some time
      EnvFactory.getEnv().shutdownDataNode(1);
      EnvFactory.getEnv().shutdownDataNode(2);
    } catch (final Throwable e) {
      e.printStackTrace();
      // Avoid failure
      return;
    }

    // Subscription again
    final Map<Long, Long> timestamps = new HashMap<>();
    final AtomicBoolean isClosed = new AtomicBoolean(false);
    final Thread thread =
        new Thread(
            () -> {
              try (final SubscriptionPullConsumer consumerRef = consumer) {
                while (!isClosed.get()) {
                  LockSupport.parkNanos(IoTDBSubscriptionITConstant.SLEEP_NS); // wait some time
                  final List<SubscriptionMessage> messages;
                  try {
                    messages = consumer.poll(IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS);
                  } catch (final Exception e) {
                    e.printStackTrace();
                    // Avoid failure
                    continue;
                  }
                  for (final SubscriptionMessage message : messages) {
                    for (final SubscriptionSessionDataSet dataSet :
                        message.getSessionDataSetsHandler()) {
                      while (dataSet.hasNext()) {
                        final long timestamp = dataSet.next().getTimestamp();
                        timestamps.put(timestamp, timestamp);
                      }
                    }
                  }
                  // Auto commit
                }
                consumerRef.unsubscribe(topicName);
              } catch (final Exception e) {
                e.printStackTrace();
                // Avoid failure
              } finally {
                LOGGER.info("consumer exiting...");
              }
            },
            String.format("%s - %s", testName.getDisplayName(), consumer));
    thread.start();

    // Start DN 1 & DN 2
    try {
      Thread.sleep(10000); // wait some time
      EnvFactory.getEnv().startDataNode(1);
      EnvFactory.getEnv().startDataNode(2);
      ((AbstractEnv) EnvFactory.getEnv()).checkClusterStatusWithoutUnknown();
    } catch (final Throwable e) {
      e.printStackTrace();
      // Avoid failure
      return;
    }

    // Insert some realtime data
    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (int i = 100; i < 200; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
      }
      session.executeNonQueryStatement("flush");
    } catch (final Throwable e) {
      e.printStackTrace();
      // Avoid failure
      return;
    }

    // Check timestamps size
    try {
      // Keep retrying if there are execution failures
      AWAIT.untilAsserted(() -> Assert.assertEquals(200, timestamps.size()));
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      isClosed.set(true);
      thread.join();
    }
  }

  @Test
  public void testSubscriptionWhenConfigNodeLeaderChange() throws Exception {
    // Fetch ip and port from DN 0
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    // Create topic
    final String topicName = "topic3";
    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      session.createTopic(topicName);
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Subscription
    final SubscriptionPullConsumer consumer;
    try {
      consumer =
          new SubscriptionPullConsumer.Builder()
              .host(host)
              .port(port)
              .consumerId("c1")
              .consumerGroupId("cg1")
              .autoCommit(true)
              .heartbeatIntervalMs(1000) // narrow heartbeat interval
              .endpointsSyncIntervalMs(5000) // narrow endpoints sync interval
              .buildPullConsumer();
      consumer.open();
      consumer.subscribe(topicName);
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
      return;
    }

    // Insert some historical data
    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
      }
      session.executeNonQueryStatement("flush");
    } catch (final Throwable e) {
      e.printStackTrace();
      // Avoid failure
      return;
    }

    // Subscription again
    final Map<Long, Long> timestamps = new HashMap<>();
    final AtomicBoolean isClosed = new AtomicBoolean(false);
    final Thread thread =
        new Thread(
            () -> {
              try (final SubscriptionPullConsumer consumerRef = consumer) {
                while (!isClosed.get()) {
                  LockSupport.parkNanos(IoTDBSubscriptionITConstant.SLEEP_NS); // wait some time
                  final List<SubscriptionMessage> messages;
                  try {
                    messages = consumerRef.poll(IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS);
                  } catch (final Exception e) {
                    e.printStackTrace();
                    // Avoid failure
                    continue;
                  }
                  for (final SubscriptionMessage message : messages) {
                    for (final SubscriptionSessionDataSet dataSet :
                        message.getSessionDataSetsHandler()) {
                      while (dataSet.hasNext()) {
                        final long timestamp = dataSet.next().getTimestamp();
                        timestamps.put(timestamp, timestamp);
                      }
                    }
                  }
                  // Auto commit
                }
                consumerRef.unsubscribe(topicName);
              } catch (final Exception e) {
                e.printStackTrace();
                // Avoid failure
              } finally {
                LOGGER.info("consumer exiting...");
              }
            },
            String.format("%s - %s", testName.getDisplayName(), consumer));
    thread.start();

    // Shutdown leader CN
    try {
      EnvFactory.getEnv().shutdownConfigNode(EnvFactory.getEnv().getLeaderConfigNodeIndex());
    } catch (final Throwable e) {
      e.printStackTrace();
      // Avoid failure
      return;
    }

    // Insert some realtime data
    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (int i = 100; i < 200; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
      }
      session.executeNonQueryStatement("flush");
    } catch (final Throwable e) {
      e.printStackTrace();
      // Avoid failure
      return;
    }

    // Show topics and subscriptions
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      final TShowTopicResp showTopicResp = client.showTopic(new TShowTopicReq());
      Assert.assertEquals(RpcUtils.SUCCESS_STATUS.getCode(), showTopicResp.status.getCode());
      Assert.assertNotNull(showTopicResp.topicInfoList);
      Assert.assertEquals(1, showTopicResp.topicInfoList.size());

      final TShowSubscriptionResp showSubscriptionResp =
          client.showSubscription(new TShowSubscriptionReq());
      Assert.assertEquals(RpcUtils.SUCCESS_STATUS.getCode(), showSubscriptionResp.status.getCode());
      Assert.assertNotNull(showSubscriptionResp.subscriptionInfoList);
      Assert.assertEquals(1, showSubscriptionResp.subscriptionInfoList.size());
    }

    // Check timestamps size
    try {
      // Keep retrying if there are execution failures
      AWAIT.untilAsserted(() -> Assert.assertEquals(200, timestamps.size()));
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      isClosed.set(true);
      thread.join();
    }
  }
}
