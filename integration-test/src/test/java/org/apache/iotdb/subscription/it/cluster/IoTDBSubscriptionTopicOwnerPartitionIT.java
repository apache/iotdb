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

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionOwnerFencedException;
import org.apache.iotdb.session.subscription.SubscriptionTreeSession;
import org.apache.iotdb.session.subscription.consumer.tree.SubscriptionTreePullConsumer;
import org.apache.iotdb.session.subscription.model.Topic;
import org.apache.iotdb.subscription.it.AbstractSubscriptionIT;
import org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

/**
 * Owner-fencing correctness under owner/DataNode/ConfigNode partition topologies, simulated at the
 * node-process level via the IT framework (shutdown/start of ConfigNodes and DataNodes; ConfigNode
 * failover via leader shutdown). Runs on a 3 ConfigNode + 3 DataNode cluster.
 *
 * <p>Covers:
 *
 * <ul>
 *   <li>multi-DataNode: a stale owner is fenced for a topic whose data spans several DataNodes;
 *   <li>ConfigNode failover: the owner epoch is persisted via consensus, so the new leader keeps
 *       fencing the stale owner after the previous leader is shut down;
 *   <li>DataNode restart: after a DataNode is shut down and restarted, the owner metadata is
 *       re-synced and the stale owner stays fenced.
 * </ul>
 */
@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBSubscriptionTopicOwnerPartitionIT extends AbstractSubscriptionIT {

  private static final String HEAD_PATH = "root.topic_owner_partition";

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSubscriptionEnabled(true)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        // Lower the owner-lease floor so the test can use a short lease and stay fast.
        .setSubscriptionOwnerLeaseDurationMsMin(1000);
    // 3 ConfigNodes (for failover) + 3 DataNodes (for multi-DataNode data placement).
    EnvFactory.getEnv().initClusterEnvironment(3, 3);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    super.tearDown();
  }

  @Test
  public void testStaleOwnerFencedOnMultiDataNodeTopic() throws Exception {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());
    final String topicName = "topic_owner_multi_dn";

    createOwnedTopic(host, port, topicName, "owner2", 6L);
    try {
      // Spread data across multiple devices (and thus, with multiple data regions, multiple
      // DataNodes) before consuming.
      insertDataAcrossDevices();

      // A stale owner (epoch 5 < current 6) must be fenced regardless of which DataNode serves the
      // request.
      assertSubscribeFenced(host, port, topicName, "stale_multi_dn", "owner1", 5L);

      // The current owner can subscribe.
      try (final SubscriptionTreePullConsumer currentOwnerConsumer =
          ownerConsumer(
              host, port, "current_multi_dn", "topic_owner_multi_dn_group", "owner2", 6L)) {
        currentOwnerConsumer.open();
        currentOwnerConsumer.subscribe(topicName);
        currentOwnerConsumer.unsubscribe(topicName);
      }
    } finally {
      dropTopic(host, port, topicName);
    }
  }

  @Test
  public void testConfigNodeFailoverPersistsOwnerEpochAndKeepsFencing() throws Exception {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());
    final String topicName = "topic_owner_cn_failover";
    final long ownerLeaseDurationMs = 2000L;

    try (final SubscriptionTreeSession session = new SubscriptionTreeSession(host, port)) {
      session.open();
      final Properties properties = new Properties();
      properties.put(TopicConstant.PATH_KEY, HEAD_PATH + ".**");
      properties.put(TopicConstant.START_TIME_KEY, "0");
      properties.put(TopicConstant.OWNER_ID_KEY, "owner1");
      properties.put(TopicConstant.OWNER_EPOCH_KEY, "5");
      properties.put(
          TopicConstant.OWNER_LEASE_DURATION_MS_KEY, String.valueOf(ownerLeaseDurationMs));
      session.createTopic(topicName, properties);

      // Transfer to owner2/epoch 6 (this persists owner + epoch via ConfigNode consensus).
      session.alterTopicOwner(topicName, "owner2", 6L);
      Assert.assertTrue(getTopicAttributes(session, topicName).contains("owner-epoch=6"));
    }

    // Fail over: shut down the current leader ConfigNode.
    final int leaderIndex = EnvFactory.getEnv().getLeaderConfigNodeIndex();
    EnvFactory.getEnv().shutdownConfigNode(leaderIndex);

    try {
      // After failover, the new leader must still report the persisted owner/epoch (consensus), and
      // a stale owner (epoch 5) must remain fenced.
      IoTDBSubscriptionITConstant.AWAIT.untilAsserted(
          () -> {
            try (final SubscriptionTreeSession session = new SubscriptionTreeSession(host, port)) {
              session.open();
              final String attributes = getTopicAttributes(session, topicName);
              Assert.assertTrue(attributes.contains("owner-id=owner2"));
              Assert.assertTrue(attributes.contains("owner-epoch=6"));
            }
          });
      assertSubscribeFenced(host, port, topicName, "stale_after_failover", "owner1", 5L);
    } finally {
      EnvFactory.getEnv().startConfigNode(leaderIndex);
      dropTopic(host, port, topicName);
    }
  }

  @Test
  public void testDataNodeRestartReSyncsOwnerFencing() throws Exception {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());
    final String topicName = "topic_owner_dn_restart";

    createOwnedTopic(host, port, topicName, "owner2", 6L);
    try {
      // Restart a DataNode: it loses in-memory owner metadata and must re-sync it on restart.
      EnvFactory.getEnv().shutdownDataNode(1);
      EnvFactory.getEnv().startDataNode(1);

      // After the DataNode comes back and re-syncs owner metadata, the stale owner stays fenced.
      IoTDBSubscriptionITConstant.AWAIT.untilAsserted(
          () ->
              assertSubscribeFenced(host, port, topicName, "stale_after_dn_restart", "owner1", 5L));
    } finally {
      dropTopic(host, port, topicName);
    }
  }

  private static void createOwnedTopic(
      final String host,
      final int port,
      final String topicName,
      final String ownerId,
      final long ownerEpoch)
      throws Exception {
    try (final SubscriptionTreeSession session = new SubscriptionTreeSession(host, port)) {
      session.open();
      final Properties properties = new Properties();
      properties.put(TopicConstant.PATH_KEY, HEAD_PATH + ".**");
      properties.put(TopicConstant.START_TIME_KEY, "0");
      properties.put(TopicConstant.OWNER_ID_KEY, ownerId);
      properties.put(TopicConstant.OWNER_EPOCH_KEY, String.valueOf(ownerEpoch));
      session.createTopic(topicName, properties);
    }
  }

  private static void dropTopic(final String host, final int port, final String topicName) {
    try (final SubscriptionTreeSession session = new SubscriptionTreeSession(host, port)) {
      session.open();
      session.dropTopicIfExists(topicName);
    } catch (final Exception e) {
      // best-effort cleanup
    }
  }

  private static SubscriptionTreePullConsumer ownerConsumer(
      final String host,
      final int port,
      final String consumerId,
      final String consumerGroupId,
      final String ownerId,
      final long ownerEpoch) {
    return new SubscriptionTreePullConsumer.Builder()
        .host(host)
        .port(port)
        .consumerId(consumerId)
        .consumerGroupId(consumerGroupId)
        .ownerId(ownerId)
        .ownerEpoch(ownerEpoch)
        .autoCommit(false)
        .buildPullConsumer();
  }

  private static void assertSubscribeFenced(
      final String host,
      final int port,
      final String topicName,
      final String consumerId,
      final String ownerId,
      final long ownerEpoch)
      throws Exception {
    try (final SubscriptionTreePullConsumer staleOwnerConsumer =
        ownerConsumer(host, port, consumerId, topicName + "_stale_group", ownerId, ownerEpoch)) {
      staleOwnerConsumer.open();
      Assert.assertThrows(
          SubscriptionOwnerFencedException.class, () -> staleOwnerConsumer.subscribe(topicName));
    }
  }

  private void insertDataAcrossDevices() throws Exception {
    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (int device = 0; device < 6; device++) {
        for (int i = 0; i < 10; i++) {
          session.executeNonQueryStatement(
              String.format(
                  "insert into %s.d%s(time, s1) values (%s, %s)", HEAD_PATH, device, i, i));
        }
      }
      session.executeNonQueryStatement("flush");
    }
  }

  private static String getTopicAttributes(
      final SubscriptionTreeSession session, final String topicName) throws Exception {
    for (final Topic topic : session.getTopics()) {
      if (topicName.equals(topic.getTopicName())) {
        return topic.getTopicAttributes();
      }
    }
    Assert.fail("Topic " + topicName + " should exist.");
    return "";
  }
}
