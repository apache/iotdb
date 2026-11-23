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

package org.apache.iotdb.subscription.it.consensus.local;

import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.config.MppBaseConfig;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.session.subscription.SubscriptionTreeSession;
import org.apache.iotdb.session.subscription.consumer.tree.SubscriptionTreePullConsumer;
import org.apache.iotdb.session.subscription.model.Subscription;
import org.apache.iotdb.subscription.it.AbstractSubscriptionIT;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.time.Duration;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBConsensusSubscriptionFallbackPollingIT extends AbstractSubscriptionIT {

  private static final int ROWS_PER_REGION = 6;
  private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

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
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultDataRegionGroupNumPerDatabase(1)
        .setSchemaReplicationFactor(1)
        .setDataReplicationFactor(3)
        .setAutoCreateSchemaEnabled(true)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);

    final Properties subscriptionProperties = new Properties();
    subscriptionProperties.setProperty("subscription_enabled", "true");
    subscriptionProperties.setProperty("subscription_consensus_fallback_polling_enabled", "false");
    ((MppBaseConfig) EnvFactory.getEnv().getConfig().getDataNodeCommonConfig())
        .updateProperties(subscriptionProperties);
    ((MppBaseConfig) EnvFactory.getEnv().getConfig().getConfigNodeCommonConfig())
        .updateProperties(subscriptionProperties);

    EnvFactory.getEnv().initClusterEnvironment(1, 3);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    try {
      EnvFactory.getEnv().cleanClusterEnvironment();
    } finally {
      super.tearDown();
    }
  }

  @Test
  public void testConsumerWithoutAssignedRegionDoesNotBorrowFallbackEvents() throws Exception {
    final ConsensusSubscriptionITSupport.TestIdentifiers ids =
        ConsensusSubscriptionITSupport.newIdentifiers("fallback_polling_disabled");
    SubscriptionTreePullConsumer consumerA = null;
    SubscriptionTreePullConsumer consumerB = null;

    try {
      ConsensusSubscriptionITSupport.bootstrapDatabase(ids.getDatabase());
      ConsensusSubscriptionITSupport.createConsensusTopic(
          ids.getTopic(), ids.getDatabase() + ".**");
      consumerA =
          ConsensusSubscriptionITSupport.createConsumer(
              ids.consumer("a"), ids.getConsumerGroupId());
      consumerA.subscribe(ids.getTopic());
      // Receiving a seed row proves that A owns the region before B starts polling.
      final Set<String> seedRowKeys =
          ConsensusSubscriptionITSupport.insertRows(ids.getDatabase(), "d0", 1L, 1, true);
      assertEventuallyConsumed(consumerA, seedRowKeys);
      consumerB =
          ConsensusSubscriptionITSupport.createConsumer(
              ids.consumer("b"), ids.getConsumerGroupId());
      consumerB.subscribe(ids.getTopic());
      awaitBothConsumersRegistered(ids);
      // Keep A's existing ownership while making both consumers active on the broker.
      consumerA.poll(POLL_TIMEOUT);
      consumerB.poll(POLL_TIMEOUT);

      final Set<String> expectedRowKeys =
          ConsensusSubscriptionITSupport.insertRows(
              ids.getDatabase(), "d0", 100L, ROWS_PER_REGION, true);

      // Leave the owner idle while B polls so that enabled fallback would borrow the ready rows.
      final SubscriptionTreePullConsumer borrower = consumerB;
      Awaitility.await()
          .pollInSameThread()
          .pollInterval(100, TimeUnit.MILLISECONDS)
          .during(5, TimeUnit.SECONDS)
          .atMost(10, TimeUnit.SECONDS)
          .until(
              () -> {
                Assert.assertTrue(
                    "A consumer without an assigned region borrowed subscription messages",
                    borrower.poll(POLL_TIMEOUT).isEmpty());
                return true;
              });
      assertEventuallyConsumed(consumerA, expectedRowKeys);
    } finally {
      closeConsumer(consumerB, ids);
      closeConsumer(consumerA, ids);
      ConsensusSubscriptionITSupport.cleanup(null, ids.getTopic(), ids.getDatabase());
    }
  }

  private static void assertEventuallyConsumed(
      final SubscriptionTreePullConsumer consumer, final Set<String> expectedRowKeys) {
    final ConsensusSubscriptionITSupport.ConsumedRecords consumed =
        new ConsensusSubscriptionITSupport.ConsumedRecords();
    Awaitility.await()
        .pollInSameThread()
        .pollInterval(100, TimeUnit.MILLISECONDS)
        .atMost(30, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              final ConsensusSubscriptionITSupport.PolledMessageBatch batch =
                  ConsensusSubscriptionITSupport.pollFirstNonEmptyBatchWithoutCommit(
                      consumer, 1, POLL_TIMEOUT);
              consumed.merge(batch.getConsumedRecords());
              consumer.commitSync(batch.getMessages());
              ConsensusSubscriptionITSupport.assertExactRowKeys(expectedRowKeys, consumed);
            });
  }

  private static void awaitBothConsumersRegistered(
      final ConsensusSubscriptionITSupport.TestIdentifiers ids) throws Exception {
    try (final SubscriptionTreeSession session =
        new SubscriptionTreeSession(
            EnvFactory.getEnv().getIP(), Integer.parseInt(EnvFactory.getEnv().getPort()))) {
      session.open();
      Awaitility.await()
          .pollInSameThread()
          .atMost(30, TimeUnit.SECONDS)
          .pollInterval(100, TimeUnit.MILLISECONDS)
          .untilAsserted(
              () -> {
                final Set<Subscription> subscriptions = session.getSubscriptions(ids.getTopic());
                Assert.assertEquals(subscriptions.toString(), 1, subscriptions.size());
                final Subscription subscription = subscriptions.iterator().next();
                Assert.assertEquals(ids.getConsumerGroupId(), subscription.getConsumerGroupId());
                final String consumers = subscription.getConsumerIds();
                Assert.assertTrue(consumers, consumers.contains(ids.consumer("a")));
                Assert.assertTrue(consumers, consumers.contains(ids.consumer("b")));
              });
    }
  }

  private static void closeConsumer(
      final SubscriptionTreePullConsumer consumer,
      final ConsensusSubscriptionITSupport.TestIdentifiers ids) {
    if (consumer == null) {
      return;
    }
    try {
      consumer.unsubscribe(ids.getTopic());
    } catch (final Exception ignored) {
      // Best effort cleanup after a failed assertion.
    }
    try {
      consumer.close();
    } catch (final Exception ignored) {
      // Best effort cleanup after a failed assertion.
    }
  }
}
