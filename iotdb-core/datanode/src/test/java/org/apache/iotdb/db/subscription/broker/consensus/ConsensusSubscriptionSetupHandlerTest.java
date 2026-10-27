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

package org.apache.iotdb.db.subscription.broker.consensus;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;
import org.apache.iotdb.consensus.iot.IoTConsensus;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.SubscriptionWalRetentionPolicy;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.subscription.agent.SubscriptionBrokerAgent;
import org.apache.iotdb.db.subscription.broker.ConsensusSubscriptionBroker;
import org.apache.iotdb.db.subscription.broker.ISubscriptionBroker;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsensusSubscriptionSetupHandlerTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testRuntimeUpdatesAreIgnoredWhenSubscriptionIsDisabled() {
    ConsensusSubscriptionSetupHandler.applyRuntimeState(null, null);
    ConsensusSubscriptionSetupHandler.onRegionRouteChanged(null, 0);
  }

  @Test
  public void testSingleTopicSetupFailurePropagates() {
    SubscriptionException failure = null;
    try {
      ConsensusSubscriptionSetupHandler.setupConsensusTopics(
          "consumerGroup",
          Collections.singleton("topic"),
          topicName -> true,
          topicName -> {
            throw new IllegalStateException("setup failed");
          },
          ignored -> {});
    } catch (final SubscriptionException e) {
      failure = e;
    }

    assertNotNull(failure);
    assertTrue(failure.getMessage().contains("topic"));
    assertTrue(failure.getMessage().contains("consumerGroup"));
    assertTrue(failure.getCause() instanceof IllegalStateException);
  }

  @Test
  public void testTopicModeLookupFailurePropagates() {
    SubscriptionException failure = null;
    try {
      ConsensusSubscriptionSetupHandler.setupConsensusTopics(
          "consumerGroup",
          Collections.singleton("topic"),
          topicName -> {
            throw new IllegalStateException("topic metadata missing");
          },
          topicName -> {},
          ignored -> {});
    } catch (final SubscriptionException e) {
      failure = e;
    }

    assertNotNull(failure);
    assertTrue(failure.getCause() instanceof IllegalStateException);
  }

  @Test
  public void testMultiTopicSetupFailureRollsBackAllAttemptedTopics() {
    final Set<String> attemptedTopicNames = new LinkedHashSet<>();
    final Set<String> rolledBackTopicNames = new LinkedHashSet<>();
    final Set<String> topicNames = new LinkedHashSet<>(Arrays.asList("first", "second", "third"));
    SubscriptionException failure = null;

    try {
      ConsensusSubscriptionSetupHandler.setupConsensusTopics(
          "consumerGroup",
          topicNames,
          topicName -> true,
          topicName -> failOnSecondTopic(topicName, attemptedTopicNames),
          attemptedTopics -> rolledBackTopicNames.addAll(attemptedTopics));
    } catch (final SubscriptionException e) {
      failure = e;
    }

    assertNotNull(failure);
    assertEquals(new LinkedHashSet<>(Arrays.asList("first", "second")), attemptedTopicNames);
    assertEquals(attemptedTopicNames, rolledBackTopicNames);
  }

  @Test
  public void testRepairFailurePreservesExistingQueuesAndConcurrentCommitProgress()
      throws Exception {
    final IConsensus originalConsensus = DataRegionConsensusImpl.getInstance();
    final String consumerGroupId = "group-" + UUID.randomUUID();
    final String topicName = "topic-" + UUID.randomUUID();
    final DataRegionId existingRegion = new DataRegionId(51);
    final DataRegionId addedRegion = new DataRegionId(52);
    final WriterId writerId = new WriterId(existingRegion.toString(), 7);
    final ConsensusSubscriptionCommitManager commitManager =
        ConsensusSubscriptionCommitManager.getInstance();
    final ConsensusSubscriptionBroker broker = new ConsensusSubscriptionBroker(consumerGroupId);
    final ConsensusPrefetchingQueue existingQueue = mock(ConsensusPrefetchingQueue.class);
    final ConsensusPrefetchingQueue addedQueue = mock(ConsensusPrefetchingQueue.class);
    when(existingQueue.getConsensusGroupId()).thenReturn(existingRegion);
    when(addedQueue.getConsensusGroupId()).thenReturn(addedRegion);
    final List<ConsensusPrefetchingQueue> queues =
        new CopyOnWriteArrayList<>(Collections.singletonList(existingQueue));
    getConsensusQueues(broker).put(topicName, queues);
    getBrokers()
        .put(consumerGroupId, new CopyOnWriteArrayList<>(Collections.singletonList(broker)));
    try {
      assertNull(
          SubscriptionAgent.topic()
              .handleSingleTopicMetaChanges(
                  new TopicMeta(
                      topicName,
                      1L,
                      Collections.singletonMap(
                          TopicConstant.MODE_KEY, TopicConstant.MODE_INCREMENTAL_VALUE))));
      commitManager.receiveProgressBroadcast(
          consumerGroupId,
          topicName,
          existingRegion.toString(),
          writerId,
          new WriterProgress(100L, 1L));
      final ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState existingState =
          commitManager.getOrCreateState(consumerGroupId, topicName, existingRegion);

      final IoTConsensus consensus = mock(IoTConsensus.class);
      final AtomicInteger discoveryCount = new AtomicInteger();
      when(consensus.getAllConsensusGroupIds())
          .thenAnswer(
              ignored -> {
                if (discoveryCount.incrementAndGet() == 2) {
                  // These changes happen after setup captured its rollback snapshot.
                  queues.add(addedQueue);
                  commitManager.receiveProgressBroadcast(
                      consumerGroupId,
                      topicName,
                      existingRegion.toString(),
                      writerId,
                      new WriterProgress(200L, 2L));
                  commitManager.receiveProgressBroadcast(
                      consumerGroupId,
                      topicName,
                      addedRegion.toString(),
                      new WriterId(addedRegion.toString(), 7),
                      new WriterProgress(100L, 1L));
                }
                return Collections.emptyList();
              });
      DataRegionConsensusImpl.setInstance(consensus);

      try {
        ConsensusSubscriptionSetupHandler.setupConsensusSubscriptions(
            consumerGroupId,
            new LinkedHashSet<>(Arrays.asList(topicName, "missing-" + UUID.randomUUID())),
            false);
        fail();
      } catch (final SubscriptionException expected) {
        assertTrue(expected.getMessage().contains("missing-"));
      }

      assertEquals(
          Collections.singleton(existingRegion.toString()), broker.getQueueRegionIds(topicName));
      assertEquals(Collections.singletonList(existingQueue), queues);
      verify(existingQueue, never()).closeAsync(false, false);
      verify(addedQueue).closeAsync(false, false);
      assertSame(
          existingState,
          commitManager.getOrCreateState(consumerGroupId, topicName, existingRegion));
      assertEquals(
          new WriterProgress(200L, 2L),
          commitManager
              .getCommittedRegionProgress(consumerGroupId, topicName, existingRegion)
              .getWriterPositions()
              .get(writerId));
      assertFalse(commitManager.hasPersistedState(consumerGroupId, topicName, addedRegion));
    } finally {
      getBrokers().remove(consumerGroupId);
      commitManager.removeAllStatesForConsumerGroup(consumerGroupId);
      SubscriptionAgent.topic().handleDropTopic(topicName, false);
      DataRegionConsensusImpl.setInstance(originalConsensus);
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, List<ConsensusPrefetchingQueue>> getConsensusQueues(
      final ConsensusSubscriptionBroker broker) throws Exception {
    final Field field =
        ConsensusSubscriptionBroker.class.getDeclaredField("topicNameToConsensusPrefetchingQueues");
    field.setAccessible(true);
    return (Map<String, List<ConsensusPrefetchingQueue>>) field.get(broker);
  }

  @Test
  public void testRepairPreservesInitializationOfExistingNewRegionQueue() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final String consumerGroupId = "group-" + UUID.randomUUID();
    final String topicName = "topic-" + UUID.randomUUID();
    final DataRegionId regionId = new DataRegionId(53);
    final DataRegion region = mock(DataRegion.class);
    when(region.getDatabaseName()).thenReturn("root.repair");
    final Field regionField = StorageEngine.class.getDeclaredField("dataRegionMap");
    regionField.setAccessible(true);
    @SuppressWarnings("unchecked")
    final Map<DataRegionId, DataRegion> regions =
        (Map<DataRegionId, DataRegion>) regionField.get(StorageEngine.getInstance());
    final DataRegion previousRegion = regions.put(regionId, region);
    try {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setSystemDir(temporaryFolder.newFolder("existingNewRegion").getAbsolutePath());
      final ConsensusSubscriptionCommitManager manager =
          new ConsensusSubscriptionCommitManager(
              (group, topic, id) ->
                  ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.absent());
      manager.initializeStateWithoutTailProposal(consumerGroupId, topicName, regionId);
      assertFalse(manager.isInitialProgressPending(consumerGroupId, topicName, regionId));
      assertTrue(manager.isConfigNodeProgressAvailable(consumerGroupId, topicName, regionId));
      assertNull(
          SubscriptionAgent.topic()
              .handleSingleTopicMetaChanges(
                  new TopicMeta(
                      topicName,
                      1L,
                      Collections.singletonMap(
                          TopicConstant.MODE_KEY, TopicConstant.MODE_INCREMENTAL_VALUE))));

      final ConsensusPrefetchingQueue queue = mock(ConsensusPrefetchingQueue.class);
      when(queue.getConsensusGroupId()).thenReturn(regionId);
      when(queue.computeTailRegionProgress())
          .thenReturn(new RegionProgress(Collections.emptyMap()));
      final ConsensusSubscriptionBroker broker = new ConsensusSubscriptionBroker(consumerGroupId);
      getConsensusQueues(broker)
          .put(topicName, new CopyOnWriteArrayList<>(Collections.singletonList(queue)));
      getBrokers()
          .put(consumerGroupId, new CopyOnWriteArrayList<>(Collections.singletonList(broker)));
      final IoTConsensus consensus = mock(IoTConsensus.class);
      final IoTConsensusServerImpl server = mock(IoTConsensusServerImpl.class);
      when(server.getConfig()).thenReturn(IoTConsensusConfig.newBuilder().build());
      when(consensus.getImpl(regionId)).thenReturn(server);
      when(consensus.getAllConsensusGroupIds()).thenReturn(Collections.singletonList(regionId));

      ConsensusSubscriptionSetupHandler.setupConsensusQueueForTopic(
          consumerGroupId, topicName, false, consensus, manager);

      assertFalse(manager.isInitialProgressPending(consumerGroupId, topicName, regionId));
      assertTrue(manager.isConfigNodeProgressAvailable(consumerGroupId, topicName, regionId));
      verify(queue, never()).computeTailRegionProgress();
    } finally {
      if (previousRegion == null) {
        regions.remove(regionId);
      } else {
        regions.put(regionId, previousRegion);
      }
      getBrokers().remove(consumerGroupId);
      SubscriptionAgent.topic().handleDropTopic(topicName, false);
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testRepairSkipsRegionWhileNewPeerCallbackIsPending() throws Exception {
    final String consumerGroupId = "group-" + UUID.randomUUID();
    final String topicName = "topic-" + UUID.randomUUID();
    final DataRegionId regionId = new DataRegionId(54);
    final IConsensus originalConsensus = DataRegionConsensusImpl.getInstance();
    final IoTConsensus consensus = mock(IoTConsensus.class);
    when(consensus.getAllConsensusGroupIds()).thenReturn(Collections.emptyList());
    DataRegionConsensusImpl.setInstance(consensus);
    try {
      assertNull(
          SubscriptionAgent.topic()
              .handleSingleTopicMetaChanges(
                  new TopicMeta(
                      topicName,
                      1L,
                      Collections.singletonMap(
                          TopicConstant.MODE_KEY, TopicConstant.MODE_INCREMENTAL_VALUE))));
      createSubscribedConsumerGroup(consumerGroupId, topicName);
      when(consensus.getAllConsensusGroupIds()).thenReturn(Collections.singletonList(regionId));
      when(consensus.isPeerCreationCallbackPending(regionId)).thenReturn(true);

      ConsensusSubscriptionSetupHandler.setupConsensusQueueForTopic(
          consumerGroupId,
          topicName,
          false,
          consensus,
          ConsensusSubscriptionCommitManager.getInstance());

      verify(consensus, never()).getImpl(regionId);
      assertTrue(
          getBrokers().getOrDefault(consumerGroupId, Collections.emptyList()).stream()
              .noneMatch(ConsensusSubscriptionBroker.class::isInstance));
    } finally {
      SubscriptionAgent.consumer().handleDropConsumerGroup(consumerGroupId);
      SubscriptionAgent.topic().handleDropTopic(topicName, false);
      DataRegionConsensusImpl.setInstance(originalConsensus);
    }
  }

  @Test
  public void testNewSubscriptionDoesNotWaitForAnUnrelatedPeerCallback() throws Exception {
    final String consumerGroupId = "group-" + UUID.randomUUID();
    final String topicName = "topic-" + UUID.randomUUID();
    final DataRegionId regionId = new DataRegionId(55);
    final IoTConsensus consensus = mock(IoTConsensus.class);
    when(consensus.getAllConsensusGroupIds()).thenReturn(Collections.singletonList(regionId));
    when(consensus.isPeerCreationCallbackPending(regionId)).thenReturn(true);
    try {
      assertNull(
          SubscriptionAgent.topic()
              .handleSingleTopicMetaChanges(
                  new TopicMeta(
                      topicName,
                      1L,
                      Collections.singletonMap(
                          TopicConstant.MODE_KEY, TopicConstant.MODE_INCREMENTAL_VALUE))));

      ConsensusSubscriptionSetupHandler.setupConsensusQueueForTopic(
          consumerGroupId,
          topicName,
          false,
          consensus,
          ConsensusSubscriptionCommitManager.getInstance());

      verify(consensus).getImpl(regionId);
    } finally {
      SubscriptionAgent.topic().handleDropTopic(topicName, false);
    }
  }

  @Test
  public void testNewRegionCallbackWaitsForRepairAndRereadsSubscriptions() throws Exception {
    final String consumerGroupId = "group-" + UUID.randomUUID();
    final String topicName = "topic-" + UUID.randomUUID();
    final DataRegionId regionId = new DataRegionId(56);
    final IConsensus originalConsensus = DataRegionConsensusImpl.getInstance();
    final IoTConsensus consensus = mock(IoTConsensus.class);
    when(consensus.getAllConsensusGroupIds()).thenReturn(Collections.emptyList());
    DataRegionConsensusImpl.setInstance(consensus);
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final CountDownLatch callbackStarted = new CountDownLatch(1);
    final CountDownLatch callbackFinished = new CountDownLatch(1);
    final AtomicReference<Future<?>> callback = new AtomicReference<>();
    final IoTConsensusServerImpl server = mock(IoTConsensusServerImpl.class);
    when(server.getSearchIndex())
        .thenAnswer(
            ignored -> {
              callbackStarted.countDown();
              return 1L;
            });
    try {
      assertNull(
          SubscriptionAgent.topic()
              .handleSingleTopicMetaChanges(
                  new TopicMeta(
                      topicName,
                      1L,
                      Collections.singletonMap(
                          TopicConstant.MODE_KEY, TopicConstant.MODE_INCREMENTAL_VALUE))));
      final ConsumerGroupMeta meta = createSubscribedConsumerGroup(consumerGroupId, topicName);

      SubscriptionAgent.consumer()
          .executeWithConsumerGroupLock(
              consumerGroupId,
              () -> {
                callback.set(
                    executor.submit(
                        () -> {
                          try {
                            ConsensusSubscriptionSetupHandler.onNewRegionCreated(regionId, server);
                          } finally {
                            callbackFinished.countDown();
                          }
                        }));
                try {
                  assertTrue(callbackStarted.await(5, TimeUnit.SECONDS));
                  assertFalse(callbackFinished.await(200, TimeUnit.MILLISECONDS));
                } catch (final InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new AssertionError(e);
                }
                final ConsumerGroupMeta unsubscribedMeta = meta.deepCopy();
                unsubscribedMeta.removeSubscription(null, Collections.singleton(topicName));
                assertNull(
                    SubscriptionAgent.consumer()
                        .handleSingleConsumerGroupMetaChanges(unsubscribedMeta));
              });

      callback.get().get(5, TimeUnit.SECONDS);
      assertTrue(
          SubscriptionAgent.consumer()
              .getTopicNamesSubscribedByConsumerGroup(consumerGroupId)
              .isEmpty());
      assertTrue(
          getBrokers().getOrDefault(consumerGroupId, Collections.emptyList()).stream()
              .noneMatch(ConsensusSubscriptionBroker.class::isInstance));
    } finally {
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
      SubscriptionAgent.consumer().handleDropConsumerGroup(consumerGroupId);
      SubscriptionAgent.topic().handleDropTopic(topicName, false);
      DataRegionConsensusImpl.setInstance(originalConsensus);
    }
  }

  private static ConsumerGroupMeta createSubscribedConsumerGroup(
      final String consumerGroupId, final String topicName) {
    final String consumerId = "consumer-" + UUID.randomUUID();
    final ConsumerGroupMeta meta =
        new ConsumerGroupMeta(
            consumerGroupId, 1L, new ConsumerMeta(consumerId, 1L, Collections.emptyMap()));
    meta.addSubscription(consumerId, Collections.singleton(topicName));
    assertNull(SubscriptionAgent.consumer().handleSingleConsumerGroupMetaChanges(meta));
    return meta;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, List<ISubscriptionBroker>> getBrokers() throws Exception {
    final Field field = SubscriptionBrokerAgent.class.getDeclaredField("consumerGroupIdToBrokers");
    field.setAccessible(true);
    return (Map<String, List<ISubscriptionBroker>>) field.get(SubscriptionAgent.broker());
  }

  @Test
  public void testFallbackLookupDoesNotQueryConfigNodeWithoutLocalPersistence() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("noLocalPersistence");
    try {
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(systemDir.getAbsolutePath());
      final AtomicInteger queryCount = new AtomicInteger();
      final ConsensusSubscriptionCommitManager commitManager =
          new ConsensusSubscriptionCommitManager(
              (consumerGroupId, topicName, regionId) -> {
                queryCount.incrementAndGet();
                return ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.absent();
              });

      assertNull(
          ConsensusSubscriptionSetupHandler.resolveFallbackCommittedRegionProgress(
              commitManager, "consumerGroup", "topic", new DataRegionId(1)));
      assertEquals(0, queryCount.get());
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testStartupDetachedRetentionProtectsPersistedProgress() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("startupDetachedRetention");
    try {
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(systemDir.getAbsolutePath());
      final ConsensusSubscriptionCommitManager commitManager =
          new ConsensusSubscriptionCommitManager(
              (consumerGroupId, topicName, regionId) ->
                  ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.absent());
      final DataRegionId regionId = new DataRegionId(41);
      commitManager.receiveProgressBroadcast(
          "consumerGroup",
          "topic",
          regionId.toString(),
          new WriterId(regionId.toString(), 7),
          new WriterProgress(100L, 1L));
      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);

      ConsensusSubscriptionSetupHandler.registerStartupDetachedRetentions(
          regionId, serverImpl, commitManager);

      verify(serverImpl)
          .registerDetachedSubscriptionRetention(
              eq(
                  ConsensusSubscriptionWalRetention.generateRetentionId(
                      "consumerGroup", "topic", regionId)),
              argThat(
                  policy ->
                      policy.getRetentionBytes() == SubscriptionWalRetentionPolicy.UNBOUNDED
                          && policy.getRetentionMs() == SubscriptionWalRetentionPolicy.UNBOUNDED),
              any(LongSupplier.class));
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testAuditDatabaseNeverMatchesTopic() {
    final Map<String, String> tableTopicAttributes = new HashMap<>();
    tableTopicAttributes.put(
        SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);
    tableTopicAttributes.put(TopicConstant.DATABASE_KEY, ".*");
    final TopicConfig tableTopicConfig = new TopicConfig(tableTopicAttributes);

    assertFalse(
        ConsensusSubscriptionSetupHandler.matchesTopicDatabase(tableTopicConfig, "__audit"));
    assertFalse(
        ConsensusSubscriptionSetupHandler.matchesTopicDatabase(tableTopicConfig, "root.__audit"));
    assertTrue(ConsensusSubscriptionSetupHandler.matchesTopicDatabase(tableTopicConfig, "user_db"));

    final TopicConfig treeTopicConfig = new TopicConfig(Collections.emptyMap());
    assertFalse(ConsensusSubscriptionSetupHandler.matchesTopicDatabase(treeTopicConfig, "__audit"));
    assertTrue(ConsensusSubscriptionSetupHandler.matchesTopicDatabase(treeTopicConfig, "user_db"));
  }

  @Test
  public void testTopicDataRegionModelIsolation() {
    final TopicConfig treeTopicConfig = new TopicConfig(Collections.emptyMap());
    final Map<String, String> tableTopicAttributes = new HashMap<>();
    tableTopicAttributes.put(
        SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);
    tableTopicAttributes.put(TopicConstant.DATABASE_KEY, "table_db");
    final TopicConfig tableTopicConfig = new TopicConfig(tableTopicAttributes);

    assertTrue(
        ConsensusSubscriptionSetupHandler.matchesTopicDataRegion(
            "root.tree_db", treeTopicConfig, false));
    assertFalse(
        ConsensusSubscriptionSetupHandler.matchesTopicDataRegion(
            "table_db", treeTopicConfig, false));
    assertFalse(
        ConsensusSubscriptionSetupHandler.matchesTopicDataRegion(
            "table_db", treeTopicConfig, true));
    assertFalse(
        ConsensusSubscriptionSetupHandler.matchesTopicDataRegion(
            "root.table_db", tableTopicConfig, true));
    assertTrue(
        ConsensusSubscriptionSetupHandler.matchesTopicDataRegion(
            "table_db", tableTopicConfig, true));
    assertFalse(
        ConsensusSubscriptionSetupHandler.matchesTopicDataRegion(
            "other_table_db", tableTopicConfig, true));
    assertFalse(
        ConsensusSubscriptionSetupHandler.matchesTopicDataRegion(
            "table_db", tableTopicConfig, false));
    assertFalse(
        ConsensusSubscriptionSetupHandler.matchesTopicDataRegion(
            "root.table_db", tableTopicConfig, false));
  }

  private static void failOnSecondTopic(
      final String topicName, final Set<String> attemptedTopicNames) {
    attemptedTopicNames.add(topicName);
    if ("second".equals(topicName)) {
      throw new IllegalStateException("setup failed");
    }
  }
}
