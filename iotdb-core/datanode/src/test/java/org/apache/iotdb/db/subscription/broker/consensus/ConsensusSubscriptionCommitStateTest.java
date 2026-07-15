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
import org.apache.iotdb.commons.subscription.meta.consumer.CommitProgressKeeper;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ConsensusSubscriptionCommitStateTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testCommitAdvancesContiguousWriterProgress() {
    final WriterId writerId = new WriterId("1_1", 7);
    final Map<WriterId, WriterProgress> initialCommitted = new LinkedHashMap<>();
    initialCommitted.put(writerId, new WriterProgress(100L, 0L));
    final ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState state =
        new ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState(
            "1_1", new SubscriptionConsensusProgress(new RegionProgress(initialCommitted), 0L));

    state.recordMapping(writerId, new WriterProgress(101L, 1L));
    state.recordMapping(writerId, new WriterProgress(102L, 2L));
    state.recordMapping(writerId, new WriterProgress(103L, 3L));

    assertTrue(state.commit(writerId, new WriterProgress(102L, 2L)));
    assertEquals(100L, state.getCommittedPhysicalTime());
    assertEquals(0L, state.getCommittedLocalSeq());
    assertEquals(
        new WriterProgress(100L, 0L),
        state.getCommittedRegionProgress().getWriterPositions().get(writerId));

    assertTrue(state.commit(writerId, new WriterProgress(101L, 1L)));
    assertEquals(102L, state.getCommittedPhysicalTime());
    assertEquals(2L, state.getCommittedLocalSeq());
    assertEquals(7, state.getCommittedWriterNodeId());
    assertEquals(writerId, state.getCommittedWriterId());
    assertEquals(
        new WriterProgress(102L, 2L),
        state.getCommittedRegionProgress().getWriterPositions().get(writerId));

    assertTrue(state.commit(writerId, new WriterProgress(103L, 3L)));
    assertEquals(103L, state.getCommittedPhysicalTime());
    assertEquals(3L, state.getCommittedLocalSeq());
    assertEquals(7, state.getCommittedWriterNodeId());
  }

  @Test
  public void testSerializeDeserializeWriterProgress() throws Exception {
    final ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState state =
        new ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState(
            "2_5", new SubscriptionConsensusProgress());
    final Map<WriterId, WriterProgress> seekProgress = new LinkedHashMap<>();
    final WriterId writerA = new WriterId("2_5", 4);
    final WriterId writerB = new WriterId("2_5", 5);
    seekProgress.put(writerA, new WriterProgress(222L, 11L));
    seekProgress.put(writerB, new WriterProgress(230L, 4L));
    state.resetForSeek(new RegionProgress(seekProgress));

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DataOutputStream dos = new DataOutputStream(baos)) {
      state.serialize(dos);
    }

    final ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState restored =
        ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState.deserialize(
            "2_5", ByteBuffer.wrap(baos.toByteArray()));

    assertEquals(new RegionProgress(seekProgress), restored.getCommittedRegionProgress());
    assertEquals(230L, restored.getCommittedPhysicalTime());
    assertEquals(4L, restored.getCommittedLocalSeq());
    assertEquals(5, restored.getCommittedWriterNodeId());
    assertEquals(writerB, restored.getCommittedWriterId());
    assertEquals(new WriterProgress(230L, 4L), restored.getCommittedWriterProgress());
  }

  @Test
  public void testDerivedCommittedWriterProgressOrdersByPhysicalTimeNodeIdLocalSeq() {
    final WriterId writerA = new WriterId("2_6", 7);
    final WriterId writerB = new WriterId("2_6", 8);

    final Map<WriterId, WriterProgress> samePhysicalTimeProgress = new LinkedHashMap<>();
    samePhysicalTimeProgress.put(writerA, new WriterProgress(100L, 100L));
    samePhysicalTimeProgress.put(writerB, new WriterProgress(100L, 1L));
    final SubscriptionConsensusProgress samePhysicalTime =
        new SubscriptionConsensusProgress(new RegionProgress(samePhysicalTimeProgress), 0L);
    assertEquals(writerB, samePhysicalTime.getCommittedWriterId());
    assertEquals(new WriterProgress(100L, 1L), samePhysicalTime.getCommittedWriterProgress());

    final WriterId writerC = new WriterId("2_6", 6);
    samePhysicalTimeProgress.put(writerC, new WriterProgress(101L, 0L));
    final SubscriptionConsensusProgress higherPhysicalTime =
        new SubscriptionConsensusProgress(new RegionProgress(samePhysicalTimeProgress), 0L);
    assertEquals(writerC, higherPhysicalTime.getCommittedWriterId());
    assertEquals(new WriterProgress(101L, 0L), higherPhysicalTime.getCommittedWriterProgress());
  }

  @Test
  public void testDirectCommitWithoutOutstandingRequiresOutstandingMapping() {
    final WriterId writerId = new WriterId("3_1", 9);
    final Map<WriterId, WriterProgress> initialCommitted = new LinkedHashMap<>();
    initialCommitted.put(writerId, new WriterProgress(100L, 0L));
    final ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState state =
        new ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState(
            "3_1", new SubscriptionConsensusProgress(new RegionProgress(initialCommitted), 0L));

    assertFalse(state.commitWithoutOutstanding(writerId, new WriterProgress(103L, 3L)));
    assertEquals(100L, state.getCommittedPhysicalTime());
    assertEquals(0L, state.getCommittedLocalSeq());
  }

  @Test
  public void testDirectCommitWithoutOutstandingRespectsOutstandingGap() {
    final ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState state =
        new ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState(
            "3_2", new SubscriptionConsensusProgress());

    final WriterId writerId = new WriterId("3_2", 8);
    state.recordMapping(writerId, new WriterProgress(101L, 1L));
    state.recordMapping(writerId, new WriterProgress(102L, 2L));
    state.recordMapping(writerId, new WriterProgress(103L, 3L));

    assertTrue(state.commitWithoutOutstanding(writerId, new WriterProgress(103L, 3L)));
    assertEquals(new WriterProgress(0L, -1L), state.getCommittedWriterProgress());

    assertTrue(state.commitWithoutOutstanding(writerId, new WriterProgress(101L, 1L)));
    assertEquals(new WriterProgress(101L, 1L), state.getCommittedWriterProgress());

    assertTrue(state.commitWithoutOutstanding(writerId, new WriterProgress(102L, 2L)));
    assertEquals(new WriterProgress(103L, 3L), state.getCommittedWriterProgress());
  }

  @Test
  public void testBroadcastAdvanceIncrementsPersistenceCounter() {
    final String regionId = "3_3";
    final WriterId writerId = new WriterId(regionId, 8);
    final SubscriptionConsensusProgress progress =
        new SubscriptionConsensusProgress(new RegionProgress(new LinkedHashMap<>()), 7L);
    final ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState state =
        new ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState(regionId, progress);

    state.updateFromBroadcast(writerId, new WriterProgress(101L, 1L));
    assertEquals(8L, progress.getPersistenceThrottleCounter());

    state.updateFromBroadcast(writerId, new WriterProgress(100L, 0L));
    assertEquals(8L, progress.getPersistenceThrottleCounter());
  }

  @Test
  public void testBroadcastThrottleKeyIsPerWriter() {
    final String baseKey = CommitProgressKeeper.generateRegionKey("cg", "topic", "1_1");
    final WriterId writerA = new WriterId("1_1", 7);
    final WriterId writerB = new WriterId("1_1", 8);

    assertNotEquals(
        ConsensusSubscriptionCommitManager.buildBroadcastKey(baseKey, writerA),
        ConsensusSubscriptionCommitManager.buildBroadcastKey(baseKey, writerB));
  }

  @Test
  public void testFreshTailProposalWaitsForExplicitConfigNodeProgress() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("pendingInitialProposal");
    try {
      final AtomicReference<ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult>
          queryResult =
              new AtomicReference<>(
                  ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.absent());
      final ConsensusSubscriptionCommitManager manager =
          newCommitManager(systemDir, (consumerGroupId, topicName, regionId) -> queryResult.get());
      final DataRegionId regionId = new DataRegionId(21);
      final WriterId writerId = new WriterId(regionId.toString(), 7);
      final RegionProgress tailProposal =
          new RegionProgress(Collections.singletonMap(writerId, new WriterProgress(200L, 2L)));

      manager.initializeStateFromTailProposal("cg", "topic", regionId, tailProposal);

      assertTrue(manager.hasPersistedState("cg", "topic", regionId));
      assertTrue(manager.isInitialProgressPending("cg", "topic", regionId));
      assertEquals(tailProposal, manager.getCommittedRegionProgress("cg", "topic", regionId));
      assertFalse(manager.refreshPendingInitialProgress("cg", "topic", regionId));
      assertFalse(manager.refreshAuthoritativeProgress("cg", "topic", regionId).isAvailable());

      queryResult.set(
          ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.available(
              new RegionProgress(Collections.emptyMap())));
      assertTrue(manager.refreshPendingInitialProgress("cg", "topic", regionId));
      assertFalse(manager.isInitialProgressPending("cg", "topic", regionId));
      assertTrue(manager.isConfigNodeProgressAvailable("cg", "topic", regionId));
      assertTrue(
          manager
              .getCommittedRegionProgress("cg", "topic", regionId)
              .getWriterPositions()
              .isEmpty());
      assertTrue(manager.refreshAuthoritativeProgress("cg", "topic", regionId).isAvailable());
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testFreshTailProposalUsesAvailableConfigNodeProgressExactly() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("availableInitialProgress");
    try {
      final DataRegionId regionId = new DataRegionId(22);
      final WriterId writerId = new WriterId(regionId.toString(), 7);
      final RegionProgress configNodeProgress =
          new RegionProgress(Collections.singletonMap(writerId, new WriterProgress(100L, 1L)));
      final ConsensusSubscriptionCommitManager manager =
          newCommitManager(
              systemDir,
              (consumerGroupId, topicName, ignoredRegionId) ->
                  ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.available(
                      configNodeProgress));
      final RegionProgress laterTailProposal =
          new RegionProgress(Collections.singletonMap(writerId, new WriterProgress(200L, 2L)));

      manager.initializeStateFromTailProposal("cg", "topic", regionId, laterTailProposal);

      assertEquals(configNodeProgress, manager.getCommittedRegionProgress("cg", "topic", regionId));
      assertFalse(manager.isInitialProgressPending("cg", "topic", regionId));
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testNewRegionStartsFromBeginningWhenConfigNodeExplicitlyHasNoProgress()
      throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("newRegionAbsentProgress");
    try {
      final DataRegionId regionId = new DataRegionId(24);
      final AtomicReference<ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult>
          queryResult =
              new AtomicReference<>(
                  ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.absent());
      final ConsensusSubscriptionCommitManager manager =
          newCommitManager(
              systemDir, (consumerGroupId, topicName, ignoredRegionId) -> queryResult.get());

      manager.initializeStateWithoutTailProposal("cg", "topic", regionId);

      assertTrue(manager.hasPersistedState("cg", "topic", regionId));
      assertFalse(manager.isInitialProgressPending("cg", "topic", regionId));
      assertTrue(manager.isConfigNodeProgressAvailable("cg", "topic", regionId));
      assertTrue(
          manager
              .getCommittedRegionProgress("cg", "topic", regionId)
              .getWriterPositions()
              .isEmpty());
      manager.initializeStateWithoutTailProposal("cg", "topic", regionId);
      assertFalse(manager.isInitialProgressPending("cg", "topic", regionId));
      assertTrue(manager.isConfigNodeProgressAvailable("cg", "topic", regionId));
      assertTrue(manager.refreshAuthoritativeProgress("cg", "topic", regionId).isAvailable());

      queryResult.set(
          ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.available(
              new RegionProgress(
                  Collections.singletonMap(
                      new WriterId(regionId.toString(), 7), new WriterProgress(100L, 1L)))));
      assertTrue(manager.refreshAuthoritativeProgress("cg", "topic", regionId).isAvailable());

      queryResult.set(ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.absent());
      assertFalse(manager.refreshAuthoritativeProgress("cg", "topic", regionId).isAvailable());
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testNewRegionWaitsThroughUnavailableConfigNodeThenAcceptsExplicitAbsence()
      throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("newRegionUnavailableProgress");
    try {
      final AtomicReference<ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult>
          queryResult =
              new AtomicReference<>(
                  ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.unavailable());
      final ConsensusSubscriptionCommitManager manager =
          newCommitManager(
              systemDir, (consumerGroupId, topicName, ignoredRegionId) -> queryResult.get());
      final DataRegionId regionId = new DataRegionId(25);

      manager.initializeStateWithoutTailProposal("cg", "topic", regionId);

      assertFalse(manager.hasPersistedState("cg", "topic", regionId));
      assertTrue(manager.isInitialProgressPending("cg", "topic", regionId));
      assertFalse(manager.refreshPendingInitialProgress("cg", "topic", regionId));

      queryResult.set(ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.absent());
      assertTrue(manager.refreshPendingInitialProgress("cg", "topic", regionId));
      assertTrue(manager.hasPersistedState("cg", "topic", regionId));
      assertFalse(manager.isInitialProgressPending("cg", "topic", regionId));
      assertTrue(manager.isConfigNodeProgressAvailable("cg", "topic", regionId));
      assertTrue(
          manager
              .getCommittedRegionProgress("cg", "topic", regionId)
              .getWriterPositions()
              .isEmpty());
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testPersistedProgressWaitsForAndUsesAuthoritativeConfigNodeProgress()
      throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("persistedProgressMerge");
    try {
      final DataRegionId regionId = new DataRegionId(23);
      final WriterId writerId = new WriterId(regionId.toString(), 7);
      final WriterProgress persistedProgress = new WriterProgress(200L, 2L);
      final ConsensusSubscriptionCommitManager firstManager =
          newCommitManager(
              systemDir,
              (consumerGroupId, topicName, ignoredRegionId) ->
                  ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.unavailable());
      firstManager.receiveProgressBroadcast(
          "cg", "topic", regionId.toString(), writerId, persistedProgress);

      final RegionProgress olderConfigNodeProgress =
          new RegionProgress(Collections.singletonMap(writerId, new WriterProgress(100L, 1L)));
      final AtomicReference<ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult>
          queryResult =
              new AtomicReference<>(
                  ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.absent());
      final ConsensusSubscriptionCommitManager recoveredManager =
          newCommitManager(
              systemDir, (consumerGroupId, topicName, ignoredRegionId) -> queryResult.get());
      recoveredManager.initializeStateFromTailProposal(
          "cg",
          "topic",
          regionId,
          new RegionProgress(Collections.singletonMap(writerId, new WriterProgress(300L, 3L))));

      assertTrue(recoveredManager.isInitialProgressPending("cg", "topic", regionId));
      assertEquals(
          persistedProgress,
          recoveredManager
              .getCommittedRegionProgress("cg", "topic", regionId)
              .getWriterPositions()
              .get(writerId));

      queryResult.set(
          ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.available(
              olderConfigNodeProgress));
      assertTrue(recoveredManager.refreshPendingInitialProgress("cg", "topic", regionId));
      assertFalse(recoveredManager.isInitialProgressPending("cg", "topic", regionId));
      assertEquals(
          olderConfigNodeProgress,
          recoveredManager.getCommittedRegionProgress("cg", "topic", regionId));
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testSetupRollbackRestoresExistingProgressAndInitializationMarkers() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("setupRollbackExistingProgress");
    try {
      final DataRegionId regionId = new DataRegionId(26);
      final WriterId writerId = new WriterId(regionId.toString(), 7);
      final RegionProgress existingProgress =
          new RegionProgress(Collections.singletonMap(writerId, new WriterProgress(100L, 1L)));
      final AtomicReference<ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult>
          queryResult =
              new AtomicReference<>(
                  ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.available(
                      existingProgress));
      final ConsensusSubscriptionCommitManager manager =
          newCommitManager(
              systemDir, (consumerGroupId, topicName, ignoredRegionId) -> queryResult.get());
      manager.getOrCreateState("cg", "topic", regionId);
      manager.persistAll();
      final ConsensusSubscriptionCommitManager.SetupSnapshot setupSnapshot =
          manager.captureSetupSnapshot("cg", Collections.singleton("topic"));

      queryResult.set(ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.absent());
      manager.initializeStateFromTailProposal(
          "cg",
          "topic",
          regionId,
          new RegionProgress(Collections.singletonMap(writerId, new WriterProgress(200L, 2L))));
      assertTrue(manager.isInitialProgressPending("cg", "topic", regionId));

      manager.restoreSetupSnapshot(setupSnapshot, Collections.singleton("topic"));

      assertEquals(existingProgress, manager.getCommittedRegionProgress("cg", "topic", regionId));
      assertFalse(manager.isInitialProgressPending("cg", "topic", regionId));
      assertTrue(manager.isConfigNodeProgressAvailable("cg", "topic", regionId));

      manager.initializeStateFromTailProposal(
          "cg",
          "topic",
          regionId,
          new RegionProgress(Collections.singletonMap(writerId, new WriterProgress(300L, 3L))));
      assertTrue(manager.isInitialProgressPending("cg", "topic", regionId));
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testSetupRollbackRemovesFreshProposalAndReportedProgress() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("setupRollbackFreshProgress");
    try {
      final ConsensusSubscriptionCommitManager manager =
          newCommitManager(
              systemDir,
              (consumerGroupId, topicName, ignoredRegionId) ->
                  ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.absent());
      final DataRegionId regionId = new DataRegionId(27);
      final WriterId writerId = new WriterId(regionId.toString(), 7);
      final ConsensusSubscriptionCommitManager.SetupSnapshot setupSnapshot =
          manager.captureSetupSnapshot("cg", Collections.singleton("topic"));

      manager.initializeStateFromTailProposal(
          "cg",
          "topic",
          regionId,
          new RegionProgress(Collections.singletonMap(writerId, new WriterProgress(200L, 2L))));
      assertTrue(manager.hasPersistedState("cg", "topic", regionId));

      manager.restoreSetupSnapshot(setupSnapshot, Collections.singleton("topic"));

      assertFalse(manager.hasPersistedState("cg", "topic", regionId));
      assertFalse(manager.isInitialProgressPending("cg", "topic", regionId));
      assertFalse(
          manager
              .collectAllRegionProgress(11)
              .containsKey(
                  CommitProgressKeeper.generateKey("cg", "topic", regionId.toString(), 11)));
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testPersistGroupedTopicProgressAndRecoverAllRegions() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("system");
    try {
      final ConsensusSubscriptionCommitManager manager = newCommitManager(systemDir);
      final DataRegionId region1 = new DataRegionId(1);
      final DataRegionId region2 = new DataRegionId(2);
      final String region1Id = region1.toString();
      final String region2Id = region2.toString();
      final WriterId writer1 = new WriterId(region1Id, 7);
      final WriterId writer2 = new WriterId(region2Id, 8);
      final WriterProgress progress1 = new WriterProgress(100L, 1L);
      final WriterProgress progress2 = new WriterProgress(200L, 2L);

      manager.receiveProgressBroadcast("cg", "topic", region1Id, writer1, progress1);
      manager.receiveProgressBroadcast("cg", "topic", region2Id, writer2, progress2);

      final File progressDir = new File(systemDir, "subscription/consensus_progress");
      final File[] metaFiles = progressDir.listFiles((dir, name) -> name.endsWith(".meta"));
      final File[] indexFiles = progressDir.listFiles((dir, name) -> name.endsWith(".meta.index"));
      assertNotNull(metaFiles);
      assertNotNull(indexFiles);
      assertEquals(1, metaFiles.length);
      assertEquals(0, indexFiles.length);
      assertTrue(manager.hasPersistedState("cg", "topic", region1));
      assertTrue(manager.hasPersistedState("cg", "topic", region2));

      final ConsensusSubscriptionCommitManager recoveredManager = newCommitManager(systemDir);
      final Map<String, ByteBuffer> collectedProgress =
          recoveredManager.collectAllRegionProgress(11);
      for (final ByteBuffer progressBuffer : collectedProgress.values()) {
        assertTrue(progressBuffer.hasArray());
        assertFalse(progressBuffer.isReadOnly());
      }

      assertEquals(
          progress1,
          RegionProgress.deserialize(
                  collectedProgress.get(
                      CommitProgressKeeper.generateKey("cg", "topic", region1Id, 11)))
              .getWriterPositions()
              .get(writer1));
      assertEquals(
          progress1,
          RegionProgress.deserialize(
                  collectedProgress.get(
                      CommitProgressKeeper.generateLegacyKey("cg", "topic", region1Id, 11)))
              .getWriterPositions()
              .get(writer1));
      assertEquals(
          progress2,
          RegionProgress.deserialize(
                  collectedProgress.get(
                      CommitProgressKeeper.generateKey("cg", "topic", region2Id, 11)))
              .getWriterPositions()
              .get(writer2));
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testProgressFileNameEncodesForbiddenTopicComponents() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("systemWithForbiddenTopicComponents");
    try {
      final ConsensusSubscriptionCommitManager manager = newCommitManager(systemDir);
      final DataRegionId region = new DataRegionId(9);
      final String regionId = region.toString();
      final WriterId writerId = new WriterId(regionId, 9);
      final WriterProgress progress = new WriterProgress(900L, 9L);
      final String consumerGroupId = "cg:/\\*?\"<>|";
      final String topicName = "topic:/\\*?\"<>|";

      manager.receiveProgressBroadcast(consumerGroupId, topicName, regionId, writerId, progress);

      final File progressDir = new File(systemDir, "subscription/consensus_progress");
      final File[] metaFiles = progressDir.listFiles((dir, name) -> name.endsWith(".meta"));
      assertNotNull(metaFiles);
      assertEquals(1, metaFiles.length);
      for (final char forbidden : ":/\\*?\"<>|".toCharArray()) {
        assertFalse(
            "Meta file should not contain OS-forbidden character " + forbidden,
            metaFiles[0].getName().indexOf(forbidden) >= 0);
      }

      final ConsensusSubscriptionCommitManager recoveredManager = newCommitManager(systemDir);
      final Map<String, ByteBuffer> collectedProgress =
          recoveredManager.collectAllRegionProgress(13);
      assertEquals(
          progress,
          RegionProgress.deserialize(
                  collectedProgress.get(
                      CommitProgressKeeper.generateKey(consumerGroupId, topicName, regionId, 13)))
              .getWriterPositions()
              .get(writerId));
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testRecoverAllSkipsMalformedTopicProgressFile() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("systemWithMalformedProgress");
    try {
      final ConsensusSubscriptionCommitManager manager = newCommitManager(systemDir);
      final DataRegionId region = new DataRegionId(3);
      final String regionId = region.toString();
      final WriterId writerId = new WriterId(regionId, 9);
      final WriterProgress progress = new WriterProgress(300L, 3L);

      manager.receiveProgressBroadcast("cg", "goodTopic", regionId, writerId, progress);
      writeMalformedTopicProgressFile(systemDir);

      final ConsensusSubscriptionCommitManager recoveredManager = newCommitManager(systemDir);
      final Map<String, ByteBuffer> collectedProgress =
          recoveredManager.collectAllRegionProgress(12);

      assertEquals(
          progress,
          RegionProgress.deserialize(
                  collectedProgress.get(
                      CommitProgressKeeper.generateKey("cg", "goodTopic", regionId, 12)))
              .getWriterPositions()
              .get(writerId));
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testCommitStatesDoNotCollideWhenIdentifiersContainSeparator() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("systemWithCollidingLegacyKeys");
    try {
      final ConsensusSubscriptionCommitManager manager = newCommitManager(systemDir);
      final DataRegionId region = new DataRegionId(10);
      final String regionId = region.toString();
      final WriterId firstWriterId = new WriterId(regionId, 7);
      final WriterId secondWriterId = new WriterId(regionId, 8);
      final WriterProgress firstProgress = new WriterProgress(100L, 1L);
      final WriterProgress secondProgress = new WriterProgress(200L, 2L);

      manager.receiveProgressBroadcast("a##b", "c", regionId, firstWriterId, firstProgress);
      manager.receiveProgressBroadcast("a", "b##c", regionId, secondWriterId, secondProgress);

      final Map<String, ByteBuffer> collectedProgress =
          newCommitManager(systemDir).collectAllRegionProgress(14);
      assertEquals(2, collectedProgress.size());
      assertEquals(
          firstProgress,
          RegionProgress.deserialize(
                  collectedProgress.get(
                      CommitProgressKeeper.generateKey("a##b", "c", regionId, 14)))
              .getWriterPositions()
              .get(firstWriterId));
      assertEquals(
          secondProgress,
          RegionProgress.deserialize(
                  collectedProgress.get(
                      CommitProgressKeeper.generateKey("a", "b##c", regionId, 14)))
              .getWriterPositions()
              .get(secondWriterId));
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  private static void writeMalformedTopicProgressFile(final File systemDir) throws Exception {
    final File progressDir = new File(systemDir, "subscription/consensus_progress");
    final File malformedFile =
        new File(progressDir, "consensus_subscription_progress_" + "Y2c##YmFkVG9waWM" + ".meta");
    try (final FileOutputStream outputStream = new FileOutputStream(malformedFile)) {
      outputStream.write(new byte[] {0, 0, 0, 1, 0});
    }
  }

  private static ConsensusSubscriptionCommitManager newCommitManager(final File systemDir)
      throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setSystemDir(systemDir.getAbsolutePath());
    final Constructor<ConsensusSubscriptionCommitManager> constructor =
        ConsensusSubscriptionCommitManager.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    return constructor.newInstance();
  }

  private static ConsensusSubscriptionCommitManager newCommitManager(
      final File systemDir,
      final ConsensusSubscriptionCommitManager.ConfigNodeProgressFetcher progressFetcher) {
    IoTDBDescriptor.getInstance().getConfig().setSystemDir(systemDir.getAbsolutePath());
    return new ConsensusSubscriptionCommitManager(progressFetcher);
  }
}
