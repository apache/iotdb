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
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.SubscriptionWalRetentionPolicy;
import org.apache.iotdb.consensus.iot.WriterSafeFrontierTracker;
import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.statement.StatementTestUtils;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;

import org.apache.tsfile.utils.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConsensusPrefetchingQueueTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testFilteredEmptyEntryAdvancesProgressWithoutEvent() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("system");
    ConsensusPrefetchingQueue queue = null;
    try {
      final DataRegionId regionId = new DataRegionId(1);
      final FakeConsensusReqReader reader = new FakeConsensusReqReader();
      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(reader);
      when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());

      final ConsensusLogToTabletConverter converter = mock(ConsensusLogToTabletConverter.class);
      when(converter.convert(any())).thenReturn(Collections.emptyList());

      final ConsensusSubscriptionCommitManager commitManager = newCommitManager(systemDir);
      queue =
          new ConsensusPrefetchingQueue(
              "consumerGroup",
              "topic",
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
              regionId,
              serverImpl,
              new SubscriptionWalRetentionPolicy(
                  "topic",
                  SubscriptionWalRetentionPolicy.UNBOUNDED,
                  SubscriptionWalRetentionPolicy.UNBOUNDED),
              converter,
              commitManager,
              new RegionProgress(Collections.emptyMap()),
              1L,
              1L,
              true);

      final IndexedConsensusRequest request =
          new IndexedConsensusRequest(
                  1L, Collections.singletonList(StatementTestUtils.genInsertRowNode(1)))
              .setPhysicalTime(1000L)
              .setNodeId(7);
      reader.currentSearchIndex = 1L;
      pendingEntries(queue).offer(request);

      assertNull(queue.poll("consumer"));
      queue.drivePrefetchOnce();
      Thread.sleep(
          SubscriptionTestConfig.consensusBatchMaxDelayInMs() + TimeUnit.MILLISECONDS.toMillis(10));
      queue.drivePrefetchOnce();

      assertEquals(0, queue.getPrefetchedEventCount());
      assertEquals(2L, queue.getCurrentReadSearchIndex());
      assertEquals(
          new WriterProgress(1000L, 1L),
          commitManager
              .getCommittedRegionProgress("consumerGroup", "topic", regionId)
              .getWriterPositions()
              .get(new WriterId(regionId.toString(), 7)));
    } finally {
      if (queue != null) {
        queue.close();
      }
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @SuppressWarnings("unchecked")
  private static BlockingQueue<IndexedConsensusRequest> pendingEntries(
      final ConsensusPrefetchingQueue queue) throws Exception {
    final Field field = ConsensusPrefetchingQueue.class.getDeclaredField("pendingEntries");
    field.setAccessible(true);
    return (BlockingQueue<IndexedConsensusRequest>) field.get(queue);
  }

  private static ConsensusSubscriptionCommitManager newCommitManager(final File systemDir)
      throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setSystemDir(systemDir.getAbsolutePath());
    final Constructor<ConsensusSubscriptionCommitManager> constructor =
        ConsensusSubscriptionCommitManager.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    return constructor.newInstance();
  }

  private static final class FakeConsensusReqReader implements ConsensusReqReader {

    private long currentSearchIndex;

    @Override
    public void setSafelyDeletedSearchIndex(final long safelyDeletedSearchIndex) {
      // no-op
    }

    @Override
    public ReqIterator getReqIterator(final long startIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getCurrentSearchIndex() {
      return currentSearchIndex;
    }

    @Override
    public long getCurrentWALFileVersion() {
      return 0;
    }

    @Override
    public long getTotalSize() {
      return 0;
    }

    @Override
    public Pair<Long, Long> getDeletionBoundToFreeAtLeast(final long bytesToFree) {
      return new Pair<>(DEFAULT_SAFELY_DELETED_SEARCH_INDEX, 0L);
    }
  }

  private static final class SubscriptionTestConfig {

    private SubscriptionTestConfig() {
      // utility class
    }

    private static long consensusBatchMaxDelayInMs() {
      return org.apache.iotdb.commons.subscription.config.SubscriptionConfig.getInstance()
          .getSubscriptionConsensusBatchMaxDelayInMs();
    }
  }
}
