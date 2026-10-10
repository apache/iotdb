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

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.SubscriptionWalRetentionPolicy;
import org.apache.iotdb.consensus.iot.WriterSafeFrontierTracker;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementTestUtils;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.resource.SubscriptionMemoryManager;
import org.apache.iotdb.db.subscription.task.execution.ConsensusSubscriptionPrefetchExecutor;
import org.apache.iotdb.db.subscription.task.execution.ConsensusSubscriptionPrefetchExecutorManager;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ConsensusSubscriptionPrefetchExecutorManager.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
public class ConsensusPrefetchingQueueSeekTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test(timeout = 10_000L)
  public void testSeekToBeginningReplaysFirstLocalEntryWithoutWalGap() throws Exception {
    assertBeginningReplay(Collections.singletonList(createRequest(1L, 1L, 7)), 2L, 0L);
  }

  @Test(timeout = 10_000L)
  public void testSeekToBeginningPreservesFollowerEntryBeforeFirstLocalEntry() throws Exception {
    assertBeginningReplay(
        Arrays.asList(createRequest(-1L, 10L, 8), createRequest(1L, 1L, 7)), 2L, 0L);
  }

  @Test(timeout = 10_000L)
  public void testSeekToBeginningCountsOnlyMissingValidLocalIndexes() throws Exception {
    // Local indexes 1 and 2 are absent. Index 0 is the empty-WAL sentinel, not a missing entry.
    assertBeginningReplay(
        Arrays.asList(createRequest(3L, 3L, 7), createRequest(4L, 4L, 7)), 5L, 2L);
  }

  private void assertBeginningReplay(
      final List<IndexedConsensusRequest> requests,
      final long expectedNextIndex,
      final long expectedSkippedEntries)
      throws Exception {
    final CommonConfig config = CommonDescriptor.getInstance().getConfig();
    final int originalBatchMaxDelay = config.getSubscriptionConsensusBatchMaxDelayInMs();
    final int originalPrefetchThreads =
        config.getSubscriptionConsensusPrefetchExecutorMaxThreadNum();
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder();
    ConsensusSubscriptionPrefetchExecutor executor = null;
    ConsensusPrefetchingQueue queue = null;
    try {
      config.setSubscriptionConsensusBatchMaxDelayInMs(0);
      config.setSubscriptionConsensusPrefetchExecutorMaxThreadNum(1);
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(systemDir.getAbsolutePath());

      // Supply an isolated runtime even in builds where subscriptions are disabled. Seek reset
      // and replay still run through the real serial prefetch worker and the public queue API.
      executor = new ConsensusSubscriptionPrefetchExecutor();
      final ConsensusSubscriptionPrefetchExecutorManager manager =
          mock(ConsensusSubscriptionPrefetchExecutorManager.class);
      PowerMockito.mockStatic(ConsensusSubscriptionPrefetchExecutorManager.class);
      PowerMockito.when(ConsensusSubscriptionPrefetchExecutorManager.getInstance())
          .thenReturn(manager);
      when(manager.getExecutor()).thenReturn(executor);

      final WALNode walNode = mock(WALNode.class);
      when(walNode.getCurrentSearchIndex()).thenReturn(expectedNextIndex - 1L);
      when(walNode.getLogDirectory()).thenReturn(systemDir);
      final IoTConsensusServerImpl server = mock(IoTConsensusServerImpl.class);
      when(server.getConsensusReqReader()).thenReturn(walNode);
      when(server.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());
      final ConsensusLogToTabletConverter converter = mock(ConsensusLogToTabletConverter.class);
      when(converter.getDatabaseName()).thenReturn("db");
      when(converter.isTableModel()).thenReturn(true);
      when(converter.convert(any()))
          .thenAnswer(
              invocation ->
                  Collections.singletonList(
                      createTablet(((InsertRowNode) invocation.getArgument(0)).getTime())));

      queue =
          new ConsensusPrefetchingQueue(
              "consumerGroup",
              "topic",
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
              new DataRegionId(1),
              server,
              new SubscriptionWalRetentionPolicy(
                  "topic",
                  SubscriptionWalRetentionPolicy.UNBOUNDED,
                  SubscriptionWalRetentionPolicy.UNBOUNDED),
              converter,
              new ConsensusSubscriptionCommitManager(
                  (consumerGroupId, topicName, regionId) ->
                      ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.absent()),
              new RegionProgress(Collections.emptyMap()),
              expectedNextIndex,
              1L,
              true) {
            @Override
            protected ProgressWALIterator createSubscriptionWALIterator(
                final long startSearchIndex) {
              final Iterator<IndexedConsensusRequest> retainedEntries =
                  requests.stream()
                      .filter(
                          request ->
                              request.getSearchIndex() < 0
                                  || request.getSearchIndex() >= startSearchIndex)
                      .iterator();
              final ProgressWALIterator iterator = mock(ProgressWALIterator.class);
              when(iterator.hasNext()).thenAnswer(ignored -> retainedEntries.hasNext());
              when(iterator.next()).thenAnswer(ignored -> retainedEntries.next());
              return iterator;
            }
          };
      queue.setSubscriptionMemoryManager(new SubscriptionMemoryManager(16L * 1024 * 1024));

      queue.seekToBeginning();

      final List<Long> actualTimestamps = new ArrayList<>();
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
      while (actualTimestamps.size() < requests.size() && System.nanoTime() < deadline) {
        final SubscriptionEvent event = queue.poll("consumer");
        if (event == null) {
          TimeUnit.MILLISECONDS.sleep(10L);
          continue;
        }
        assertEquals(
            SubscriptionPollResponseType.TABLETS.getType(),
            event.getCurrentResponse().getResponseType());
        for (final Tablet tablet :
            ((TabletsPayload) event.getCurrentResponse().getPayload()).getTablets()) {
          assertEquals(1, tablet.getRowSize());
          actualTimestamps.add(tablet.getTimestamps()[0]);
        }
      }
      final List<Long> expectedTimestamps = new ArrayList<>();
      for (final IndexedConsensusRequest request : requests) {
        expectedTimestamps.add(((InsertRowNode) request.getRequests().get(0)).getTime());
      }
      Collections.sort(expectedTimestamps);
      Collections.sort(actualTimestamps);
      assertEquals(expectedTimestamps, actualTimestamps);
      assertEquals(expectedNextIndex, queue.getCurrentReadSearchIndex());
      assertEquals(expectedSkippedEntries, queue.getWalGapSkippedEntries());
      verify(converter, times(requests.size())).convert(any());
    } finally {
      if (queue != null) {
        queue.close();
      }
      if (executor != null) {
        executor.shutdown();
      }
      config.setSubscriptionConsensusBatchMaxDelayInMs(originalBatchMaxDelay);
      config.setSubscriptionConsensusPrefetchExecutorMaxThreadNum(originalPrefetchThreads);
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  private static IndexedConsensusRequest createRequest(
      final long searchIndex, final long localSeq, final int writerNodeId) {
    return new IndexedConsensusRequest(
            searchIndex,
            localSeq,
            Collections.singletonList(
                StatementTestUtils.genInsertRowNode(Math.toIntExact(localSeq))))
        .setPhysicalTime(1000L + localSeq)
        .setNodeId(writerNodeId);
  }

  private static Tablet createTablet(final long timestamp) {
    final Tablet tablet =
        new Tablet(
            "sensors",
            Arrays.asList("device", "temperature"),
            Arrays.asList(TSDataType.STRING, TSDataType.DOUBLE),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD),
            1);
    tablet.addTimestamp(0, timestamp);
    tablet.addValue(0, 0, "d1");
    tablet.addValue(0, 1, 36.5);
    tablet.setRowSize(1);
    return tablet;
  }
}
