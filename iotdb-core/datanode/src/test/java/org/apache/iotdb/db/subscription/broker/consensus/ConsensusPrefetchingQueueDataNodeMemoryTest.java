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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.SubscriptionWalRetentionPolicy;
import org.apache.iotdb.consensus.iot.WriterSafeFrontierTracker;
import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;
import org.apache.iotdb.consensus.iot.subscription.SubscriptionQueueAdmission;
import org.apache.iotdb.consensus.iot.subscription.SubscriptionQueueRejectionReason;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.queryengine.plan.statement.StatementTestUtils;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.resource.SubscriptionMemoryManager;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.payload.poll.ErrorPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConsensusPrefetchingQueueDataNodeMemoryTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testLargeQueueQuotaDoesNotBlockSmallQueueAndPreservesPendingEntries()
      throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final int originalBatchMaxDelay =
        CommonDescriptor.getInstance().getConfig().getSubscriptionConsensusBatchMaxDelayInMs();
    final int originalMaxWalEntries =
        CommonDescriptor.getInstance().getConfig().getSubscriptionConsensusBatchMaxWalEntries();
    ConsensusPrefetchingQueue largeQueue = null;
    ConsensusPrefetchingQueue smallQueue = null;
    try {
      CommonDescriptor.getInstance().getConfig().setSubscriptionConsensusBatchMaxDelayInMs(0);
      CommonDescriptor.getInstance().getConfig().setSubscriptionConsensusBatchMaxWalEntries(1);
      final Tablet largeTablet = createLargeTablet();
      final long largeBytes = PipeMemoryWeightUtil.calculateTabletSizeInBytes(largeTablet);
      final long smallBytes = PipeMemoryWeightUtil.calculateTabletSizeInBytes(createTablet());
      final SubscriptionMemoryManager memoryManager =
          // With two queues, a queue may use 3/4 of the budget; make that exactly one large entry.
          new SubscriptionMemoryManager(largeBytes + largeBytes / 3L);
      final ConsensusSubscriptionCommitManager commitManager =
          newCommitManager(temporaryFolder.newFolder("large-small-fair-share"));
      final FakeConsensusReqReader largeReader = new FakeConsensusReqReader();
      final FakeConsensusReqReader smallReader = new FakeConsensusReqReader();
      final AtomicInteger largeConversions = new AtomicInteger();
      final AtomicInteger smallConversions = new AtomicInteger();
      largeQueue =
          newQueue(
              "largeGroup",
              new DataRegionId(1),
              largeReader,
              newConverter(largeConversions, largeTablet),
              commitManager,
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE);
      smallQueue =
          newQueue(
              "smallGroup",
              new DataRegionId(2),
              smallReader,
              newConverter(smallConversions),
              commitManager,
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE);
      largeQueue.setSubscriptionMemoryManager(memoryManager);
      smallQueue.setSubscriptionMemoryManager(memoryManager);
      assertNull(largeQueue.poll("largeConsumer"));
      assertNull(smallQueue.poll("smallConsumer"));

      largeReader.currentSearchIndex = 2L;
      assertTrue(pendingEntries(largeQueue).offer(createRequest(1L)));
      assertTrue(pendingEntries(largeQueue).offer(createRequest(2L)));
      largeQueue.drivePrefetchOnce();
      assertEquals(largeBytes, largeQueue.getRetainedTabletBytes());
      final long retainedPendingBytes = largeQueue.getRetainedRequestBytes();
      assertTrue(retainedPendingBytes > 0L);
      largeQueue.drivePrefetchOnce();
      assertEquals(1, largeConversions.get());
      assertEquals(1, pendingEntries(largeQueue).size());
      assertEquals(retainedPendingBytes, largeQueue.getRetainedRequestBytes());
      assertFalse(pendingEntries(largeQueue).offer(createRequest(3L)));
      assertEquals(
          SubscriptionQueueRejectionReason.SUBSCRIPTION_MEMORY_QUOTA,
          ((SubscriptionQueueAdmission) pendingEntries(largeQueue)).getLastRejectionReason());
      assertEquals(1L, largeQueue.getRealtimeAdmissionRejectionCount());
      assertEquals(
          1L,
          largeQueue.getRealtimeAdmissionRejectionCount(
              SubscriptionQueueRejectionReason.SUBSCRIPTION_MEMORY_QUOTA));

      smallReader.currentSearchIndex = 1L;
      assertTrue(pendingEntries(smallQueue).offer(createRequest(1L)));
      smallQueue.drivePrefetchOnce();
      assertEquals(1, smallConversions.get());
      assertEquals(largeBytes + smallBytes, memoryManager.getUsedMemorySizeInBytes());
      final SubscriptionEvent smallEvent = smallQueue.poll("smallConsumer");
      assertNotNull(smallEvent);
      assertTrue(smallQueue.ack("smallConsumer", smallEvent.getCommitContext()));
      assertEquals(largeBytes, memoryManager.getUsedMemorySizeInBytes());

      final SubscriptionEvent largeEvent = largeQueue.poll("largeConsumer");
      assertNotNull(largeEvent);
      assertTrue(largeQueue.ack("largeConsumer", largeEvent.getCommitContext()));
      largeQueue.drivePrefetchOnce();
      assertEquals(2, largeConversions.get());
      assertEquals(0, pendingEntries(largeQueue).size());
      assertEquals(0L, largeQueue.getRetainedRequestBytes());
      final SubscriptionEvent secondLargeEvent = largeQueue.poll("largeConsumer");
      assertNotNull(secondLargeEvent);
      assertTrue(largeQueue.ack("largeConsumer", secondLargeEvent.getCommitContext()));
      assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
    } finally {
      if (largeQueue != null) {
        largeQueue.close();
      }
      if (smallQueue != null) {
        smallQueue.close();
      }
      CommonDescriptor.getInstance()
          .getConfig()
          .setSubscriptionConsensusBatchMaxDelayInMs(originalBatchMaxDelay);
      CommonDescriptor.getInstance()
          .getConfig()
          .setSubscriptionConsensusBatchMaxWalEntries(originalMaxWalEntries);
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testOversizedEntryReportsErrorWhileOtherQueueHoldsMemory() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final int originalBatchMaxDelay =
        CommonDescriptor.getInstance().getConfig().getSubscriptionConsensusBatchMaxDelayInMs();
    ConsensusPrefetchingQueue largeQueue = null;
    ConsensusPrefetchingQueue smallQueue = null;
    try {
      CommonDescriptor.getInstance().getConfig().setSubscriptionConsensusBatchMaxDelayInMs(0);
      final Tablet largeTablet = createLargeTablet();
      final long largeBytes = PipeMemoryWeightUtil.calculateTabletSizeInBytes(largeTablet);
      // This entry exceeds the entire node budget, which is also partly held by another queue.
      final SubscriptionMemoryManager memoryManager =
          new SubscriptionMemoryManager(largeBytes / 2L);
      final ConsensusSubscriptionCommitManager commitManager =
          newCommitManager(temporaryFolder.newFolder("oversized-queue-error"));
      final FakeConsensusReqReader largeReader = new FakeConsensusReqReader();
      final FakeConsensusReqReader smallReader = new FakeConsensusReqReader();
      final AtomicInteger largeConversions = new AtomicInteger();
      largeQueue =
          newQueue(
              "largeGroup",
              new DataRegionId(1),
              largeReader,
              newConverter(largeConversions, largeTablet),
              commitManager,
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE);
      smallQueue =
          newQueue(
              "smallGroup",
              new DataRegionId(2),
              smallReader,
              newConverter(new AtomicInteger()),
              commitManager,
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE);
      largeQueue.setSubscriptionMemoryManager(memoryManager);
      smallQueue.setSubscriptionMemoryManager(memoryManager);
      assertNull(largeQueue.poll("largeConsumer"));
      assertNull(smallQueue.poll("smallConsumer"));
      smallReader.currentSearchIndex = 1L;
      assertTrue(pendingEntries(smallQueue).offer(createRequest(1L)));
      smallQueue.drivePrefetchOnce();
      final long smallBytes = memoryManager.getUsedMemorySizeInBytes();
      assertTrue(smallBytes > 0L);

      largeReader.currentSearchIndex = 1L;
      assertTrue(pendingEntries(largeQueue).offer(createRequest(1L)));
      largeQueue.drivePrefetchOnce();
      assertEquals(1, largeConversions.get());
      assertEquals(1L, largeQueue.getCurrentReadSearchIndex());
      assertEquals(1L, largeQueue.getOversizedEntryRejectionCount());
      assertEquals(0L, largeQueue.getRealtimeAdmissionRejectionCount());
      assertEquals(smallBytes, memoryManager.getUsedMemorySizeInBytes());
      assertEquals(0L, memoryManager.getOvercommitSizeInBytes());
      assertEquals(0L, memoryManager.getOversizedEntryCount());
      assertEquals("0", largeQueue.coreReportMessage().get("subscriptionMemoryUsedInBytes"));
      assertEquals(
          String.valueOf(smallBytes),
          largeQueue.coreReportMessage().get("dataNodeSubscriptionMemoryUsedInBytes"));
      final SubscriptionEvent error = largeQueue.poll("largeConsumer");
      assertNotNull(error);
      final ErrorPayload payload = (ErrorPayload) error.getCurrentResponse().getPayload();
      assertTrue(payload.isCritical());
      assertTrue(payload.getErrorMessage().contains("SUBSCRIPTION_OVERSIZED_ENTRY"));
      for (int i = 0; i < 10; i++) {
        largeQueue.drivePrefetchOnce();
      }
      assertEquals(1, largeConversions.get());
      assertEquals(1L, largeQueue.getCurrentReadSearchIndex());
      final SubscriptionEvent smallEvent = smallQueue.poll("smallConsumer");
      assertNotNull(smallEvent);
      assertTrue(smallQueue.ack("smallConsumer", smallEvent.getCommitContext()));
      assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
    } finally {
      if (largeQueue != null) {
        largeQueue.close();
      }
      if (smallQueue != null) {
        smallQueue.close();
      }
      CommonDescriptor.getInstance()
          .getConfig()
          .setSubscriptionConsensusBatchMaxDelayInMs(originalBatchMaxDelay);
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testQueuesShareDataNodeMemoryBudget() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final int originalBatchMaxDelay =
        CommonDescriptor.getInstance().getConfig().getSubscriptionConsensusBatchMaxDelayInMs();
    final File systemDir = temporaryFolder.newFolder("shared-datanode-memory");
    ConsensusPrefetchingQueue queueA = null;
    ConsensusPrefetchingQueue queueB = null;
    try {
      CommonDescriptor.getInstance().getConfig().setSubscriptionConsensusBatchMaxDelayInMs(0);
      final Tablet tablet = createTablet();
      final long oneTabletBytes = PipeMemoryWeightUtil.calculateTabletSizeInBytes(tablet);
      final SubscriptionMemoryManager memoryManager =
          new SubscriptionMemoryManager(oneTabletBytes * 2L);
      final ConsensusSubscriptionCommitManager commitManager = newCommitManager(systemDir);

      final FakeConsensusReqReader readerA = new FakeConsensusReqReader();
      final AtomicInteger conversionCountA = new AtomicInteger();
      queueA =
          newQueue(
              "consumerGroupA",
              new DataRegionId(1),
              readerA,
              newConverter(conversionCountA),
              commitManager,
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE);
      queueA.setSubscriptionMemoryManager(memoryManager);

      final FakeConsensusReqReader readerB = new FakeConsensusReqReader();
      final AtomicInteger conversionCountB = new AtomicInteger();
      queueB =
          newQueue(
              "consumerGroupB",
              new DataRegionId(2),
              readerB,
              newConverter(conversionCountB),
              commitManager,
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE);
      queueB.setSubscriptionMemoryManager(memoryManager);

      assertNull(queueA.poll("consumerA"));
      assertNull(queueB.poll("consumerB"));

      readerA.currentSearchIndex = 1L;
      assertTrue(pendingEntries(queueA).offer(createRequest(1L)));
      queueA.drivePrefetchOnce();

      assertEquals(1, conversionCountA.get());
      assertEquals(0, conversionCountB.get());
      assertEquals(oneTabletBytes, queueA.getRetainedTabletBytes());
      assertEquals(0L, queueB.getRetainedTabletBytes());
      assertEquals(oneTabletBytes, memoryManager.getUsedMemorySizeInBytes());
      assertEquals(0L, memoryManager.getOvercommitSizeInBytes());
      assertEquals(0L, memoryManager.getOversizedEntryCount());
      assertEquals(
          oneTabletBytes, queueA.getRetainedTabletBytes() + queueB.getRetainedTabletBytes());

      readerB.currentSearchIndex = 1L;
      assertTrue(pendingEntries(queueB).offer(createRequest(1L)));
      queueB.drivePrefetchOnce();
      assertEquals(1, conversionCountB.get());
      assertEquals("false", queueB.coreReportMessage().get("realtimeAdmissionBlocked"));
      assertEquals(oneTabletBytes * 2L, memoryManager.getUsedMemorySizeInBytes());
      assertEquals(oneTabletBytes, queueB.getRetainedTabletBytes());

      final SubscriptionEvent eventA = queueA.poll("consumerA");
      assertNotNull(eventA);
      assertTrue(queueA.ack("consumerA", eventA.getCommitContext()));
      assertEquals(0L, queueA.getRetainedTabletBytes());
      assertEquals(oneTabletBytes, memoryManager.getUsedMemorySizeInBytes());
      assertEquals(0L, memoryManager.getOvercommitSizeInBytes());

      queueB.drivePrefetchOnce();
      assertEquals("false", queueB.coreReportMessage().get("realtimeAdmissionBlocked"));

      final SubscriptionEvent eventB = queueB.poll("consumerB");
      assertNotNull(eventB);
      assertTrue(queueB.ack("consumerB", eventB.getCommitContext()));
      assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
      assertEquals(0L, memoryManager.getOvercommitSizeInBytes());
      queueB.drivePrefetchOnce();
      assertEquals("false", queueB.coreReportMessage().get("realtimeAdmissionBlocked"));
    } finally {
      if (queueA != null) {
        queueA.close();
      }
      if (queueB != null) {
        queueB.close();
      }
      CommonDescriptor.getInstance()
          .getConfig()
          .setSubscriptionConsensusBatchMaxDelayInMs(originalBatchMaxDelay);
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testWatermarkAndAckDoNotHideDataDeliveryGap() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final int originalBatchMaxDelay =
        CommonDescriptor.getInstance().getConfig().getSubscriptionConsensusBatchMaxDelayInMs();
    final File systemDir = temporaryFolder.newFolder("delivery-gap");
    ConsensusPrefetchingQueue queue = null;
    try {
      CommonDescriptor.getInstance().getConfig().setSubscriptionConsensusBatchMaxDelayInMs(0);
      final FakeConsensusReqReader reader = new FakeConsensusReqReader();
      queue =
          newQueue(
              "consumerGroup",
              new DataRegionId(1),
              reader,
              newConverter(new AtomicInteger()),
              newCommitManager(systemDir),
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE);
      final AtomicLong clockNs = new AtomicLong(-1_000_000L);
      setTimeTracker(queue, new SubscriptionQueueTimeTracker(clockNs::get));
      queue.setSubscriptionMemoryManager(new SubscriptionMemoryManager(1_000_000L));

      assertNull(queue.poll("consumer"));
      reader.currentSearchIndex = 1L;
      assertTrue(pendingEntries(queue).offer(createRequest(1L)));
      queue.drivePrefetchOnce();
      final SubscriptionEvent first = queue.poll("consumer");
      assertNotNull(first);

      clockNs.addAndGet(TimeUnit.MILLISECONDS.toNanos(60_000L));
      final Method injectWatermark =
          ConsensusPrefetchingQueue.class.getDeclaredMethod("injectWatermark", long.class);
      injectWatermark.setAccessible(true);
      injectWatermark.invoke(queue, 1L);
      final SubscriptionEvent watermark = queue.poll("consumer");
      assertNotNull(watermark);
      assertEquals(
          SubscriptionPollResponseType.WATERMARK.getType(),
          watermark.getCurrentResponse().getResponseType());
      assertEquals(60_000L, queue.getDeliveryIdleTimeMs());
      assertTrue(queue.ack("consumer", first.getCommitContext()));
      assertEquals(60_000L, queue.getDeliveryIdleTimeMs());
      assertEquals(0L, queue.getLastDeliveryIntervalMs());

      clockNs.addAndGet(TimeUnit.MILLISECONDS.toNanos(1L));
      reader.currentSearchIndex = 2L;
      assertTrue(pendingEntries(queue).offer(createRequest(2L)));
      queue.drivePrefetchOnce();
      final SubscriptionEvent second = queue.poll("consumer");
      assertNotNull(second);
      assertEquals(60_001L, queue.getLastDeliveryIntervalMs());
      assertEquals(60_001L, queue.getMaxDeliveryIntervalMs());
      assertEquals(0L, queue.getDeliveryIdleTimeMs());
      assertTrue(queue.ack("consumer", second.getCommitContext()));

      clockNs.addAndGet(TimeUnit.MILLISECONDS.toNanos(1L));
      reader.currentSearchIndex = 3L;
      assertTrue(pendingEntries(queue).offer(createRequest(3L)));
      queue.drivePrefetchOnce();
      assertNotNull(queue.poll("consumer"));
      assertEquals(1L, queue.getLastDeliveryIntervalMs());
      assertEquals(60_001L, queue.getMaxDeliveryIntervalMs());
    } finally {
      if (queue != null) {
        queue.close();
      }
      CommonDescriptor.getInstance()
          .getConfig()
          .setSubscriptionConsensusBatchMaxDelayInMs(originalBatchMaxDelay);
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testTimingRemainsReadableDuringBlockedPrefetch() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("blocked-prefetch-timing");
    final CountDownLatch conversionStarted = new CountDownLatch(1);
    final CountDownLatch releaseConversion = new CountDownLatch(1);
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    ConsensusPrefetchingQueue queue = null;
    try {
      final FakeConsensusReqReader reader = new FakeConsensusReqReader();
      final ConsensusLogToTabletConverter converter = mock(ConsensusLogToTabletConverter.class);
      when(converter.getDatabaseName()).thenReturn("db");
      when(converter.convert(any()))
          .thenAnswer(
              ignored -> {
                conversionStarted.countDown();
                assertTrue(releaseConversion.await(5L, TimeUnit.SECONDS));
                return Collections.singletonList(createTablet());
              });
      queue =
          newQueue(
              "consumerGroup",
              new DataRegionId(1),
              reader,
              converter,
              newCommitManager(systemDir),
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE);
      final AtomicLong clockNs = new AtomicLong(Long.MAX_VALUE - 1_000_000L);
      setTimeTracker(queue, new SubscriptionQueueTimeTracker(clockNs::get));
      queue.setSubscriptionMemoryManager(new SubscriptionMemoryManager(1_000_000L));

      assertNull(queue.poll("consumer"));
      reader.currentSearchIndex = 1L;
      assertTrue(pendingEntries(queue).offer(createRequest(1L)));
      final ConsensusPrefetchingQueue blockedQueue = queue;
      final Future<PrefetchRoundResult> round = executor.submit(blockedQueue::drivePrefetchOnce);
      assertTrue(conversionStarted.await(5L, TimeUnit.SECONDS));
      clockNs.addAndGet(TimeUnit.MILLISECONDS.toNanos(65_000L));

      assertEquals(65_000L, queue.getPrefetchDurationMs());
      assertEquals(65_000L, queue.getMaxPrefetchDurationMs());
      assertEquals(65_000L, queue.getPrefetchIdleTimeMs());
      assertEquals(65_000L, queue.getDeliveryIdleTimeMs());
      assertEquals("65000", queue.coreReportMessage().get("prefetchDurationMs"));
      assertEquals("0", queue.coreReportMessage().get("subscriptionMemoryUsedInBytes"));
      assertEquals("0", queue.coreReportMessage().get("dataNodeSubscriptionMemoryUsedInBytes"));

      releaseConversion.countDown();
      round.get(5L, TimeUnit.SECONDS);
      assertEquals(0L, queue.getPrefetchDurationMs());
      assertEquals(65_000L, queue.getMaxPrefetchDurationMs());
      assertEquals(0L, queue.getPrefetchIdleTimeMs());
      queue.drivePrefetchOnce();
      assertEquals(65_000L, queue.getMaxPrefetchDurationMs());
    } finally {
      releaseConversion.countDown();
      executor.shutdownNow();
      if (queue != null) {
        queue.close();
      }
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  private static void setTimeTracker(
      final ConsensusPrefetchingQueue queue, final SubscriptionQueueTimeTracker tracker)
      throws Exception {
    final Field field = ConsensusPrefetchingQueue.class.getDeclaredField("timeTracker");
    field.setAccessible(true);
    field.set(queue, tracker);
  }

  @Test
  public void testRealtimeBacklogIsDrainedBeforeMorePendingEntriesAreConverted() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final int originalBatchMaxDelay =
        CommonDescriptor.getInstance().getConfig().getSubscriptionConsensusBatchMaxDelayInMs();
    final File systemDir = temporaryFolder.newFolder("realtime-backlog-first");
    ConsensusPrefetchingQueue queue = null;
    try {
      CommonDescriptor.getInstance().getConfig().setSubscriptionConsensusBatchMaxDelayInMs(60_000);
      final FakeConsensusReqReader reader = new FakeConsensusReqReader();
      final AtomicInteger conversionCount = new AtomicInteger();
      queue =
          newQueue(
              "consumerGroup",
              new DataRegionId(1),
              reader,
              newConverter(conversionCount),
              newCommitManager(systemDir),
              TopicConstant.ORDER_MODE_MULTI_WRITER_VALUE);
      queue.setSubscriptionMemoryManager(
          new SubscriptionMemoryManager(
              PipeMemoryWeightUtil.calculateTabletSizeInBytes(createTablet()) * 4L));
      queue.setActiveWriterNodeIds(Set.of(7, 8));

      assertNull(queue.poll("consumer"));
      reader.currentSearchIndex = 2L;
      assertTrue(pendingEntries(queue).offer(createRequest(1L)));
      queue.drivePrefetchOnce();

      assertEquals(1, conversionCount.get());
      assertEquals(2L, queue.getCurrentReadSearchIndex());
      assertEquals("1", queue.coreReportMessage().get("bufferedRealtimeEntryCount"));
      assertEquals(0, queue.getPrefetchedEventCount());

      assertTrue(pendingEntries(queue).offer(createRequest(2L)));
      queue.drivePrefetchOnce();

      assertEquals(1, conversionCount.get());
      assertEquals(2L, queue.getCurrentReadSearchIndex());
      assertEquals("1", queue.coreReportMessage().get("bufferedRealtimeEntryCount"));
      assertEquals("1", queue.coreReportMessage().get("pendingEntriesSize"));
      assertEquals("true", queue.coreReportMessage().get("realtimeAdmissionBlocked"));
      assertFalse(pendingEntries(queue).offer(createRequest(3L)));
      assertEquals("SUBSCRIPTION_WRITER_BACKLOG", queue.getLastAdmissionRejectionCode());
      assertEquals(0L, queue.getSubscriptionMemoryRejectionCount());

      queue.setActiveWriterNodeIds(Collections.singleton(7));
      queue.drivePrefetchOnce();

      assertEquals(2, conversionCount.get());
      assertEquals(3L, queue.getCurrentReadSearchIndex());
      assertEquals("0", queue.coreReportMessage().get("bufferedRealtimeEntryCount"));
      assertEquals("0", queue.coreReportMessage().get("pendingEntriesSize"));
      assertEquals("false", queue.coreReportMessage().get("realtimeAdmissionBlocked"));
    } finally {
      if (queue != null) {
        queue.close();
      }
      CommonDescriptor.getInstance()
          .getConfig()
          .setSubscriptionConsensusBatchMaxDelayInMs(originalBatchMaxDelay);
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  private static ConsensusPrefetchingQueue newQueue(
      final String consumerGroupId,
      final DataRegionId regionId,
      final FakeConsensusReqReader reader,
      final ConsensusLogToTabletConverter converter,
      final ConsensusSubscriptionCommitManager commitManager,
      final String orderMode) {
    final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
    when(serverImpl.getConsensusReqReader()).thenReturn(reader);
    when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());
    return new ConsensusPrefetchingQueue(
        consumerGroupId,
        "topic",
        orderMode,
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
  }

  private static ConsensusLogToTabletConverter newConverter(final AtomicInteger conversionCount) {
    return newConverter(conversionCount, createTablet());
  }

  private static ConsensusLogToTabletConverter newConverter(
      final AtomicInteger conversionCount, final Tablet tablet) {
    final ConsensusLogToTabletConverter converter = mock(ConsensusLogToTabletConverter.class);
    when(converter.convert(any()))
        .thenAnswer(
            ignored -> {
              conversionCount.incrementAndGet();
              return Collections.singletonList(tablet);
            });
    when(converter.getDatabaseName()).thenReturn("db");
    return converter;
  }

  @SuppressWarnings("unchecked")
  private static BlockingQueue<IndexedConsensusRequest> pendingEntries(
      final ConsensusPrefetchingQueue queue) throws Exception {
    final Field field = ConsensusPrefetchingQueue.class.getDeclaredField("pendingEntries");
    field.setAccessible(true);
    return (BlockingQueue<IndexedConsensusRequest>) field.get(queue);
  }

  private static Tablet createTablet() {
    final List<String> columnNames = Arrays.asList("device", "temperature");
    final List<TSDataType> dataTypes = Arrays.asList(TSDataType.STRING, TSDataType.DOUBLE);
    final List<ColumnCategory> categories = Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD);
    final Tablet tablet = new Tablet("sensors", columnNames, dataTypes, categories, 1);
    tablet.addTimestamp(0, 1L);
    tablet.addValue(0, 0, "d1");
    tablet.addValue(0, 1, 36.5);
    tablet.setRowSize(1);
    return tablet;
  }

  private static Tablet createLargeTablet() {
    final Tablet tablet =
        new Tablet(
            "sensors",
            Arrays.asList("device", "payload"),
            Arrays.asList(TSDataType.STRING, TSDataType.STRING),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD),
            1);
    tablet.addTimestamp(0, 1L);
    tablet.addValue(0, 0, "d1");
    tablet.addValue(0, 1, "v".repeat(20_000));
    tablet.setRowSize(1);
    return tablet;
  }

  private static IndexedConsensusRequest createRequest(final long searchIndex) {
    return new IndexedConsensusRequest(
            searchIndex,
            Collections.singletonList(
                StatementTestUtils.genInsertRowNode(Math.toIntExact(searchIndex))))
        .setPhysicalTime(1000L + searchIndex)
        .setNodeId(7);
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
}
