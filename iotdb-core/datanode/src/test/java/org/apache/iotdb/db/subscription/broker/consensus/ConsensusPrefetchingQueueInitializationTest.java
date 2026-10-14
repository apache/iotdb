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
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.SubscriptionWalRetentionPolicy;
import org.apache.iotdb.consensus.iot.WriterSafeFrontierTracker;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementTestUtils;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALInfoEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALFileVersion;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALMetaData;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALWriter;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALByteBufferForTest;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsensusPrefetchingQueueInitializationTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final DataRegionId regionId = new DataRegionId(3);
  private final RegionProgress committedProgress =
      new RegionProgress(
          Collections.singletonMap(
              new WriterId(regionId.toString(), 5), new WriterProgress(1000L, 1L)));
  private final WALNode walNode = mock(WALNode.class);
  private final ConsensusLogToTabletConverter converter = mock(ConsensusLogToTabletConverter.class);
  private String originalSystemDir;
  private File walDirectory;
  private ConsensusPrefetchingQueue queue;

  @Before
  public void setUp() throws Exception {
    originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setSystemDir(temporaryFolder.newFolder("system").getAbsolutePath());
    walDirectory = temporaryFolder.newFolder("wal");
    when(walNode.getLogDirectory()).thenReturn(walDirectory);
    when(walNode.getCurrentSearchIndex()).thenReturn(3L);
    when(walNode.getCurrentWALFileVersion()).thenReturn(3L);
    when(walNode.getCurrentWALMetaDataSnapshot()).thenReturn(new WALMetaData());
    when(converter.convert(any())).thenReturn(Collections.emptyList());
    when(converter.isTableModel()).thenReturn(true);

    final IoTConsensusServerImpl server = mock(IoTConsensusServerImpl.class);
    when(server.getConsensusReqReader()).thenReturn(walNode);
    when(server.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());
    final ConsensusSubscriptionCommitManager commitManager =
        new ConsensusSubscriptionCommitManager(
            (consumerGroupId, topicName, ignored) ->
                ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.available(
                    committedProgress));
    queue =
        new ConsensusPrefetchingQueue(
            "consumerGroup",
            "topic",
            TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
            regionId,
            server,
            new SubscriptionWalRetentionPolicy(
                "topic",
                SubscriptionWalRetentionPolicy.UNBOUNDED,
                SubscriptionWalRetentionPolicy.UNBOUNDED),
            converter,
            commitManager,
            committedProgress,
            4L,
            1L,
            true);
  }

  @After
  public void tearDown() {
    try {
      if (queue != null) {
        queue.close();
      }
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testInitializationRetriesFileDisappearingDuringLookup() throws Exception {
    final File dataWal = walFile(1L);
    writeRequests(dataWal);
    try (WALWriter ignored = new WALWriter(walFile(3L), WALFileVersion.V3)) {
      // Keep the data file sealed, with a live successor in the directory listing.
    }
    final File hiddenWal = new File(temporaryFolder.getRoot(), "hidden.wal");
    final AtomicBoolean hideFile = new AtomicBoolean(true);
    when(walNode.getCurrentWALFileVersion())
        .thenAnswer(
            invocation -> {
              // The iterator has listed the old filename but has not opened it yet.
              if (hideFile.compareAndSet(true, false)) {
                Files.move(dataWal.toPath(), hiddenWal.toPath());
              }
              return 3L;
            });

    assertInitializationDeferred();
    Files.move(hiddenWal.toPath(), dataWal.toPath());
    assertResumesAfterCommittedProgress();
  }

  @Test
  public void testRepeatedUnreadablePollsKeepInitializationDormant() throws Exception {
    final File liveWal = walFile(3L);
    assertTrue(liveWal.mkdir());
    for (int attempt = 0; attempt < 3; attempt++) {
      assertInitializationDeferred();
      queue.drivePrefetchOnce();
      verify(converter, never()).convert(any());
    }

    Files.delete(liveWal.toPath());
    final WALMetaData metadata = writeRequests(liveWal);
    when(walNode.getCurrentWALMetaDataSnapshot()).thenReturn(metadata);
    assertResumesAfterCommittedProgress();
  }

  @Test
  public void testConfirmedRetainedCorruptionStillFailsInitialization() throws Exception {
    Files.write(walFile(1L).toPath(), new byte[128]);
    try (WALWriter ignored = new WALWriter(walFile(3L), WALFileVersion.V3)) {
      // Version 1 is retained history, outside the near-live retry window.
    }

    assertThrows(IllegalStateException.class, () -> queue.poll("consumer"));
    assertEquals(0L, queue.getInitializedStatus());
    assertEquals(4L, queue.getCurrentReadSearchIndex());
    verify(converter, never()).convert(any());
  }

  @Test
  public void testUnreadableLiveFileDoesNotMaskRetainedCorruption() throws Exception {
    Files.write(walFile(1L).toPath(), new byte[128]);
    assertTrue(walFile(3L).mkdir());

    assertThrows(IllegalStateException.class, () -> queue.poll("consumer"));
    assertEquals(0L, queue.getInitializedStatus());
    assertEquals(4L, queue.getCurrentReadSearchIndex());
    verify(converter, never()).convert(any());
  }

  private void assertInitializationDeferred() throws Exception {
    assertNull(queue.poll("consumer"));
    assertEquals(0L, queue.getInitializedStatus());
    assertEquals(4L, queue.getCurrentReadSearchIndex());
    assertEquals(0L, queue.getWalGapSkippedEntries());
    assertEquals(0L, queue.getUnreadableWalFileCount());
    assertEquals(0L, queue.getUnreadableWalEntryCount());
    verify(converter, never()).convert(any());
  }

  private void assertResumesAfterCommittedProgress() throws Exception {
    assertNull(queue.poll("consumer"));
    assertEquals(1L, queue.getInitializedStatus());
    assertEquals(2L, queue.getCurrentReadSearchIndex());
    queue.drivePrefetchOnce();
    final ArgumentCaptor<InsertNode> requests = ArgumentCaptor.forClass(InsertNode.class);
    verify(converter, times(2)).convert(requests.capture());
    assertEquals(
        Arrays.asList(2L, 3L),
        Arrays.asList(
            requests.getAllValues().get(0).getSearchIndex(),
            requests.getAllValues().get(1).getSearchIndex()));
    assertEquals(4L, queue.getCurrentReadSearchIndex());
    assertEquals(0L, queue.getWalGapSkippedEntries());
    assertEquals(0L, queue.getUnreadableWalFileCount());
    assertEquals(0L, queue.getUnreadableWalEntryCount());
  }

  private File walFile(final long versionId) {
    return new File(
        walDirectory,
        WALFileUtils.getLogFileName(versionId, 1L, WALFileStatus.CONTAINS_SEARCH_INDEX));
  }

  private static WALMetaData writeRequests(final File walFile) throws Exception {
    final WALMetaData metadata = new WALMetaData();
    try (WALWriter writer = new WALWriter(walFile, WALFileVersion.V3)) {
      for (long searchIndex = 1L; searchIndex <= 3L; searchIndex++) {
        final RelationalInsertRowNode node =
            StatementTestUtils.genInsertRowNode(Math.toIntExact(searchIndex));
        node.setSearchIndex(searchIndex);
        final WALInfoEntry entry = new WALInfoEntry(1L, node);
        final ByteBuffer buffer = ByteBuffer.allocate(entry.serializedSize());
        entry.serialize(new WALByteBufferForTest(buffer));
        final WALMetaData entryMetadata = new WALMetaData();
        entryMetadata.add(entry.serializedSize(), searchIndex, 1L, 1000L, 5, searchIndex);
        writer.write(buffer, entryMetadata);
        metadata.addAll(entryMetadata);
      }
    }
    return metadata;
  }
}
