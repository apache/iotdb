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

package org.apache.iotdb.db.storageengine.dataregion.wal.checkpoint;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALBuffer;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALInfoEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.CheckpointWriter;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALMetaData;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALWriter;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.CheckpointRecoverUtils;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.WALNodeRecoverTask;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.WALRecoverManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.file.UnsealedTsFileRecoverPerformer;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.CheckpointFileUtils;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALByteBufferForTest;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.WALFlushListener;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.WALRecoverListener;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.db.storageengine.dataregion.wal.WALTestUtils.getInsertRowNode;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CheckpointRollRecoveryTest {
  private static final String DEVICE = "root.recovery.d1";

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
  private final WALRecoverManager recovery = WALRecoverManager.getInstance();
  private long previousThreshold;
  private String previousConsensus;
  private NodeStatus previousStatus;
  private String previousReason;
  private File directory;

  @Before
  public void setUp() throws IOException {
    previousThreshold = config.getCheckpointFileSizeThresholdInByte();
    previousConsensus = config.getDataRegionConsensusProtocolClass();
    previousStatus = commonConfig.getNodeStatus();
    previousReason = commonConfig.getStatusReason();
    config.setCheckpointFileSizeThresholdInByte(Long.MAX_VALUE);
    config.setDataRegionConsensusProtocolClass(ConsensusFactory.SIMPLE_CONSENSUS);
    commonConfig.setNodeStatus(NodeStatus.Running);
    recovery.clear();
    directory = folder.newFolder("wal");
  }

  @After
  public void tearDown() {
    recovery.clear();
    config.setCheckpointFileSizeThresholdInByte(previousThreshold);
    config.setDataRegionConsensusProtocolClass(previousConsensus);
    commonConfig.setNodeStatus(NodeStatus.Running);
    commonConfig.setNodeStatusWithReason(previousStatus, previousReason);
  }

  @Test
  public void testCandidateOpenFailureKeepsCheckpointForReplay() throws Exception {
    assertFailedRollKeepsCheckpoint(FailedOperation.OPEN);
  }

  @Test
  public void testMaxMemTableIdWriteFailureKeepsCheckpointForReplay() throws Exception {
    assertFailedRollKeepsCheckpoint(FailedOperation.WRITE_MAX_ID);
  }

  @Test
  public void testGlobalSnapshotWriteFailureKeepsCheckpointForReplay() throws Exception {
    assertFailedRollKeepsCheckpoint(FailedOperation.WRITE_SNAPSHOT);
  }

  @Test
  public void testCandidateForceFailureKeepsCheckpointForReplay() throws Exception {
    assertFailedRollKeepsCheckpoint(FailedOperation.FORCE);
  }

  @Test
  public void testCandidatePublishFailureKeepsCheckpointForReplay() throws Exception {
    assertFailedRollKeepsCheckpoint(FailedOperation.PUBLISH);
  }

  @Test
  public void testDirectorySyncFailureRollsBackAndKeepsCheckpointForReplay() throws Exception {
    assertFailedRollKeepsCheckpoint(FailedOperation.SYNC_DIRECTORY);
  }

  private void assertFailedRollKeepsCheckpoint(FailedOperation operation) throws Exception {
    File oldFile = checkpointFile(0);
    MemTableInfo table = memTable(1);
    try (CheckpointManager manager =
        spy(new CheckpointManager("checkpoint-roll", directory.getPath()))) {
      addCheckpoint(manager, table);
      manager.fsyncCheckpointFile();
      writeWalEntry(table, 10);
      byte[] original = Files.readAllBytes(oldFile.toPath());

      failCandidate(manager, operation);
      config.setCheckpointFileSizeThresholdInByte(original.length);
      manager.fsyncCheckpointFile();
      assertArrayEquals(original, Files.readAllBytes(oldFile.toPath()));
      assertArrayEquals(
          new File[] {oldFile}, directory.listFiles((dir, name) -> !name.endsWith(".wal")));
      assertEquals(NodeStatus.ReadOnly, commonConfig.getNodeStatus());

      // A later checkpoint write must still reach the old writer.
      config.setCheckpointFileSizeThresholdInByte(Long.MAX_VALUE);
      addCheckpoint(manager, memTable(2));
      manager.fsyncCheckpointFile();
      assertEquals(2, CheckpointRecoverUtils.recoverMemTableInfo(directory).getMaxMemTableId());
    }
    assertReplay(table, 10);
  }

  private void failCandidate(CheckpointManager manager, FailedOperation operation)
      throws IOException {
    IOException failure = new IOException("injected checkpoint failure: " + operation);
    doAnswer(
            invocation -> {
              if (operation == FailedOperation.OPEN) {
                throw failure;
              }
              File file = invocation.getArgument(0);
              if (operation == FailedOperation.SYNC_DIRECTORY) {
                return writerWithDirectorySyncFailure(file, failure, null);
              }
              CheckpointWriter candidate = spy(new CheckpointWriter(file));
              switch (operation) {
                case WRITE_MAX_ID:
                  doThrow(failure).when(candidate).write(any(ByteBuffer.class));
                  break;
                case WRITE_SNAPSHOT:
                  doCallRealMethod().doThrow(failure).when(candidate).write(any(ByteBuffer.class));
                  break;
                case FORCE:
                  doThrow(failure).when(candidate).force();
                  break;
                case PUBLISH:
                  doThrow(failure).when(candidate).publish(any(File.class));
                  break;
                default:
                  throw new AssertionError(operation);
              }
              return candidate;
            })
        .when(manager)
        .openCheckpointWriter(any(File.class));
  }

  @Test(timeout = 30000)
  public void testFailedPublicationRollbackStopsWalWrites() throws Exception {
    File oldFile = checkpointFile(0);
    File nextFile = checkpointFile(1);
    MemTableInfo table = memTable(1);
    IOException syncFailure = new IOException("injected directory sync failure");
    IOException rollbackFailure = new IOException("injected rollback rename failure");
    try (CheckpointManager manager =
            spy(new CheckpointManager("checkpoint-roll", directory.getPath()));
        WALBuffer buffer = new WALBuffer("checkpoint-roll", directory.getPath(), manager, 0, 0)) {
      addCheckpoint(manager, table);
      manager.fsyncCheckpointFile();
      assertEquals(WALFlushListener.Status.SUCCESS, writeWalRow(buffer, 1, 10).waitForResult());
      long walSize = buffer.getCurrentWALOriginalFileSize();
      byte[] oldBytes = Files.readAllBytes(oldFile.toPath());

      doAnswer(
              invocation ->
                  writerWithDirectorySyncFailure(
                      invocation.getArgument(0), syncFailure, rollbackFailure))
          .when(manager)
          .openCheckpointWriter(any(File.class));
      config.setCheckpointFileSizeThresholdInByte(oldBytes.length);
      manager.fsyncCheckpointFile();
      assertEquals(NodeStatus.ReadOnly, commonConfig.getNodeStatus());
      assertTrue(nextFile.exists());
      byte[] nextBytes = Files.readAllBytes(nextFile.toPath());

      commonConfig.setNodeStatus(NodeStatus.Running);
      addCheckpoint(manager, memTable(2));
      WALFlushListener listener = writeWalRow(buffer, 2, 20);
      assertEquals(WALFlushListener.Status.FAILURE, listener.waitForResult());
      assertSame(syncFailure, listener.getCause());
      assertArrayEquals(new Throwable[] {rollbackFailure}, syncFailure.getSuppressed());
      assertEquals(walSize, buffer.getCurrentWALOriginalFileSize());
      assertArrayEquals(oldBytes, Files.readAllBytes(oldFile.toPath()));
      assertArrayEquals(nextBytes, Files.readAllBytes(nextFile.toPath()));
    }
    assertReplay(table, 10);
  }

  @Test(timeout = 30000)
  public void testActiveCheckpointForceFailureFailsWalBatch() throws Exception {
    assertActiveCheckpointFailureFailsWalBatch(true);
  }

  @Test(timeout = 30000)
  public void testActiveCheckpointWriteFailureFailsWalBatch() throws Exception {
    assertActiveCheckpointFailureFailsWalBatch(false);
  }

  private void assertActiveCheckpointFailureFailsWalBatch(boolean failForce) throws Exception {
    MemTableInfo first = memTable(1);
    File activeFile = checkpointFile(1);
    byte[] durableCheckpoint;
    try (CheckpointManager manager =
            spy(new CheckpointManager("checkpoint-roll", directory.getPath()));
        WALBuffer buffer = new WALBuffer("checkpoint-roll", directory.getPath(), manager, 0, 0)) {
      // Publish a real successor and capture its writer so the fault affects the active file.
      AtomicReference<CheckpointWriter> rolledWriter = new AtomicReference<>();
      doAnswer(
              invocation -> {
                CheckpointWriter writer = spy(new CheckpointWriter(invocation.getArgument(0)));
                rolledWriter.set(writer);
                return writer;
              })
          .when(manager)
          .openCheckpointWriter(any(File.class));
      config.setCheckpointFileSizeThresholdInByte(1);
      addCheckpoint(manager, first);
      manager.fsyncCheckpointFile();
      CheckpointWriter active = rolledWriter.get();
      assertEquals(activeFile, active.getLogFile());
      durableCheckpoint = Files.readAllBytes(activeFile.toPath());
      assertEquals(WALFlushListener.Status.SUCCESS, writeWalRow(buffer, 1, 10).waitForResult());
      long walSize = buffer.getCurrentWALOriginalFileSize();

      IOException failure = new IOException("injected active checkpoint I/O failure");
      if (failForce) {
        doThrow(failure).doCallRealMethod().when(active).force();
      } else {
        doThrow(failure).when(active).write(any(ByteBuffer.class));
      }
      MemTableInfo second = memTable(2);
      manager.makeCreateMemTableCPInMemory(second);
      buffer.write(
          new WALInfoEntry(2, new Checkpoint(CheckpointType.CREATE_MEMORY_TABLE, List.of(second))));
      WALFlushListener listener = writeWalRow(buffer, 2, 20);
      assertEquals(WALFlushListener.Status.FAILURE, listener.waitForResult());
      assertSame(failure, listener.getCause());
      assertEquals(walSize, buffer.getCurrentWALOriginalFileSize());
      assertFalse(checkpointFile(2).exists());
    }

    // Recover from the bytes that were durable before the active checkpoint failed.
    Files.write(activeFile.toPath(), durableCheckpoint);
    assertReplay(first, 10);
  }

  private CheckpointWriter writerWithDirectorySyncFailure(
      File candidate, IOException syncFailure, IOException rollbackFailure) throws IOException {
    return new CheckpointWriter(candidate) {
      @Override
      public void publish(File target) throws IOException {
        if (target.equals(candidate) && rollbackFailure != null) {
          throw rollbackFailure;
        }
        super.publish(target);
      }

      @Override
      protected void forceParentDirectory(Path parent) throws IOException {
        if (getLogFile().equals(checkpointFile(1))) {
          assertTrue(checkpointFile(0).isFile());
          assertTrue(checkpointFile(1).isFile());
          throw syncFailure;
        }
        super.forceParentDirectory(parent);
      }
    };
  }

  @Test
  public void testRollWithOversizedSnapshotThenAppendAndRecover() throws Exception {
    MemTableInfo first = memTable(1);
    MemTableInfo second = memTable(2);
    config.setCheckpointFileSizeThresholdInByte(1);
    try (CheckpointManager manager =
        new CheckpointManager("checkpoint-roll", directory.getPath())) {
      assertTrue(checkpointFile(0).length() > 1);
      assertFalse(checkpointFile(1).exists());
      addCheckpoint(manager, first);
      manager.fsyncCheckpointFile();
      manager.fsyncCheckpointFile();
      assertFalse(checkpointFile(0).exists());
      assertTrue(checkpointFile(1).length() > 1);
      assertFalse(checkpointFile(2).exists());

      config.setCheckpointFileSizeThresholdInByte(Long.MAX_VALUE);
      addCheckpoint(manager, second);
      manager.fsyncCheckpointFile();
      CheckpointRecoverUtils.CheckpointInfo recovered =
          CheckpointRecoverUtils.recoverMemTableInfo(directory);
      assertEquals(2, recovered.getMaxMemTableId());
      assertEquals(Set.of(1L, 2L), recovered.getMemTableId2Info().keySet());
      writeWalEntry(second, 20);
    }
    assertReplay(second, 20);
  }

  @Test
  public void testRecoveryReplaysLegacyIncrementsAfterSnapshotAndSkipsCandidate() throws Exception {
    MemTableInfo first = memTable(1);
    MemTableInfo flushed = memTable(2);
    MemTableInfo latest = memTable(3);
    writeCheckpointFile(
        checkpointFile(0),
        2,
        new Checkpoint(CheckpointType.GLOBAL_MEMORY_TABLE_INFO, List.of(first, flushed)));
    // A legacy roll can contain a create or flush record even if its snapshot write failed.
    writeCheckpointFile(
        checkpointFile(1),
        2,
        new Checkpoint(CheckpointType.CREATE_MEMORY_TABLE, Collections.singletonList(latest)));
    writeCheckpointFile(
        checkpointFile(2),
        3,
        new Checkpoint(CheckpointType.FLUSH_MEMORY_TABLE, Collections.singletonList(flushed)));
    // Simulate interruption before publishing a candidate with an empty global snapshot.
    writeCheckpointFile(
        CheckpointFileUtils.createCheckpointCandidate(checkpointFile(3)),
        100,
        new Checkpoint(CheckpointType.GLOBAL_MEMORY_TABLE_INFO, Collections.emptyList()));
    writeWalEntry(latest, 30);
    CheckpointRecoverUtils.CheckpointInfo recovered =
        CheckpointRecoverUtils.recoverMemTableInfo(directory);
    assertEquals(3, recovered.getMaxMemTableId());
    assertEquals(Set.of(1L, 3L), recovered.getMemTableId2Info().keySet());
    assertReplay(latest, 30);
  }

  @Test
  public void testRecoveryReplaysLegacyIncrementsWithoutSnapshot() throws Exception {
    MemTableInfo first = memTable(1);
    writeCheckpointFile(
        checkpointFile(1),
        0,
        new Checkpoint(CheckpointType.CREATE_MEMORY_TABLE, Collections.singletonList(first)));
    writeCheckpointFile(
        checkpointFile(2),
        1,
        new Checkpoint(CheckpointType.CREATE_MEMORY_TABLE, Collections.singletonList(memTable(2))));
    writeWalEntry(first, 10);
    CheckpointRecoverUtils.CheckpointInfo recovered =
        CheckpointRecoverUtils.recoverMemTableInfo(directory);
    assertEquals(2, recovered.getMaxMemTableId());
    assertEquals(Set.of(1L, 2L), recovered.getMemTableId2Info().keySet());
    assertReplay(first, 10);
  }

  @Test
  public void testRecoveryReplaysLegacyIncrementsAfterMissingVersion() throws Exception {
    assertRecoveryReplaysLegacyIncrementsAfterGap(false);
  }

  @Test
  public void testRecoveryReplaysLegacyIncrementsAfterUnreadableVersion() throws Exception {
    assertRecoveryReplaysLegacyIncrementsAfterGap(true);
  }

  private void assertRecoveryReplaysLegacyIncrementsAfterGap(boolean keepEmptyFile)
      throws Exception {
    MemTableInfo stale = memTable(1);
    MemTableInfo replacement = memTable(2, stale.getTsFilePath());
    writeCheckpointFile(
        checkpointFile(0),
        1,
        new Checkpoint(CheckpointType.GLOBAL_MEMORY_TABLE_INFO, List.of(stale)));
    // The checkpoint that flushed the old memtable is missing or has no readable records.
    if (keepEmptyFile) {
      Files.createFile(checkpointFile(1).toPath());
    }
    writeCheckpointFile(
        checkpointFile(2),
        2,
        new Checkpoint(CheckpointType.CREATE_MEMORY_TABLE, List.of(replacement)));
    writeCheckpointFile(
        checkpointFile(3),
        3,
        new Checkpoint(CheckpointType.CREATE_MEMORY_TABLE, List.of(memTable(3))));
    Files.createFile(checkpointFile(5).toPath());
    writeWalEntry(replacement, 20);

    CheckpointRecoverUtils.CheckpointInfo recovered =
        CheckpointRecoverUtils.recoverMemTableInfo(directory);
    assertReplay(replacement, 20);
    assertEquals(3, recovered.getMaxMemTableId());
    assertEquals(Set.of(2L, 3L), recovered.getMemTableId2Info().keySet());
  }

  private File checkpointFile(long version) {
    return new File(directory, CheckpointFileUtils.getLogFileName(version));
  }

  private MemTableInfo memTable(long id) {
    File tsFile = new File(folder.getRoot(), "sequence/root.recovery/0/0/" + id + "-1-0-0.tsfile");
    return memTable(id, tsFile.getAbsolutePath());
  }

  private MemTableInfo memTable(long id, String tsFilePath) {
    IMemTable table = mock(IMemTable.class);
    when(table.getMemTableId()).thenReturn(id);
    when(table.getDataRegionId()).thenReturn("0");
    return new MemTableInfo(table, tsFilePath, 0);
  }

  private void addCheckpoint(CheckpointManager manager, MemTableInfo table) throws IOException {
    manager.makeCreateMemTableCPInMemory(table);
    manager.makeCreateMemTableCPOnDisk(table.getMemTableId());
  }

  private WALFlushListener writeWalRow(WALBuffer buffer, long memTableId, long time)
      throws Exception {
    WALInfoEntry entry = new WALInfoEntry(memTableId, getInsertRowNode(DEVICE, time), true);
    buffer.write(entry);
    return entry.getWalFlushListener();
  }

  private void writeCheckpointFile(File file, long maxMemTableId, Checkpoint checkpoint)
      throws IOException {
    try (CheckpointWriter writer = new CheckpointWriter(file)) {
      ByteBuffer data = ByteBuffer.allocate(Long.BYTES + checkpoint.serializedSize());
      data.putLong(maxMemTableId);
      checkpoint.serialize(data);
      writer.write(data);
    }
  }

  private void writeWalEntry(MemTableInfo table, long time) throws Exception {
    WALEntry entry = new WALInfoEntry(table.getMemTableId(), getInsertRowNode(DEVICE, time));
    ByteBuffer data = ByteBuffer.allocate(entry.serializedSize());
    entry.serialize(new WALByteBufferForTest(data));
    WALMetaData metadata = new WALMetaData();
    metadata.add(data.position(), -1, table.getMemTableId());
    try (WALWriter writer = new WALWriter(new File(directory, "_0-0-0.wal"))) {
      writer.write(data, metadata);
    }
  }

  private void assertReplay(MemTableInfo table, long time) throws Exception {
    File tsFile = new File(table.getTsFilePath());
    UnsealedTsFileRecoverPerformer performer = mock(UnsealedTsFileRecoverPerformer.class);
    WALRecoverListener listener = new WALRecoverListener(tsFile.getAbsolutePath());
    when(performer.getTsFileResource()).thenReturn(new TsFileResource(tsFile));
    when(performer.getRecoverListener()).thenReturn(listener);
    recovery.addRecoverPerformer(performer);

    CountDownLatch latch = new CountDownLatch(1);
    new WALNodeRecoverTask(directory, latch).run();
    assertEquals(0, latch.getCount());
    ArgumentCaptor<WALEntry> replay = ArgumentCaptor.forClass(WALEntry.class);
    verify(performer).redoLog(replay.capture());
    assertEquals(WALRecoverListener.Status.SUCCESS, listener.waitForResult());
    assertEquals(table.getMemTableId(), replay.getValue().getMemTableId());
    assertEquals(time, ((InsertRowNode) replay.getValue().getValue()).getTime());
  }

  private enum FailedOperation {
    OPEN,
    WRITE_MAX_ID,
    WRITE_SNAPSHOT,
    FORCE,
    PUBLISH,
    SYNC_DIRECTORY
  }
}
