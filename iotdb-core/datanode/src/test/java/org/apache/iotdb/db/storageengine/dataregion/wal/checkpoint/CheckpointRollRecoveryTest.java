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
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.apache.iotdb.db.storageengine.dataregion.wal.WALTestUtils.getInsertRowNode;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
              CheckpointWriter candidate = spy(new CheckpointWriter(invocation.getArgument(0)));
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
  public void testRecoverySkipsUnpublishedCandidateAndIncompleteLegacyCheckpoint()
      throws Exception {
    MemTableInfo table = memTable(1);
    try (CheckpointManager manager =
        new CheckpointManager("checkpoint-roll", directory.getPath())) {
      addCheckpoint(manager, table);
      manager.fsyncCheckpointFile();
    }
    writeWalEntry(table, 10);
    // An older writer could append incremental records after failing to write its snapshot.
    writeCheckpointFile(
        checkpointFile(1),
        new Checkpoint(CheckpointType.CREATE_MEMORY_TABLE, Collections.singletonList(memTable(2))));
    // Simulate interruption before publishing a candidate with an empty global snapshot.
    writeCheckpointFile(
        new File(directory, checkpointFile(2).getName() + ".tmp"),
        new Checkpoint(CheckpointType.GLOBAL_MEMORY_TABLE_INFO, Collections.emptyList()));
    assertEquals(1, CheckpointRecoverUtils.recoverMemTableInfo(directory).getMaxMemTableId());
    assertReplay(table, 10);
  }

  private File checkpointFile(long version) {
    return new File(directory, CheckpointFileUtils.getLogFileName(version));
  }

  private MemTableInfo memTable(long id) {
    IMemTable table = mock(IMemTable.class);
    when(table.getMemTableId()).thenReturn(id);
    when(table.getDataRegionId()).thenReturn("0");
    File tsFile = new File(folder.getRoot(), "sequence/root.recovery/0/0/" + id + "-1-0-0.tsfile");
    return new MemTableInfo(table, tsFile.getAbsolutePath(), 0);
  }

  private void addCheckpoint(CheckpointManager manager, MemTableInfo table) {
    manager.makeCreateMemTableCPInMemory(table);
    manager.makeCreateMemTableCPOnDisk(table.getMemTableId());
  }

  private void writeCheckpointFile(File file, Checkpoint checkpoint) throws IOException {
    try (CheckpointWriter writer = new CheckpointWriter(file)) {
      ByteBuffer data = ByteBuffer.allocate(Long.BYTES + checkpoint.serializedSize());
      data.putLong(100);
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
    PUBLISH
  }
}
