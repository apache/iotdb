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
package org.apache.iotdb.db.storageengine.dataregion.wal.recover;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALInfoEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.checkpoint.Checkpoint;
import org.apache.iotdb.db.storageengine.dataregion.wal.checkpoint.CheckpointType;
import org.apache.iotdb.db.storageengine.dataregion.wal.checkpoint.MemTableInfo;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALRecoverException;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.CheckpointWriter;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALMetaData;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALWriter;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.file.UnsealedTsFileRecoverPerformer;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.CheckpointFileUtils;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALByteBufferForTest;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALMode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.WALRecoverListener;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.write.schema.MeasurementSchema;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.apache.iotdb.db.storageengine.dataregion.wal.WALTestUtils.getInsertRowNode;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WALNodeRecoverTaskTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();
  private final WALRecoverManager recovery = WALRecoverManager.getInstance();
  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private String previousConsensus;

  @Before
  public void setUp() throws Exception {
    previousConsensus = config.getDataRegionConsensusProtocolClass();
    config.setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    EnvironmentUtils.envSetUp();
    recovery.clear();
    config.setDataRegionConsensusProtocolClass(ConsensusFactory.SIMPLE_CONSENSUS);
  }

  @After
  public void tearDown() throws Exception {
    recovery.clear();
    config.setDataRegionConsensusProtocolClass(previousConsensus);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testInitializationIoFailureKeepsLogsAndAllowsRetry() throws Exception {
    assertFailedRecovery(new IOException("injected initialization failure"), false, true);
  }

  @Test
  public void testInitializationRuntimeFailureKeepsLogsAndAllowsRetry() throws Exception {
    assertFailedRecovery(new IllegalStateException("injected provider failure"), false, true);
  }

  @Test
  public void testFinalizationFailureKeepsLogsAndAllowsRetry() throws Exception {
    assertFailedRecovery(
        new WALRecoverException(
            "injected flush failure", new IOException("injected flush failure")),
        true,
        true);
  }

  @Test
  public void testIoTConsensusFailureKeepsCheckpoint() throws Exception {
    config.setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    assertFailedRecovery(new IOException("injected initialization failure"), false, true);
  }

  @Test
  public void testIoTConsensusV2FailureKeepsLogs() throws Exception {
    config.setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS_V2);
    assertFailedRecovery(new IOException("injected initialization failure"), false, true);
  }

  @Test
  public void testIoTConsensusFinalizationFailureBlocksNodeReuse() throws Exception {
    config.setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    assertFailedRecovery(
        new WALRecoverException(
            "injected flush failure", new IOException("injected flush failure")),
        true,
        false);
  }

  @Test(timeout = 30000)
  public void testRecoveryReplaysAllMemTablesForSameTsFile() throws Exception {
    File node = folder.newFolder("node");
    String relativePath = "sequence/root.recovery/0/0/1-1-0-0.tsfile";
    File tsFile = new File(folder.getRoot(), relativePath);
    // A legacy checkpoint may still refer to the file's previous data directory.
    File previousPath = new File(folder.getRoot(), "previous/" + relativePath);
    Files.createDirectories(tsFile.toPath().getParent());
    Files.createFile(tsFile.toPath());
    File oldCheckpoint = new File(node, "_0.checkpoint");
    File nextCheckpoint = new File(node, "_1.checkpoint");
    writeCheckpoint(
        oldCheckpoint,
        1,
        new Checkpoint(
            CheckpointType.GLOBAL_MEMORY_TABLE_INFO, List.of(memTable(1, previousPath, 0))));
    writeCheckpoint(
        nextCheckpoint,
        2,
        new Checkpoint(CheckpointType.CREATE_MEMORY_TABLE, List.of(memTable(2, tsFile, 0))));
    String device = "root.recovery.d1";
    File wal = new File(node, "_0-0-0.wal");
    writeWalEntries(
        wal,
        List.of(
            new WALInfoEntry(1, getInsertRowNode(device, 10)),
            new WALInfoEntry(2, getInsertRowNode(device, 20))));

    UnsealedTsFileRecoverPerformer failed = register(tsFile);
    IOException failure = new IOException("injected initialization failure");
    doThrow(failure).when(failed).startRecovery();
    new WALNodeRecoverTask(node, new CountDownLatch(1)).run();
    assertEquals(WALRecoverListener.Status.FAILURE, failed.getRecoverListener().waitForResult());
    assertSame(failure, failed.getRecoverListener().getCause());
    verify(failed).startRecovery();
    verify(failed, never()).redoLog(any());
    verify(failed, never()).endRecovery();
    verify(failed).close();
    assertTrue(oldCheckpoint.exists());
    assertTrue(nextCheckpoint.exists());
    assertTrue(wal.exists());

    UnsealedTsFileRecoverPerformer performer =
        spy(new UnsealedTsFileRecoverPerformer(new TsFileResource(tsFile), true, false, p -> {}));
    WALRecoverListener listener = recovery.addRecoverPerformer(performer);
    new WALNodeRecoverTask(node, new CountDownLatch(1)).run();
    assertEquals(WALRecoverListener.Status.SUCCESS, listener.waitForResult());
    verify(performer).startRecovery();
    verify(performer).endRecovery();
    verify(performer).close();
    assertFalse(node.exists());
    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      Statistics<?> statistics =
          reader
              .readTimeseriesMetadata(IDeviceID.Factory.DEFAULT_FACTORY.create(device), "s1", true)
              .getStatistics();
      assertEquals(2, statistics.getCount());
      assertEquals(10, statistics.getStartTime());
      assertEquals(20, statistics.getEndTime());
    }
  }

  private void assertFailedRecovery(Exception failure, boolean finalization, boolean retry)
      throws Exception {
    File node = folder.newFolder("node");
    boolean stableNode =
        config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS)
            || config
                .getDataRegionConsensusProtocolClass()
                .equals(ConsensusFactory.IOT_CONSENSUS_V2);
    long firstVersion = stableNode ? 7 : 0;
    List<File> files = new ArrayList<>();
    List<UnsealedTsFileRecoverPerformer> performers = new ArrayList<>();
    List<MemTableInfo> tables = new ArrayList<>();
    List<WALEntry> entries = new ArrayList<>();
    for (long id = 1; id <= 2; id++) {
      File file = new File(folder.getRoot(), "sequence/root.recovery/0/0/" + id + "-1-0-0.tsfile");
      files.add(file);
      performers.add(register(file));
      tables.add(memTable(id, file, firstVersion));
      InsertRowNode row =
          new InsertRowNode(
              new PlanNodeId(""),
              new PartialPath("root.recovery.d1"),
              false,
              new String[] {"s1"},
              new TSDataType[] {TSDataType.INT64},
              id,
              new Object[] {id * 10},
              false);
      row.setMeasurementSchemas(
          new MeasurementSchema[] {new MeasurementSchema("s1", TSDataType.INT64)});
      if (stableNode) {
        row.setSearchIndex(10 + id);
      }
      entries.add(new WALInfoEntry(id, row));
    }
    File checkpointFile = new File(node, "_0.checkpoint");
    writeCheckpoint(
        checkpointFile, 2, new Checkpoint(CheckpointType.GLOBAL_MEMORY_TABLE_INFO, tables));
    File wal = new File(node, stableNode ? "_7-11-1.wal" : "_0-0-0.wal");
    writeWalEntries(wal, entries);
    byte[] walBefore = Files.readAllBytes(wal.toPath());
    byte[] checkpointBefore = Files.readAllBytes(checkpointFile.toPath());
    File candidate = CheckpointFileUtils.createCheckpointCandidate(new File(node, "_1.checkpoint"));
    Files.writeString(candidate.toPath(), "V99-WAL");
    File unrelatedTemp = new File(node, "_1.checkpoint.backup.tmp");
    Files.createFile(unrelatedTemp.toPath());
    UnsealedTsFileRecoverPerformer failed = performers.get(0);
    when(failed.canWrite()).thenReturn(true);
    if (finalization) {
      doThrow(failure).when(failed).endRecovery();
    } else {
      doThrow(failure).when(failed).startRecovery();
    }
    CountDownLatch latch = new CountDownLatch(1);
    new WALNodeRecoverTask(node, latch).run();
    assertEquals(0, latch.getCount());
    assertEquals(WALRecoverListener.Status.FAILURE, failed.getRecoverListener().waitForResult());
    assertSame(failure, failed.getRecoverListener().getCause());
    verify(failed).close();
    if (!finalization) {
      verify(failed, never()).redoLog(any());
      verify(failed, never()).endRecovery();
    }
    UnsealedTsFileRecoverPerformer healthy = performers.get(1);
    assertEquals(WALRecoverListener.Status.SUCCESS, healthy.getRecoverListener().waitForResult());
    ArgumentCaptor<WALEntry> replay = ArgumentCaptor.forClass(WALEntry.class);
    verify(healthy).redoLog(replay.capture());
    assertEquals(2, replay.getValue().getMemTableId());
    verify(healthy).endRecovery();
    assertArrayEquals(walBefore, Files.readAllBytes(wal.toPath()));
    assertArrayEquals(checkpointBefore, Files.readAllBytes(checkpointFile.toPath()));
    assertTrue(candidate.exists());
    if (stableNode) {
      WALManager manager = WALManager.getInstance();
      for (int i = 0; i < 3; i++) {
        IllegalStateException allocation =
            assertThrows(
                IllegalStateException.class, () -> manager.applyForWALNode(node.getName()));
        assertSame(failure, allocation.getCause());
        IllegalStateException registration =
            assertThrows(
                IllegalStateException.class,
                () -> manager.registerWALNode(node.getName(), node.getAbsolutePath(), 8, 12));
        assertSame(failure, registration.getCause());
      }
      WALMode previousMode = config.getWalMode();
      try {
        config.setWalMode(WALMode.DISABLE);
        assertThrows(IllegalStateException.class, () -> manager.applyForWALNode(node.getName()));
      } finally {
        config.setWalMode(previousMode);
      }
      assertArrayEquals(walBefore, Files.readAllBytes(wal.toPath()));
      assertArrayEquals(checkpointBefore, Files.readAllBytes(checkpointFile.toPath()));
      assertEquals(
          new HashSet<>(Arrays.asList(1L, 2L)),
          CheckpointRecoverUtils.recoverMemTableInfo(node).getMemTableId2Info().keySet());
      assertTrue(manager.applyForWALNode("unrelated-healthy-node") instanceof WALNode);
    }
    if (retry) {
      if (stableNode) {
        // A fresh WAL service must recover the retained state before accepting this identifier.
        WALManager.getInstance().clear();
      }
      recovery.clear();
      List<UnsealedTsFileRecoverPerformer> retried = new ArrayList<>();
      for (File file : files) {
        retried.add(register(file));
      }
      CountDownLatch retryLatch = new CountDownLatch(1);
      new WALNodeRecoverTask(node, retryLatch).run();
      assertEquals(0, retryLatch.getCount());
      assertFalse(candidate.exists());
      if (config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS)) {
        assertTrue(unrelatedTemp.exists());
      }
      for (UnsealedTsFileRecoverPerformer performer : retried) {
        assertEquals(
            WALRecoverListener.Status.SUCCESS, performer.getRecoverListener().waitForResult());
        verify(performer).redoLog(any());
        verify(performer).endRecovery();
      }
      if (stableNode) {
        WALNode restored = (WALNode) WALManager.getInstance().applyForWALNode(node.getName());
        assertEquals(8, restored.getCurrentWALFileVersion());
        assertEquals(12, restored.getCurrentSearchIndex());
      } else {
        assertFalse(node.exists());
      }
      assertEquals(WALRecoverListener.Status.FAILURE, failed.getRecoverListener().waitForResult());
    } else {
      assertTrue(node.isDirectory());
    }
  }

  private static MemTableInfo memTable(long id, File tsFile, long firstVersion) {
    IMemTable table = mock(IMemTable.class);
    when(table.getMemTableId()).thenReturn(id);
    when(table.getDataRegionId()).thenReturn("0");
    return new MemTableInfo(table, tsFile.getAbsolutePath(), firstVersion);
  }

  private static void writeCheckpoint(File file, long maxMemTableId, Checkpoint checkpoint)
      throws IOException {
    ByteBuffer bytes = ByteBuffer.allocate(Long.BYTES + checkpoint.serializedSize());
    bytes.putLong(maxMemTableId);
    checkpoint.serialize(bytes);
    try (CheckpointWriter writer = new CheckpointWriter(file)) {
      writer.write(bytes, false);
    }
  }

  private static void writeWalEntries(File wal, List<WALEntry> entries) throws IOException {
    try (WALWriter writer = new WALWriter(wal)) {
      for (WALEntry entry : entries) {
        ByteBuffer data = ByteBuffer.allocate(entry.serializedSize());
        entry.serialize(new WALByteBufferForTest(data));
        WALMetaData metadata = new WALMetaData();
        metadata.add(
            data.position(),
            ((InsertRowNode) entry.getValue()).getSearchIndex(),
            entry.getMemTableId());
        writer.write(data, metadata);
      }
    }
  }

  private UnsealedTsFileRecoverPerformer register(File file) {
    UnsealedTsFileRecoverPerformer performer = mock(UnsealedTsFileRecoverPerformer.class);
    WALRecoverListener listener = new WALRecoverListener(file.getAbsolutePath());
    when(performer.getTsFileResource()).thenReturn(new TsFileResource(file));
    when(performer.getTsFileAbsolutePath()).thenReturn(file.getAbsolutePath());
    when(performer.getRecoverListener()).thenReturn(listener);
    recovery.addRecoverPerformer(performer);
    return performer;
  }
}
