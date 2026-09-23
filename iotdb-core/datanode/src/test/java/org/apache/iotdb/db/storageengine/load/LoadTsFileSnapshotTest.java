/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.load;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusNode;
import org.apache.iotdb.db.queryengine.plan.scheduler.load.ChunkOffsetCalculator;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;
import org.apache.iotdb.db.storageengine.load.splitter.NonAlignedChunkData;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Covers the snapshot of the staged files of an in-progress LOAD.
 *
 * <p>A snapshot is read by the transfer layer while it is still being filled, and it is restored
 * into the staging area of another replica, which resumes the tasks it finds there. Both facts are
 * what the tests below pin: nothing is published under its final name while it is incomplete, and
 * the progress log that is copied next to a staged file describes the bytes that were copied with
 * it.
 */
public class LoadTsFileSnapshotTest {

  private File tempDir;
  private String[] originalLoadBaseDirs;
  private IoTDBConfig config;
  private DataRegion dataRegion;

  @Before
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("load-tsfile-snapshot-test").toFile();
    config = IoTDBDescriptor.getInstance().getConfig();
    originalLoadBaseDirs = config.getLoadTsFileDirs();
    config.setLoadTsFileDirs(new String[] {tempDir.getAbsolutePath()});

    dataRegion = Mockito.mock(DataRegion.class);
    Mockito.when(dataRegion.getDatabaseName()).thenReturn("root.snapshot_test");
    Mockito.when(dataRegion.getDataRegionIdString()).thenReturn("0");
    Mockito.when(dataRegion.getNonSystemDatabaseName())
        .thenReturn(Optional.of("root.snapshot_test"));
  }

  @After
  public void tearDown() throws Exception {
    config.setLoadTsFileDirs(originalLoadBaseDirs);
    deleteRecursively(tempDir);
  }

  @Test
  public void testSnapshotCopiesTheStagedFilesOfAnInProgressTask() throws Exception {
    final String loadId = "snapshot-load";
    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    Mockito.when(dataRegion.getLoadTsFileManagerIfPresent()).thenReturn(Optional.of(manager));
    final NonAlignedChunkData chunkData = laidOutChunk();
    final LoadTsFileConsensusNode piece = stagedPiece(loadId, chunkData);
    manager.writePiece(piece);
    final File staged = new File(piece.getPieceRefs().get(0).getRelativePath());
    final File taskDir = staged.getParentFile();
    assertTrue(staged.isFile());
    final File snapshotDir = new File(tempDir, "snapshot");

    assertTrue(LoadTsFileSnapshot.snapshot(dataRegion, snapshotDir));

    final File copiedDir =
        new File(new File(snapshotDir, LoadTsFileSnapshot.SNAPSHOT_SUBDIR_NAME), taskDir.getName());
    final File copiedStaged = new File(copiedDir, staged.getName());
    assertTrue(copiedStaged.isFile());
    assertEquals(staged.length(), copiedStaged.length());

    // The progress log copied next to the staged file describes the bytes that were copied with it,
    // which is what the replica resuming from this snapshot continues from.
    final LoadTsFileProgress copiedProgress = new LoadTsFileProgress(copiedStaged);
    assertTrue(copiedProgress.isReady(copiedStaged.length()));

    // Nothing is ever published under a name the transfer layer would pick up while it is still
    // being copied, and every file of the task is there.
    for (final File file : copiedDir.listFiles()) {
      assertFalse(file.getName().contains(".copying."));
    }
    assertEquals(taskDir.listFiles().length, copiedDir.listFiles().length);
    assertEquals(
        copiedDir.listFiles().length, LoadTsFileSnapshot.collectSnapshotFiles(snapshotDir).size());
  }

  @Test
  public void testSnapshotOfARegionWithoutStagingTasksIsEmpty() throws Exception {
    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    Mockito.when(dataRegion.getLoadTsFileManagerIfPresent()).thenReturn(Optional.of(manager));
    final File snapshotDir = new File(tempDir, "snapshot");

    assertTrue(LoadTsFileSnapshot.snapshot(dataRegion, snapshotDir));

    // A region that holds no staged task contributes nothing, and the transfer layer sees an empty
    // snapshot rather than a failure.
    assertTrue(LoadTsFileSnapshot.collectSnapshotFiles(snapshotDir).isEmpty());
  }

  @Test
  public void testSnapshotOfARegionWithoutALoadManagerIsEmpty() throws Exception {
    Mockito.when(dataRegion.getLoadTsFileManagerIfPresent()).thenReturn(Optional.empty());
    final File snapshotDir = new File(tempDir, "snapshot");

    assertTrue(LoadTsFileSnapshot.snapshot(dataRegion, snapshotDir));

    assertTrue(LoadTsFileSnapshot.collectSnapshotFiles(snapshotDir).isEmpty());
  }

  /** A PIECE that carries one chunk, which stages it and records where its payload landed. */
  private static LoadTsFileConsensusNode stagedPiece(
      final String loadId, final NonAlignedChunkData chunkData) {
    return LoadTsFileConsensusNode.piece(
        new PlanNodeId("load-piece-" + loadId),
        loadId,
        "file-1",
        0L,
        new ArrayList<>(Collections.singletonList(chunkData)));
  }

  private static NonAlignedChunkData laidOutChunk() {
    final NonAlignedChunkData chunkData =
        createNonAlignedChunkData(
            new StringArrayDeviceID("root", "snapshot_test", "d0"), "s0", 0, 10);
    // The staged writer only accepts chunks whose layout was assigned before writing.
    new ChunkOffsetCalculator().assign(chunkData);
    return chunkData;
  }

  private static NonAlignedChunkData createNonAlignedChunkData(
      final IDeviceID device, final String measurement, final int valueBase, final int pointCount) {
    final NonAlignedChunkData chunkData =
        (NonAlignedChunkData)
            ChunkData.createChunkData(
                false,
                device,
                new ChunkHeader(
                    measurement,
                    0,
                    TSDataType.INT32,
                    CompressionType.UNCOMPRESSED,
                    TSEncoding.PLAIN,
                    0),
                new TTimePartitionSlot(0L));
    final long[] times = new long[pointCount];
    final Object[] values = new Object[pointCount];
    for (int i = 0; i < pointCount; i++) {
      times[i] = i + 1L;
      values[i] = valueBase + i;
    }
    chunkData.writeDecodePage(times, values, pointCount);
    chunkData.endChunk();
    return chunkData;
  }

  private static void deleteRecursively(final File file) {
    if (file == null || !file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      final File[] files = file.listFiles();
      if (files != null) {
        for (final File child : files) {
          deleteRecursively(child);
        }
      }
    }
    file.delete();
  }
}
