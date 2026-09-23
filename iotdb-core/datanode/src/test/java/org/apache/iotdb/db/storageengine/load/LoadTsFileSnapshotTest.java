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
import org.apache.iotdb.db.storageengine.load.splitter.ChunkPayloadRef;
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
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertArrayEquals;
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

  private static final String DATABASE_NAME = "root.snapshot_test";
  private static final String REGION_ID = "0";
  private static final String REGION_DIR_NAME = DATABASE_NAME + "-" + REGION_ID;

  /** Bounds the loop that snapshots a task while its pieces are being applied. */
  private static final int MAX_SNAPSHOTS = 500;

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
    final File staged = new File(tempDir, piece.getPieceRefs().get(0).getRelativePath());
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

    // The transfer layer sees every file of the task plus the manifest that records which staging
    // root the task was staged in.
    final Set<String> collected = new HashSet<>();
    for (final File file : LoadTsFileSnapshot.collectSnapshotFiles(snapshotDir)) {
      collected.add(file.getParentFile().getName() + "/" + file.getName());
    }
    for (final File file : copiedDir.listFiles()) {
      assertTrue(collected.contains(copiedDir.getName() + "/" + file.getName()));
    }
    assertTrue(
        "the snapshot has to record the staging root of its tasks",
        collected.contains("load/roots"));
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

  /**
   * A DataNode can stage its tasks in several roots, and the snapshot has to bring every task back
   * to the root it was staged in - and, more importantly, the references it recorded have to keep
   * describing the files wherever they are restored.
   */
  @Test
  public void testSnapshotRestoresEveryTaskIntoTheRootItWasStagedIn() throws Exception {
    final File firstRoot = new File(tempDir, "root-0");
    final File secondRoot = new File(tempDir, "root-1");
    assertTrue(firstRoot.mkdirs() && secondRoot.mkdirs());
    setLoadRoots(firstRoot, secondRoot);

    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    Mockito.when(dataRegion.getLoadTsFileManagerIfPresent()).thenReturn(Optional.of(manager));
    final LoadTsFileConsensusNode firstPiece = stagedPiece("multi-root-a", laidOutChunk());
    manager.writePiece(firstPiece);
    final LoadTsFileConsensusNode secondPiece =
        stagedPiece(
            "multi-root-b",
            laidOutChunk(
                createNonAlignedChunkData(
                    new StringArrayDeviceID("root", "snapshot_test", "d1"), "s1", 100, 10)));
    manager.writePiece(secondPiece);

    // The folder manager hands the tasks out across the configured roots in turn.
    final File firstTaskDir = new File(new File(firstRoot, REGION_DIR_NAME), "multi-root-a");
    final File secondTaskDir = new File(new File(secondRoot, REGION_DIR_NAME), "multi-root-b");
    assertTrue(firstTaskDir.isDirectory());
    assertTrue(secondTaskDir.isDirectory());

    // The reference a replica reads a payload back with, kept across the round trip below.
    final ChunkPayloadRef reference =
        ((ChunkData) firstPiece.getTsFileDataList().get(0)).getChunkPayloadRefs().get(0);
    final byte[] payloadBefore = reference.readPayload();

    final File snapshotDir = new File(tempDir, "snapshot");
    assertTrue(LoadTsFileSnapshot.snapshot(dataRegion, snapshotDir));
    LoadTsFileSnapshot.clear(DATABASE_NAME, REGION_ID);
    assertFalse(firstTaskDir.exists());
    assertFalse(secondTaskDir.exists());

    LoadTsFileSnapshot.restore(DATABASE_NAME, REGION_ID, snapshotDir);

    // Every task is back in the root it came from, and the recorded reference still reads its
    // bytes.
    assertTrue(firstTaskDir.isDirectory());
    assertTrue(secondTaskDir.isDirectory());
    assertArrayEquals(payloadBefore, reference.readPayload());
  }

  /**
   * The node that restores a snapshot need not have the roots the snapshot was taken on: a task
   * then lands on a root this node has, and the references it recorded still resolve there.
   */
  @Test
  public void testSnapshotRestoresOntoAGodeWithFewerRoots() throws Exception {
    final File firstRoot = new File(tempDir, "root-0");
    final File secondRoot = new File(tempDir, "root-1");
    assertTrue(firstRoot.mkdirs() && secondRoot.mkdirs());
    setLoadRoots(firstRoot, secondRoot);

    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    Mockito.when(dataRegion.getLoadTsFileManagerIfPresent()).thenReturn(Optional.of(manager));
    final LoadTsFileConsensusNode piece = stagedPiece("single-root", laidOutChunk());
    manager.writePiece(piece);
    final ChunkPayloadRef reference =
        ((ChunkData) piece.getTsFileDataList().get(0)).getChunkPayloadRefs().get(0);
    final byte[] payloadBefore = reference.readPayload();

    final File snapshotDir = new File(tempDir, "snapshot");
    assertTrue(LoadTsFileSnapshot.snapshot(dataRegion, snapshotDir));
    LoadTsFileSnapshot.clear(DATABASE_NAME, REGION_ID);

    // The restoring node has one root only, which is not the one the task was staged in.
    final File onlyRoot = new File(tempDir, "only-root");
    assertTrue(onlyRoot.mkdirs());
    setLoadRoots(onlyRoot);
    LoadTsFileSnapshot.restore(DATABASE_NAME, REGION_ID, snapshotDir);

    final File restoredTaskDir = new File(new File(onlyRoot, REGION_DIR_NAME), "single-root");
    assertTrue(restoredTaskDir.isDirectory());
    assertArrayEquals(payloadBefore, reference.readPayload());
  }

  /**
   * A snapshot taken while pieces are still being applied has to be restorable: every range a
   * copied progress log describes has to be inside the bytes that were copied with it.
   *
   * <p>The copy is not serialized with the pieces - the route of a task cannot be locked against
   * the thread that applies them without stopping the region - so the order of the copy is what
   * makes it safe: the logs are copied before the staged files they describe, and a staged file
   * only ever grows at its end. A trailing log entry can still be caught half appended, and reading
   * the copied log drops that fragment.
   */
  @Test
  public void testSnapshotTakenWhilePiecesAreAppliedStaysRestorable() throws Exception {
    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    Mockito.when(dataRegion.getLoadTsFileManagerIfPresent()).thenReturn(Optional.of(manager));
    final String loadId = "concurrent-snapshot";

    // Ten chunks of one partition, laid out one after the other, so every piece appends to the same
    // staged file and its log.
    final ChunkOffsetCalculator calculator = new ChunkOffsetCalculator();
    final List<NonAlignedChunkData> chunks = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      final NonAlignedChunkData chunk =
          createNonAlignedChunkData(
              new StringArrayDeviceID("root", "snapshot_test", "d" + i), "s" + i, i * 100, 10);
      calculator.assign(chunk);
      chunks.add(chunk);
    }

    final Set<Long> stagedLengths = new HashSet<>();
    final AtomicBoolean writing = new AtomicBoolean(true);
    final Thread writer =
        new Thread(
            () -> {
              try {
                for (final NonAlignedChunkData chunk : chunks) {
                  manager.writePiece(loadId, Collections.singletonList(chunk));
                  Thread.sleep(5L);
                }
              } catch (final Exception e) {
                // Whatever was staged is what the snapshots below are checked against.
              } finally {
                writing.set(false);
              }
            });
    writer.start();
    try {
      // Snapshot the task while the pieces of the writer thread are still being applied.
      int snapshots = 0;
      while (writing.get() && snapshots < MAX_SNAPSHOTS) {
        final File snapshotDir = new File(tempDir, "snapshot-" + snapshots);
        assertTrue(LoadTsFileSnapshot.snapshot(dataRegion, snapshotDir));
        assertEveryCopiedLogFitsItsCopiedFile(snapshotDir);
        stagedLengths.add(lengthOfTheStagedFile(snapshotDir));
        snapshots++;
        Thread.sleep(1L);
      }
      assertTrue("the snapshots have to have been taken while the task was staged", snapshots > 0);
    } finally {
      writer.join();
    }
    assertFalse("the writer has to have finished by now", writing.get());
    assertTrue(
        "the snapshots have to have caught the staged file while it was still growing",
        stagedLengths.size() > 1);
  }

  /** The length of the staged file a snapshot holds, or -1 when it holds none yet. */
  private static long lengthOfTheStagedFile(final File snapshotDir) {
    final File[] taskDirs =
        new File(snapshotDir, LoadTsFileSnapshot.SNAPSHOT_SUBDIR_NAME).listFiles();
    if (taskDirs == null) {
      return -1L;
    }
    for (final File taskDir : taskDirs) {
      final File[] files = taskDir.listFiles();
      if (files == null) {
        continue;
      }
      for (final File file : files) {
        if (!file.getName().endsWith(LoadTsFileProgress.PROGRESS_SUFFIX)) {
          return file.length();
        }
      }
    }
    return -1L;
  }

  /** Every range of a copied log has to be inside the staged file that was copied with it. */
  private static void assertEveryCopiedLogFitsItsCopiedFile(final File snapshotDir)
      throws Exception {
    final File[] taskDirs =
        new File(snapshotDir, LoadTsFileSnapshot.SNAPSHOT_SUBDIR_NAME).listFiles();
    if (taskDirs == null) {
      return;
    }
    for (final File taskDir : taskDirs) {
      final File[] files = taskDir.listFiles();
      if (files == null) {
        continue;
      }
      for (final File file : files) {
        if (!file.getName().endsWith(LoadTsFileProgress.PROGRESS_SUFFIX)) {
          continue;
        }
        final File staged =
            new File(
                taskDir,
                file.getName()
                    .substring(
                        0, file.getName().length() - LoadTsFileProgress.PROGRESS_SUFFIX.length()));
        final LoadTsFileProgress progress = new LoadTsFileProgress(staged);
        // Reading a copied log repairs the fragment of an entry that was caught half appended,
        // exactly as the node that restores this snapshot does.
        for (final LoadTsFileProgress.ChunkRangeRecord record :
            progress.readAllRecordsRepairingTornTail()) {
          assertTrue(
              "a copied log describes bytes the copied file does not hold: "
                  + record.physicalEnd()
                  + " > "
                  + staged.length(),
              record.physicalEnd() <= staged.length());
        }
      }
    }
  }

  /** Configures the staging roots of this node, the way the folder manager reads them. */
  private void setLoadRoots(final File... roots) throws Exception {
    final String[] dirs = new String[roots.length];
    for (int i = 0; i < roots.length; i++) {
      dirs[i] = roots[i].getAbsolutePath();
    }
    config.setLoadTsFileDirs(dirs);
    // Rebuilds the folder manager, which is what makes the new roots the ones tasks are staged in.
    LoadStagingDirs.folderManager();
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

  /** The same chunk, with its layout assigned as the piece pipeline assigns it. */
  private static NonAlignedChunkData laidOutChunk(final NonAlignedChunkData chunkData) {
    new ChunkOffsetCalculator().assign(chunkData);
    return chunkData;
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
