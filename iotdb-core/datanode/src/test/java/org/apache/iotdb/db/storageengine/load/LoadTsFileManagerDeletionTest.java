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

package org.apache.iotdb.db.storageengine.load;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusNode;
import org.apache.iotdb.db.queryengine.plan.scheduler.load.ChunkOffsetCalculator;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;
import org.apache.iotdb.db.storageengine.load.splitter.DeletionData;
import org.apache.iotdb.db.storageengine.load.splitter.NonAlignedChunkData;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileData;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.write.TsFilePrecalculatedChunkWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Covers the deletions a LOAD piece carries: they are staged as the exclusive modification file
 * (the {@code .mods2} sidecar) of the staged TsFile of the task, they never count as staged TsFile
 * bytes, and they face the same lifecycle as the staged files they belong to - a restart resumes
 * them, PREPARE seals them, ABORT takes them away.
 */
public class LoadTsFileManagerDeletionTest {

  private static final String DATABASE = "root.load_manager_test";
  private static final String DEVICE = DATABASE + ".d0";
  private static final String MEASUREMENT = "s0";

  private File tempDir;
  private String[] originalLoadBaseDirs;
  private IoTDBConfig config;
  private DataRegion dataRegion;

  @Before
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("load-manager-deletion-test").toFile();
    config = IoTDBDescriptor.getInstance().getConfig();
    originalLoadBaseDirs = config.getLoadTsFileDirs();
    config.setLoadTsFileDirs(new String[] {tempDir.getAbsolutePath()});

    dataRegion = Mockito.mock(DataRegion.class);
    Mockito.when(dataRegion.getDatabaseName()).thenReturn(DATABASE);
    Mockito.when(dataRegion.getDataRegionIdString()).thenReturn("0");
    Mockito.when(dataRegion.getNonSystemDatabaseName()).thenReturn(Optional.of(DATABASE));
  }

  @After
  public void tearDown() throws Exception {
    config.setLoadTsFileDirs(originalLoadBaseDirs);
    deleteRecursively(tempDir);
  }

  /**
   * A piece that carries a deletion stages it in the exclusive modification file next to the staged
   * TsFile, and the piece announces no staged bytes: the modification file belongs to the staged
   * TsFile, so it is not a TsFile of its own and a replica must not be asked for it.
   */
  @Test
  public void testDeletionPieceStagesTheExclusiveModsFile() throws Exception {
    final String loadId = "deletion-staged";
    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    final List<LoadTsFileConsensusNode.PieceRef> chunkRefs =
        manager.writePiece(loadId, toTsFileDataList(Collections.singletonList(laidOutChunk())));
    Assert.assertEquals(1, chunkRefs.size());
    final File staged = new File(tempDir, chunkRefs.get(0).getRelativePath());

    final List<LoadTsFileConsensusNode.PieceRef> deletionRefs =
        manager.writePiece(loadId, deletionPiece(100L, 200L));

    Assert.assertTrue(
        "a deletion stages no TsFile bytes, so it must not be announced as a piece ref",
        deletionRefs.isEmpty());
    final File modsFile = ModificationFile.getExclusiveMods(staged);
    Assert.assertTrue(
        "the deletion of a piece must be staged next to the staged TsFile: " + modsFile,
        modsFile.isFile());
    final List<ModEntry> stagedDeletions = ModificationFile.readAllModifications(staged, false);
    Assert.assertEquals(1, stagedDeletions.size());
    assertDeletion(stagedDeletions.get(0), 100L, 200L);
  }

  /**
   * A deletion that arrives before the task staged any TsFile is a no-op: the deletions of a file
   * are dispatched after the chunks that stage its partitions, so there is nothing the deletion
   * could belong to yet, and no modification file is created for a task without staged data.
   */
  @Test
  public void testDeletionPieceOfATaskWithoutStagedFileStagesNothing() throws Exception {
    final String loadId = "deletion-without-staged-file";
    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);

    final List<LoadTsFileConsensusNode.PieceRef> refs =
        manager.writePiece(loadId, deletionPiece(1L, 2L));

    Assert.assertTrue(refs.isEmpty());
    Assert.assertTrue(
        "no staged TsFile exists, so the deletion must not create a modification file",
        modificationFiles(tempDir).isEmpty());
  }

  /**
   * The deletions of a task survive a restart, and the recovered task appends its next deletion to
   * the modification file it finds on disk instead of replacing it.
   */
  @Test
  public void testDeletionsSurviveARestartAndKeepAppending() throws Exception {
    final String loadId = "deletion-survives-restart";
    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    final List<LoadTsFileConsensusNode.PieceRef> chunkRefs =
        manager.writePiece(loadId, toTsFileDataList(Collections.singletonList(laidOutChunk())));
    final File staged = new File(tempDir, chunkRefs.get(0).getRelativePath());
    manager.writePiece(loadId, deletionPiece(100L, 200L));

    // A restart drops every in-memory object of the task and leaves the staged TsFile, its progress
    // records and its modification file on disk. The staging directories are pinned again because a
    // DataNode pins them once at startup, before any load.
    final long stagedLengthBeforeRestart = staged.length();
    dropInMemoryWriters(manager);
    config.setLoadTsFileDirs(new String[] {tempDir.getAbsolutePath()});
    final LoadTsFileManager recovered = new LoadTsFileManager(dataRegion);
    Assert.assertTrue(
        "the restarted manager must resume the staged directory of the task",
        recovered.getActiveTaskDirs().contains(staged.getParentFile()));
    recovered.writePiece(loadId, deletionPiece(300L, 400L));

    Assert.assertEquals(
        "a deletion stages no TsFile bytes, not even into a resumed staged file",
        stagedLengthBeforeRestart,
        staged.length());
    final List<ModEntry> stagedDeletions = ModificationFile.readAllModifications(staged, false);
    Assert.assertEquals(
        "the deletion staged before the restart and the one staged after it must both be there",
        2,
        stagedDeletions.size());
    Assert.assertTrue(
        "the deletion of the first run must have survived the restart",
        containsDeletion(stagedDeletions, 100L, 200L));
    Assert.assertTrue(
        "the recovered task must append to the modification file it found instead of replacing it",
        containsDeletion(stagedDeletions, 300L, 400L));
  }

  /**
   * PREPARE seals the staged TsFile and closes its modification file: the sealed file carries the
   * chunks that were staged before the deletion, and the deletion itself is still readable.
   */
  @Test
  public void testPrepareKeepsTheDeletionsOfTheSealedStagedFile() throws Exception {
    final String loadId = "deletion-prepared";
    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    final List<LoadTsFileConsensusNode.PieceRef> chunkRefs =
        manager.writePiece(loadId, toTsFileDataList(Collections.singletonList(laidOutChunk())));
    final File staged = new File(tempDir, chunkRefs.get(0).getRelativePath());
    manager.writePiece(loadId, deletionPiece(100L, 200L));

    Assert.assertTrue(manager.prepare(prepareNode(loadId), Collections.emptyMap()));

    final List<ModEntry> stagedDeletions = ModificationFile.readAllModifications(staged, false);
    Assert.assertEquals(1, stagedDeletions.size());
    assertDeletion(stagedDeletions.get(0), 100L, 200L);
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(staged.getAbsolutePath())) {
      Assert.assertEquals(
          "the sealed staged file must still carry the chunks staged together with the deletion",
          1,
          reader.getAllMeasurements().size());
    }
  }

  /**
   * A restart resumes the staged deletions as well: the writer manager built again from the staged
   * directory alone holds the modification file of the staged TsFile, so the deletions it staged
   * before the restart are still the ones this task owns - PREPARE seals them with the file, and
   * releasing the task takes them away with it.
   */
  @Test
  public void testRecoverFromDiskRestoresTheStagedDeletions() throws Exception {
    final String loadId = "deletion-recovered";
    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    final List<LoadTsFileConsensusNode.PieceRef> chunkRefs =
        manager.writePiece(loadId, toTsFileDataList(Collections.singletonList(laidOutChunk())));
    final File staged = new File(tempDir, chunkRefs.get(0).getRelativePath());
    manager.writePiece(loadId, deletionPiece(100L, 200L));
    final File modsFile = ModificationFile.getExclusiveMods(staged);
    Assert.assertTrue(modsFile.isFile());

    // The restart at the level of the writer manager: a new one is built from the staged directory
    // and reads the task back from the progress records next to the staged TsFile.
    final File taskDir = staged.getParentFile();
    final TsFileWriterManager recovered =
        new TsFileWriterManager(dataRegion, taskDir).recoverFromDisk();

    Assert.assertTrue(
        "the restart must resume the staged TsFile and the deletions staged with it",
        recovered.hasStagedData());
    Assert.assertEquals(
        "the resumed manager must own the modification file of the staged TsFile",
        Collections.singletonList(modsFile),
        modificationFilesOf(recovered));
    final List<ModEntry> stagedDeletions = ModificationFile.readAllModifications(staged, false);
    Assert.assertEquals(1, stagedDeletions.size());
    assertDeletion(stagedDeletions.get(0), 100L, 200L);

    // Owning the file is what makes the release of the task take the deletions away: a modification
    // file the recovered manager did not know about would be left behind in the staging directory.
    recovered.close();
    Assert.assertFalse("releasing the task must take the staged deletions away", modsFile.exists());
    Assert.assertFalse(staged.exists());
    Assert.assertFalse(taskDir.exists());
  }

  /** ABORT discards the staged deletions together with the staged TsFiles they belong to. */
  @Test
  public void testAbortTakesTheDeletionsAwayWithTheStagedFiles() throws Exception {
    final String loadId = "deletion-aborted";
    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    final List<LoadTsFileConsensusNode.PieceRef> chunkRefs =
        manager.writePiece(loadId, toTsFileDataList(Collections.singletonList(laidOutChunk())));
    final File staged = new File(tempDir, chunkRefs.get(0).getRelativePath());
    manager.writePiece(loadId, deletionPiece(100L, 200L));
    final File modsFile = ModificationFile.getExclusiveMods(staged);
    Assert.assertTrue(modsFile.isFile());

    final LoadTsFileConsensusNode abort = abortNode(loadId);
    Assert.assertTrue(manager.deleteAll(abort));

    Assert.assertFalse("ABORT must take the staged TsFile away", staged.exists());
    Assert.assertFalse("ABORT must take its staged deletions away as well", modsFile.exists());
    Assert.assertFalse("the task directory must be gone", staged.getParentFile().exists());
    // The command layer answers ABORT with success whatever this returns, so a replay is harmless.
    Assert.assertFalse(manager.deleteAll(abort));
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private static void assertDeletion(final ModEntry entry, final long start, final long end) {
    Assert.assertTrue(
        "a staged tree deletion must read back as a tree deletion",
        entry instanceof TreeDeletionEntry);
    final TreeDeletionEntry deletion = (TreeDeletionEntry) entry;
    Assert.assertEquals(DEVICE + "." + MEASUREMENT, deletion.getPathPattern().getFullPath());
    Assert.assertEquals(start, deletion.getTimeRange().getMin());
    Assert.assertEquals(end, deletion.getTimeRange().getMax());
  }

  private static boolean containsDeletion(
      final List<ModEntry> entries, final long start, final long end) {
    for (final ModEntry entry : entries) {
      if (entry instanceof TreeDeletionEntry
          && ((TreeDeletionEntry) entry).getTimeRange().getMin() == start
          && ((TreeDeletionEntry) entry).getTimeRange().getMax() == end) {
        return true;
      }
    }
    return false;
  }

  /** The deletions staged for one measurement range, as a piece carries them. */
  private static List<TsFileData> deletionPiece(final long start, final long end) throws Exception {
    return Collections.singletonList(
        new DeletionData(
            new TreeDeletionEntry(new MeasurementPath(DEVICE + "." + MEASUREMENT), start, end)));
  }

  private static List<File> modificationFiles(final File dir) {
    final List<File> result = new ArrayList<>();
    final File[] files = dir.listFiles();
    if (files == null) {
      return result;
    }
    for (final File file : files) {
      if (file.isDirectory()) {
        result.addAll(modificationFiles(file));
      } else if (file.getName().endsWith(ModificationFile.FILE_SUFFIX)) {
        result.add(file);
      }
    }
    return result;
  }

  /** The modification files the writer manager of a task holds, in the order they were staged. */
  @SuppressWarnings("unchecked")
  private static List<File> modificationFilesOf(final TsFileWriterManager writerManager)
      throws Exception {
    final Field field =
        TsFileWriterManager.class.getDeclaredField("dataPartition2ModificationFile");
    field.setAccessible(true);
    final Map<?, ModificationFile> modificationFiles =
        (Map<?, ModificationFile>) field.get(writerManager);
    final List<File> result = new ArrayList<>(modificationFiles.size());
    for (final ModificationFile modificationFile : modificationFiles.values()) {
      result.add(modificationFile.getFile());
    }
    return result;
  }

  /**
   * Drops the in-memory objects of every task the way a restart drops them: the buffered bytes are
   * flushed and the staged files with their progress records are all that is left on disk.
   */
  private static void dropInMemoryWriters(final LoadTsFileManager manager) throws Exception {
    final Field writersField = LoadTsFileManager.class.getDeclaredField("uuid2WriterManager");
    writersField.setAccessible(true);
    @SuppressWarnings("unchecked")
    final Map<String, TsFileWriterManager> writers =
        (Map<String, TsFileWriterManager>) writersField.get(manager);
    for (final TsFileWriterManager writerManager : writers.values()) {
      final Field partitionsField =
          TsFileWriterManager.class.getDeclaredField("dataPartition2Writer");
      partitionsField.setAccessible(true);
      @SuppressWarnings("unchecked")
      final Map<?, TsFilePrecalculatedChunkWriter> partitionWriters =
          (Map<?, TsFilePrecalculatedChunkWriter>) partitionsField.get(writerManager);
      for (final TsFilePrecalculatedChunkWriter writer : partitionWriters.values()) {
        writer.getOutput().flush();
      }
    }
    writers.clear();
  }

  private static NonAlignedChunkData laidOutChunk() {
    final NonAlignedChunkData chunkData =
        createNonAlignedChunkData(
            new StringArrayDeviceID("root", "load_manager_test", "d0"), MEASUREMENT, 0, 10);
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

  private static List<TsFileData> toTsFileDataList(final List<ChunkData> chunkDataList) {
    final List<TsFileData> result = new ArrayList<>();
    for (final ChunkData chunkData : chunkDataList) {
      result.add(chunkData);
    }
    return result;
  }

  private static LoadTsFileConsensusNode abortNode(final String loadId) {
    return LoadTsFileConsensusNode.abort(
        new PlanNodeId("load-abort-" + loadId), loadId, null, false);
  }

  /** The PREPARE command of a task, which is what seals the staged files. */
  private static LoadTsFileConsensusNode prepareNode(final String loadId) {
    return LoadTsFileConsensusNode.prepare(
        new PlanNodeId("load-prepare-" + loadId),
        loadId,
        null,
        0,
        0L,
        0L,
        false,
        Collections.emptyMap());
  }

  private static void deleteRecursively(final File file) {
    if (!file.exists()) {
      return;
    }
    final File[] files = file.listFiles();
    if (files != null) {
      for (final File child : files) {
        deleteRecursively(child);
      }
    }
    Assert.assertTrue(file.delete());
  }
}
