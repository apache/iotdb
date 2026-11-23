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

import org.apache.iotdb.commons.conf.ConfigurationFileUtils;
import org.apache.iotdb.commons.conf.TrimProperties;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusOp;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.IWALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.WALFlushListener;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.Chunk;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/** How the staged directory of a LOAD that received no command is reclaimed. */
public class LoadTsFileCleanerTest {

  private static final long IDLE_TIMEOUT_MILLIS = 15 * 60 * 1000L;

  private static final String DATABASE = "root.load_cleaner_test";

  private File tempDir;
  private String[] originalLoadBaseDirs;
  private IoTDBConfig config;
  private DataRegion dataRegion;
  private final AtomicLong walWatermark = new AtomicLong();

  @Before
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("load-cleaner-test").toFile();
    config = IoTDBDescriptor.getInstance().getConfig();
    originalLoadBaseDirs = config.getLoadTsFileDirs();
    config.setLoadTsFileDirs(new String[] {tempDir.getAbsolutePath()});

    final IWALNode walNode = Mockito.mock(IWALNode.class);
    Mockito.when(walNode.getSafelyDeletedSearchIndex()).thenAnswer(ignored -> walWatermark.get());
    final WALFlushListener flushListener = Mockito.mock(WALFlushListener.class);
    Mockito.when(flushListener.waitForResult()).thenReturn(WALFlushListener.Status.SUCCESS);
    Mockito.when(walNode.log(Mockito.anyLong(), Mockito.any(LoadTsFileConsensusNode.class)))
        .thenReturn(flushListener);
    dataRegion = Mockito.mock(DataRegion.class);
    Mockito.when(dataRegion.getDatabaseName()).thenReturn(DATABASE);
    Mockito.when(dataRegion.getDataRegionIdString()).thenReturn("0");
    Mockito.when(dataRegion.getWALNode()).thenReturn(Optional.of(walNode));
  }

  @After
  public void tearDown() throws Exception {
    config.setLoadTsFileDirs(originalLoadBaseDirs);
    deleteRecursively(tempDir);
  }

  @Test
  public void testTheIdleJudgement() {
    Assert.assertFalse(
        "a task this region never heard from is not idle", LoadTsFileCleaner.isIdle(-1L, 1000L));
    Assert.assertFalse(
        "a task that just received a command is not idle",
        LoadTsFileCleaner.isIdle(System.currentTimeMillis(), 1000L));
    Assert.assertTrue(
        "a task that received no command for the timeout is idle",
        LoadTsFileCleaner.isIdle(System.currentTimeMillis() - 2000L, 1000L));
  }

  @Test
  public void testACommandStampsTheTaskAndItsTerminalCommandForgetsIt() throws Exception {
    final String loadId = "stamped-task";
    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    try {
      Assert.assertEquals(-1L, manager.getLastCommandTimeMillis(loadId));

      manager.writePiece(emptyPiece(loadId));

      final long stamped = manager.getLastCommandTimeMillis(loadId);
      Assert.assertTrue("a staged piece must stamp the task", stamped > 0);
      Assert.assertFalse(
          "a task that just staged a piece is not idle",
          LoadTsFileCleaner.isIdle(stamped, IDLE_TIMEOUT_MILLIS));

      manager.deleteAll(abortNode(loadId));
      Assert.assertEquals(
          "a finished task is forgotten", -1L, manager.getLastCommandTimeMillis(loadId));
    } finally {
      manager.stop();
    }
  }

  @Test
  public void testADirectoryTheRegionDoesNotKnowIsReclaimedAfterTheTimeout() throws Exception {
    config.setLoadCleanupTaskIdleTimeoutSeconds(1L);
    final String loadId = "unknown-task";
    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    try {
      Mockito.when(dataRegion.getLoadTsFileManagerIfPresent()).thenReturn(Optional.of(manager));
      walWatermark.set(100L);
      final File taskDir = new File(tempDir, loadId);
      Assert.assertTrue(taskDir.mkdirs());
      stageOneChunk(taskDir, 5L);

      // The region knows nothing about the task, so the sweep judges it by when it first saw the
      // directory: that is how the leftovers of a crashed run are reclaimed as well.
      final LoadTsFileCleaner cleaner = LoadTsFileCleaner.getInstance();
      Assert.assertFalse(
          "a directory the sweep has just seen is not idle",
          LoadTsFileCleaner.isIdle(cleaner.lastCommandTimeMillis(dataRegion, taskDir), 1000L));

      Thread.sleep(1100L);
      cleaner.reclaimIfIdle(
          dataRegion, loadId, taskDir, cleaner.lastCommandTimeMillis(dataRegion, taskDir));

      Assert.assertFalse("an unattended directory must be reclaimed", taskDir.exists());
    } finally {
      manager.stop();
      config.setLoadCleanupTaskIdleTimeoutSeconds(900L);
    }
  }

  @Test
  public void testAnIdleTaskIsReclaimedOnlyOnceItsPiecesAreReplicated() throws Exception {
    config.setLoadCleanupTaskIdleTimeoutSeconds(1L);
    final String loadId = "abandoned-task";
    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    try {
      Mockito.when(dataRegion.getLoadTsFileManagerIfPresent()).thenReturn(Optional.of(manager));
      manager.writePiece(emptyPiece(loadId));
      final File taskDir = manager.getActiveTaskDirs().get(0);
      stageOneChunk(taskDir, 5L);

      // The task received nothing for longer than the timeout.
      Thread.sleep(1100L);

      walWatermark.set(3L);
      LoadTsFileCleaner.getInstance()
          .reclaimIfIdle(dataRegion, loadId, taskDir, manager.getLastCommandTimeMillis(loadId));
      Assert.assertTrue(
          "an idle task whose pieces are not replicated yet must be kept", taskDir.exists());

      walWatermark.set(7L);
      LoadTsFileCleaner.getInstance()
          .reclaimIfIdle(dataRegion, loadId, taskDir, manager.getLastCommandTimeMillis(loadId));
      Assert.assertFalse(
          "an idle task whose pieces every replica got must be reclaimed", taskDir.exists());
    } finally {
      manager.stop();
      config.setLoadCleanupTaskIdleTimeoutSeconds(900L);
    }
  }

  @Test
  public void testAFinishedTaskWaitsForTheFirstWatermark() throws Exception {
    final String loadId = "unreported-watermark-task";
    final File taskDir = new File(tempDir, loadId);
    Assert.assertTrue(taskDir.mkdirs());
    stageOneChunk(taskDir, 5L);
    final LoadTsFileCleaner cleaner = LoadTsFileCleaner.getInstance();
    cleaner.register(loadId, dataRegion, taskDir, 7L, LoadTsFileConsensusOp.ABORT);

    walWatermark.set(ConsensusReqReader.DEFAULT_SAFELY_DELETED_SEARCH_INDEX);
    cleaner.sweepRegisteredTasks();
    Assert.assertTrue("an unreported watermark does not confirm replication", taskDir.exists());

    walWatermark.set(5L);
    cleaner.sweepRegisteredTasks();
    Assert.assertTrue("the terminal command is not replicated yet", taskDir.exists());

    walWatermark.set(7L);
    cleaner.sweepRegisteredTasks();
    Assert.assertFalse("the terminal command is replicated", taskDir.exists());
  }

  @Test
  public void testIoTConsensusV2DoesNotWaitForAWALWatermark() throws Exception {
    final String loadId = "v2-finished-task";
    final File taskDir = new File(tempDir, loadId);
    Assert.assertTrue(taskDir.mkdirs());
    stageOneChunk(taskDir, 5L);
    final LoadTsFileCleaner cleaner = LoadTsFileCleaner.getInstance();
    cleaner.register(loadId, dataRegion, taskDir, 7L, LoadTsFileConsensusOp.ABORT);
    walWatermark.set(ConsensusReqReader.DEFAULT_SAFELY_DELETED_SEARCH_INDEX);

    LoadConsensusCompat.setIoTConsensusV2ForTest(Boolean.TRUE);
    try {
      cleaner.sweepRegisteredTasks();
      Assert.assertFalse("V2 does not expand staged references from the WAL", taskDir.exists());
    } finally {
      LoadConsensusCompat.setIoTConsensusV2ForTest(null);
    }
  }

  @Test
  public void testAFinishedCommittedTaskIsReleasedOnceItsStagedFilesAreComplete() throws Exception {
    final String loadId = "finished-task";
    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    try {
      Mockito.when(dataRegion.getLoadTsFileManagerIfPresent()).thenReturn(Optional.of(manager));
      manager.writePiece(emptyPiece(loadId));
      final File taskDir = manager.getActiveTaskDirs().get(0);
      stageOneChunk(taskDir, 5L);
      final File stagedFile = new File(taskDir, "root.sg-0-0.tsfile");
      final LoadTsFileProgress progress = new LoadTsFileProgress(stagedFile);
      progress.readAllRecords();
      Assert.assertFalse(
          "the staged file does not cover its records yet", progress.isReady(stagedFile.length()));

      LoadTsFileCleaner.getInstance()
          .register(loadId, dataRegion, taskDir, 7L, LoadTsFileConsensusOp.COMMIT);

      walWatermark.set(5L);
      LoadTsFileCleaner.getInstance().sweep();
      Assert.assertTrue(
          "a committed task is kept until every replica got past its command", taskDir.exists());

      walWatermark.set(7L);
      LoadTsFileCleaner.getInstance().sweep();
      Assert.assertTrue(
          "a committed task whose staged file is incomplete is still kept", taskDir.exists());

      Files.write(stagedFile.toPath(), new byte[(int) progress.getTotalLength()]);
      LoadTsFileCleaner.getInstance().sweep();
      Assert.assertFalse(
          "once its staged files are complete and the watermark passed, it is reclaimed",
          taskDir.exists());
    } finally {
      manager.stop();
    }
  }

  @Test
  public void testALoadCommandReclaimsAnIdleTaskOfTheSameRegion() throws Exception {
    config.setLoadCleanupTaskIdleTimeoutSeconds(1L);
    final String idleLoadId = "idle-task";
    final String freshLoadId = "fresh-task";
    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    try {
      Mockito.when(dataRegion.getLoadTsFileManagerIfPresent()).thenReturn(Optional.of(manager));
      walWatermark.set(100L);
      manager.writePiece(emptyPiece(idleLoadId));
      final File idleTaskDir =
          manager.getActiveTaskDirs().stream()
              .filter(dir -> dir.getName().equals(idleLoadId))
              .findFirst()
              .orElseThrow(AssertionError::new);
      stageOneChunk(idleTaskDir, 5L);

      Thread.sleep(1100L);

      // A command of another task sweeps the region, so an idle task does not wait for the periodic
      // sweep.
      manager.writePiece(emptyPiece(freshLoadId));

      Assert.assertFalse(
          "the idle task of the same region must be reclaimed", idleTaskDir.exists());
      Assert.assertEquals(
          "only the task that just received a command may stay staged",
          Collections.singleton(freshLoadId),
          manager.getActiveTaskDirs().stream().map(File::getName).collect(Collectors.toSet()));
    } finally {
      manager.stop();
      config.setLoadCleanupTaskIdleTimeoutSeconds(900L);
    }
  }

  @Test
  public void testTheIdleTimeoutIsHotReloadable() throws Exception {
    final IoTDBDescriptor descriptor = IoTDBDescriptor.getInstance();
    final long original = descriptor.getConfig().getLoadCleanupTaskIdleTimeoutSeconds();
    try {
      // The template is what set configuration validates and a hot reload falls back to.
      Assert.assertEquals(
          "900",
          ConfigurationFileUtils.getConfigurationDefaultValue(
              "load_clean_up_task_idle_timeout_seconds"));

      final TrimProperties properties = new TrimProperties();
      properties.setProperty("load_clean_up_task_idle_timeout_seconds", "300");
      descriptor.loadHotModifiedProps(properties);

      Assert.assertEquals(300L, descriptor.getConfig().getLoadCleanupTaskIdleTimeoutSeconds());
    } finally {
      descriptor.getConfig().setLoadCleanupTaskIdleTimeoutSeconds(original);
    }
  }

  private static LoadTsFileConsensusNode emptyPiece(final String loadId) {
    return LoadTsFileConsensusNode.piece(
        new PlanNodeId("load-piece"), loadId, "file.tsfile", 0L, new ArrayList<>());
  }

  private static LoadTsFileConsensusNode abortNode(final String loadId) {
    return LoadTsFileConsensusNode.abort(new PlanNodeId("load-abort"), loadId, false);
  }

  private static void stageOneChunk(final File taskDir, final long searchIndex) throws Exception {
    final File tsFile = new File(taskDir, "root.sg-0-0.tsfile");
    Assert.assertTrue(tsFile.createNewFile());
    new LoadTsFileProgress(tsFile)
        .recordChunk(
            new StringArrayDeviceID("root", "sg", "d1"),
            false,
            7L,
            7L,
            true,
            createChunk(),
            100L,
            searchIndex);
  }

  private static Chunk createChunk() {
    final ChunkHeader chunkHeader =
        new ChunkHeader(
            "s1", 4, TSDataType.INT32, CompressionType.UNCOMPRESSED, TSEncoding.PLAIN, 1);
    final Statistics<?> statistics = Statistics.getStatsByType(TSDataType.INT32);
    statistics.update(1L, 1);
    statistics.update(2L, 2);
    return new Chunk(chunkHeader, ByteBuffer.wrap(new byte[] {0, 0, 0, 1}), null, statistics);
  }

  private static void deleteRecursively(final File file) {
    final File[] children = file.listFiles();
    if (children != null) {
      for (final File child : children) {
        deleteRecursively(child);
      }
    }
    file.delete();
  }
}
