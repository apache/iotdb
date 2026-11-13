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

package org.apache.iotdb.db.pipe.resource;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.resource.tsfile.PipeTsFileDirPathResolver;
import org.apache.iotdb.db.pipe.resource.tsfile.PipeTsFileResourceManager;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;

public class PipeTsFileResourceManagerTest {

  private static final String ROOT_DIR = "target" + File.separator + "PipeTsFileHolderTest";
  private static final String SEQUENCE_DIR =
      ROOT_DIR + File.separator + IoTDBConstant.SEQUENCE_FOLDER_NAME;
  private static final String TS_FILE_NAME = SEQUENCE_DIR + File.separator + "test.tsfile";
  private static final String MODS_FILE_NAME = TS_FILE_NAME + ".mods";
  private static final String PIPE_NAME = "pipe";

  private PipeTsFileResourceManager pipeTsFileResourceManager;

  @Before
  public void setUp() throws Exception {
    pipeTsFileResourceManager = new PipeTsFileResourceManager();
    PipeDataNodeAgent.runtime().startPeriodicalJobExecutor();

    createTsfile(TS_FILE_NAME);
    creatModsFile(MODS_FILE_NAME);
  }

  private void createTsfile(String tsfilePath) throws Exception {
    File file = new File(tsfilePath);
    if (file.exists()) {
      boolean ignored = file.delete();
    }

    Schema schema = new Schema();
    String template = "template";
    IMeasurementSchema s1 = new MeasurementSchema("sensor1", TSDataType.FLOAT, TSEncoding.RLE);
    IMeasurementSchema s2 = new MeasurementSchema("sensor2", TSDataType.INT32, TSEncoding.TS_2DIFF);
    IMeasurementSchema s3 = new MeasurementSchema("sensor3", TSDataType.INT32, TSEncoding.TS_2DIFF);
    schema.extendTemplate(
        template, new MeasurementSchema("sensor1", TSDataType.FLOAT, TSEncoding.RLE));
    schema.extendTemplate(
        template, new MeasurementSchema("sensor2", TSDataType.INT32, TSEncoding.TS_2DIFF));
    schema.extendTemplate(
        template, new MeasurementSchema("sensor3", TSDataType.INT32, TSEncoding.TS_2DIFF));

    TsFileWriter tsFileWriter = new TsFileWriter(file, schema);
    tsFileWriter.registerDevice("root.lemming.device1", template);
    tsFileWriter.registerDevice("root.lemming.device2", template);
    tsFileWriter.registerDevice("root.lemming.device3", template);

    // construct TSRecord
    TSRecord tsRecord = new TSRecord("root.lemming.device1", 1617206403001L);
    DataPoint dPoint1 = new FloatDataPoint("sensor1", 1.1f);
    DataPoint dPoint2 = new IntDataPoint("sensor2", 12);
    DataPoint dPoint3 = new IntDataPoint("sensor3", 13);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.writeRecord(tsRecord);
    tsFileWriter.flush(); // flush above data to disk at once

    tsRecord = new TSRecord("root.lemming.device2", 1617206403002L);
    dPoint2 = new IntDataPoint("sensor2", 22);
    tsRecord.addTuple(dPoint2);
    tsFileWriter.writeRecord(tsRecord);
    tsFileWriter.flush(); // flush above data to disk at once

    tsRecord = new TSRecord("root.lemming.device3", 1617206403003L);
    dPoint1 = new FloatDataPoint("sensor1", 3.1f);
    dPoint2 = new IntDataPoint("sensor2", 32);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsFileWriter.writeRecord(tsRecord);
    tsFileWriter.flush(); // flush above data to disk at once

    tsRecord = new TSRecord("root.lemming.device1", 1617206403004L);
    dPoint1 = new FloatDataPoint("sensor1", 4.1f);
    dPoint2 = new IntDataPoint("sensor2", 42);
    dPoint3 = new IntDataPoint("sensor3", 43);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.writeRecord(tsRecord);
    tsFileWriter.flush(); // flush above data to disk at once

    // close TsFile
    tsFileWriter.close();
  }

  private void creatModsFile(String modsFilePath) throws IllegalPathException {
    ModEntry[] modifications =
        new ModEntry[] {
          new TreeDeletionEntry(new MeasurementPath("root.lemming.device1.sensor1"), 1),
          new TreeDeletionEntry(new MeasurementPath("root.lemming.device1.sensor1"), 2, 5),
          new TreeDeletionEntry(new MeasurementPath("root.lemming.**"), 1, Long.MAX_VALUE)
        };

    try (ModificationFile mFile = new ModificationFile(new File(modsFilePath), false)) {
      for (ModEntry mod : modifications) {
        mFile.write(mod);
      }
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  @After
  public void tearDown() throws Exception {
    PipeTsFileDirPathResolver.reset();

    File pipeFolder = new File(ROOT_DIR);
    if (pipeFolder.exists()) {
      FileUtils.deleteFileOrDirectory(pipeFolder);
    }

    PipeDataNodeAgent.runtime().stopPeriodicalJobExecutor();
    PipeDataNodeAgent.runtime().clearPeriodicalJobExecutor();
  }

  @Test
  public void testIncreaseTsFile() throws IOException {
    final File originTsfile = new File(TS_FILE_NAME);
    final File originModFile = new File(MODS_FILE_NAME);
    Assert.assertEquals(0, pipeTsFileResourceManager.getFileReferenceCount(originTsfile, null));
    Assert.assertEquals(0, pipeTsFileResourceManager.getFileReferenceCount(originModFile, null));

    final File pipeTsfile =
        pipeTsFileResourceManager.increaseFileReference(originTsfile, true, PIPE_NAME);
    final File pipeModFile =
        pipeTsFileResourceManager.increaseFileReference(originModFile, false, PIPE_NAME);
    Assert.assertEquals(1, pipeTsFileResourceManager.getFileReferenceCount(pipeTsfile, null));
    Assert.assertEquals(1, pipeTsFileResourceManager.getFileReferenceCount(pipeModFile, null));
    Assert.assertTrue(Files.exists(originTsfile.toPath()));
    Assert.assertTrue(Files.exists(originModFile.toPath()));
    Assert.assertTrue(Files.exists(pipeTsfile.toPath()));
    Assert.assertTrue(Files.exists(pipeModFile.toPath()));

    // test use assigner's hardlinkTsFile to increase reference counts
    // test null, shall not reuse the pipe's tsFile
    pipeTsFileResourceManager.increaseFileReference(pipeTsfile, true, PIPE_NAME);
    Assert.assertEquals(2, pipeTsFileResourceManager.getFileReferenceCount(pipeTsfile, null));
    Assert.assertEquals(2, pipeTsFileResourceManager.getFileReferenceCount(pipeTsfile, PIPE_NAME));
    Assert.assertTrue(Files.exists(originTsfile.toPath()));
    Assert.assertTrue(Files.exists(pipeTsfile.toPath()));

    // test use copyFile to increase reference counts
    pipeTsFileResourceManager.increaseFileReference(pipeModFile, false, PIPE_NAME);
    Assert.assertEquals(2, pipeTsFileResourceManager.getFileReferenceCount(pipeModFile, null));
    Assert.assertEquals(2, pipeTsFileResourceManager.getFileReferenceCount(pipeModFile, PIPE_NAME));
    Assert.assertTrue(Files.exists(originModFile.toPath()));
    Assert.assertTrue(Files.exists(pipeModFile.toPath()));
  }

  @Test
  public void testDecreaseTsFile() throws IOException {
    final File originFile = new File(TS_FILE_NAME);
    final File originModFile = new File(MODS_FILE_NAME);

    pipeTsFileResourceManager.decreaseFileReference(originFile, PIPE_NAME);
    pipeTsFileResourceManager.decreaseFileReference(originModFile, PIPE_NAME);
    Assert.assertEquals(0, pipeTsFileResourceManager.getFileReferenceCount(originFile, null));
    Assert.assertEquals(0, pipeTsFileResourceManager.getFileReferenceCount(originModFile, null));

    final File pipeTsfile =
        pipeTsFileResourceManager.increaseFileReference(originFile, true, PIPE_NAME);
    final File pipeModFile =
        pipeTsFileResourceManager.increaseFileReference(originModFile, false, PIPE_NAME);
    Assert.assertEquals(1, pipeTsFileResourceManager.getFileReferenceCount(pipeTsfile, null));
    Assert.assertEquals(1, pipeTsFileResourceManager.getFileReferenceCount(pipeModFile, null));
    Assert.assertTrue(Files.exists(pipeTsfile.toPath()));
    Assert.assertTrue(Files.exists(pipeModFile.toPath()));
    Assert.assertTrue(Files.exists(pipeTsfile.toPath()));
    Assert.assertTrue(Files.exists(pipeModFile.toPath()));

    Assert.assertTrue(originFile.delete());
    Assert.assertTrue(originModFile.delete());
    Assert.assertFalse(Files.exists(originFile.toPath()));
    Assert.assertFalse(Files.exists(originModFile.toPath()));

    Assert.assertEquals(1, pipeTsFileResourceManager.getFileReferenceCount(pipeTsfile, PIPE_NAME));
    Assert.assertEquals(1, pipeTsFileResourceManager.getFileReferenceCount(pipeTsfile, null));
    Assert.assertEquals(1, pipeTsFileResourceManager.getFileReferenceCount(pipeModFile, PIPE_NAME));
    Assert.assertEquals(1, pipeTsFileResourceManager.getFileReferenceCount(pipeModFile, null));
    Assert.assertFalse(Files.exists(originFile.toPath()));
    Assert.assertFalse(Files.exists(originModFile.toPath()));
    Assert.assertTrue(Files.exists(pipeTsfile.toPath()));
    Assert.assertTrue(Files.exists(pipeModFile.toPath()));

    pipeTsFileResourceManager.decreaseFileReference(pipeTsfile, PIPE_NAME);
    pipeTsFileResourceManager.decreaseFileReference(pipeModFile, PIPE_NAME);
    Assert.assertEquals(0, pipeTsFileResourceManager.getFileReferenceCount(pipeTsfile, PIPE_NAME));
    Assert.assertEquals(0, pipeTsFileResourceManager.getFileReferenceCount(pipeTsfile, null));
    Assert.assertEquals(0, pipeTsFileResourceManager.getFileReferenceCount(pipeModFile, PIPE_NAME));
    Assert.assertEquals(0, pipeTsFileResourceManager.getFileReferenceCount(pipeModFile, null));
    Assert.assertFalse(Files.exists(originFile.toPath()));
    Assert.assertFalse(Files.exists(originModFile.toPath()));
  }

  @Test
  public void testUnpinAfterOriginalModsRemoved() throws Exception {
    final TsFileResource resource = new TsFileResource(new File(TS_FILE_NAME));
    resource
        .getExclusiveModFile()
        .write(new TreeDeletionEntry(new MeasurementPath("root.lemming.**"), 0, Long.MAX_VALUE));
    resource.getExclusiveModFile().close();

    final boolean pinnedModFile =
        pipeTsFileResourceManager.pinTsFileResource(resource, true, PIPE_NAME);
    Assert.assertTrue(pinnedModFile);
    final File pipeTsFile =
        PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(resource.getTsFile(), PIPE_NAME);
    final File pipeMods = ModificationFile.getExclusiveMods(pipeTsFile);
    Assert.assertEquals(1, pipeTsFileResourceManager.getFileReferenceCount(pipeMods, PIPE_NAME));

    Assert.assertTrue(resource.remove());
    Assert.assertFalse(resource.exclusiveModFileExists());
    Assert.assertTrue(pipeMods.exists());
    pipeTsFileResourceManager.unpinTsFileResource(resource, PIPE_NAME, pinnedModFile);

    Assert.assertEquals(0, pipeTsFileResourceManager.getFileReferenceCount(pipeTsFile, PIPE_NAME));
    Assert.assertEquals(0, pipeTsFileResourceManager.getFileReferenceCount(pipeMods, PIPE_NAME));
    Assert.assertEquals(0, pipeTsFileResourceManager.getFileReferenceCount(pipeMods, null));
    Assert.assertFalse(pipeMods.exists());
  }

  @Test
  public void testUnpinDoesNotReleaseModsCreatedAfterPinning() throws Exception {
    final TsFileResource resource = new TsFileResource(new File(TS_FILE_NAME));
    final boolean pinnedModFile =
        pipeTsFileResourceManager.pinTsFileResource(resource, true, PIPE_NAME);
    Assert.assertFalse(pinnedModFile);

    // An event can acquire mods created after historical selection pinned the TsFile alone.
    resource
        .getExclusiveModFile()
        .write(new TreeDeletionEntry(new MeasurementPath("root.lemming.**"), 0, Long.MAX_VALUE));
    resource.getExclusiveModFile().close();
    final File eventMods =
        pipeTsFileResourceManager.increaseFileReference(
            resource.getExclusiveModFile().getFile(), false, PIPE_NAME);
    pipeTsFileResourceManager.unpinTsFileResource(resource, PIPE_NAME, pinnedModFile);

    Assert.assertEquals(1, pipeTsFileResourceManager.getFileReferenceCount(eventMods, PIPE_NAME));
    Assert.assertTrue(eventMods.exists());
    pipeTsFileResourceManager.decreaseFileReference(eventMods, PIPE_NAME);
    Assert.assertEquals(0, pipeTsFileResourceManager.getFileReferenceCount(eventMods, PIPE_NAME));
    Assert.assertFalse(eventMods.exists());
  }

  @Test
  public void testConcurrentIncreaseTsFile() throws Exception {
    assertConcurrentIncreaseFileReference(new File(TS_FILE_NAME), true);
  }

  @Test
  public void testConcurrentIncreaseCopiedFile() throws Exception {
    assertConcurrentIncreaseFileReference(new File(MODS_FILE_NAME), false);
  }

  @Test
  public void testIncreaseFileReferenceRollsBackOnPublicReferenceFailure() throws Exception {
    final File originModFile = new File(MODS_FILE_NAME);
    final File pipeModFile =
        PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(originModFile, PIPE_NAME);
    final File publicModFile =
        new File(pipeModFile.getParentFile().getParentFile(), pipeModFile.getName());
    Assert.assertTrue(publicModFile.mkdirs());

    Assert.assertThrows(
        IOException.class,
        () -> pipeTsFileResourceManager.increaseFileReference(originModFile, false, PIPE_NAME));

    Assert.assertEquals(0, pipeTsFileResourceManager.getFileReferenceCount(pipeModFile, PIPE_NAME));
    Assert.assertFalse(Files.exists(pipeModFile.toPath()));
  }

  private void assertConcurrentIncreaseFileReference(final File originFile, final boolean isTsFile)
      throws Exception {
    final int concurrency = 64;
    final CountDownLatch readyLatch = new CountDownLatch(concurrency);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final ExecutorService executor = Executors.newFixedThreadPool(concurrency);
    final List<Future<File>> futures = new ArrayList<>(concurrency);

    try {
      for (int i = 0; i < concurrency; i++) {
        futures.add(
            executor.submit(
                () -> {
                  readyLatch.countDown();
                  startLatch.await();
                  return pipeTsFileResourceManager.increaseFileReference(
                      originFile, isTsFile, PIPE_NAME);
                }));
      }

      Assert.assertTrue(readyLatch.await(30, TimeUnit.SECONDS));
      startLatch.countDown();

      File pipeFile = null;
      for (final Future<File> future : futures) {
        final File referencedFile = future.get(30, TimeUnit.SECONDS);
        if (pipeFile == null) {
          pipeFile = referencedFile;
        } else {
          Assert.assertEquals(pipeFile, referencedFile);
        }
      }

      Assert.assertNotNull(pipeFile);
      Assert.assertEquals(
          concurrency, pipeTsFileResourceManager.getFileReferenceCount(pipeFile, PIPE_NAME));
      Assert.assertEquals(
          concurrency, pipeTsFileResourceManager.getFileReferenceCount(pipeFile, null));
      Assert.assertTrue(Files.exists(pipeFile.toPath()));

      for (int i = 0; i < concurrency; i++) {
        pipeTsFileResourceManager.decreaseFileReference(pipeFile, PIPE_NAME);
      }
      Assert.assertEquals(0, pipeTsFileResourceManager.getFileReferenceCount(pipeFile, PIPE_NAME));
      Assert.assertEquals(0, pipeTsFileResourceManager.getFileReferenceCount(pipeFile, null));
      Assert.assertFalse(Files.exists(pipeFile.toPath()));
    } finally {
      startLatch.countDown();
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testUnclosedTsFileIsNotCopiedWhenHardlinkFails() throws Exception {
    final File originTsFile = new File(TS_FILE_NAME);
    final File pipeTsFileDir =
        PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(originTsFile, PIPE_NAME)
            .getParentFile();
    // Block the pipe dir with a regular file, so that the hardlink fails and the copy is taken.
    final File blockingFile = pipeTsFileDir.getParentFile();
    Assert.assertTrue(blockingFile.getParentFile().mkdirs());
    Assert.assertTrue(blockingFile.createNewFile());

    final TsFileResource unclosedResource = Mockito.mock(TsFileResource.class);
    Mockito.when(unclosedResource.getTsFile()).thenReturn(originTsFile);
    Mockito.when(unclosedResource.isClosed()).thenReturn(false);

    // A copy of an open TsFile would be truncated, so the pin must fail and be retried later.
    final String unclosedMessage =
        String.format(
            DataNodePipeMessages
                .EXCEPTION_FAILED_TO_PIN_THE_TSFILE_ARG_BY_CREATING_A_HARDLINK_AND_THE_TSFILE_CAN_NOT_BE_COPIED_BECAUSE_IT_IS_NOT_CLOSED_YET_4E346D68,
            originTsFile.getPath());
    try {
      pipeTsFileResourceManager.pinTsFileResource(unclosedResource, false, PIPE_NAME);
      Assert.fail("The TsFile which is not closed yet should not be copied");
    } catch (final IOException e) {
      Assert.assertEquals(unclosedMessage, e.getMessage());
      // The reason why the hardlink failed is kept as a suppressed exception.
      Assert.assertEquals(1, e.getSuppressed().length);
      Assert.assertTrue(e.getSuppressed()[0] instanceof IOException);
    }

    // A closed TsFile does fall back to copying, which fails only because the dir is blocked.
    final TsFileResource closedResource = Mockito.mock(TsFileResource.class);
    Mockito.when(closedResource.getTsFile()).thenReturn(originTsFile);
    Mockito.when(closedResource.isClosed()).thenReturn(true);
    try {
      pipeTsFileResourceManager.pinTsFileResource(closedResource, false, PIPE_NAME);
      Assert.fail("The pipe directory is blocked by a file, so the copy should fail");
    } catch (final IOException e) {
      Assert.assertNotEquals(unclosedMessage, e.getMessage());
    }
    // Nothing must have been pinned or copied.
    Assert.assertFalse(pipeTsFileDir.toPath().toFile().exists());
  }

  @Test
  public void testExistingPipeFileIsReusedInsteadOfCopiedOntoItself() throws Exception {
    final File originTsFile = new File(TS_FILE_NAME);
    final File pipeTsFile =
        PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(originTsFile, PIPE_NAME);
    Assert.assertTrue(pipeTsFile.getParentFile().mkdirs());
    // Simulate a pipe file which has been left by a previous process and is not tracked yet.
    FileUtils.copyFile(originTsFile, pipeTsFile);

    // The target is the source here, so a failed pin must not delete it.
    final File referencedFile =
        pipeTsFileResourceManager.increaseFileReference(pipeTsFile, true, PIPE_NAME);
    Assert.assertEquals(pipeTsFile, referencedFile);
    Assert.assertTrue(pipeTsFile.exists());
    Assert.assertEquals(
        1, pipeTsFileResourceManager.getFileReferenceCount(referencedFile, PIPE_NAME));

    pipeTsFileResourceManager.decreaseFileReference(referencedFile, PIPE_NAME);
    Assert.assertEquals(0, pipeTsFileResourceManager.getFileReferenceCount(pipeTsFile, PIPE_NAME));
    Assert.assertFalse(pipeTsFile.exists());
  }

  @Test
  public void testPinTsFileUnderConfiguredDataDirKeepsItsOwnDisk() throws Exception {
    final File dataDir = new File(ROOT_DIR, "data").getCanonicalFile();
    final File tsFile =
        new File(dataDir, IoTDBConstant.SEQUENCE_FOLDER_NAME + File.separator + "test.tsfile");
    createTsfile(tsFile.getPath());
    PipeTsFileDirPathResolver.setInstanceForTest(() -> new String[] {dataDir.getPath()});

    final File pipeTsFile =
        PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(tsFile, PIPE_NAME);
    Assert.assertEquals(
        new File(
            new File(new File(dataDir, "pipe"), "tsfile"),
            PIPE_NAME + File.separator + "sequence-test.tsfile"),
        pipeTsFile);

    pipeTsFileResourceManager.pinTsFileResource(closedResourceOf(tsFile), false, PIPE_NAME);
    Assert.assertTrue(pipeTsFile.isFile());
    Assert.assertEquals(tsFile.length(), pipeTsFile.length());

    pipeTsFileResourceManager.unpinTsFileResource(closedResourceOf(tsFile), PIPE_NAME, false);
    Assert.assertFalse(pipeTsFile.exists());
  }

  @Test
  public void testCleanPipeTsFileDirCleansEveryDataDir() throws Exception {
    final File dataDir1 = new File(ROOT_DIR, "data1").getCanonicalFile();
    final File dataDir2 = new File(ROOT_DIR, "data2").getCanonicalFile();
    final File tsFile1 =
        new File(dataDir1, IoTDBConstant.SEQUENCE_FOLDER_NAME + File.separator + "f1.tsfile");
    final File tsFile2 =
        new File(dataDir2, IoTDBConstant.SEQUENCE_FOLDER_NAME + File.separator + "f2.tsfile");
    createTsfile(tsFile1.getPath());
    createTsfile(tsFile2.getPath());
    PipeTsFileDirPathResolver.setInstanceForTest(
        () -> new String[] {dataDir1.getPath(), dataDir2.getPath()});

    pipeTsFileResourceManager.pinTsFileResource(closedResourceOf(tsFile1), false, PIPE_NAME);
    pipeTsFileResourceManager.pinTsFileResource(closedResourceOf(tsFile2), false, PIPE_NAME);
    final File pipeTsFileDir1 =
        PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(tsFile1, PIPE_NAME)
            .getParentFile();
    final File pipeTsFileDir2 =
        PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(tsFile2, PIPE_NAME)
            .getParentFile();
    Assert.assertNotEquals(pipeTsFileDir1, pipeTsFileDir2);
    Assert.assertTrue(pipeTsFileDir1.isDirectory());
    Assert.assertTrue(pipeTsFileDir2.isDirectory());

    pipeTsFileResourceManager.cleanPipeTsFileDir(PIPE_NAME);

    Assert.assertFalse(pipeTsFileDir1.exists());
    Assert.assertFalse(pipeTsFileDir2.exists());
    Assert.assertTrue(pipeTsFileDir1.getParentFile().isDirectory());
    Assert.assertTrue(pipeTsFileDir2.getParentFile().isDirectory());
  }

  @Test
  public void testPinSucceedsOnRetryAfterTheHardlinkFailed() throws Exception {
    final File originTsFile = new File(TS_FILE_NAME);
    final File pipeTsFile =
        PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(originTsFile, PIPE_NAME);
    final File blockingFile = pipeTsFile.getParentFile().getParentFile();
    Assert.assertTrue(blockingFile.getParentFile().mkdirs());
    Assert.assertTrue(blockingFile.createNewFile());

    try {
      pipeTsFileResourceManager.pinTsFileResource(closedResourceOf(originTsFile), false, PIPE_NAME);
      Assert.fail("The pipe directory is blocked by a file, so the pin should fail");
    } catch (final IOException e) {
      // Expected: the failed pin must not leave anything behind.
      Assert.assertFalse(pipeTsFile.exists());
      Assert.assertFalse(new File(pipeTsFile.getPath() + ".unfinished").exists());
    }

    Assert.assertTrue(blockingFile.delete());
    pipeTsFileResourceManager.pinTsFileResource(closedResourceOf(originTsFile), false, PIPE_NAME);
    Assert.assertTrue(pipeTsFile.isFile());
    Assert.assertEquals(originTsFile.length(), pipeTsFile.length());
    Assert.assertFalse(new File(pipeTsFile.getPath() + ".unfinished").exists());

    pipeTsFileResourceManager.unpinTsFileResource(closedResourceOf(originTsFile), PIPE_NAME, false);
    Assert.assertFalse(pipeTsFile.exists());
  }

  @Test
  public void testStalePipeFileWithDifferentContentIsReplaced() throws Exception {
    final File originTsFile = new File(TS_FILE_NAME);
    final File pipeTsFile =
        PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(originTsFile, PIPE_NAME);
    Assert.assertTrue(pipeTsFile.getParentFile().mkdirs());
    // A stale pipe file, e.g. left by a killed process: it is replaced, not reused.
    Files.write(pipeTsFile.toPath(), new byte[] {1, 2, 3});

    pipeTsFileResourceManager.pinTsFileResource(closedResourceOf(originTsFile), false, PIPE_NAME);

    Assert.assertEquals(originTsFile.length(), pipeTsFile.length());
  }

  @Test
  public void testStaleCopiedFileIsReplacedInsteadOfFailingThePin() throws Exception {
    final File originModFile = new File(MODS_FILE_NAME);
    final File pipeModFile =
        PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(originModFile, PIPE_NAME);
    Assert.assertTrue(pipeModFile.getParentFile().mkdirs());
    // A partial copy and the temporary file of a failure that has been killed before.
    Files.write(pipeModFile.toPath(), new byte[] {1, 2, 3});
    Files.write(new File(pipeModFile.getPath() + ".unfinished").toPath(), new byte[] {4});

    final TsFileResource resource = closedResourceOf(new File(TS_FILE_NAME));
    final ModificationFile modFile = Mockito.mock(ModificationFile.class);
    Mockito.when(modFile.exists()).thenReturn(true);
    Mockito.when(modFile.getFile()).thenReturn(originModFile);
    Mockito.when(resource.getExclusiveModFile()).thenReturn(modFile);

    Assert.assertTrue(pipeTsFileResourceManager.pinTsFileResource(resource, true, PIPE_NAME));

    Assert.assertEquals(originModFile.length(), pipeModFile.length());
    Assert.assertFalse(new File(pipeModFile.getPath() + ".unfinished").exists());
  }

  @Test
  public void testAtomicCopyLeavesNothingBehindWhenItFails() throws Exception {
    final File targetFile =
        PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(
            new File(MODS_FILE_NAME), PIPE_NAME);
    try {
      PipeTsFileResourceManager.copyFileAtomically(new File(ROOT_DIR, "missing.mod"), targetFile);
      Assert.fail("Copying a missing file should fail");
    } catch (final IOException e) {
      Assert.assertFalse(targetFile.exists());
      Assert.assertFalse(new File(targetFile.getPath() + ".unfinished").exists());
    }
  }

  @Test
  public void testMissingPipeFileFailsThePin() throws Exception {
    final File pipeTsFile =
        PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(
            new File(TS_FILE_NAME), PIPE_NAME);
    // An event pinned after the pipe directory has been moved aside must not be reported as pinned.
    final File missingPipeFile = new File(pipeTsFile.getParentFile(), "missing.tsfile");
    try {
      pipeTsFileResourceManager.increaseFileReference(missingPipeFile, true, PIPE_NAME);
      Assert.fail("The pipe file does not exist, so the pin should fail");
    } catch (final IOException e) {
      Assert.assertEquals(
          String.format(
              DataNodePipeMessages
                  .EXCEPTION_FAILED_TO_PIN_THE_PIPE_FILE_ARG_BECAUSE_IT_DOES_NOT_EXIST_1741B8E4,
              missingPipeFile.getPath()),
          e.getMessage());
    }
    Assert.assertEquals(
        0, pipeTsFileResourceManager.getFileReferenceCount(missingPipeFile, PIPE_NAME));
  }

  private static TsFileResource closedResourceOf(final File tsFile) {
    final TsFileResource resource = Mockito.mock(TsFileResource.class);
    Mockito.when(resource.getTsFile()).thenReturn(tsFile);
    Mockito.when(resource.isClosed()).thenReturn(true);
    return resource;
  }
}
