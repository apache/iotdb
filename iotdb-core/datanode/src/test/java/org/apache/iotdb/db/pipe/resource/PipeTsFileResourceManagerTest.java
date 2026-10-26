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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.resource.tsfile.PipeTsFileResourceManager;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
    schema.extendTemplate(
        template, new MeasurementSchema("sensor1", TSDataType.FLOAT, TSEncoding.RLE));
    schema.extendTemplate(
        template, new MeasurementSchema("sensor2", TSDataType.INT32, TSEncoding.TS_2DIFF));
    schema.extendTemplate(
        template, new MeasurementSchema("sensor3", TSDataType.INT32, TSEncoding.TS_2DIFF));

    TsFileWriter tsFileWriter = new TsFileWriter(file, schema);

    // construct TSRecord
    TSRecord tsRecord = new TSRecord(1617206403001L, "root.lemming.device1");
    DataPoint dPoint1 = new FloatDataPoint("sensor1", 1.1f);
    DataPoint dPoint2 = new IntDataPoint("sensor2", 12);
    DataPoint dPoint3 = new IntDataPoint("sensor3", 13);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);
    tsFileWriter.flushAllChunkGroups(); // flush above data to disk at once

    tsRecord = new TSRecord(1617206403002L, "root.lemming.device2");
    dPoint2 = new IntDataPoint("sensor2", 22);
    tsRecord.addTuple(dPoint2);
    tsFileWriter.write(tsRecord);
    tsFileWriter.flushAllChunkGroups(); // flush above data to disk at once

    tsRecord = new TSRecord(1617206403003L, "root.lemming.device3");
    dPoint1 = new FloatDataPoint("sensor1", 3.1f);
    dPoint2 = new IntDataPoint("sensor2", 32);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsFileWriter.write(tsRecord);
    tsFileWriter.flushAllChunkGroups(); // flush above data to disk at once

    tsRecord = new TSRecord(1617206403004L, "root.lemming.device1");
    dPoint1 = new FloatDataPoint("sensor1", 4.1f);
    dPoint2 = new IntDataPoint("sensor2", 42);
    dPoint3 = new IntDataPoint("sensor3", 43);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);
    tsFileWriter.flushAllChunkGroups(); // flush above data to disk at once

    // close TsFile
    tsFileWriter.close();
  }

  private void creatModsFile(String modsFilePath) throws IllegalPathException {
    Modification[] modifications =
        new Modification[] {
          new Deletion(new PartialPath("root.lemming.device1.sensor1"), 2, 1),
          new Deletion(new PartialPath("root.lemming.device1.sensor1"), 3, 2, 5),
          new Deletion(new PartialPath("root.lemming.**"), 11, 1, Long.MAX_VALUE)
        };

    try (ModificationFile mFile = new ModificationFile(modsFilePath)) {
      for (Modification mod : modifications) {
        mFile.write(mod);
      }
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  @After
  public void tearDown() throws Exception {
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
}
