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
package org.apache.iotdb.db.storageengine.dataregion.compaction.cross;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InsertionCrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.SimpleCompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleContext;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.RewriteCrossSpaceCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.CrossSpaceCompactionCandidate;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.InsertionCrossCompactionTaskResource;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ArrayDeviceTimeIndex;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Phaser;

import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.writeNonAlignedChunk;

public class InsertionCrossSpaceCompactionRecoverTest extends AbstractCompactionTest {
  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
  }

  @Test
  public void testRecoverWithTargetModFileNotExistedAndSourceModFileExisted()
      throws IOException, MergeException, IllegalPathException {
    IDeviceID d1 = IDeviceID.Factory.DEFAULT_FACTORY.create("root.testsg.d1");
    IDeviceID d2 = IDeviceID.Factory.DEFAULT_FACTORY.create("root.testsg.d2");

    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    seqResource1.updateStartTime(d2, 20);
    seqResource1.updateEndTime(d2, 30);
    createTsFileByResource(seqResource1);
    seqResource1.serialize();

    TsFileResource seqResource2 = createTsFileResource("3-3-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 30);
    seqResource2.updateEndTime(d1, 40);
    seqResource2.updateStartTime(d2, 40);
    seqResource2.updateEndTime(d2, 50);
    createTsFileByResource(seqResource2);
    seqResource2.serialize();

    seqResources.add(seqResource1);
    seqResources.add(seqResource2);

    TsFileResource unseqResource1 = createTsFileResource("9-9-0-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 22);
    unseqResource1.updateEndTime(d1, 25);
    unseqResource1.updateStartTime(d2, 31);
    unseqResource1.updateEndTime(d2, 37);
    createTsFileByResource(unseqResource1);
    unseqResource1.serialize();

    Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
    deleteMap.put(d1.toString() + ".s1", new Pair<>(0L, 300L));
    CompactionFileGeneratorUtils.generateMods(deleteMap, unseqResource1, false);

    unseqResources.add(unseqResource1);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector(
            "root.testsg", "0", 0, tsFileManager, new CompactionScheduleContext());
    InsertionCrossCompactionTaskResource taskResource =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, taskResource.toInsertUnSeqFile);
    Assert.assertEquals(seqResource1, taskResource.prevSeqFile);
    Assert.assertEquals(seqResource2, taskResource.nextSeqFile);
    Assert.assertEquals(unseqResource1, taskResource.firstUnSeqFileInParitition);

    InsertionCrossSpaceCompactionTask task =
        new InsertionCrossSpaceCompactionTask(new Phaser(), 0, tsFileManager, taskResource, 0);
    TsFileResource targetFile = new TsFileResource(task.generateTargetFile());
    File logFile =
        new File(
            targetFile.getTsFilePath() + CompactionLogger.INSERTION_COMPACTION_LOG_NAME_SUFFIX);

    CompactionFileGeneratorUtils.generateMods(deleteMap, unseqResource1, true);

    try (SimpleCompactionLogger logger = new SimpleCompactionLogger(logFile)) {
      logger.logSourceFile(taskResource.toInsertUnSeqFile);
      logger.logTargetFile(targetFile);
      logger.force();

      File sourceTsFile = unseqResource1.getTsFile();
      File targetTsFile = targetFile.getTsFile();
      Files.createLink(targetTsFile.toPath(), sourceTsFile.toPath());
      Files.createLink(
          new File(targetTsFile.getPath() + TsFileResource.RESOURCE_SUFFIX).toPath(),
          new File(sourceTsFile.getPath() + TsFileResource.RESOURCE_SUFFIX).toPath());
    }

    // recover compaction, all source file should exist and target file should be deleted
    new InsertionCrossSpaceCompactionTask("root.testsg", "0", tsFileManager, logFile).recover();

    Assert.assertTrue(unseqResource1.getTsFile().exists());
    Assert.assertTrue(
        new File(unseqResource1.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
    Assert.assertTrue(unseqResource1.anyModFileExists());
    Assert.assertFalse(unseqResource1.getCompactionModFile().getFileLength() > 0);

    Assert.assertFalse(targetFile.tsFileExists());
    Assert.assertFalse(targetFile.resourceFileExists());
    Assert.assertFalse(targetFile.anyModFileExists());
  }

  @Test
  public void testRecoverWithTargetModFileNotExistedAndSourceModNotExisted()
      throws IOException, MergeException, IllegalPathException {
    IDeviceID d1 = IDeviceID.Factory.DEFAULT_FACTORY.create("root.testsg.d1");
    IDeviceID d2 = IDeviceID.Factory.DEFAULT_FACTORY.create("root.testsg.d2");

    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    seqResource1.updateStartTime(d2, 20);
    seqResource1.updateEndTime(d2, 30);
    createTsFileByResource(seqResource1);
    seqResource1.serialize();

    TsFileResource seqResource2 = createTsFileResource("3-3-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 30);
    seqResource2.updateEndTime(d1, 40);
    seqResource2.updateStartTime(d2, 40);
    seqResource2.updateEndTime(d2, 50);
    createTsFileByResource(seqResource2);
    seqResource2.serialize();

    seqResources.add(seqResource1);
    seqResources.add(seqResource2);

    TsFileResource unseqResource1 = createTsFileResource("9-9-0-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 22);
    unseqResource1.updateEndTime(d1, 25);
    unseqResource1.updateStartTime(d2, 31);
    unseqResource1.updateEndTime(d2, 37);
    createTsFileByResource(unseqResource1);
    unseqResource1.serialize();

    Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
    deleteMap.put(d1.toString() + ".s1", new Pair<>(0L, 300L));

    unseqResources.add(unseqResource1);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector(
            "root.testsg", "0", 0, tsFileManager, new CompactionScheduleContext());
    InsertionCrossCompactionTaskResource taskResource =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, taskResource.toInsertUnSeqFile);
    Assert.assertEquals(seqResource1, taskResource.prevSeqFile);
    Assert.assertEquals(seqResource2, taskResource.nextSeqFile);
    Assert.assertEquals(unseqResource1, taskResource.firstUnSeqFileInParitition);

    InsertionCrossSpaceCompactionTask task =
        new InsertionCrossSpaceCompactionTask(new Phaser(), 0, tsFileManager, taskResource, 0);
    TsFileResource targetFile = new TsFileResource(task.generateTargetFile());
    File logFile =
        new File(
            targetFile.getTsFilePath() + CompactionLogger.INSERTION_COMPACTION_LOG_NAME_SUFFIX);

    try (SimpleCompactionLogger logger = new SimpleCompactionLogger(logFile)) {
      logger.logSourceFile(taskResource.toInsertUnSeqFile);
      logger.logTargetFile(targetFile);
      logger.force();

      File sourceTsFile = unseqResource1.getTsFile();
      File targetTsFile = targetFile.getTsFile();
      Files.createLink(targetTsFile.toPath(), sourceTsFile.toPath());
      Files.createLink(
          new File(targetTsFile.getPath() + TsFileResource.RESOURCE_SUFFIX).toPath(),
          new File(sourceTsFile.getPath() + TsFileResource.RESOURCE_SUFFIX).toPath());
      if (unseqResource1.anyModFileExists()) {
        Files.createLink(
            ModificationFile.getExclusiveMods(targetTsFile).toPath(),
            ModificationFile.getExclusiveMods(sourceTsFile).toPath());
      }
    }

    // recover compaction, all source file should exist and target file should be deleted
    new InsertionCrossSpaceCompactionTask("root.testsg", "0", tsFileManager, logFile).recover();

    Assert.assertFalse(unseqResource1.getTsFile().exists());
    Assert.assertFalse(
        new File(unseqResource1.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
    Assert.assertFalse(unseqResource1.anyModFileExists());
    Assert.assertFalse(unseqResource1.getCompactionModFile().exists());

    Assert.assertTrue(targetFile.tsFileExists());
    Assert.assertTrue(targetFile.resourceFileExists());
    Assert.assertFalse(targetFile.anyModFileExists());
  }

  @Test
  public void testRecoverWithAllTargetFileExisted()
      throws IllegalPathException, IOException, MergeException {
    IDeviceID d1 = IDeviceID.Factory.DEFAULT_FACTORY.create("root.testsg.d1");
    IDeviceID d2 = IDeviceID.Factory.DEFAULT_FACTORY.create("root.testsg.d2");

    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    seqResource1.updateStartTime(d2, 20);
    seqResource1.updateEndTime(d2, 30);
    createTsFileByResource(seqResource1);
    seqResource1.serialize();

    TsFileResource seqResource2 = createTsFileResource("3-3-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 30);
    seqResource2.updateEndTime(d1, 40);
    seqResource2.updateStartTime(d2, 40);
    seqResource2.updateEndTime(d2, 50);
    createTsFileByResource(seqResource2);
    seqResource2.serialize();

    seqResources.add(seqResource1);
    seqResources.add(seqResource2);

    TsFileResource unseqResource1 = createTsFileResource("9-9-0-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 22);
    unseqResource1.updateEndTime(d1, 25);
    unseqResource1.updateStartTime(d2, 31);
    unseqResource1.updateEndTime(d2, 37);
    createTsFileByResource(unseqResource1);
    unseqResource1.serialize();

    Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
    deleteMap.put(d1.toString() + ".s1", new Pair<>(0L, 300L));
    CompactionFileGeneratorUtils.generateMods(deleteMap, unseqResource1, false);

    unseqResources.add(unseqResource1);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector(
            "root.testsg", "0", 0, tsFileManager, new CompactionScheduleContext());
    InsertionCrossCompactionTaskResource taskResource =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, taskResource.toInsertUnSeqFile);
    Assert.assertEquals(seqResource1, taskResource.prevSeqFile);
    Assert.assertEquals(seqResource2, taskResource.nextSeqFile);
    Assert.assertEquals(unseqResource1, taskResource.firstUnSeqFileInParitition);

    InsertionCrossSpaceCompactionTask task =
        new InsertionCrossSpaceCompactionTask(new Phaser(), 0, tsFileManager, taskResource, 0);
    TsFileResource targetFile = new TsFileResource(task.generateTargetFile());
    File logFile =
        new File(
            targetFile.getTsFilePath() + CompactionLogger.INSERTION_COMPACTION_LOG_NAME_SUFFIX);

    CompactionFileGeneratorUtils.generateMods(deleteMap, unseqResource1, true);

    try (SimpleCompactionLogger logger = new SimpleCompactionLogger(logFile)) {
      logger.logSourceFile(taskResource.toInsertUnSeqFile);
      logger.logTargetFile(targetFile);
      logger.force();

      File sourceTsFile = unseqResource1.getTsFile();
      File targetTsFile = targetFile.getTsFile();
      Files.createLink(targetTsFile.toPath(), sourceTsFile.toPath());
      Files.createLink(
          new File(targetTsFile.getPath() + TsFileResource.RESOURCE_SUFFIX).toPath(),
          new File(sourceTsFile.getPath() + TsFileResource.RESOURCE_SUFFIX).toPath());
      if (unseqResource1.anyModFileExists()) {
        Files.createLink(
            ModificationFile.getExclusiveMods(targetTsFile).toPath(),
            ModificationFile.getExclusiveMods(sourceTsFile).toPath());
      }
    }

    // recover compaction, all source file should be deleted and target file should be existed
    new InsertionCrossSpaceCompactionTask("root.testsg", "0", tsFileManager, logFile).recover();

    Assert.assertFalse(unseqResource1.getTsFile().exists());
    Assert.assertFalse(
        new File(unseqResource1.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
    Assert.assertFalse(unseqResource1.getTotalModSizeInByte() > 0);
    Assert.assertFalse(unseqResource1.getCompactionModFile().getFileLength() > 0);

    Assert.assertTrue(targetFile.tsFileExists());
    Assert.assertTrue(targetFile.resourceFileExists());
    Assert.assertTrue(targetFile.anyModFileExists());
  }

  @Test
  public void testRecoverWithTargetFileNotExist()
      throws IllegalPathException, IOException, MergeException {
    IDeviceID d1 = IDeviceID.Factory.DEFAULT_FACTORY.create("root.testsg.d1");
    IDeviceID d2 = IDeviceID.Factory.DEFAULT_FACTORY.create("root.testsg.d2");

    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    seqResource1.updateStartTime(d2, 20);
    seqResource1.updateEndTime(d2, 30);
    createTsFileByResource(seqResource1);
    seqResource1.serialize();

    TsFileResource seqResource2 = createTsFileResource("3-3-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 30);
    seqResource2.updateEndTime(d1, 40);
    seqResource2.updateStartTime(d2, 40);
    seqResource2.updateEndTime(d2, 50);
    createTsFileByResource(seqResource2);
    seqResource2.serialize();

    seqResources.add(seqResource1);
    seqResources.add(seqResource2);

    TsFileResource unseqResource1 = createTsFileResource("9-9-0-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 22);
    unseqResource1.updateEndTime(d1, 25);
    unseqResource1.updateStartTime(d2, 31);
    unseqResource1.updateEndTime(d2, 37);
    createTsFileByResource(unseqResource1);
    unseqResource1.serialize();

    Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
    deleteMap.put(d1.toString() + ".s1", new Pair<>(0L, 300L));
    CompactionFileGeneratorUtils.generateMods(deleteMap, unseqResource1, false);

    unseqResources.add(unseqResource1);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector(
            "root.testsg", "0", 0, tsFileManager, new CompactionScheduleContext());
    InsertionCrossCompactionTaskResource taskResource =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, taskResource.toInsertUnSeqFile);
    Assert.assertEquals(seqResource1, taskResource.prevSeqFile);
    Assert.assertEquals(seqResource2, taskResource.nextSeqFile);
    Assert.assertEquals(unseqResource1, taskResource.firstUnSeqFileInParitition);

    InsertionCrossSpaceCompactionTask task =
        new InsertionCrossSpaceCompactionTask(new Phaser(), 0, tsFileManager, taskResource, 0);
    TsFileResource targetFile = new TsFileResource(task.generateTargetFile());
    File logFile =
        new File(
            targetFile.getTsFilePath() + CompactionLogger.INSERTION_COMPACTION_LOG_NAME_SUFFIX);

    CompactionFileGeneratorUtils.generateMods(deleteMap, unseqResource1, true);

    try (SimpleCompactionLogger logger = new SimpleCompactionLogger(logFile)) {
      logger.logSourceFile(taskResource.toInsertUnSeqFile);
      logger.logTargetFile(targetFile);
      logger.force();
    }

    // recover compaction, all target file and compaction.mods file should be deleted and source
    // file should be existed
    new InsertionCrossSpaceCompactionTask("root.testsg", "0", tsFileManager, logFile).recover();

    Assert.assertTrue(unseqResource1.getTsFile().exists());
    Assert.assertTrue(
        new File(unseqResource1.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
    Assert.assertTrue(unseqResource1.anyModFileExists());
    Assert.assertFalse(unseqResource1.getCompactionModFile().getFileLength() > 0);

    Assert.assertFalse(targetFile.tsFileExists());
    Assert.assertFalse(targetFile.resourceFileExists());
    Assert.assertFalse(targetFile.anyModFileExists());
  }

  private TsFileResource createTsFileResource(String name, boolean seq) {
    String filePath = (seq ? SEQ_DIRS : UNSEQ_DIRS) + File.separator + name;
    TsFileResource resource = new TsFileResource();
    resource.setTimeIndex(new ArrayDeviceTimeIndex());
    resource.setFile(new File(filePath));
    resource.setStatusForTest(TsFileResourceStatus.NORMAL);
    resource.setSeq(seq);
    return resource;
  }

  private void createTsFileByResource(TsFileResource resource) throws IOException {
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      for (IDeviceID device : resource.getDevices()) {
        // write d1
        tsFileIOWriter.startChunkGroup(device);
        MeasurementSchema schema =
            new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY);
        ChunkWriterImpl iChunkWriter = new ChunkWriterImpl(schema);
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(resource.getStartTime(device), resource.getEndTime(device)));
        writeNonAlignedChunk(iChunkWriter, tsFileIOWriter, pages, resource.isSeq());
        tsFileIOWriter.endChunkGroup();
      }
      tsFileIOWriter.endFile();
    }
  }
}
