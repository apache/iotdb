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
package org.apache.iotdb.db.engine.merge;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.TsFileManagement;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.recover.MergeLogger;
import org.apache.iotdb.db.engine.merge.task.MergeTask;
import org.apache.iotdb.db.engine.merge.task.RecoverMergeTask;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.MERGING_MODIFICATION_FILE_NAME;
import static org.apache.iotdb.db.engine.storagegroup.TsFileResource.modifyTsFileNameUnseqMergCnt;

public class MergeRecoverTest extends MergeTest {
  private List<TsFileResource> sourceSeqFiles = new ArrayList<>();
  private List<TsFileResource> sourceUnseqFiles = new ArrayList<>();
  private List<TsFileResource> tmpSourceSeqFiles = new ArrayList<>();
  private List<TsFileResource> tmpSourceUnseqFiles = new ArrayList<>();
  private File logFile = new File(TestConstant.SEQUENCE_DATA_DIR, MergeLogger.MERGE_LOG_NAME);
  private final int seqFileNum = 10;
  private final int unseqFileNum = 5;
  private final File seqDataDir = new File(TestConstant.SEQUENCE_DATA_DIR);
  private final File unseqDataDir = new File(TestConstant.UNSEQUENCE_DATA_DIR);
  private final ModificationFile mergingModsFile =
      new ModificationFile(
          TestConstant.SEQUENCE_DATA_DIR + File.separator + MERGING_MODIFICATION_FILE_NAME);
  private TsFileManagement tsFileManagement =
      IoTDBDescriptor.getInstance()
          .getConfig()
          .getCompactionStrategy()
          .getTsFileManagement("root.sg1", "0", TestConstant.SEQUENCE_DATA_DIR);

  @Before
  public void setUp() throws IOException, MetadataException, WriteProcessException {
    IoTDB.metaManager.init();
    Assert.assertTrue(seqDataDir.mkdirs());
    Assert.assertTrue(unseqDataDir.mkdirs());
    prepareSeries();
    createFiles();
    tsFileManagement.addAll(sourceSeqFiles, true);
    tsFileManagement.addAll(sourceUnseqFiles, false);
    tsFileManagement.mergingModification = mergingModsFile;
    MergeManager.getINSTANCE().start();
  }

  @After
  public void tearDown() throws IOException {
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    IoTDB.metaManager.clear();
    EnvironmentUtils.cleanAllDir();
    MergeManager.getINSTANCE().stop();
    deleteFiles();
  }

  /**
   * source seq file index: 1~10<br>
   * source unseq file index: 11~15<br>
   * deleted file: 1.tsfile <br>
   * no target file exist
   */
  @Test
  public void testRecoverWithSomeSourceFilesLost() throws IOException, MetadataException {
    sourceSeqFiles.get(0).getTsFile().delete();
    tsFileManagement.remove(sourceSeqFiles.get(0), true);
    RecoverMergeTask recoverMergeTask =
        new RecoverMergeTask(
            tsFileManagement,
            TestConstant.SEQUENCE_DATA_DIR,
            tsFileManagement::mergeEndAction,
            "recoverTest",
            true,
            "root.sg1");
    recoverMergeTask.recoverMerge();
    for (TsFileResource seqResource : tmpSourceSeqFiles) {
      Assert.assertFalse(seqResource.getTsFile().exists());
      Assert.assertFalse(
          new File(seqResource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertFalse(new File(seqResource.getTsFilePath() + MergeTask.MERGE_SUFFIX).exists());
      Assert.assertFalse(seqResource.getModFile().exists());
      File targetFile = modifyTsFileNameUnseqMergCnt(seqResource.getTsFile());
      Assert.assertTrue(targetFile.exists());
      Assert.assertTrue(new File(targetFile.getPath() + TsFileResource.RESOURCE_SUFFIX).exists());
      ModificationFile targetModsFile =
          new ModificationFile(targetFile.getPath() + ModificationFile.FILE_SUFFIX);
      Assert.assertTrue(targetModsFile.exists());
      Assert.assertEquals(2, targetModsFile.getModifications().size());
    }
    for (TsFileResource unseqResource : tmpSourceUnseqFiles) {
      Assert.assertFalse(unseqResource.getTsFile().exists());
      Assert.assertFalse(
          new File(unseqResource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertFalse(unseqResource.getModFile().exists());
    }
    Assert.assertFalse(mergingModsFile.exists());
    Assert.assertFalse(logFile.exists());
  }

  /**
   * source seq file index: 1~10 <br>
   * source unseq file index: 11~15 <br>
   * deleted file: 1.tsfile, 1.tsfile.resource, 2.tsfile <br>
   * existed target file: 1.tsfile, 1.tsfile.resource, 2.tsfile
   */
  @Test
  public void testRecoverWithSomeSourceFilesLostAndSomeTargetFilesExist()
      throws IOException, MetadataException {
    File targetFile = modifyTsFileNameUnseqMergCnt(sourceSeqFiles.get(0).getTsFile());
    FSFactoryProducer.getFSFactory().moveFile(sourceSeqFiles.get(0).getTsFile(), targetFile);
    FSFactoryProducer.getFSFactory()
        .moveFile(
            new File(sourceSeqFiles.get(0).getTsFilePath() + TsFileResource.RESOURCE_SUFFIX),
            new File(targetFile.getPath() + TsFileResource.RESOURCE_SUFFIX));
    tsFileManagement.remove(sourceSeqFiles.get(0), true);
    tsFileManagement.add(new TsFileResource(targetFile), true);
    targetFile = modifyTsFileNameUnseqMergCnt(sourceSeqFiles.get(1).getTsFile());
    FSFactoryProducer.getFSFactory().moveFile(sourceSeqFiles.get(1).getTsFile(), targetFile);
    // sg recover will serialize resouce file
    TsFileResource targetTsFileResouce = new TsFileResource(targetFile);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getAbsolutePath())) {
      FileLoaderUtils.updateTsFileResource(reader, targetTsFileResouce);
    }
    targetTsFileResouce.serialize();
    tsFileManagement.remove(sourceSeqFiles.get(1), true);
    tsFileManagement.add(targetTsFileResouce, true);

    RecoverMergeTask recoverMergeTask =
        new RecoverMergeTask(
            tsFileManagement,
            TestConstant.SEQUENCE_DATA_DIR,
            tsFileManagement::mergeEndAction,
            "recoverTest",
            true,
            "root.sg1");
    recoverMergeTask.recoverMerge();
    for (TsFileResource seqResource : tmpSourceSeqFiles) {
      Assert.assertFalse(seqResource.getTsFile().exists());
      Assert.assertFalse(
          new File(seqResource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertFalse(new File(seqResource.getTsFilePath() + MergeTask.MERGE_SUFFIX).exists());
      targetFile = modifyTsFileNameUnseqMergCnt(seqResource.getTsFile());
      Assert.assertTrue(targetFile.exists());
      Assert.assertTrue(new File(targetFile.getPath() + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertFalse(seqResource.getModFile().exists());
      ModificationFile targetModsFile =
          new ModificationFile(targetFile.getPath() + ModificationFile.FILE_SUFFIX);
      Assert.assertTrue(targetModsFile.exists());
      Assert.assertEquals(2, targetModsFile.getModifications().size());
    }
    for (TsFileResource unseqResource : tmpSourceUnseqFiles) {
      Assert.assertFalse(unseqResource.getTsFile().exists());
      Assert.assertFalse(
          new File(unseqResource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertFalse(unseqResource.getModFile().exists());
    }
    Assert.assertFalse(mergingModsFile.exists());
    Assert.assertFalse(logFile.exists());
  }

  @Test
  public void testRecoverWithAllSeqSourceFilesLost() throws IOException, MetadataException {
    for (TsFileResource resource : sourceSeqFiles) {
      File targetFile = modifyTsFileNameUnseqMergCnt(resource.getTsFile());
      FSFactoryProducer.getFSFactory()
          .moveFile(new File(resource.getTsFilePath() + MergeTask.MERGE_SUFFIX), targetFile);
      FSFactoryProducer.getFSFactory()
          .moveFile(
              new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX),
              new File(targetFile.getPath() + TsFileResource.RESOURCE_SUFFIX));
      resource.remove();
      tsFileManagement.remove(resource, true);
      tsFileManagement.add(new TsFileResource(targetFile), true);
    }

    RecoverMergeTask recoverMergeTask =
        new RecoverMergeTask(
            tsFileManagement,
            TestConstant.SEQUENCE_DATA_DIR,
            tsFileManagement::mergeEndAction,
            "recoverTest",
            true,
            "root.sg1");
    recoverMergeTask.recoverMerge();
    for (TsFileResource seqResource : tmpSourceSeqFiles) {
      Assert.assertFalse(seqResource.getTsFile().exists());
      Assert.assertFalse(
          new File(seqResource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertFalse(new File(seqResource.getTsFilePath() + MergeTask.MERGE_SUFFIX).exists());
      File targetFile = modifyTsFileNameUnseqMergCnt(seqResource.getTsFile());
      Assert.assertTrue(targetFile.exists());
      Assert.assertTrue(new File(targetFile.getPath() + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertFalse(seqResource.getModFile().exists());
      ModificationFile targetModsFile =
          new ModificationFile(targetFile.getPath() + ModificationFile.FILE_SUFFIX);
      Assert.assertTrue(targetModsFile.exists());
      Assert.assertEquals(2, targetModsFile.getModifications().size());
    }
    for (TsFileResource unseqResource : tmpSourceUnseqFiles) {
      Assert.assertFalse(unseqResource.getTsFile().exists());
      Assert.assertFalse(
          new File(unseqResource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertFalse(unseqResource.getModFile().exists());
    }
    Assert.assertFalse(mergingModsFile.exists());
    Assert.assertFalse(logFile.exists());
  }

  @Test
  public void testRecoverWithAllSourceFilesLost() throws IOException, MetadataException {
    for (TsFileResource resource : sourceSeqFiles) {
      File targetFile = modifyTsFileNameUnseqMergCnt(resource.getTsFile());
      FSFactoryProducer.getFSFactory()
          .moveFile(new File(resource.getTsFilePath() + MergeTask.MERGE_SUFFIX), targetFile);
      FSFactoryProducer.getFSFactory()
          .moveFile(
              new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX),
              new File(targetFile.getPath() + TsFileResource.RESOURCE_SUFFIX));
      resource.remove();
      tsFileManagement.remove(resource, true);
      tsFileManagement.add(new TsFileResource(targetFile), true);
    }
    for (TsFileResource resource : sourceUnseqFiles) {
      resource.remove();
      tsFileManagement.remove(resource, false);
    }
    RecoverMergeTask recoverMergeTask =
        new RecoverMergeTask(
            tsFileManagement,
            TestConstant.SEQUENCE_DATA_DIR,
            tsFileManagement::mergeEndAction,
            "recoverTest",
            true,
            "root.sg1");
    recoverMergeTask.recoverMerge();
    for (TsFileResource seqResource : tmpSourceSeqFiles) {
      Assert.assertFalse(seqResource.getTsFile().exists());
      Assert.assertFalse(
          new File(seqResource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertFalse(new File(seqResource.getTsFilePath() + MergeTask.MERGE_SUFFIX).exists());
      File targetFile = modifyTsFileNameUnseqMergCnt(seqResource.getTsFile());
      Assert.assertTrue(targetFile.exists());
      Assert.assertTrue(new File(targetFile.getPath() + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertFalse(seqResource.getModFile().exists());
      ModificationFile targetModsFile =
          new ModificationFile(targetFile.getPath() + ModificationFile.FILE_SUFFIX);
      Assert.assertTrue(targetModsFile.exists());
      Assert.assertEquals(2, targetModsFile.getModifications().size());
    }
    for (TsFileResource unseqResource : tmpSourceUnseqFiles) {
      Assert.assertFalse(unseqResource.getTsFile().exists());
      Assert.assertFalse(
          new File(unseqResource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertFalse(unseqResource.getModFile().exists());
    }
    Assert.assertFalse(mergingModsFile.exists());
    Assert.assertFalse(logFile.exists());
  }

  @Test
  public void testRecoverWithAllSourceFilesExist() throws IOException, MetadataException {
    RecoverMergeTask recoverMergeTask =
        new RecoverMergeTask(
            tsFileManagement,
            TestConstant.SEQUENCE_DATA_DIR,
            tsFileManagement::mergeEndAction,
            "recoverTest",
            true,
            "root.sg1");
    recoverMergeTask.recoverMerge();
    for (TsFileResource seqResource : tmpSourceSeqFiles) {
      Assert.assertTrue(seqResource.getTsFile().exists());
      Assert.assertTrue(
          new File(seqResource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertFalse(new File(seqResource.getTsFilePath() + MergeTask.MERGE_SUFFIX).exists());
      Assert.assertTrue(seqResource.getModFile().exists());
      File targetFile = modifyTsFileNameUnseqMergCnt(seqResource.getTsFile());
      Assert.assertFalse(targetFile.exists());
      Assert.assertFalse(new File(targetFile.getPath() + TsFileResource.RESOURCE_SUFFIX).exists());
      ModificationFile targetModsFile =
          new ModificationFile(targetFile.getPath() + ModificationFile.FILE_SUFFIX);
      Assert.assertFalse(targetModsFile.exists());
    }
    for (TsFileResource unseqResource : tmpSourceUnseqFiles) {
      Assert.assertTrue(unseqResource.getTsFile().exists());
      Assert.assertTrue(
          new File(unseqResource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
      Assert.assertTrue(unseqResource.getModFile().exists());
    }
    Assert.assertFalse(mergingModsFile.exists());
    Assert.assertFalse(logFile.exists());
  }

  private void createFiles() throws IOException, IllegalPathException, WriteProcessException {
    // create source seq files
    for (int i = 0; i < seqFileNum; i++) {
      File file =
          new File(
              TestConstant.SEQUENCE_DATA_DIR.concat(
                  i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile"));
      Assert.assertTrue(file.createNewFile());
      TsFileResource tsFileResource = new TsFileResource(file);
      prepareFile(tsFileResource, i * 10, 10, i * 10);
      tsFileResource.setClosed(true);
      tsFileResource.setMinPlanIndex(i);
      tsFileResource.setMaxPlanIndex(i);
      tsFileResource.setVersion(i);
      tsFileResource.serialize();
      sourceSeqFiles.add(tsFileResource);
      tmpSourceSeqFiles.add(new TsFileResource(file));
      // create mods file
      Deletion deletion = new Deletion(new PartialPath("root.sg1.d1", "s0"), 1, 0, 100);
      tsFileResource.getModFile().write(deletion);
      deletion = new Deletion(new PartialPath("root.sg1.d1", "s0"), 1, 200, 300);
      tsFileResource.getModFile().write(deletion);
      tsFileResource.getModFile().close();
    }

    // create source unseq files
    for (int i = seqFileNum; i < seqFileNum + unseqFileNum; i++) {
      File file =
          new File(
              TestConstant.UNSEQUENCE_DATA_DIR.concat(
                  i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile"));
      Assert.assertTrue(file.createNewFile());
      TsFileResource tsFileResource = new TsFileResource(file);
      prepareFile(tsFileResource, i * 10, 5, i * 10);
      tsFileResource.setClosed(true);
      tsFileResource.setMinPlanIndex(i);
      tsFileResource.setMaxPlanIndex(i);
      tsFileResource.setVersion(i);
      tsFileResource.serialize();
      sourceUnseqFiles.add(tsFileResource);
      tmpSourceUnseqFiles.add(new TsFileResource(file));
      // create mods file
      Deletion deletion = new Deletion(new PartialPath("root.sg1.d1", "s0"), 1, 0, 100);
      tsFileResource.getModFile().write(deletion);
      deletion = new Deletion(new PartialPath("root.sg1.d1", "s0"), 1, 200, 300);
      tsFileResource.getModFile().write(deletion);
      tsFileResource.getModFile().close();
    }

    // create .merge files
    for (int i = 0; i < seqFileNum; i++) {
      File file =
          new File(
              TestConstant.SEQUENCE_DATA_DIR.concat(
                  i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile"
                      + MergeTask.MERGE_SUFFIX));
      Assert.assertTrue(file.createNewFile());
      TsFileResource tsFileResource = new TsFileResource(file);
      prepareFile(tsFileResource, i * 10, 10, i * 10);
    }

    // create merging mods file
    Deletion deletion = new Deletion(new PartialPath("root.sg1.d1", "s1"), 1, 0, 100);
    mergingModsFile.write(deletion);
    deletion = new Deletion(new PartialPath("root.sg1.d1", "s1"), 1, 200, 30000);
    mergingModsFile.write(deletion);
    mergingModsFile.close();

    // create log
    MergeLogger mergeLogger = new MergeLogger(TestConstant.SEQUENCE_DATA_DIR);
    MergeResource mergeResource = new MergeResource(sourceSeqFiles, sourceUnseqFiles);
    mergeLogger.logFiles(mergeResource);
    mergeLogger.close();
  }

  private void deleteFiles() {
    for (TsFileResource seqResource : sourceSeqFiles) {
      seqResource.remove();
      File mergeFile = new File(seqResource.getTsFilePath() + MergeTask.MERGE_SUFFIX);
      if (mergeFile.exists()) {
        mergeFile.delete();
      }
    }
    for (TsFileResource seqResource : sourceSeqFiles) {
      seqResource.remove();
    }
  }
}
