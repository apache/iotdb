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

package org.apache.iotdb.db.storageengine.dataregion.compaction.settle;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.SettleCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.SettleSelectorImpl;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.utils.TsFileGeneratorUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SettleCompactionSelectorTest extends AbstractCompactionTest {
  boolean originUseMultiType = TsFileGeneratorUtils.useMultiType;

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    TsFileGeneratorUtils.useMultiType = true;
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    DataNodeTTLCache.getInstance().clearAllTTL();
    TsFileGeneratorUtils.useMultiType = originUseMultiType;
  }

  // region nonAligned test
  @Test
  public void testSelectContinuousFileWithLightSelect()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(3);
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionTaskSelectionModsFileThreshold(1);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, false, true);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, false, false);

    // file 0 1 2 3 4 has mods file, whose size is over threshold
    generateModsFile(4, 10, seqResources, 0, 200);
    generateModsFile(4, 10, unseqResources, 0, 200);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // inner task, continuous
    // select first time
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(false, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    List<SettleCompactionTask> unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(1, unseqTasks.size());
    Assert.assertEquals(3, (seqTasks.get(0).getPartiallyDirtyFiles().size()));
    Assert.assertEquals(0, seqTasks.get(0).getFullyDirtyFiles().size());
    Assert.assertEquals(3, (unseqTasks.get(0).getPartiallyDirtyFiles().size()));
    Assert.assertEquals(0, unseqTasks.get(0).getFullyDirtyFiles().size());

    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertTrue(unseqTasks.get(0).start());
    Assert.assertEquals(3, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(3, tsFileManager.getTsFileList(false).size());

    Assert.assertFalse(tsFileManager.getTsFileList(true).get(0).getModFile().exists());
    Assert.assertFalse(tsFileManager.getTsFileList(false).get(0).getModFile().exists());

    // select second time
    seqTasks = settleSelector.selectSettleTask(seqResources);
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(1, unseqTasks.size());
    Assert.assertEquals(2, (seqTasks.get(0).getPartiallyDirtyFiles().size()));
    Assert.assertEquals(2, (unseqTasks.get(0).getPartiallyDirtyFiles().size()));

    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertTrue(unseqTasks.get(0).start());
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(2, tsFileManager.getTsFileList(false).size());
    for (int i = 0; i < 2; i++) {
      Assert.assertFalse(tsFileManager.getTsFileList(true).get(i).getModFile().exists());
      Assert.assertFalse(tsFileManager.getTsFileList(false).get(i).getModFile().exists());
    }

    seqTasks = settleSelector.selectSettleTask(seqResources);
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(0, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());
  }

  @Test
  public void testSelectUnContinuousFileWithLightSelect()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(2);
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionTaskSelectionModsFileThreshold(1);
    createFiles(10, 5, 10, 200, 0, 0, 100, 100, false, true);
    createFiles(10, 5, 10, 200, 0, 0, 100, 100, false, false);

    // file 0 2 4 6 8 has mods file, whose size is over threshold
    for (int i = 0; i < 10; i++) {
      if (i % 2 == 0) {
        generateModsFile(5, 10, Collections.singletonList(seqResources.get(i)), 0, Long.MAX_VALUE);
        generateModsFile(
            5, 10, Collections.singletonList(unseqResources.get(i)), 0, Long.MAX_VALUE);
      }
    }
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // settle task, not continuous
    // select first time
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(false, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    int num = 10;
    for (int i = 0; i < 5; i++) {
      List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
      List<SettleCompactionTask> unseqTasks = settleSelector.selectSettleTask(unseqResources);
      Assert.assertEquals(1, seqTasks.size());
      Assert.assertEquals(1, unseqTasks.size());
      Assert.assertEquals(1, (seqTasks.get(0).getPartiallyDirtyFiles().size()));
      Assert.assertEquals(
          seqResources.get(i * 2), (seqTasks.get(0).getPartiallyDirtyFiles().get(0)));
      Assert.assertEquals(0, (seqTasks.get(0).getFullyDirtyFiles().size()));
      Assert.assertEquals(1, (unseqTasks.get(0).getPartiallyDirtyFiles().size()));
      Assert.assertEquals(
          unseqResources.get(i * 2), (unseqTasks.get(0).getPartiallyDirtyFiles().get(0)));
      Assert.assertEquals(0, (unseqTasks.get(0)).getFullyDirtyFiles().size());

      Assert.assertTrue(seqTasks.get(0).start());
      Assert.assertTrue(unseqTasks.get(0).start());
      Assert.assertEquals(--num, tsFileManager.getTsFileList(true).size());
      Assert.assertEquals(num, tsFileManager.getTsFileList(false).size());
    }

    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    List<SettleCompactionTask> unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(0, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());
  }

  @Test
  public void testSelectContinuousFilesBaseOnModsSizeWithHeavySelect()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(3);
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionTaskSelectionModsFileThreshold(1);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, false, true);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, false, false);

    // the first file is all deleted, the rest file is partial deleted because its mods file size is
    // over threshold
    generateModsFile(4, 10, seqResources, 0, 200);
    generateModsFile(4, 10, unseqResources, 0, 200);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // select first time
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(true, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    List<SettleCompactionTask> unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(0, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());
  }

  @Test
  public void testSelectContinuousFilesBaseOnDirtyRateByModsWithHeavySelect()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(3);
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionTaskSelectionModsFileThreshold(1);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, false, true);

    // the first file is all deleted, the rest file is partial deleted because its mods file size is
    // over threshold
    generateModsFile(4, 10, seqResources, 0, 200);
    for (int i = 0; i < 5; i++) {
      for (TsFileResource resource : seqResources) {
        addFileMods(
            resource,
            new MeasurementPath(
                COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d" + i + ".**"),
            Long.MAX_VALUE,
            0,
            200);
      }
    }
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // select first time, file 0 is all deleted, file 1 2 3 4 is not satisfied by heavy select
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(true, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(1, seqTasks.get(0).getFullyDirtyFiles().size());
    Assert.assertEquals(0, seqTasks.get(0).getPartiallyDirtyFiles().size());

    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertEquals(4, tsFileManager.getTsFileList(true).size());

    // select second time
    settleSelector = new SettleSelectorImpl(false, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    seqTasks = settleSelector.selectSettleTask(seqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(0, seqTasks.get(0).getFullyDirtyFiles().size());
    Assert.assertEquals(3, seqTasks.get(0).getPartiallyDirtyFiles().size());
    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).size());

    // select third time
    seqTasks = settleSelector.selectSettleTask(seqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(0, seqTasks.get(0).getFullyDirtyFiles().size());
    Assert.assertEquals(1, seqTasks.get(0).getPartiallyDirtyFiles().size());
    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).size());

    for (int i = 0; i < 2; i++) {
      Assert.assertFalse(tsFileManager.getTsFileList(true).get(i).getModFile().exists());
    }
  }

  // base on dirty data rate
  @Test
  public void testSelectContinuousFileBaseOnDirtyDataRateWithHeavySelect()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setMaxExpiredTime(Long.MAX_VALUE);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, false, true);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, false, false);

    generateModsFile(4, 10, seqResources, 0, 200);
    generateModsFile(4, 10, unseqResources, 0, 200);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    DataNodeTTLCache.getInstance()
        .setTTL(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d1", 100);
    generateModsFile(1, 10, seqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    generateModsFile(1, 10, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE);

    // select first time, none partial_deleted and all_deleted files
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(true, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    List<SettleCompactionTask> unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(0, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());

    // select second time
    // all unseq files is partial_deleted
    for (TsFileResource resource : unseqResources) {
      addFileMods(
          resource,
          new MeasurementPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d0.**"),
          Long.MAX_VALUE,
          Long.MIN_VALUE,
          Long.MAX_VALUE);
    }
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(1, unseqTasks.size());

    Assert.assertEquals(5, unseqTasks.get(0).getPartiallyDirtyFiles().size());
    Assert.assertEquals(0, unseqTasks.get(0).getFullyDirtyFiles().size());

    Assert.assertTrue(unseqTasks.get(0).start());
    Assert.assertEquals(1, tsFileManager.getTsFileList(false).size());
    Assert.assertFalse(tsFileManager.getTsFileList(false).get(0).getModFile().exists());

    // select third time
    // all seq files is partial_deleted
    for (TsFileResource resource : seqResources) {
      addFileMods(
          resource,
          new MeasurementPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d0.**"),
          Long.MAX_VALUE,
          Long.MIN_VALUE,
          Long.MAX_VALUE);
    }
    seqTasks = settleSelector.selectSettleTask(seqResources);
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());
    Assert.assertEquals(5, seqTasks.get(0).getPartiallyDirtyFiles().size());
    Assert.assertEquals(0, seqTasks.get(0).getFullyDirtyFiles().size());

    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertEquals(1, tsFileManager.getTsFileList(true).size());
  }

  // base on outdated too long
  @Test
  public void testSelectContinuousFileBaseOnDirtyDataOutdatedTooLongWithHeavySelect()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(3);
    IoTDBDescriptor.getInstance().getConfig().setMaxExpiredTime(Long.MAX_VALUE);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, false, true);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, false, false);

    generateModsFile(4, 10, seqResources, 0, 200);
    generateModsFile(4, 10, unseqResources, 0, 200);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    DataNodeTTLCache.getInstance()
        .setTTL(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d1", 100);
    generateModsFile(1, 10, seqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    generateModsFile(1, 10, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE);

    // inner task, continuous
    // select first time, none partial_deleted and all_deleted files
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(true, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    List<SettleCompactionTask> unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(0, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());

    // select second time
    // add the longest expired time, all file is partial_deleted
    IoTDBDescriptor.getInstance().getConfig().setMaxExpiredTime(1);
    seqTasks = settleSelector.selectSettleTask(seqResources);
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(2, seqTasks.size());
    Assert.assertEquals(2, unseqTasks.size());

    Assert.assertEquals(3, seqTasks.get(0).getPartiallyDirtyFiles().size());
    Assert.assertEquals(0, seqTasks.get(0).getFullyDirtyFiles().size());
    Assert.assertEquals(2, seqTasks.get(1).getPartiallyDirtyFiles().size());
    Assert.assertEquals(0, seqTasks.get(1).getFullyDirtyFiles().size());
    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertTrue(seqTasks.get(1).start());
    Assert.assertTrue(unseqTasks.get(0).start());
    Assert.assertTrue(unseqTasks.get(1).start());
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(2, tsFileManager.getTsFileList(false).size());

    for (int i = 0; i < 2; i++) {
      Assert.assertFalse(tsFileManager.getTsFileList(true).get(i).getModFile().exists());
      Assert.assertFalse(tsFileManager.getTsFileList(false).get(i).getModFile().exists());
    }

    // select third time
    seqTasks = settleSelector.selectSettleTask(seqResources);
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(0, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());
  }

  // base on dirty data rate
  @Test
  public void testSelectUncontinuousFileBaseOnDirtyDataRateWithHeavySelect()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(3);
    IoTDBDescriptor.getInstance().getConfig().setMaxExpiredTime(Long.MAX_VALUE);
    createFiles(10, 5, 10, 200, 0, 0, 100, 100, false, true);

    generateModsFile(4, 10, seqResources, 0, 200);
    generateModsFile(4, 10, unseqResources, 0, 200);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // add one device ttl
    DataNodeTTLCache.getInstance()
        .setTTL(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d0", 100);

    // select first time， none partial_deleted and all_deleted files
    // add device mods with already outdated device
    for (int i = 0; i < 10; i++) {
      if (i % 2 == 1) {
        addFileMods(
            seqResources.get(i),
            new MeasurementPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d0.**"),
            Long.MAX_VALUE,
            Long.MIN_VALUE,
            Long.MAX_VALUE);
      }
    }
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(true, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    Assert.assertEquals(0, seqTasks.size());

    // select second time
    // file 0 2 4 6 8 is all deleted
    for (int i = 0; i < 10; i++) {
      if (i % 2 == 0) {
        for (int d = 1; d < 5; d++) {
          addFileMods(
              seqResources.get(i),
              new MeasurementPath(
                  COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d" + d + ".**"),
              Long.MAX_VALUE,
              Long.MIN_VALUE,
              Long.MAX_VALUE);
        }
      }
    }
    seqTasks = settleSelector.selectSettleTask(seqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(0, seqTasks.get(0).getPartiallyDirtyFiles().size());
    Assert.assertEquals(5, seqTasks.get(0).getFullyDirtyFiles().size());

    // select third time
    // file 0 2 4 6 8 is all deleted, file 1 3 5 7 9 is partial_deleted
    for (int i = 0; i < 10; i++) {
      if (i % 2 == 1) {
        addFileMods(
            seqResources.get(i),
            new MeasurementPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d1.**"),
            Long.MAX_VALUE,
            Long.MIN_VALUE,
            Long.MAX_VALUE);
      }
    }
    seqTasks = settleSelector.selectSettleTask(seqResources);
    Assert.assertEquals(2, seqTasks.size());
    Assert.assertEquals(5, seqTasks.get(0).getFullyDirtyFiles().size());
    Assert.assertEquals(3, seqTasks.get(0).getPartiallyDirtyFiles().size());
    Assert.assertEquals(seqResources.get(1), seqTasks.get(0).getPartiallyDirtyFiles().get(0));
    Assert.assertEquals(seqResources.get(3), seqTasks.get(0).getPartiallyDirtyFiles().get(1));
    Assert.assertEquals(seqResources.get(5), seqTasks.get(0).getPartiallyDirtyFiles().get(2));
    Assert.assertEquals(seqResources.get(7), seqTasks.get(1).getPartiallyDirtyFiles().get(0));
    Assert.assertEquals(seqResources.get(9), seqTasks.get(1).getPartiallyDirtyFiles().get(1));
    Assert.assertEquals(0, seqTasks.get(1).getFullyDirtyFiles().size());
    Assert.assertEquals(2, seqTasks.get(1).getPartiallyDirtyFiles().size());
    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertTrue(seqTasks.get(1).start());
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).size());
  }

  /**
   * File 0 2 4 6 8 is partial_deleted, file 1 is all_deleted. Partial_deleted group is (0 2) (4)
   * (6) (8).
   */
  @Test
  public void testSelectFileBaseOnDirtyDataRateWithHeavySelect()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(3);
    IoTDBDescriptor.getInstance().getConfig().setMaxExpiredTime(Long.MAX_VALUE);
    createFiles(10, 5, 10, 200, 0, 0, 100, 100, false, true);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // select first time
    // file 0 2 4 6 8 is partial_deleted, file 1 is all_deleted
    for (int i = 0; i < 10; i++) {
      if (i % 2 == 0) {
        addFileMods(
            seqResources.get(i),
            new MeasurementPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d0.**"),
            Long.MAX_VALUE,
            Long.MIN_VALUE,
            Long.MAX_VALUE);
        addFileMods(
            seqResources.get(i),
            new MeasurementPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d1.**"),
            Long.MAX_VALUE,
            Long.MIN_VALUE,
            Long.MAX_VALUE);
      }
    }
    for (int d = 0; d < 5; d++) {
      addFileMods(
          seqResources.get(1),
          new MeasurementPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d" + d + ".**"),
          Long.MAX_VALUE,
          Long.MIN_VALUE,
          Long.MAX_VALUE);
    }
    // compact all_deleted file and partial_deleted file 0 2
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(true, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    Assert.assertEquals(4, seqTasks.size());
    Assert.assertEquals(1, seqTasks.get(0).getFullyDirtyFiles().size());
    Assert.assertEquals(2, seqTasks.get(0).getPartiallyDirtyFiles().size());
    Assert.assertEquals(seqResources.get(0), seqTasks.get(0).getPartiallyDirtyFiles().get(0));
    Assert.assertEquals(seqResources.get(2), seqTasks.get(0).getPartiallyDirtyFiles().get(1));

    Assert.assertEquals(0, seqTasks.get(1).getFullyDirtyFiles().size());
    Assert.assertEquals(1, seqTasks.get(1).getPartiallyDirtyFiles().size());
    Assert.assertEquals(seqResources.get(4), seqTasks.get(1).getPartiallyDirtyFiles().get(0));

    Assert.assertEquals(0, seqTasks.get(2).getFullyDirtyFiles().size());
    Assert.assertEquals(1, seqTasks.get(2).getPartiallyDirtyFiles().size());
    Assert.assertEquals(seqResources.get(6), seqTasks.get(2).getPartiallyDirtyFiles().get(0));

    Assert.assertEquals(0, seqTasks.get(3).getFullyDirtyFiles().size());
    Assert.assertEquals(1, seqTasks.get(3).getPartiallyDirtyFiles().size());
    Assert.assertEquals(seqResources.get(8), seqTasks.get(3).getPartiallyDirtyFiles().get(0));

    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertTrue(seqTasks.get(1).start());
    Assert.assertTrue(seqTasks.get(2).start());
    Assert.assertTrue(seqTasks.get(3).start());
    Assert.assertEquals(8, tsFileManager.getTsFileList(true).size());
  }

  /**
   * File 0 4 6 8 is partial_deleted, file 2 3 7 is all_deleted. Partial_deleted group is (0) (4) (6
   * 8).
   */
  @Test
  public void testSelectFileBaseOnDirtyDataRateWithHeavySelect2()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(3);
    IoTDBDescriptor.getInstance().getConfig().setMaxExpiredTime(Long.MAX_VALUE);
    createFiles(10, 5, 10, 200, 0, 0, 100, 100, false, true);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // select first time
    // file 0 4 6 8 is partial_deleted, file 2 3 7 is all_deleted
    for (int i = 0; i < 10; i++) {
      if (i % 2 == 0) {
        addFileMods(
            seqResources.get(i),
            new MeasurementPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d0.**"),
            Long.MAX_VALUE,
            Long.MIN_VALUE,
            Long.MAX_VALUE);
        addFileMods(
            seqResources.get(i),
            new MeasurementPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d1.**"),
            Long.MAX_VALUE,
            Long.MIN_VALUE,
            Long.MAX_VALUE);
      }
    }
    for (int d = 0; d < 5; d++) {
      MeasurementPath path =
          new MeasurementPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d" + d + ".**");
      addFileMods(seqResources.get(2), path, Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE);
      addFileMods(seqResources.get(3), path, Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE);
      addFileMods(seqResources.get(7), path, Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE);
    }
    for (TsFileResource resource : seqResources) {
      resource.getModFile().close();
    }

    // compact all_deleted file and partial_deleted file 0
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(true, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    Assert.assertEquals(3, seqTasks.size());
    Assert.assertEquals(3, seqTasks.get(0).getFullyDirtyFiles().size());
    Assert.assertEquals(1, seqTasks.get(0).getPartiallyDirtyFiles().size());
    Assert.assertEquals(seqResources.get(0), seqTasks.get(0).getPartiallyDirtyFiles().get(0));

    Assert.assertEquals(0, seqTasks.get(1).getFullyDirtyFiles().size());
    Assert.assertEquals(1, seqTasks.get(1).getPartiallyDirtyFiles().size());
    Assert.assertEquals(seqResources.get(4), seqTasks.get(1).getPartiallyDirtyFiles().get(0));

    Assert.assertEquals(0, seqTasks.get(2).getFullyDirtyFiles().size());
    Assert.assertEquals(2, seqTasks.get(2).getPartiallyDirtyFiles().size());
    Assert.assertEquals(seqResources.get(6), seqTasks.get(2).getPartiallyDirtyFiles().get(0));
    Assert.assertEquals(seqResources.get(8), seqTasks.get(2).getPartiallyDirtyFiles().get(1));

    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertTrue(seqTasks.get(1).start());
    Assert.assertTrue(seqTasks.get(2).start());
    Assert.assertEquals(6, tsFileManager.getTsFileList(true).size());
  }

  // endregion

  // region aligned test
  @Test
  public void testSelectContinuousFileWithLightSelectAligned()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(3);
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionTaskSelectionModsFileThreshold(1);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, true, true);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, true, false);

    // file 0 1 2 3 4 has mods file, whose size is over threshold
    generateModsFile(4, 10, seqResources, 0, 200, true);
    generateModsFile(4, 10, unseqResources, 0, 200, true);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // inner task, continuous
    // select first time
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(false, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    List<SettleCompactionTask> unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(1, unseqTasks.size());
    Assert.assertEquals(3, (seqTasks.get(0).getPartiallyDirtyFiles().size()));
    Assert.assertEquals(0, seqTasks.get(0).getFullyDirtyFiles().size());
    Assert.assertEquals(3, (unseqTasks.get(0).getPartiallyDirtyFiles().size()));
    Assert.assertEquals(0, unseqTasks.get(0).getFullyDirtyFiles().size());

    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertTrue(unseqTasks.get(0).start());
    Assert.assertEquals(3, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(3, tsFileManager.getTsFileList(false).size());

    Assert.assertFalse(tsFileManager.getTsFileList(true).get(0).getModFile().exists());
    Assert.assertFalse(tsFileManager.getTsFileList(false).get(0).getModFile().exists());

    // select second time
    seqTasks = settleSelector.selectSettleTask(seqResources);
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(1, unseqTasks.size());
    Assert.assertEquals(2, (seqTasks.get(0).getPartiallyDirtyFiles().size()));
    Assert.assertEquals(2, (unseqTasks.get(0).getPartiallyDirtyFiles().size()));

    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertTrue(unseqTasks.get(0).start());
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(2, tsFileManager.getTsFileList(false).size());
    for (int i = 0; i < 2; i++) {
      Assert.assertFalse(tsFileManager.getTsFileList(true).get(i).getModFile().exists());
      Assert.assertFalse(tsFileManager.getTsFileList(false).get(i).getModFile().exists());
    }

    seqTasks = settleSelector.selectSettleTask(seqResources);
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(0, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());
  }

  @Test
  public void testSelectUnContinuousFileWithLightSelectAligned()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(2);
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionTaskSelectionModsFileThreshold(1);
    createFiles(10, 5, 10, 200, 0, 0, 100, 100, true, true);
    createFiles(10, 5, 10, 200, 0, 0, 100, 100, true, false);

    // file 0 2 4 6 8 has mods file, whose size is over threshold
    for (int i = 0; i < 10; i++) {
      if (i % 2 == 0) {
        generateModsFile(
            5, 10, Collections.singletonList(seqResources.get(i)), 0, Long.MAX_VALUE, true);
        generateModsFile(
            5, 10, Collections.singletonList(unseqResources.get(i)), 0, Long.MAX_VALUE, true);
      }
    }
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // settle task, not continuous
    // select first time
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(false, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    int num = 10;
    for (int i = 0; i < 5; i++) {
      List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
      List<SettleCompactionTask> unseqTasks = settleSelector.selectSettleTask(unseqResources);
      Assert.assertEquals(1, seqTasks.size());
      Assert.assertEquals(1, unseqTasks.size());
      Assert.assertEquals(1, (seqTasks.get(0).getPartiallyDirtyFiles().size()));
      Assert.assertEquals(
          seqResources.get(i * 2), (seqTasks.get(0).getPartiallyDirtyFiles().get(0)));
      Assert.assertEquals(0, (seqTasks.get(0).getFullyDirtyFiles().size()));
      Assert.assertEquals(1, (unseqTasks.get(0).getPartiallyDirtyFiles().size()));
      Assert.assertEquals(
          unseqResources.get(i * 2), (unseqTasks.get(0).getPartiallyDirtyFiles().get(0)));
      Assert.assertEquals(0, (unseqTasks.get(0)).getFullyDirtyFiles().size());

      Assert.assertTrue(seqTasks.get(0).start());
      Assert.assertTrue(unseqTasks.get(0).start());
      Assert.assertEquals(--num, tsFileManager.getTsFileList(true).size());
      Assert.assertEquals(num, tsFileManager.getTsFileList(false).size());
    }

    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    List<SettleCompactionTask> unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(0, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());
  }

  @Test
  public void testSelectContinuousFilesBaseOnModsSizeWithHeavySelectAligned()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(3);
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionTaskSelectionModsFileThreshold(1);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, true, true);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, true, false);

    // the first file is all deleted, the rest file is partial deleted because its mods file size is
    // over threshold
    generateModsFile(4, 10, seqResources, 0, 200, true);
    generateModsFile(4, 10, unseqResources, 0, 200, true);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // select first time
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(true, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    List<SettleCompactionTask> unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(0, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());
  }

  @Test
  public void testSelectContinuousFilesBaseOnDirtyRateByModsWithHeavySelectAligned()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(3);
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionTaskSelectionModsFileThreshold(1);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, true, true);

    // the first file is all deleted, the rest file is partial deleted because its mods file size is
    // over threshold
    generateModsFile(4, 10, seqResources, 0, 200, true);
    for (int i = 0; i < 5; i++) {
      for (TsFileResource resource : seqResources) {
        addFileMods(
            resource,
            new MeasurementPath(
                COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d" + (10000 + i) + ".**"),
            Long.MAX_VALUE,
            0,
            200);
      }
    }
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // select first time, file 0 is all deleted, file 1 2 3 4 is not satisfied by heavy select
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(true, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(1, seqTasks.get(0).getFullyDirtyFiles().size());
    Assert.assertEquals(0, seqTasks.get(0).getPartiallyDirtyFiles().size());

    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertEquals(4, tsFileManager.getTsFileList(true).size());

    // select second time
    settleSelector = new SettleSelectorImpl(false, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    seqTasks = settleSelector.selectSettleTask(seqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(0, seqTasks.get(0).getFullyDirtyFiles().size());
    Assert.assertEquals(3, seqTasks.get(0).getPartiallyDirtyFiles().size());
    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).size());

    // select third time
    seqTasks = settleSelector.selectSettleTask(seqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(0, seqTasks.get(0).getFullyDirtyFiles().size());
    Assert.assertEquals(1, seqTasks.get(0).getPartiallyDirtyFiles().size());
    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).size());

    for (int i = 0; i < 2; i++) {
      Assert.assertFalse(tsFileManager.getTsFileList(true).get(i).getModFile().exists());
    }
  }

  // base on dirty data rate
  @Test
  public void testSelectContinuousFileBaseOnDirtyDataRateWithHeavySelectAligned()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setMaxExpiredTime(Long.MAX_VALUE);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, true, true);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, true, false);

    generateModsFile(4, 10, seqResources, 0, 200, true);
    generateModsFile(4, 10, unseqResources, 0, 200, true);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    DataNodeTTLCache.getInstance()
        .setTTL(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d10001", 100);
    generateModsFile(1, 10, seqResources, Long.MIN_VALUE, Long.MAX_VALUE, true);
    generateModsFile(1, 10, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE, true);

    // select first time, none partial_deleted and all_deleted files
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(true, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    List<SettleCompactionTask> unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(0, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());

    // select second time
    // all unseq files is partial_deleted
    for (TsFileResource resource : unseqResources) {
      addFileMods(
          resource,
          new MeasurementPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d10000.**"),
          Long.MAX_VALUE,
          Long.MIN_VALUE,
          Long.MAX_VALUE);
    }
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(1, unseqTasks.size());

    Assert.assertEquals(5, unseqTasks.get(0).getPartiallyDirtyFiles().size());
    Assert.assertEquals(0, unseqTasks.get(0).getFullyDirtyFiles().size());

    Assert.assertTrue(unseqTasks.get(0).start());
    Assert.assertEquals(1, tsFileManager.getTsFileList(false).size());
    Assert.assertFalse(tsFileManager.getTsFileList(false).get(0).getModFile().exists());

    // select third time
    // all seq files is partial_deleted
    for (TsFileResource resource : seqResources) {
      addFileMods(
          resource,
          new MeasurementPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d10000.**"),
          Long.MAX_VALUE,
          Long.MIN_VALUE,
          Long.MAX_VALUE);
    }
    seqTasks = settleSelector.selectSettleTask(seqResources);
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());
    Assert.assertEquals(5, seqTasks.get(0).getPartiallyDirtyFiles().size());
    Assert.assertEquals(0, seqTasks.get(0).getFullyDirtyFiles().size());

    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertEquals(1, tsFileManager.getTsFileList(true).size());
  }

  // base on outdated too long
  @Test
  public void testSelectContinuousFileBaseOnDirtyDataOutdatedTooLongWithHeavySelectAligned()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(3);
    IoTDBDescriptor.getInstance().getConfig().setMaxExpiredTime(Long.MAX_VALUE);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, true, true);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, true, false);

    generateModsFile(4, 10, seqResources, 0, 200, true);
    generateModsFile(4, 10, unseqResources, 0, 200, true);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    DataNodeTTLCache.getInstance()
        .setTTL(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d10001", 100);
    generateModsFile(1, 10, seqResources, Long.MIN_VALUE, Long.MAX_VALUE, true);
    generateModsFile(1, 10, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE, true);

    // inner task, continuous
    // select first time, none partial_deleted and all_deleted files
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(true, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    List<SettleCompactionTask> unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(0, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());

    // select second time
    // add the longest expired time, all file is partial_deleted
    IoTDBDescriptor.getInstance().getConfig().setMaxExpiredTime(1);
    seqTasks = settleSelector.selectSettleTask(seqResources);
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(2, seqTasks.size());
    Assert.assertEquals(2, unseqTasks.size());

    Assert.assertEquals(3, seqTasks.get(0).getPartiallyDirtyFiles().size());
    Assert.assertEquals(0, seqTasks.get(0).getFullyDirtyFiles().size());
    Assert.assertEquals(2, seqTasks.get(1).getPartiallyDirtyFiles().size());
    Assert.assertEquals(0, seqTasks.get(1).getFullyDirtyFiles().size());
    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertTrue(seqTasks.get(1).start());
    Assert.assertTrue(unseqTasks.get(0).start());
    Assert.assertTrue(unseqTasks.get(1).start());
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(2, tsFileManager.getTsFileList(false).size());

    for (int i = 0; i < 2; i++) {
      Assert.assertFalse(tsFileManager.getTsFileList(true).get(i).getModFile().exists());
      Assert.assertFalse(tsFileManager.getTsFileList(false).get(i).getModFile().exists());
    }

    // select third time
    seqTasks = settleSelector.selectSettleTask(seqResources);
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(0, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());
  }

  // base on dirty data rate
  @Test
  public void testSelectUncontinuousFileBaseOnDirtyDataRateWithHeavySelectAligned()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(3);
    IoTDBDescriptor.getInstance().getConfig().setMaxExpiredTime(Long.MAX_VALUE);
    createFiles(10, 5, 10, 200, 0, 0, 100, 100, true, true);

    generateModsFile(4, 10, seqResources, 0, 200, true);
    generateModsFile(4, 10, unseqResources, 0, 200, true);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // add one device ttl
    DataNodeTTLCache.getInstance()
        .setTTL(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d10000", 100);

    // select first time， none partial_deleted and all_deleted files
    // add device mods with already outdated device
    for (int i = 0; i < 10; i++) {
      if (i % 2 == 1) {
        addFileMods(
            seqResources.get(i),
            new MeasurementPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d10000.**"),
            Long.MAX_VALUE,
            Long.MIN_VALUE,
            Long.MAX_VALUE);
      }
    }
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(true, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    Assert.assertEquals(0, seqTasks.size());

    // select second time
    // file 0 2 4 6 8 is all deleted
    for (int i = 0; i < 10; i++) {
      if (i % 2 == 0) {
        for (int d = 1; d < 5; d++) {
          addFileMods(
              seqResources.get(i),
              new MeasurementPath(
                  COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d" + (10000 + d) + ".**"),
              Long.MAX_VALUE,
              Long.MIN_VALUE,
              Long.MAX_VALUE);
        }
      }
    }
    seqTasks = settleSelector.selectSettleTask(seqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(0, seqTasks.get(0).getPartiallyDirtyFiles().size());
    Assert.assertEquals(5, seqTasks.get(0).getFullyDirtyFiles().size());

    // select third time
    // file 0 2 4 6 8 is all deleted, file 1 3 5 7 9 is partial_deleted
    for (int i = 0; i < 10; i++) {
      if (i % 2 == 1) {
        addFileMods(
            seqResources.get(i),
            new MeasurementPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d10001.**"),
            Long.MAX_VALUE,
            Long.MIN_VALUE,
            Long.MAX_VALUE);
      }
    }
    seqTasks = settleSelector.selectSettleTask(seqResources);
    Assert.assertEquals(2, seqTasks.size());
    Assert.assertEquals(5, seqTasks.get(0).getFullyDirtyFiles().size());
    Assert.assertEquals(3, seqTasks.get(0).getPartiallyDirtyFiles().size());
    Assert.assertEquals(seqResources.get(1), seqTasks.get(0).getPartiallyDirtyFiles().get(0));
    Assert.assertEquals(seqResources.get(3), seqTasks.get(0).getPartiallyDirtyFiles().get(1));
    Assert.assertEquals(seqResources.get(5), seqTasks.get(0).getPartiallyDirtyFiles().get(2));
    Assert.assertEquals(seqResources.get(7), seqTasks.get(1).getPartiallyDirtyFiles().get(0));
    Assert.assertEquals(seqResources.get(9), seqTasks.get(1).getPartiallyDirtyFiles().get(1));
    Assert.assertEquals(0, seqTasks.get(1).getFullyDirtyFiles().size());
    Assert.assertEquals(2, seqTasks.get(1).getPartiallyDirtyFiles().size());
    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertTrue(seqTasks.get(1).start());
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).size());
  }

  /**
   * File 0 2 4 6 8 is partial_deleted, file 1 is all_deleted. Partial_deleted group is (0 2) (4)
   * (6) (8).
   */
  @Test
  public void testSelectFileBaseOnDirtyDataRateWithHeavySelectAligned()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(3);
    IoTDBDescriptor.getInstance().getConfig().setMaxExpiredTime(Long.MAX_VALUE);
    createFiles(10, 5, 10, 200, 0, 0, 100, 100, true, true);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // select first time
    // file 0 2 4 6 8 is partial_deleted, file 1 is all_deleted
    for (int i = 0; i < 10; i++) {
      if (i % 2 == 0) {
        addFileMods(
            seqResources.get(i),
            new MeasurementPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d10000.**"),
            Long.MAX_VALUE,
            Long.MIN_VALUE,
            Long.MAX_VALUE);
        addFileMods(
            seqResources.get(i),
            new MeasurementPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d10001.**"),
            Long.MAX_VALUE,
            Long.MIN_VALUE,
            Long.MAX_VALUE);
      }
    }
    for (int d = 0; d < 5; d++) {
      addFileMods(
          seqResources.get(1),
          new MeasurementPath(
              COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d" + (10000 + d) + ".**"),
          Long.MAX_VALUE,
          Long.MIN_VALUE,
          Long.MAX_VALUE);
    }
    // compact all_deleted file and partial_deleted file 0 2
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(true, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    Assert.assertEquals(4, seqTasks.size());
    Assert.assertEquals(1, seqTasks.get(0).getFullyDirtyFiles().size());
    Assert.assertEquals(2, seqTasks.get(0).getPartiallyDirtyFiles().size());
    Assert.assertEquals(seqResources.get(0), seqTasks.get(0).getPartiallyDirtyFiles().get(0));
    Assert.assertEquals(seqResources.get(2), seqTasks.get(0).getPartiallyDirtyFiles().get(1));

    Assert.assertEquals(0, seqTasks.get(1).getFullyDirtyFiles().size());
    Assert.assertEquals(1, seqTasks.get(1).getPartiallyDirtyFiles().size());
    Assert.assertEquals(seqResources.get(4), seqTasks.get(1).getPartiallyDirtyFiles().get(0));

    Assert.assertEquals(0, seqTasks.get(2).getFullyDirtyFiles().size());
    Assert.assertEquals(1, seqTasks.get(2).getPartiallyDirtyFiles().size());
    Assert.assertEquals(seqResources.get(6), seqTasks.get(2).getPartiallyDirtyFiles().get(0));

    Assert.assertEquals(0, seqTasks.get(3).getFullyDirtyFiles().size());
    Assert.assertEquals(1, seqTasks.get(3).getPartiallyDirtyFiles().size());
    Assert.assertEquals(seqResources.get(8), seqTasks.get(3).getPartiallyDirtyFiles().get(0));

    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertTrue(seqTasks.get(1).start());
    Assert.assertTrue(seqTasks.get(2).start());
    Assert.assertTrue(seqTasks.get(3).start());
    Assert.assertEquals(8, tsFileManager.getTsFileList(true).size());
  }

  /**
   * File 0 4 6 8 is partial_deleted, file 2 3 7 is all_deleted. Partial_deleted group is (0) (4) (6
   * 8).
   */
  @Test
  public void testSelectFileBaseOnDirtyDataRateWithHeavySelect2Aligned()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(3);
    IoTDBDescriptor.getInstance().getConfig().setMaxExpiredTime(Long.MAX_VALUE);
    createFiles(10, 5, 10, 200, 0, 0, 100, 100, true, true);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // select first time
    // file 0 4 6 8 is partial_deleted, file 2 3 7 is all_deleted
    for (int i = 0; i < 10; i++) {
      if (i % 2 == 0) {
        addFileMods(
            seqResources.get(i),
            new MeasurementPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d10000.**"),
            Long.MAX_VALUE,
            Long.MIN_VALUE,
            Long.MAX_VALUE);
        addFileMods(
            seqResources.get(i),
            new MeasurementPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d10001.**"),
            Long.MAX_VALUE,
            Long.MIN_VALUE,
            Long.MAX_VALUE);
      }
    }
    for (int d = 0; d < 5; d++) {
      MeasurementPath path =
          new MeasurementPath(
              COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d" + (10000 + d) + ".**");
      addFileMods(seqResources.get(2), path, Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE);
      addFileMods(seqResources.get(3), path, Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE);
      addFileMods(seqResources.get(7), path, Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE);
    }
    for (TsFileResource resource : seqResources) {
      resource.getModFile().close();
    }

    // compact all_deleted file and partial_deleted file 0
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(true, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    Assert.assertEquals(3, seqTasks.size());
    Assert.assertEquals(3, seqTasks.get(0).getFullyDirtyFiles().size());
    Assert.assertEquals(1, seqTasks.get(0).getPartiallyDirtyFiles().size());
    Assert.assertEquals(seqResources.get(0), seqTasks.get(0).getPartiallyDirtyFiles().get(0));

    Assert.assertEquals(0, seqTasks.get(1).getFullyDirtyFiles().size());
    Assert.assertEquals(1, seqTasks.get(1).getPartiallyDirtyFiles().size());
    Assert.assertEquals(seqResources.get(4), seqTasks.get(1).getPartiallyDirtyFiles().get(0));

    Assert.assertEquals(0, seqTasks.get(2).getFullyDirtyFiles().size());
    Assert.assertEquals(2, seqTasks.get(2).getPartiallyDirtyFiles().size());
    Assert.assertEquals(seqResources.get(6), seqTasks.get(2).getPartiallyDirtyFiles().get(0));
    Assert.assertEquals(seqResources.get(8), seqTasks.get(2).getPartiallyDirtyFiles().get(1));

    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertTrue(seqTasks.get(1).start());
    Assert.assertTrue(seqTasks.get(2).start());
    Assert.assertEquals(6, tsFileManager.getTsFileList(true).size());
  }

  // endregion

  private void addFileMods(
      TsFileResource resource, MeasurementPath path, long fileOffset, long startTime, long endTime)
      throws IOException {
    try (ModificationFile modificationFile = resource.getModFile()) {
      modificationFile.write(new Deletion(path, fileOffset, startTime, endTime));
    }
  }

  protected void generateModsFile(
      int deviceNum,
      int measurementNum,
      List<TsFileResource> resources,
      long startTime,
      long endTime,
      boolean isAligned)
      throws IllegalPathException, IOException {
    List<String> seriesPaths = new ArrayList<>();
    for (int dIndex = 0; dIndex < deviceNum; dIndex++) {
      for (int mIndex = 0; mIndex < measurementNum; mIndex++) {
        seriesPaths.add(
            COMPACTION_TEST_SG
                + IoTDBConstant.PATH_SEPARATOR
                + "d"
                + (isAligned ? 10000 + dIndex : dIndex)
                + IoTDBConstant.PATH_SEPARATOR
                + "s"
                + mIndex);
      }
    }
    generateModsFile(seriesPaths, resources, startTime, endTime);
  }
}
