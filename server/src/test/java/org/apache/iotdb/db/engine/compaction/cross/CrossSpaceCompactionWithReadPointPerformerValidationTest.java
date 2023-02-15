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

package org.apache.iotdb.db.engine.compaction.cross;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.engine.compaction.execute.performer.ICrossCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.execute.performer.impl.ReadPointCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.selector.ICrossSpaceSelector;
import org.apache.iotdb.db.engine.compaction.selector.impl.RewriteCrossSpaceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.selector.utils.CrossCompactionTaskResource;
import org.apache.iotdb.db.engine.compaction.selector.utils.CrossSpaceCompactionCandidate;
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.tools.validate.TsFileValidationTool;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class CrossSpaceCompactionWithReadPointPerformerValidationTest
    extends AbstractCompactionTest {
  TsFileManager tsFileManager =
      new TsFileManager(COMPACTION_TEST_SG, "0", STORAGE_GROUP_DIR.getPath());

  private final String oldThreadName = Thread.currentThread().getName();

  private ICrossCompactionPerformer performer = new ReadPointCompactionPerformer();

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    IoTDBDescriptor.getInstance().getConfig().setMinCrossCompactionUnseqFileLevel(0);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(1024);
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    Thread.currentThread().setName("pool-1-IoTDB-Compaction-1");
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    Thread.currentThread().setName(oldThreadName);
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    TsFileValidationTool.badFileNum = 0;
  }

  /**
   * 4 Seq files: 0 ~ 1000, 1100 ~ 2100, 4200 ~ 5200, 5300 ~ 6300<br>
   * 2 Unseq files: 2100 ~ 3100, 3200 ~ 4200<br>
   * Selected seq file index: 3<br>
   * Selected unseq file index: 1, 2
   */
  @Test
  public void test1() throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(2, 10, 10, 1000, 0, 0, 100, 100, true, true);
    createFiles(2, 10, 10, 1001, 2100, 2100, 100, 100, true, false);
    createFiles(1, 10, 10, 1000, 4200, 4200, 100, 100, true, true);
    createFiles(1, 10, 10, 1000, 5300, 5300, 100, 100, true, true);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);
    Assert.assertEquals(1, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());
    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(2));

    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(1), unseqResources.get(1));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * 4 Seq files: 0 ~ 1000, 1100 ~ 2100, 4200 ~ 5200, 5300 ~ 6300<br>
   * 2 Unseq files: 2100 ~ 3100, 3200 ~ 4700<br>
   * Selected seq file index: 3<br>
   * Selected unseq file index: 1, 2
   */
  @Test
  public void test2() throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(2, 5, 10, 1000, 0, 0, 100, 100, true, true);
    createFiles(1, 5, 10, 1000, 2100, 2100, 100, 100, true, false);
    createFiles(1, 5, 10, 1500, 3200, 3200, 100, 100, true, false);
    createFiles(2, 5, 10, 1000, 4200, 4200, 100, 100, true, true);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(1, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());

    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(2));

    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(1), unseqResources.get(1));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * 4 Seq files: 0 ~ 1000, 1100 ~ 2100, 4200 ~ 5200, 5300 ~ 6300<br>
   * 2 Unseq files: 1500 ~ 3000, 3100 ~ 4100<br>
   * Selected seq file index: 2, 3<br>
   * Selected unseq file index: 1, 2
   */
  @Test
  public void test3() throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(2, 5, 10, 1000, 0, 0, 100, 100, true, true);
    createFiles(1, 5, 10, 1500, 1500, 1500, 100, 100, true, false);
    createFiles(1, 5, 10, 1000, 3100, 3100, 100, 100, true, false);
    createFiles(2, 5, 10, 1000, 4200, 4200, 100, 100, true, true);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(2, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());

    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(1));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(1), unseqResources.get(1));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * 5 Seq files: 0 ~ 1000, 1100 ~ 2100, 4200 ~ 5200, 5300 ~ 6300, 6500 ~ 7500<br>
   * 5 Unseq files: 1500 ~ 2500, 1700 ~ 2000, 2400 ~ 3400, 3300 ~ 4500, 6301 ~ 7301<br>
   * Notice: the last seq file is unsealed<br>
   * Selected seq file index: 2, 3<br>
   * Selected unseq file index: 1, 2, 3, 4
   */
  @Test
  public void test4() throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(2, 5, 10, 1000, 0, 0, 100, 100, true, true);
    createFiles(1, 5, 10, 1000, 1500, 1500, 100, 100, true, false);
    createFiles(1, 5, 10, 300, 1700, 1700, 100, 100, true, false);
    createFiles(1, 5, 10, 1000, 2400, 2400, 100, 100, true, false);
    createFiles(1, 5, 10, 1200, 3300, 3300, 100, 100, true, false);
    createFiles(2, 5, 10, 1000, 4200, 4200, 100, 100, true, true);
    createFiles(1, 5, 10, 1000, 6500, 6500, 100, 100, true, true);
    createFiles(1, 5, 10, 1000, 6301, 6301, 100, 100, true, false);
    seqResources.get(4).setStatus(TsFileResourceStatus.UNCLOSED);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(2, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(4, selected.get(0).getUnseqFiles().size());
    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(1));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(2));

    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(1), unseqResources.get(1));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(2), unseqResources.get(2));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(3), unseqResources.get(3));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * 7 Seq files: 0 ~ 1000, 1100 ~ 2100, 2200 ~ 3200, 3300 ~ 4300, 4400 ~ 5400, 5500 ~ 6500, 6600 ~
   * 7600<br>
   * 2 Unseq files: 2150 ~ 5450, 1150 ～ 5550<br>
   * Selected seq file index: 2, 3, 4, 5, 6<br>
   * Selected unseq file index: 1, 2
   */
  @Test
  public void test5() throws MetadataException, IOException, WriteProcessException, MergeException {
    IoTDBDescriptor.getInstance().getConfig().setMaxCrossCompactionCandidateFileNum(7);
    registerTimeseriesInMManger(5, 10, true);
    createFiles(7, 5, 10, 1000, 0, 0, 100, 100, true, true);
    createFiles(1, 5, 10, 3300, 2150, 2150, 100, 100, true, false);
    createFiles(1, 5, 10, 4400, 1150, 1150, 100, 100, true, false);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(5, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());
    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(1));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(2), seqResources.get(3));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(3), seqResources.get(4));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(4), seqResources.get(5));

    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(1), unseqResources.get(1));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * 4 Seq files: 0 ~ 1000, 1100 ~ 4100, 4200 ~ 5200, 5300 ~ 6300<br>
   * 3 Unseq files: 1500 ~ 2500, 2600 ～ 3600, 2000 ~ 3000<br>
   * Selected seq file index: 2<br>
   * Selected unseq file index: 1, 2, 3
   */
  @Test
  public void test6() throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(1, 5, 10, 1000, 0, 0, 100, 100, true, true);
    createFiles(1, 5, 10, 3000, 1100, 1100, 100, 100, true, true);
    createFiles(2, 5, 10, 1000, 4200, 4200, 100, 100, true, true);
    createFiles(2, 5, 10, 1000, 1500, 1500, 100, 100, true, false);
    createFiles(1, 5, 10, 1000, 2000, 2000, 100, 100, true, false);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(1, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(3, selected.get(0).getUnseqFiles().size());
    for (TsFileResource selectedResource : (List<TsFileResource>) selected.get(0).getSeqFiles()) {
      Assert.assertEquals(selectedResource, seqResources.get(1));
    }
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(1), unseqResources.get(1));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(2), unseqResources.get(2));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * 5 Seq files: 0 ~ 1000, 1100 ~ 4100, 4200 ~ 5200, 5300 ~ 6300, 6400 ~ 7400<br>
   * 3 Unseq files: 1500 ~ 2500, 2600 ～ 3600, 2000 ~ 3000, 1200 ~ 5250<br>
   * Selected seq file index: 2, 3, 4<br>
   * Selected unseq file index: 1, 2, 3, 4
   */
  @Test
  public void test7() throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(1, 5, 10, 1000, 0, 0, 100, 100, true, true);
    createFiles(1, 5, 10, 3000, 1100, 1100, 100, 100, true, true);
    createFiles(3, 5, 10, 1000, 4200, 4200, 100, 100, true, true);
    createFiles(2, 5, 10, 1000, 1500, 1500, 100, 100, true, false);
    createFiles(1, 5, 10, 1000, 2000, 2000, 100, 100, true, false);
    createFiles(1, 5, 10, 4050, 1200, 1200, 100, 100, true, false);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(3, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(4, selected.get(0).getUnseqFiles().size());

    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(1));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(2), seqResources.get(3));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(1), unseqResources.get(1));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(2), unseqResources.get(2));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(3), unseqResources.get(3));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * 4 Seq files: 0 ~ 1000, 1100 ~ 2100, 4200 ~ 5200, 5300 ~ 6300<br>
   * 4 Unseq files: 1500 ~ 2500, 1700 ~ 2000, 2400 ~ 3400, 3300 ~ 4500<br>
   * Selected seq file index: 2, 3<br>
   * Selected unseq file index: 1, 2, 3, 4
   */
  @Test
  public void test8() throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(2, 5, 10, 1000, 0, 0, 100, 100, true, true);
    createFiles(1, 5, 10, 1000, 1500, 1500, 100, 100, true, false);
    createFiles(1, 5, 10, 300, 1700, 1700, 100, 100, true, false);
    createFiles(1, 5, 10, 1000, 2400, 2400, 100, 100, true, false);
    createFiles(1, 5, 10, 1200, 3300, 3300, 100, 100, true, false);
    createFiles(2, 5, 10, 1000, 4200, 4200, 100, 100, true, true);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(2, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(4, selected.get(0).getUnseqFiles().size());

    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(1));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(2));

    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(1), unseqResources.get(1));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(2), unseqResources.get(2));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(3), unseqResources.get(3));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * [Time range]:<br>
   * 5 Seq files: 0 ~ 1000, 1100 ~ 2100, 4200 ~ 5200, 5300 ~ 6300, 6400 ~ 7400<br>
   * 2 Unseq files: 2100 ~ 3100, 3200 ~ 4200<br>
   * [DeviceNum, SensorNum]:<br>
   * seq files: [10,10], [10,10], [5,5], [10,10], [10,10]<br>
   * unseq files: [10,10], [10,10]<br>
   * Selected seq file index: 3, 4<br>
   * Selected unseq file index: 1, 2
   */
  @Test
  public void testWithNewAlignedDeviceAndSensorInUnseqFile10()
      throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(2, 10, 10, 1000, 0, 0, 100, 100, true, true);
    createFiles(2, 10, 10, 1000, 2100, 2100, 100, 100, true, false);
    createFiles(1, 5, 5, 1000, 4200, 4200, 100, 100, true, true);
    createFiles(2, 10, 10, 1000, 5300, 5300, 100, 100, true, true);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(2, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());
    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(3));

    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(1), unseqResources.get(1));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * [Time range]:<br>
   * 5 Seq files: 0 ~ 1000, 1100 ~ 2100, 4200 ~ 5200, 5300 ~ 6300, 6400 ~ 7400<br>
   * 2 Unseq files: 2100 ~ 3100, 3200 ~ 4200<br>
   * [DeviceNum, SensorNum]:<br>
   * seq files: [10,10], [10,10], [5,5], [10,5], [10,10]<br>
   * unseq files: [10,10], [10,10]<br>
   * Selected seq file index: 3, 4<br>
   * Selected unseq file index: 1, 2
   */
  @Test
  public void testWithNewAlignedDeviceAndSensorInUnseqFile11()
      throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(2, 10, 10, 1000, 0, 0, 100, 100, true, true);
    createFiles(2, 10, 10, 1000, 2100, 2100, 100, 100, true, false);
    createFiles(1, 5, 5, 1000, 4200, 4200, 100, 100, true, true);
    createFiles(1, 10, 5, 1000, 5300, 5300, 100, 100, true, true);
    createFiles(1, 10, 10, 1000, 6400, 6400, 100, 100, true, true);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(2, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());
    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(3));

    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(1), unseqResources.get(1));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * [Time range]:<br>
   * 6 Seq files: 0 ~ 1000, 1100 ~ 2100, 4200 ~ 5200, 5300 ~ 6300, 6400 ~ 7400, 7500 ~ 8500<br>
   * 2 Unseq files: 2100 ~ 3100, 3200 ~ 4200<br>
   * [DeviceNum, SensorNum]:<br>
   * seq files: [10,10], [10,10], [5,5], [5,10], [10,7], [10,10]<br>
   * unseq files: [10,10], [10,10]<br>
   * Selected seq file index: 3, 5<br>
   * Selected unseq file index: 1, 2
   */
  @Test
  public void testWithNewAlignedDeviceAndSensorInUnseqFile12()
      throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(2, 10, 10, 1000, 0, 0, 100, 100, true, true);
    createFiles(2, 10, 10, 1000, 2100, 2100, 100, 100, true, false);
    createFiles(1, 5, 5, 1000, 4200, 4200, 100, 100, true, true);
    createFiles(1, 5, 10, 1000, 5300, 5300, 100, 100, true, true);
    createFiles(1, 10, 7, 1000, 6400, 6400, 100, 100, true, true);
    createFiles(1, 10, 10, 1000, 7500, 7500, 100, 100, true, true);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(2, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());
    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(4));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(1), unseqResources.get(1));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * [Time range]:<br>
   * 5 Seq files: 0 ~ 1000, 1100 ~ 2100, 4200 ~ 5200, 5300 ~ 6300, 6400 ~ 7400<br>
   * 2 Unseq files: 2100 ~ 3100, 3200 ~ 4700<br>
   * [DeviceNum, SensorNum]:<br>
   * seq files: [10,10], [10,10], [5,5], [10,10], [10,10]<br>
   * unseq files: [10,10], [10,10]<br>
   * Selected seq file index: 3, 4<br>
   * Selected unseq file index: 1, 2
   */
  @Test
  public void testWithNewAlignedDeviceAndSensorInUnseqFile20()
      throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(2, 10, 10, 1000, 0, 0, 100, 100, true, true);
    createFiles(1, 10, 10, 1000, 2100, 2100, 100, 100, true, false);
    createFiles(1, 10, 10, 1500, 3200, 3200, 100, 100, true, false);
    createFiles(1, 5, 5, 1000, 4200, 4200, 100, 100, true, true);
    createFiles(2, 10, 10, 1000, 5300, 5300, 100, 100, true, true);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(2, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());

    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(3));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(1), unseqResources.get(1));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * [Time range]:<br>
   * 5 Seq files: 0 ~ 1000, 1100 ~ 2100, 4200 ~ 5200, 5300 ~ 6300, 6400 ~ 7400<br>
   * 2 Unseq files: 2100 ~ 3100, 3200 ~ 4700<br>
   * [DeviceNum, SensorNum]:<br>
   * seq files: [10,10], [10,10], [5,5], [10,5], [10,10]<br>
   * unseq files: [10,10], [10,10]<br>
   * Selected seq file index: 3, 4<br>
   * Selected unseq file index: 1, 2
   */
  @Test
  public void testWithNewAlignedDeviceAndSensorInUnseqFile21()
      throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(2, 10, 10, 1000, 0, 0, 100, 100, true, true);
    createFiles(1, 10, 10, 1000, 2100, 2100, 100, 100, true, false);
    createFiles(1, 10, 10, 1500, 3200, 3200, 100, 100, true, false);
    createFiles(1, 5, 5, 1000, 4200, 4200, 100, 100, true, true);
    createFiles(1, 10, 5, 1000, 5300, 5300, 100, 100, true, true);
    createFiles(1, 10, 10, 1000, 6400, 6400, 100, 100, true, true);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(2, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());

    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(3));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(1), unseqResources.get(1));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * [Time range]:<br>
   * 6 Seq files: 0 ~ 1000, 1100 ~ 2100, 4200 ~ 5200, 5300 ~ 6300, 6400 ~ 7400, 7500 ~ 8500<br>
   * 2 Unseq files: 2100 ~ 3100, 3200 ~ 4700<br>
   * [DeviceNum, SensorNum]:<br>
   * seq files: [10,10], [10,10], [5,5], [5,10], [10,7], [10,10]<br>
   * unseq files: [10,10], [10,10]<br>
   * Selected seq file index: 3, 5<br>
   * Selected unseq file index: 1, 2
   */
  @Test
  public void testWithNewAlignedDeviceAndSensorInUnseqFile22()
      throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(2, 10, 10, 1000, 0, 0, 100, 100, true, true);
    createFiles(1, 10, 10, 1000, 2100, 2100, 100, 100, true, false);
    createFiles(1, 10, 10, 1500, 3200, 3200, 100, 100, true, false);
    createFiles(1, 5, 5, 1000, 4200, 4200, 100, 100, true, true);
    createFiles(1, 5, 10, 1000, 5300, 5300, 100, 100, true, true);
    createFiles(1, 10, 7, 1000, 6400, 6400, 100, 100, true, true);
    createFiles(1, 10, 10, 1000, 7500, 7500, 100, 100, true, true);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(2, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());

    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(4));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(1), unseqResources.get(1));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * [Time range]:<br>
   * 5 Seq files: 0 ~ 1000, 1100 ~ 2100, 4200 ~ 5200, 5300 ~ 6300, 6400 ~ 7400<br>
   * 2 Unseq files: 1500 ~ 3000, 3100 ~ 4100<br>
   * [DeviceNum, SensorNum]:<br>
   * seq files: [10,10], [5,5], [5,5], [5,10], [10,10], [10,10]<br>
   * unseq files: [10,10], [10,10]<br>
   * Selected seq file index: 2, 3, 4<br>
   * Selected unseq file index: 1, 2
   */
  @Test
  public void testWithNewAlignedDeviceAndSensorInUnseqFile30()
      throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(1, 10, 10, 1000, 0, 0, 100, 100, true, true);
    createFiles(1, 5, 5, 1000, 1100, 1100, 100, 100, true, true);
    createFiles(1, 5, 5, 1000, 4200, 4200, 100, 100, true, true);
    createFiles(2, 10, 10, 1000, 5300, 5300, 100, 100, true, true);
    createFiles(1, 10, 10, 1500, 1500, 1500, 100, 100, true, false);
    createFiles(1, 10, 10, 1000, 3100, 3100, 100, 100, true, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(3, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());

    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(1));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(2), seqResources.get(3));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(1), unseqResources.get(1));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * [Time range]:<br>
   * 5 Seq files: 0 ~ 1000, 1100 ~ 2100, 4200 ~ 5200, 5300 ~ 6300, 6400 ~ 7400<br>
   * 2 Unseq files: 1500 ~ 3000, 3100 ~ 4100<br>
   * [DeviceNum, SensorNum]:<br>
   * seq files: [10,10], [5,5], [5,5], [5,10], [10,5], [10,10]<br>
   * unseq files: [10,10], [10,10]<br>
   * Selected seq file index: 2, 3, 4<br>
   * Selected unseq file index: 1, 2
   */
  @Test
  public void testWithNewAlignedDeviceAndSensorInUnseqFile31()
      throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(1, 10, 10, 1000, 0, 0, 100, 100, true, true);
    createFiles(1, 5, 5, 1000, 1100, 1100, 100, 100, true, true);
    createFiles(1, 5, 5, 1000, 4200, 4200, 100, 100, true, true);
    createFiles(1, 10, 5, 1000, 5300, 5300, 100, 100, true, true);
    createFiles(1, 10, 10, 1000, 6400, 6400, 100, 100, true, true);
    createFiles(1, 10, 10, 1500, 1500, 1500, 100, 100, true, false);
    createFiles(1, 10, 10, 1000, 3100, 3100, 100, 100, true, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(3, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());

    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(1));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(2), seqResources.get(3));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(1), unseqResources.get(1));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * [Time range]:<br>
   * 6 Seq files: 0 ~ 1000, 1100 ~ 2100, 4200 ~ 5200, 5300 ~ 6300, 6400 ~ 7400, 7500 ~ 8500<br>
   * 2 Unseq files: 1500 ~ 3000, 3100 ~ 4100<br>
   * [DeviceNum, SensorNum]:<br>
   * seq files: [10,10], [5,5], [5,5], [5,10], [10,7], [10,10]<br>
   * unseq files: [10,10], [10,10]<br>
   * Selected seq file index: 2, 3, 5<br>
   * Selected unseq file index: 1, 2
   */
  @Test
  public void testWithNewAlignedDeviceAndSensorInUnseqFile32()
      throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(1, 10, 10, 1000, 0, 0, 100, 100, true, true);
    createFiles(1, 5, 5, 1000, 1100, 1100, 100, 100, true, true);
    createFiles(1, 5, 5, 1000, 4200, 4200, 100, 100, true, true);
    createFiles(1, 5, 10, 1000, 5300, 5300, 100, 100, true, true);
    createFiles(1, 10, 7, 1000, 6400, 6400, 100, 100, true, true);
    createFiles(1, 10, 10, 1000, 7500, 7500, 100, 100, true, true);
    createFiles(1, 10, 10, 1500, 1500, 1500, 100, 100, true, false);
    createFiles(1, 10, 10, 1000, 3100, 3100, 100, 100, true, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(3, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());

    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(1));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(2), seqResources.get(4));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(1), unseqResources.get(1));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * 9 Seq files: 0 ~ 1000, 1100 ~ 2100, 2200 ~ 3200, 3300 ~ 4300, 4400 ~ 5400,......<br>
   * 1 Unseq files: 2150 ~ 5450<br>
   * Selected seq file index: 3, 4, 5, 6, 7<br>
   * Selected unseq file index: 1
   */
  @Test
  public void testWithNewAlignedDeviceAndSensorInUnseqFile50()
      throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(1, 10, 10, 1000, 0, 0, 100, 100, true, true);
    createFiles(1, 6, 6, 1000, 1100, 1100, 100, 100, true, true);
    createFiles(1, 5, 5, 1000, 2200, 2200, 100, 100, true, true);
    createFiles(1, 4, 4, 1000, 3300, 3300, 100, 100, true, true);
    createFiles(1, 3, 3, 1000, 4400, 4400, 100, 100, true, true);
    createFiles(1, 2, 2, 1000, 5500, 5500, 100, 100, true, true);
    createFiles(1, 10, 10, 1000, 6600, 6600, 100, 100, true, true);
    createFiles(1, 10, 10, 1000, 7700, 7700, 100, 100, true, true);
    createFiles(1, 10, 10, 1000, 8800, 8800, 100, 100, true, true);
    createFiles(1, 10, 10, 3300, 2150, 2150, 100, 100, true, false);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(5, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(1, selected.get(0).getUnseqFiles().size());
    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(3));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(2), seqResources.get(4));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(3), seqResources.get(5));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(4), seqResources.get(6));

    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * 9 Seq files: 0 ~ 1000, 1100 ~ 2100, 2200 ~ 3200, 3300 ~ 4300, 4400 ~ 5400,......<br>
   * 1 Unseq files: 2150 ~ 5450<br>
   * Selected seq file index: 3, 4, 5, 6, 7<br>
   * Selected unseq file index: 1
   */
  @Test
  public void testWithNewAlignedDeviceAndSensorInUnseqFile51()
      throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(1, 10, 10, 1000, 0, 0, 100, 100, true, true);
    createFiles(1, 6, 6, 1000, 1100, 1100, 100, 100, true, true);
    createFiles(1, 5, 5, 1000, 2200, 2200, 100, 100, true, true);
    createFiles(1, 4, 4, 1000, 3300, 3300, 100, 100, true, true);
    createFiles(1, 3, 3, 1000, 4400, 4400, 100, 100, true, true);
    createFiles(1, 2, 2, 1000, 5500, 5500, 100, 100, true, true);
    createFiles(1, 10, 5, 1000, 6600, 6600, 100, 100, true, true);
    createFiles(1, 10, 10, 1000, 7700, 7700, 100, 100, true, true);
    createFiles(1, 10, 10, 1000, 8800, 8800, 100, 100, true, true);
    createFiles(1, 10, 10, 3300, 2150, 2150, 100, 100, true, false);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(5, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(1, selected.get(0).getUnseqFiles().size());
    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(3));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(2), seqResources.get(4));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(3), seqResources.get(5));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(4), seqResources.get(6));

    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * 9 Seq files: 0 ~ 1000, 1100 ~ 2100, 2200 ~ 3200, 3300 ~ 4300, 4400 ~ 5400,......<br>
   * 1 Unseq files: 2150 ~ 5450<br>
   * Selected seq file index: 3, 4, 5, 6, 7, 8<br>
   * Selected unseq file index: 1
   */
  @Test
  public void testWithNewAlignedDeviceAndSensorInUnseqFile52()
      throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(1, 10, 10, 1000, 0, 0, 100, 100, true, true);
    createFiles(1, 6, 6, 1000, 1100, 1100, 100, 100, true, true);
    createFiles(1, 5, 5, 1000, 2200, 2200, 100, 100, true, true);
    createFiles(1, 4, 4, 1000, 3300, 3300, 100, 100, true, true);
    createFiles(1, 3, 3, 1000, 4400, 4400, 100, 100, true, true);
    createFiles(1, 2, 2, 1000, 5500, 5500, 100, 100, true, true);
    createFiles(1, 5, 10, 1000, 6600, 6600, 100, 100, true, true);
    createFiles(1, 10, 7, 1000, 7700, 7700, 100, 100, true, true);
    createFiles(1, 10, 10, 1000, 8800, 8800, 100, 100, true, true);
    createFiles(1, 10, 10, 3300, 2150, 2150, 100, 100, true, false);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(6, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(1, selected.get(0).getUnseqFiles().size());
    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(3));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(2), seqResources.get(4));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(3), seqResources.get(5));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(4), seqResources.get(6));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(5), seqResources.get(7));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * 9 Seq files: 0 ~ 1000, 1100 ~ 2100, 2200 ~ 3200, 3300 ~ 4300, 4400 ~ 5400,......<br>
   * 1 Unseq files: 2150 ~ 5450<br>
   * Selected seq file index: 3, 4, 5, 6, 8<br>
   * Selected unseq file index: 1
   */
  @Test
  public void testWithNewAlignedDeviceAndSensorInUnseqFile53()
      throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(1, 10, 10, 1000, 0, 0, 100, 100, true, true);
    createFiles(1, 6, 6, 1000, 1100, 1100, 100, 100, true, true);
    createFiles(1, 5, 5, 1000, 2200, 2200, 100, 100, true, true);
    createFiles(1, 4, 4, 1000, 3300, 3300, 100, 100, true, true);
    createFiles(1, 3, 3, 1000, 4400, 4400, 100, 100, true, true);
    createFiles(1, 5, 2, 1000, 5500, 5500, 100, 100, true, true);
    createFiles(1, 5, 10, 1000, 6600, 6600, 100, 100, true, true);
    createFiles(1, 10, 7, 1000, 7700, 7700, 100, 100, true, true);
    createFiles(1, 10, 10, 1000, 8800, 8800, 100, 100, true, true);
    createFiles(1, 10, 10, 3300, 2150, 2150, 100, 100, true, false);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(5, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(1, selected.get(0).getUnseqFiles().size());
    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(3));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(2), seqResources.get(4));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(3), seqResources.get(5));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(4), seqResources.get(7));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * [Time range]:<br>
   * 5 Seq files: 0 ~ 1000, 1100 ~ 2100, 4200 ~ 5200, 5300 ~ 6300, 6400 ~ 7400<br>
   * 2 Unseq files: 2100 ~ 3100, 3200 ~ 4200<br>
   * [DeviceNum, SensorNum]:<br>
   * seq files: [10,10], [10,10], [5,5], [10,10], [10,10]<br>
   * unseq files: [10,10], [10,10]<br>
   * Selected seq file index: 3, 4<br>
   * Selected unseq file index: 1, 2
   */
  @Test
  public void testWithNewDeviceAndSensorInUnseqFile10()
      throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(2, 10, 10, 1000, 0, 0, 100, 100, false, true);
    createFiles(2, 10, 10, 1000, 2100, 2100, 100, 100, false, false);
    createFiles(1, 5, 5, 1000, 4200, 4200, 100, 100, false, true);
    createFiles(2, 10, 10, 1000, 5300, 5300, 100, 100, false, true);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(2, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());
    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(3));

    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(1), unseqResources.get(1));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * [Time range]:<br>
   * 5 Seq files: 0 ~ 1000, 1100 ~ 2100, 4200 ~ 5200, 5300 ~ 6300, 6400 ~ 7400<br>
   * 2 Unseq files: 2100 ~ 3100, 3200 ~ 4200<br>
   * [DeviceNum, SensorNum]:<br>
   * seq files: [10,10], [10,10], [5,5], [10,5], [10,10]<br>
   * unseq files: [10,10], [10,10]<br>
   * Selected seq file index: 3, 4<br>
   * Selected unseq file index: 1, 2
   */
  @Test
  public void testWithNewDeviceAndSensorInUnseqFile11()
      throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(2, 10, 10, 1000, 0, 0, 100, 100, false, true);
    createFiles(2, 10, 10, 1000, 2100, 2100, 100, 100, false, false);
    createFiles(1, 5, 5, 1000, 4200, 4200, 100, 100, false, true);
    createFiles(1, 10, 5, 1000, 5300, 5300, 100, 100, false, true);
    createFiles(1, 10, 10, 1000, 6400, 6400, 100, 100, false, true);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(2, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());
    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(3));

    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(1), unseqResources.get(1));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * [Time range]:<br>
   * 6 Seq files: 0 ~ 1000, 1100 ~ 2100, 4200 ~ 5200, 5300 ~ 6300, 6400 ~ 7400, 7500 ~ 8500<br>
   * 2 Unseq files: 2100 ~ 3100, 3200 ~ 4200<br>
   * [DeviceNum, SensorNum]:<br>
   * seq files: [10,10], [10,10], [5,5], [5,10], [10,7], [10,10]<br>
   * unseq files: [10,10], [10,10]<br>
   * Selected seq file index: 3, 5<br>
   * Selected unseq file index: 1, 2
   */
  @Test
  public void testWithNewDeviceAndSensorInUnseqFile12()
      throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(2, 10, 10, 1000, 0, 0, 100, 100, false, true);
    createFiles(2, 10, 10, 1000, 2100, 2100, 100, 100, false, false);
    createFiles(1, 5, 5, 1000, 4200, 4200, 100, 100, false, true);
    createFiles(1, 5, 10, 1000, 5300, 5300, 100, 100, false, true);
    createFiles(1, 10, 7, 1000, 6400, 6400, 100, 100, false, true);
    createFiles(1, 10, 10, 1000, 7500, 7500, 100, 100, false, true);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(2, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());
    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(4));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(1), unseqResources.get(1));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * [Time range]:<br>
   * 5 Seq files: 0 ~ 1000, 1100 ~ 2100, 4200 ~ 5200, 5300 ~ 6300, 6400 ~ 7400<br>
   * 2 Unseq files: 2100 ~ 3100, 3200 ~ 4700<br>
   * [DeviceNum, SensorNum]:<br>
   * seq files: [10,10], [10,10], [5,5], [10,10], [10,10]<br>
   * unseq files: [10,10], [10,10]<br>
   * Selected seq file index: 3, 4<br>
   * Selected unseq file index: 1, 2
   */
  @Test
  public void testWithNewDeviceAndSensorInUnseqFile20()
      throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(2, 10, 10, 1000, 0, 0, 100, 100, false, true);
    createFiles(1, 10, 10, 1000, 2100, 2100, 100, 100, false, false);
    createFiles(1, 10, 10, 1500, 3200, 3200, 100, 100, false, false);
    createFiles(1, 5, 5, 1000, 4200, 4200, 100, 100, false, true);
    createFiles(2, 10, 10, 1000, 5300, 5300, 100, 100, false, true);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(2, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());

    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(3));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(1), unseqResources.get(1));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * [Time range]:<br>
   * 5 Seq files: 0 ~ 1000, 1100 ~ 2100, 4200 ~ 5200, 5300 ~ 6300, 6400 ~ 7400<br>
   * 2 Unseq files: 2100 ~ 3100, 3200 ~ 4700<br>
   * [DeviceNum, SensorNum]:<br>
   * seq files: [10,10], [10,10], [5,5], [10,5], [10,10]<br>
   * unseq files: [10,10], [10,10]<br>
   * Selected seq file index: 3, 4<br>
   * Selected unseq file index: 1, 2
   */
  @Test
  public void testWithNewDeviceAndSensorInUnseqFile21()
      throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(2, 10, 10, 1000, 0, 0, 100, 100, false, true);
    createFiles(1, 10, 10, 1000, 2100, 2100, 100, 100, false, false);
    createFiles(1, 10, 10, 1500, 3200, 3200, 100, 100, false, false);
    createFiles(1, 5, 5, 1000, 4200, 4200, 100, 100, false, true);
    createFiles(1, 10, 5, 1000, 5300, 5300, 100, 100, false, true);
    createFiles(1, 10, 10, 1000, 6400, 6400, 100, 100, false, true);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(2, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());

    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(3));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(1), unseqResources.get(1));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * [Time range]:<br>
   * 6 Seq files: 0 ~ 1000, 1100 ~ 2100, 4200 ~ 5200, 5300 ~ 6300, 6400 ~ 7400, 7500 ~ 8500<br>
   * 2 Unseq files: 2100 ~ 3100, 3200 ~ 4700<br>
   * [DeviceNum, SensorNum]:<br>
   * seq files: [10,10], [10,10], [5,5], [5,10], [10,7], [10,10]<br>
   * unseq files: [10,10], [10,10]<br>
   * Selected seq file index: 3, 5<br>
   * Selected unseq file index: 1, 2
   */
  @Test
  public void testWithNewDeviceAndSensorInUnseqFile22()
      throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(2, 10, 10, 1000, 0, 0, 100, 100, false, true);
    createFiles(1, 5, 5, 1000, 4200, 4200, 100, 100, false, true);
    createFiles(1, 5, 10, 1000, 5300, 5300, 100, 100, false, true);
    createFiles(1, 10, 7, 1000, 6400, 6400, 100, 100, false, true);
    createFiles(1, 10, 10, 1000, 7500, 7500, 100, 100, false, true);
    createFiles(1, 10, 10, 1000, 2100, 2100, 100, 100, false, false);
    createFiles(1, 10, 10, 1500, 3200, 3200, 100, 100, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(2, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());

    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(4));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(1), unseqResources.get(1));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * [Time range]:<br>
   * 5 Seq files: 0 ~ 1000, 1100 ~ 2100, 4200 ~ 5200, 5300 ~ 6300, 6400 ~ 7400<br>
   * 2 Unseq files: 1500 ~ 3000, 3100 ~ 4100<br>
   * [DeviceNum, SensorNum]:<br>
   * seq files: [10,10], [5,5], [5,5], [10,10], [10,10]<br>
   * unseq files: [10,10], [10,10]<br>
   * Selected seq file index: 2, 3, 4<br>
   * Selected unseq file index: 1, 2
   */
  @Test
  public void testWithNewDeviceAndSensorInUnseqFile30()
      throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(1, 10, 10, 1000, 0, 0, 100, 100, false, true);
    createFiles(1, 5, 5, 1000, 1100, 1100, 100, 100, false, true);
    createFiles(1, 5, 5, 1000, 4200, 4200, 100, 100, false, true);
    createFiles(2, 10, 10, 1000, 5300, 5300, 100, 100, false, true);
    createFiles(1, 10, 10, 1500, 1500, 1500, 100, 100, false, false);
    createFiles(1, 10, 10, 1000, 3100, 3100, 100, 100, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(3, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());

    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(1));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(2), seqResources.get(3));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(1), unseqResources.get(1));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * [Time range]:<br>
   * 5 Seq files: 0 ~ 1000, 1100 ~ 2100, 4200 ~ 5200, 5300 ~ 6300, 6400 ~ 7400<br>
   * 2 Unseq files: 1500 ~ 3000, 3100 ~ 4100<br>
   * [DeviceNum, SensorNum]:<br>
   * seq files: [10,10], [5,5], [5,5], [5,10], [10,5], [10,10]<br>
   * unseq files: [10,10], [10,10]<br>
   * Selected seq file index: 2, 3, 4<br>
   * Selected unseq file index: 1, 2
   */
  @Test
  public void testWithNewDeviceAndSensorInUnseqFile31()
      throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(1, 10, 10, 1000, 0, 0, 100, 100, false, true);
    createFiles(1, 5, 5, 1000, 1100, 1100, 100, 100, false, true);
    createFiles(1, 5, 5, 1000, 4200, 4200, 100, 100, false, true);
    createFiles(1, 10, 5, 1000, 5300, 5300, 100, 100, false, true);
    createFiles(1, 10, 10, 1000, 6400, 6400, 100, 100, false, true);
    createFiles(1, 10, 10, 1500, 1500, 1500, 100, 100, false, false);
    createFiles(1, 10, 10, 1000, 3100, 3100, 100, 100, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(3, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());

    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(1));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(2), seqResources.get(3));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(1), unseqResources.get(1));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * [Time range]:<br>
   * 6 Seq files: 0 ~ 1000, 1100 ~ 2100, 4200 ~ 5200, 5300 ~ 6300, 6400 ~ 7400, 7500 ~ 8500<br>
   * 2 Unseq files: 1500 ~ 3000, 3100 ~ 4100<br>
   * [DeviceNum, SensorNum]:<br>
   * seq files: [10,10], [5,5], [5,5], [5,10], [10,7], [10,10]<br>
   * unseq files: [10,10], [10,10]<br>
   * Selected seq file index: 2, 3, 5<br>
   * Selected unseq file index: 1, 2
   */
  @Test
  public void testWithNewDeviceAndSensorInUnseqFile32()
      throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(1, 10, 10, 1000, 0, 0, 100, 100, false, true);
    createFiles(1, 5, 5, 1000, 1100, 1100, 100, 100, false, true);
    createFiles(1, 5, 5, 1000, 4200, 4200, 100, 100, false, true);
    createFiles(1, 5, 10, 1000, 5300, 5300, 100, 100, false, true);
    createFiles(1, 10, 7, 1000, 6400, 6400, 100, 100, false, true);
    createFiles(1, 10, 10, 1000, 7500, 7500, 100, 100, false, true);
    createFiles(1, 10, 10, 1500, 1500, 1500, 100, 100, false, false);
    createFiles(1, 10, 10, 1000, 3100, 3100, 100, 100, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(3, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());

    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(1));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(2), seqResources.get(4));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(1), unseqResources.get(1));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * 9 Seq files: 0 ~ 1000, 1100 ~ 2100, 2200 ~ 3200, 3300 ~ 4300, 4400 ~ 5400,......<br>
   * 1 Unseq files: 2150 ~ 5450<br>
   * Selected seq file index: 3, 4, 5, 6, 7<br>
   * Selected unseq file index: 1
   */
  @Test
  public void testWithNewDeviceAndSensorInUnseqFile50()
      throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(1, 10, 10, 1000, 0, 0, 100, 100, false, true);
    createFiles(1, 6, 6, 1000, 1100, 1100, 100, 100, false, true);
    createFiles(1, 5, 5, 1000, 2200, 2200, 100, 100, false, true);
    createFiles(1, 4, 4, 1000, 3300, 3300, 100, 100, false, true);
    createFiles(1, 3, 3, 1000, 4400, 4400, 100, 100, false, true);
    createFiles(1, 2, 2, 1000, 5500, 5500, 100, 100, false, true);
    createFiles(1, 10, 10, 1000, 6600, 6600, 100, 100, false, true);
    createFiles(1, 10, 10, 1000, 7700, 7700, 100, 100, false, true);
    createFiles(1, 10, 10, 1000, 8800, 8800, 100, 100, false, true);
    createFiles(1, 10, 10, 3300, 2150, 2150, 100, 100, false, false);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(5, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(1, selected.get(0).getUnseqFiles().size());
    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(3));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(2), seqResources.get(4));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(3), seqResources.get(5));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(4), seqResources.get(6));

    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * 9 Seq files: 0 ~ 1000, 1100 ~ 2100, 2200 ~ 3200, 3300 ~ 4300, 4400 ~ 5400,......<br>
   * 1 Unseq files: 2150 ~ 5450<br>
   * Selected seq file index: 3, 4, 5, 6, 7<br>
   * Selected unseq file index: 1
   */
  @Test
  public void testWithNewDeviceAndSensorInUnseqFile51()
      throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(1, 10, 10, 1000, 0, 0, 100, 100, false, true);
    createFiles(1, 6, 6, 1000, 1100, 1100, 100, 100, false, true);
    createFiles(1, 5, 5, 1000, 2200, 2200, 100, 100, false, true);
    createFiles(1, 4, 4, 1000, 3300, 3300, 100, 100, false, true);
    createFiles(1, 3, 3, 1000, 4400, 4400, 100, 100, false, true);
    createFiles(1, 2, 2, 1000, 5500, 5500, 100, 100, false, true);
    createFiles(1, 10, 5, 1000, 6600, 6600, 100, 100, false, true);
    createFiles(1, 10, 10, 1000, 7700, 7700, 100, 100, false, true);
    createFiles(1, 10, 10, 1000, 8800, 8800, 100, 100, false, true);
    createFiles(1, 10, 10, 3300, 2150, 2150, 100, 100, false, false);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(5, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(1, selected.get(0).getUnseqFiles().size());
    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(3));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(2), seqResources.get(4));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(3), seqResources.get(5));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(4), seqResources.get(6));

    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * 9 Seq files: 0 ~ 1000, 1100 ~ 2100, 2200 ~ 3200, 3300 ~ 4300, 4400 ~ 5400,......<br>
   * 1 Unseq files: 2150 ~ 5450<br>
   * Selected seq file index: 3, 4, 5, 6, 7, 8<br>
   * Selected unseq file index: 1
   */
  @Test
  public void testWithNewDeviceAndSensorInUnseqFile52()
      throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(1, 10, 10, 1000, 0, 0, 100, 100, false, true);
    createFiles(1, 6, 6, 1000, 1100, 1100, 100, 100, false, true);
    createFiles(1, 5, 5, 1000, 2200, 2200, 100, 100, false, true);
    createFiles(1, 4, 4, 1000, 3300, 3300, 100, 100, false, true);
    createFiles(1, 3, 3, 1000, 4400, 4400, 100, 100, false, true);
    createFiles(1, 2, 2, 1000, 5500, 5500, 100, 100, false, true);
    createFiles(1, 5, 10, 1000, 6600, 6600, 100, 100, false, true);
    createFiles(1, 10, 7, 1000, 7700, 7700, 100, 100, false, true);
    createFiles(1, 10, 10, 1000, 8800, 8800, 100, 100, false, true);
    createFiles(1, 10, 10, 3300, 2150, 2150, 100, 100, false, false);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(6, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(1, selected.get(0).getUnseqFiles().size());
    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(3));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(2), seqResources.get(4));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(3), seqResources.get(5));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(4), seqResources.get(6));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(5), seqResources.get(7));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * 9 Seq files: 0 ~ 1000, 1100 ~ 2100, 2200 ~ 3200, 3300 ~ 4300, 4400 ~ 5400,......<br>
   * 1 Unseq files: 2150 ~ 5450<br>
   * Selected seq file index: 3, 4, 5, 6, 8<br>
   * Selected unseq file index: 1
   */
  @Test
  public void testWithNewDeviceAndSensorInUnseqFile53()
      throws MetadataException, IOException, WriteProcessException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(1, 10, 10, 1000, 0, 0, 100, 100, false, true);
    createFiles(1, 6, 6, 1000, 1100, 1100, 100, 100, false, true);
    createFiles(1, 5, 5, 1000, 2200, 2200, 100, 100, false, true);
    createFiles(1, 4, 4, 1000, 3300, 3300, 100, 100, false, true);
    createFiles(1, 3, 3, 1000, 4400, 4400, 100, 100, false, true);
    createFiles(1, 5, 2, 1000, 5500, 5500, 100, 100, false, true);
    createFiles(1, 5, 10, 1000, 6600, 6600, 100, 100, false, true);
    createFiles(1, 10, 7, 1000, 7700, 7700, 100, 100, false, true);
    createFiles(1, 10, 10, 1000, 8800, 8800, 100, 100, false, true);
    createFiles(1, 10, 10, 3300, 2150, 2150, 100, 100, false, false);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(5, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(1, selected.get(0).getUnseqFiles().size());
    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(3));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(2), seqResources.get(4));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(3), seqResources.get(5));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(4), seqResources.get(7));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * 5 Seq files: 0 ~ 1000, 1100 ~ 2100, 2200 ~ 3200, 3300 ~ 4300, 4400 ~ 5400<br>
   * 1 Unseq files: 2500 ~ 3500<br>
   * Selected seq file index: 3, 4<br>
   * Selected unseq file index: 1
   */
  @Test
  public void testWithUnclosedSeqFile()
      throws MergeException, IOException, MetadataException, WriteProcessException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(5, 10, 10, 1000, 0, 0, 100, 100, false, true);
    createFiles(1, 10, 10, 1000, 2500, 2500, 100, 100, false, false);

    TsFileResource unclosedSeqResource = new TsFileResource(seqResources.get(4).getTsFile());
    unclosedSeqResource.setStatus(TsFileResourceStatus.UNCLOSED);
    TsFileResource lastSeqResource = seqResources.get(4);
    for (String deviceID : lastSeqResource.getDevices()) {
      unclosedSeqResource.updateStartTime(deviceID, lastSeqResource.getStartTime(deviceID));
    }
    seqResources.remove(4);
    seqResources.add(4, unclosedSeqResource);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(2, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(1, selected.get(0).getUnseqFiles().size());
    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(3));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * 5 Seq files: 0 ~ 1000, 1100 ~ 2100, 2200 ~ 3200, 3300 ~ 4300, 4400 ~ 5400<br>
   * 1 Unseq files: 2500 ~ 3500<br>
   * Selected seq file index: 3, 4<br>
   * Selected unseq file index: 1
   */
  @Test
  public void testWithUnclosedSeqFileAndNewSensorInUnseqFile()
      throws MergeException, IOException, MetadataException, WriteProcessException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(3, 10, 10, 1000, 0, 0, 100, 100, false, true);
    createFiles(1, 5, 5, 1000, 3300, 3300, 100, 100, false, true);
    createFiles(1, 10, 10, 1000, 4400, 4400, 100, 100, false, true);
    createFiles(1, 10, 10, 1000, 2500, 2500, 100, 100, false, false);

    TsFileResource unclosedSeqResource = new TsFileResource(seqResources.get(4).getTsFile());
    unclosedSeqResource.setStatus(TsFileResourceStatus.UNCLOSED);
    TsFileResource lastSeqResource = seqResources.get(4);
    for (String deviceID : lastSeqResource.getDevices()) {
      unclosedSeqResource.updateStartTime(deviceID, lastSeqResource.getStartTime(deviceID));
    }
    seqResources.remove(4);
    seqResources.add(4, unclosedSeqResource);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);
    // Assert.assertEquals(0, result.length);
    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * 5 Seq files: 0 ~ 1000, 1100 ~ 2100, 2200 ~ 3200, 3300 ~ 4300, 4400 ~ 5400<br>
   * 2 Unseq files: 2500 ~ 3500, 1500 ~ 4500<br>
   * Selected seq file index: 3, 4<br>
   * Selected unseq file index: 1
   */
  @Test
  public void testUnseqFileOverlapWithUnclosedSeqFile()
      throws MergeException, IOException, MetadataException, WriteProcessException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(5, 10, 10, 1000, 0, 0, 100, 100, false, true);
    createFiles(1, 10, 10, 1000, 2500, 2500, 100, 100, false, false);
    createFiles(1, 5, 5, 3000, 1500, 1500, 100, 100, false, false);

    TsFileResource unclosedSeqResource = new TsFileResource(seqResources.get(4).getTsFile());
    unclosedSeqResource.setStatus(TsFileResourceStatus.UNCLOSED);
    TsFileResource lastSeqResource = seqResources.get(4);
    for (String deviceID : lastSeqResource.getDevices()) {
      unclosedSeqResource.updateStartTime(deviceID, lastSeqResource.getStartTime(deviceID));
    }
    seqResources.remove(4);
    seqResources.add(4, unclosedSeqResource);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(2, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(1, selected.get(0).getUnseqFiles().size());
    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(3));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * 5 Seq files: 0 ~ 1000, 1100 ~ 2100, 2200 ~ 3200, 3300 ~ 4300, 4400 ~ 5400<br>
   * 2 Unseq files: 2500 ~ 3500, 4310 ~ 4360<br>
   * Selected seq file index: 3, 4<br>
   * Selected unseq file index: 1, 2
   */
  @Test
  public void testUnseqFileOverlapWithUnclosedSeqFile2()
      throws MergeException, IOException, MetadataException, WriteProcessException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(5, 10, 10, 1000, 0, 0, 100, 100, false, true);
    createFiles(1, 10, 10, 1000, 2500, 2500, 100, 100, false, false);
    createFiles(1, 5, 5, 50, 4310, 4310, 100, 100, false, false);

    TsFileResource unclosedSeqResource = new TsFileResource(seqResources.get(4).getTsFile());
    unclosedSeqResource.setStatus(TsFileResourceStatus.UNCLOSED);
    TsFileResource lastSeqResource = seqResources.get(4);
    for (String deviceID : lastSeqResource.getDevices()) {
      unclosedSeqResource.updateStartTime(deviceID, lastSeqResource.getStartTime(deviceID));
    }
    seqResources.remove(4);
    seqResources.add(4, unclosedSeqResource);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(2, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());
    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(3));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(1), unseqResources.get(1));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * 5 Seq files: 0 ~ 1000, 1100 ~ 2100, 2200 ~ 3200, 3300 ~ 4300, 4400 ~ 5400<br>
   * 2 Unseq files: 2500 ~ 3500, 1500 ~ 4500<br>
   * Selected seq file index: 3, 4<br>
   * Selected unseq file index: 1
   */
  @Test
  public void testWithUnclosedUnSeqFile()
      throws MergeException, IOException, MetadataException, WriteProcessException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(5, 10, 10, 1000, 0, 0, 100, 100, false, true);
    createFiles(1, 10, 10, 1000, 2500, 2500, 100, 100, false, false);
    createFiles(1, 5, 5, 3000, 1500, 1500, 100, 100, false, false);

    TsFileResource unclosedUnSeqResource = new TsFileResource(unseqResources.get(1).getTsFile());
    unclosedUnSeqResource.setStatus(TsFileResourceStatus.UNCLOSED);
    TsFileResource lastUnSeqResource = unseqResources.get(1);
    for (String deviceID : lastUnSeqResource.getDevices()) {
      unclosedUnSeqResource.updateStartTime(deviceID, lastUnSeqResource.getStartTime(deviceID));
      unclosedUnSeqResource.updateEndTime(deviceID, lastUnSeqResource.getEndTime(deviceID));
    }
    unseqResources.remove(1);
    unseqResources.add(1, unclosedUnSeqResource);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(2, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(1, selected.get(0).getUnseqFiles().size());
    Assert.assertEquals(selected.get(0).getSeqFiles().get(0), seqResources.get(2));
    Assert.assertEquals(selected.get(0).getSeqFiles().get(1), seqResources.get(3));
    Assert.assertEquals(selected.get(0).getUnseqFiles().get(0), unseqResources.get(0));

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
            new AtomicInteger(0),
            0,
            tsFileManager.getNextCompactionTaskId())
        .doCompaction();

    validateSeqFiles(true);
  }

  /**
   * Cross space compaction select 1, 2, 3, 4, 5 seq file, but file 3 and 4 are being compacted and
   * being deleted by other inner compaction task. Cross space compaction selector should abort this
   * task.
   */
  @Test
  public void testSelectingFilesWhenSomeFilesBeingDeleted()
      throws MetadataException, IOException, WriteProcessException, StorageEngineException,
          InterruptedException, MergeException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(5, 10, 5, 1000, 0, 0, 100, 100, false, true);
    createFiles(1, 5, 10, 4500, 500, 500, 0, 100, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // seq file 3 and 4 are being compacted by inner space compaction
    List<TsFileResource> sourceFiles = new ArrayList<>();
    sourceFiles.add(seqResources.get(2));
    sourceFiles.add(seqResources.get(3));
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(sourceFiles, true);
    ReadChunkCompactionPerformer performer = new ReadChunkCompactionPerformer(sourceFiles);
    performer.setTargetFiles(targetResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();

    CompactionUtils.moveTargetFile(targetResources, true, COMPACTION_TEST_SG + "-" + "0");
    CompactionUtils.combineModsInInnerCompaction(sourceFiles, targetResources.get(0));
    tsFileManager.replace(sourceFiles, Collections.emptyList(), targetResources, 0, true);
    CompactionUtils.deleteTsFilesInDisk(sourceFiles, COMPACTION_TEST_SG + "-" + "0");

    // start selecting files and then start a cross space compaction task
    ICrossSpaceSelector selector =
        IoTDBDescriptor.getInstance()
            .getConfig()
            .getCrossCompactionSelector()
            .createInstance(COMPACTION_TEST_SG, "0", 0, tsFileManager);
    // In the process of getting the file list and starting to select files, the file list is
    // updated (the file is deleted or the status is updated)
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(0, selected.size());
  }
}
