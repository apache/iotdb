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
import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICrossCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleContext;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.ICompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.ICrossSpaceSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.RewriteCrossSpaceCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.SizeTieredCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.CrossCompactionTaskResource;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.CrossSpaceCompactionCandidate;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.tools.validate.TsFileValidationTool;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.apache.tsfile.utils.TsFileGeneratorUtils.alignDeviceOffset;

public class CrossSpaceCompactionWithFastPerformerValidationTest extends AbstractCompactionTest {
  TsFileManager tsFileManager =
      new TsFileManager(COMPACTION_TEST_SG, "0", STORAGE_GROUP_DIR.getPath());

  private final String oldThreadName = Thread.currentThread().getName();

  private ICrossCompactionPerformer performer = new FastCompactionPerformer(true);
  private long compactionMemory = SystemInfo.getInstance().getMemorySizeForCompaction();

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    IoTDBDescriptor.getInstance().getConfig().setMinCrossCompactionUnseqFileLevel(0);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(1024);
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    SystemInfo.getInstance().setMemorySizeForCompaction(100 * 1024 * 1024);
    Thread.currentThread().setName("pool-1-IoTDB-Compaction-Worker-1");
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    Thread.currentThread().setName(oldThreadName);
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    SystemInfo.getInstance().setMemorySizeForCompaction(compactionMemory);
    TsFileValidationTool.setBadFileNum(0);
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
    seqResources.get(4).setStatusForTest(TsFileResourceStatus.UNCLOSED);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
    IoTDBDescriptor.getInstance().getConfig().setFileLimitPerCrossTask(7);
    registerTimeseriesInMManger(5, 10, true);
    createFiles(7, 5, 10, 1000, 0, 0, 100, 100, true, true);
    createFiles(1, 5, 10, 3300, 2150, 2150, 100, 100, true, false);
    createFiles(1, 5, 10, 4400, 1150, 1150, 100, 100, true, false);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
    unclosedSeqResource.setStatusForTest(TsFileResourceStatus.UNCLOSED);
    TsFileResource lastSeqResource = seqResources.get(4);
    for (IDeviceID deviceID : lastSeqResource.getDevices()) {
      unclosedSeqResource.updateStartTime(deviceID, lastSeqResource.getStartTime(deviceID));
    }
    seqResources.remove(4);
    seqResources.add(4, unclosedSeqResource);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
    unclosedSeqResource.setStatusForTest(TsFileResourceStatus.UNCLOSED);
    TsFileResource lastSeqResource = seqResources.get(4);
    for (IDeviceID deviceID : lastSeqResource.getDevices()) {
      unclosedSeqResource.updateStartTime(deviceID, lastSeqResource.getStartTime(deviceID));
    }
    seqResources.remove(4);
    seqResources.add(4, unclosedSeqResource);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);
    // Assert.assertEquals(0, result.length);
    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            selected.get(0).getSeqFiles(),
            selected.get(0).getUnseqFiles(),
            performer,
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
    unclosedSeqResource.setStatusForTest(TsFileResourceStatus.UNCLOSED);
    TsFileResource lastSeqResource = seqResources.get(4);
    for (IDeviceID deviceID : lastSeqResource.getDevices()) {
      unclosedSeqResource.updateStartTime(deviceID, lastSeqResource.getStartTime(deviceID));
    }
    seqResources.remove(4);
    seqResources.add(4, unclosedSeqResource);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
    unclosedSeqResource.setStatusForTest(TsFileResourceStatus.UNCLOSED);
    TsFileResource lastSeqResource = seqResources.get(4);
    for (IDeviceID deviceID : lastSeqResource.getDevices()) {
      unclosedSeqResource.updateStartTime(deviceID, lastSeqResource.getStartTime(deviceID));
    }
    seqResources.remove(4);
    seqResources.add(4, unclosedSeqResource);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
    unclosedUnSeqResource.setStatusForTest(TsFileResourceStatus.UNCLOSED);
    TsFileResource lastUnSeqResource = unseqResources.get(1);
    for (IDeviceID deviceID : lastUnSeqResource.getDevices()) {
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
  public void testSelectingFilesWhenSomeFilesBeingDeleted() throws Exception {
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
    FastCompactionPerformer performer = new FastCompactionPerformer(false);
    performer.setSourceFiles(sourceFiles);
    performer.setTargetFiles(targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();

    CompactionUtils.moveTargetFile(
        targetResources, CompactionTaskType.INNER_SEQ, COMPACTION_TEST_SG + "-" + "0");
    CompactionUtils.combineModsInInnerCompaction(sourceFiles, targetResources.get(0));
    tsFileManager.replace(sourceFiles, Collections.emptyList(), targetResources, 0);
    CompactionUtils.deleteTsFilesInDisk(sourceFiles, COMPACTION_TEST_SG + "-" + "0");
    targetResources.forEach(x -> x.setStatusForTest(TsFileResourceStatus.NORMAL));

    // start selecting files and then start a cross space compaction task
    ICrossSpaceSelector selector =
        IoTDBDescriptor.getInstance()
            .getConfig()
            .getCrossCompactionSelector()
            .createInstance(
                COMPACTION_TEST_SG, "0", 0, tsFileManager, new CompactionScheduleContext());
    // In the process of getting the file list and starting to select files, the file list is
    // updated (the file is deleted or the status is updated)
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);

    Assert.assertEquals(0, selected.size());
  }

  /**
   * Validate seq files with file time index and device time index under this time partition after
   * compaction.
   */
  @Test
  public void testTsFileValidationWithFileTimeIndex()
      throws MetadataException, IOException, WriteProcessException {
    registerTimeseriesInMManger(5, 10, true);
    createFiles(10, 10, 5, 1000, 0, 0, 100, 100, false, true);
    createFiles(1, 5, 10, 4500, 500, 500, 0, 100, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // set the end time of d1 in the first seq file to 1100
    tsFileManager
        .getTsFileList(true)
        .get(0)
        .updateEndTime(
            IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"),
            1100L);

    // set the end time of d1 in the second seq file to 1200
    tsFileManager
        .getTsFileList(true)
        .get(1)
        .updateStartTime(
            IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"),
            1200L);

    for (int i = 1; i < seqResources.size(); i++) {
      tsFileManager.getTsFileList(true).get(i).degradeTimeIndex();
    }

    // meet overlap files
    Assert.assertFalse(
        TsFileResourceUtils.validateTsFileResourcesHasNoOverlap(
            tsFileManager.getOrCreateSequenceListByTimePartition(0).getArrayList()));

    tsFileManager.getTsFileList(true).get(0).deserialize();
    tsFileManager.getTsFileList(true).get(1).deserialize();
    tsFileManager.getTsFileList(true).get(0).degradeTimeIndex();
    tsFileManager.getTsFileList(true).get(1).degradeTimeIndex();
    Assert.assertTrue(
        TsFileResourceUtils.validateTsFileResourcesHasNoOverlap(
            tsFileManager.getOrCreateSequenceListByTimePartition(0).getArrayList()));

    // seq file 4,5 and 6 are being compacted by inner space compaction
    List<TsFileResource> sourceFiles = new ArrayList<>();
    sourceFiles.add(seqResources.get(4));
    sourceFiles.add(seqResources.get(5));
    sourceFiles.add(seqResources.get(6));
    FastCompactionPerformer performer = new FastCompactionPerformer(false);
    performer.setSourceFiles(sourceFiles);
    InnerSpaceCompactionTask innerSpaceCompactionTask =
        new InnerSpaceCompactionTask(0, tsFileManager, sourceFiles, true, performer, 0);
    Assert.assertTrue(
        TsFileResourceUtils.validateTsFileResourcesHasNoOverlap(
            tsFileManager.getOrCreateSequenceListByTimePartition(0).getArrayList()));
    innerSpaceCompactionTask.start();
    validateSeqFiles(true);
  }

  /**
   * Target files of first cross compaction task should not be selected to participate in other
   * tasks util the first task is finished.<br>
   * Seq Files index : 1 ~ 10<br>
   * Unseq Files index : 1 ~ 2<br>
   * Unseq file 1 overlaps with seq file 4,5 and unseq file 2 overlaps with seq file 5,6
   */
  @Test
  public void testCompactionSchedule() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setFileLimitPerCrossTask(1);
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(2);
    createFiles(10, 10, 5, 1000, 0, 0, 100, 100, false, true);
    createFiles(1, 5, 10, 1000, 4000, 4000, 0, 100, false, false);
    createFiles(1, 5, 10, 1000, 5000, 5000, 0, 100, false, false);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // first cross compaction task
    ICrossSpaceSelector crossSpaceCompactionSelector =
        IoTDBDescriptor.getInstance()
            .getConfig()
            .getCrossCompactionSelector()
            .createInstance(
                COMPACTION_TEST_SG, "0", 0, tsFileManager, new CompactionScheduleContext());
    CrossCompactionTaskResource sourceFiles =
        crossSpaceCompactionSelector
            .selectCrossSpaceTask(
                tsFileManager.getOrCreateSequenceListByTimePartition(0),
                tsFileManager.getOrCreateUnsequenceListByTimePartition(0))
            .get(0);
    Assert.assertEquals(2, sourceFiles.getSeqFiles().size());
    Assert.assertEquals(1, sourceFiles.getUnseqFiles().size());
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(
            sourceFiles.getSeqFiles());
    performer.setSourceFiles(sourceFiles.getSeqFiles(), sourceFiles.getUnseqFiles());
    performer.setTargetFiles(targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();

    CompactionUtils.moveTargetFile(
        targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG + "-" + "0");
    CompactionUtils.combineModsInCrossCompaction(
        sourceFiles.getSeqFiles(), sourceFiles.getUnseqFiles(), targetResources);
    tsFileManager.replace(
        sourceFiles.getSeqFiles(), sourceFiles.getUnseqFiles(), targetResources, 0);

    // Suppose the read lock of the source file is occupied by other threads, causing the first task
    // to get stuck.
    // Target file of the first task should not be selected to participate in other cross compaction
    // tasks.
    Assert.assertEquals(
        0,
        crossSpaceCompactionSelector
            .selectCrossSpaceTask(
                tsFileManager.getOrCreateSequenceListByTimePartition(0),
                tsFileManager.getOrCreateUnsequenceListByTimePartition(0))
            .size());

    // Target file of the first task should not be selected to participate in other inner compaction
    // tasks.
    ICompactionSelector innerSelector =
        IoTDBDescriptor.getInstance()
            .getConfig()
            .getInnerSequenceCompactionSelector()
            .createInstance(
                COMPACTION_TEST_SG, "0", 0, tsFileManager, new CompactionScheduleContext());
    Assert.assertEquals(0, innerSelector.selectInnerSpaceTask(targetResources).size());

    // first compaction task finishes successfully
    targetResources.forEach(x -> x.setStatusForTest(TsFileResourceStatus.NORMAL));

    // target file of first compaction task can be selected to participate in another cross
    // compaction task
    List<CrossCompactionTaskResource> pairs =
        crossSpaceCompactionSelector.selectCrossSpaceTask(
            tsFileManager.getOrCreateSequenceListByTimePartition(0),
            tsFileManager.getOrCreateUnsequenceListByTimePartition(0));
    Assert.assertEquals(1, pairs.size());
    Assert.assertEquals(2, pairs.get(0).getSeqFiles().size());
    Assert.assertEquals(1, pairs.get(0).getUnseqFiles().size());
    Assert.assertEquals(
        tsFileManager.getTsFileList(true).get(4), pairs.get(0).getSeqFiles().get(0));
    Assert.assertEquals(
        tsFileManager.getTsFileList(true).get(5), pairs.get(0).getSeqFiles().get(1));
    Assert.assertEquals(
        tsFileManager.getTsFileList(false).get(0), pairs.get(0).getUnseqFiles().get(0));

    // target file of first compaction task can be selected to participate in another inner
    // compaction task
    List<InnerSpaceCompactionTask> innerPairs = innerSelector.selectInnerSpaceTask(targetResources);
    Assert.assertEquals(1, innerPairs.size());
  }

  @Test
  public void testNonAlignedUnseqFilesNotOverlapWithSeqFiles1() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(2);
    createFiles(5, 10, 5, 1000, 0, 0, 100, 100, false, true);
    createFiles(2, 5, 10, 500, 6000, 6000, 0, 100, false, false);
    createFiles(3, 10, 5, 1000, 7500, 7500, 100, 100, false, true);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // delete d0 ~ d5 in seq files
    Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
    for (int d = 0; d < 5; d++) {
      for (int m = 0; m < 5; m++) {
        deleteMap.put(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + d + PATH_SEPARATOR + "s" + m,
            new Pair<>(Long.MIN_VALUE, Long.MAX_VALUE));
      }
    }
    for (TsFileResource resource : seqResources) {
      CompactionFileGeneratorUtils.generateMods(deleteMap, resource, false);
    }

    List<IFullPath> timeseriesPaths = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        timeseriesPaths.add(
            new NonAlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                new MeasurementSchema("s" + j, TSDataType.INT64)));
      }
    }
    Map<IFullPath, List<TimeValuePair>> sourceData =
        readSourceFiles(timeseriesPaths, Collections.emptyList());

    // inner seq space compact
    List<InnerSpaceCompactionTask> taskResources =
        new SizeTieredCompactionSelector(
                COMPACTION_TEST_SG, "0", 0, true, tsFileManager, new CompactionScheduleContext())
            .selectInnerSpaceTask(tsFileManager.getOrCreateSequenceListByTimePartition(0));
    for (InnerSpaceCompactionTask task : taskResources) {
      Assert.assertTrue(task.start());
    }

    // select cross compaction
    ICrossSpaceSelector crossSpaceCompactionSelector =
        IoTDBDescriptor.getInstance()
            .getConfig()
            .getCrossCompactionSelector()
            .createInstance(
                COMPACTION_TEST_SG, "0", 0, tsFileManager, new CompactionScheduleContext());
    CrossCompactionTaskResource sourceFiles =
        crossSpaceCompactionSelector
            .selectCrossSpaceTask(
                tsFileManager.getOrCreateSequenceListByTimePartition(0),
                tsFileManager.getOrCreateUnsequenceListByTimePartition(0))
            .get(0);
    Assert.assertEquals(1, sourceFiles.getSeqFiles().size());
    Assert.assertEquals(2, sourceFiles.getUnseqFiles().size());

    Assert.assertTrue(
        new CrossSpaceCompactionTask(
                0,
                tsFileManager,
                sourceFiles.getSeqFiles(),
                sourceFiles.getUnseqFiles(),
                new FastCompactionPerformer(true),
                sourceFiles.getTotalMemoryCost(),
                0)
            .start());

    validateSeqFiles(true);
    validateTargetDatas(sourceData, Collections.emptyList());
  }

  @Test
  public void testNonAlignedUnseqFilesNotOverlapWithSeqFiles2() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(2);
    createFiles(5, 10, 5, 1000, 0, 0, 100, 100, false, true);
    createFiles(2, 5, 10, 500, 6000, 6000, 0, 100, false, false);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // delete d0 ~ d5 in seq files
    Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
    for (int d = 0; d < 5; d++) {
      for (int m = 0; m < 5; m++) {
        deleteMap.put(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + d + PATH_SEPARATOR + "s" + m,
            new Pair<>(Long.MIN_VALUE, Long.MAX_VALUE));
      }
    }
    for (TsFileResource resource : seqResources) {
      CompactionFileGeneratorUtils.generateMods(deleteMap, resource, false);
    }

    List<IFullPath> timeseriesPaths = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        timeseriesPaths.add(
            new NonAlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                new MeasurementSchema("s" + j, TSDataType.INT64)));
      }
    }
    Map<IFullPath, List<TimeValuePair>> sourceData =
        readSourceFiles(timeseriesPaths, Collections.emptyList());

    // inner seq space compact
    List<InnerSpaceCompactionTask> taskResources =
        new SizeTieredCompactionSelector(
                COMPACTION_TEST_SG, "0", 0, true, tsFileManager, new CompactionScheduleContext())
            .selectInnerSpaceTask(tsFileManager.getOrCreateSequenceListByTimePartition(0));
    for (InnerSpaceCompactionTask task : taskResources) {
      Assert.assertTrue(task.start());
    }

    // select cross compaction
    ICrossSpaceSelector crossSpaceCompactionSelector =
        IoTDBDescriptor.getInstance()
            .getConfig()
            .getCrossCompactionSelector()
            .createInstance(
                COMPACTION_TEST_SG, "0", 0, tsFileManager, new CompactionScheduleContext());
    CrossCompactionTaskResource sourceFiles =
        crossSpaceCompactionSelector
            .selectCrossSpaceTask(
                tsFileManager.getOrCreateSequenceListByTimePartition(0),
                tsFileManager.getOrCreateUnsequenceListByTimePartition(0))
            .get(0);
    Assert.assertEquals(1, sourceFiles.getSeqFiles().size());
    Assert.assertEquals(2, sourceFiles.getUnseqFiles().size());

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            sourceFiles.getSeqFiles(),
            sourceFiles.getUnseqFiles(),
            new FastCompactionPerformer(true),
            sourceFiles.getTotalMemoryCost(),
            0)
        .start();

    validateSeqFiles(true);
    validateTargetDatas(sourceData, Collections.emptyList());
  }

  @Test
  public void testNonAlignedUnseqFilesNotOverlapWithSeqFiles3() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(2);
    createFiles(4, 10, 5, 1000, 0, 0, 100, 100, false, true);
    createFiles(2, 5, 10, 500, 6000, 6000, 0, 100, false, false);
    createFiles(1, 10, 5, 1000, 7500, 7500, 100, 100, false, true);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // delete d0 ~ d5 in seq files
    Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
    for (int d = 0; d < 5; d++) {
      for (int m = 0; m < 5; m++) {
        deleteMap.put(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + d + PATH_SEPARATOR + "s" + m,
            new Pair<>(Long.MIN_VALUE, Long.MAX_VALUE));
      }
    }
    for (TsFileResource resource : seqResources) {
      CompactionFileGeneratorUtils.generateMods(deleteMap, resource, false);
    }

    List<IFullPath> timeseriesPaths = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        timeseriesPaths.add(
            new NonAlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                new MeasurementSchema("s" + j, TSDataType.INT64)));
      }
    }
    Map<IFullPath, List<TimeValuePair>> sourceData =
        readSourceFiles(timeseriesPaths, Collections.emptyList());

    // inner seq space compact
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        new SizeTieredCompactionSelector(
                COMPACTION_TEST_SG, "0", 0, true, tsFileManager, new CompactionScheduleContext())
            .selectInnerSpaceTask(tsFileManager.getOrCreateSequenceListByTimePartition(0));
    for (InnerSpaceCompactionTask task : innerSpaceCompactionTasks) {
      Assert.assertTrue(task.start());
    }

    // select cross compaction
    ICrossSpaceSelector crossSpaceCompactionSelector =
        IoTDBDescriptor.getInstance()
            .getConfig()
            .getCrossCompactionSelector()
            .createInstance(
                COMPACTION_TEST_SG, "0", 0, tsFileManager, new CompactionScheduleContext());
    CrossCompactionTaskResource sourceFiles =
        crossSpaceCompactionSelector
            .selectCrossSpaceTask(
                tsFileManager.getOrCreateSequenceListByTimePartition(0),
                tsFileManager.getOrCreateUnsequenceListByTimePartition(0))
            .get(0);
    Assert.assertEquals(1, sourceFiles.getSeqFiles().size());
    Assert.assertEquals(2, sourceFiles.getUnseqFiles().size());

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            sourceFiles.getSeqFiles(),
            sourceFiles.getUnseqFiles(),
            new FastCompactionPerformer(true),
            sourceFiles.getTotalMemoryCost(),
            0)
        .start();

    validateSeqFiles(true);
    validateTargetDatas(sourceData, Collections.emptyList());
  }

  @Test
  public void testNonAlignedUnseqFilesNotOverlapWithSeqFiles4() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(2);
    createFiles(5, 10, 5, 1000, 0, 0, 100, 100, false, true);
    createFiles(1, 9, 10, 500, 100, 100, 0, 100, false, false);
    createFiles(2, 5, 10, 500, 6000, 6000, 0, 100, false, false);
    createFiles(3, 10, 5, 1000, 7500, 7500, 100, 100, false, true);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // delete d0 ~ d5 in seq files
    Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
    for (int d = 0; d < 5; d++) {
      for (int m = 0; m < 5; m++) {
        deleteMap.put(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + d + PATH_SEPARATOR + "s" + m,
            new Pair<>(Long.MIN_VALUE, Long.MAX_VALUE));
      }
    }
    for (TsFileResource resource : seqResources) {
      CompactionFileGeneratorUtils.generateMods(deleteMap, resource, false);
    }

    List<IFullPath> timeseriesPaths = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        timeseriesPaths.add(
            new NonAlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                new MeasurementSchema("s" + j, TSDataType.INT64)));
      }
    }
    Map<IFullPath, List<TimeValuePair>> sourceData =
        readSourceFiles(timeseriesPaths, Collections.emptyList());

    // inner seq space compact
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        new SizeTieredCompactionSelector(
                COMPACTION_TEST_SG, "0", 0, true, tsFileManager, new CompactionScheduleContext())
            .selectInnerSpaceTask(tsFileManager.getOrCreateSequenceListByTimePartition(0));
    for (InnerSpaceCompactionTask task : innerSpaceCompactionTasks) {
      Assert.assertTrue(task.start());
    }

    // select cross compaction
    ICrossSpaceSelector crossSpaceCompactionSelector =
        IoTDBDescriptor.getInstance()
            .getConfig()
            .getCrossCompactionSelector()
            .createInstance(
                COMPACTION_TEST_SG, "0", 0, tsFileManager, new CompactionScheduleContext());
    CrossCompactionTaskResource sourceFiles =
        crossSpaceCompactionSelector
            .selectCrossSpaceTask(
                tsFileManager.getOrCreateSequenceListByTimePartition(0),
                tsFileManager.getOrCreateUnsequenceListByTimePartition(0))
            .get(0);
    Assert.assertEquals(2, sourceFiles.getSeqFiles().size());
    Assert.assertEquals(3, sourceFiles.getUnseqFiles().size());

    Assert.assertTrue(
        new CrossSpaceCompactionTask(
                0,
                tsFileManager,
                sourceFiles.getSeqFiles(),
                sourceFiles.getUnseqFiles(),
                new FastCompactionPerformer(true),
                sourceFiles.getTotalMemoryCost(),
                0)
            .start());

    validateSeqFiles(true);
    validateTargetDatas(sourceData, Collections.emptyList());
  }

  @Test
  public void testAlignedUnseqFilesNotOverlapWithSeqFiles1() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(2);
    createFiles(5, 10, 5, 1000, 0, 0, 100, 100, true, true);
    createFiles(2, 5, 10, 500, 6000, 6000, 0, 100, true, false);
    createFiles(3, 10, 5, 1000, 7500, 7500, 100, 100, true, true);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // delete d0 ~ d5 in seq files
    Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
    for (int d = 0; d < 5; d++) {
      for (int m = 0; m < 5; m++) {
        deleteMap.put(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (alignDeviceOffset + d)
                + PATH_SEPARATOR
                + "s"
                + m,
            new Pair<>(Long.MIN_VALUE, Long.MAX_VALUE));
      }
    }
    for (TsFileResource resource : seqResources) {
      CompactionFileGeneratorUtils.generateMods(deleteMap, resource, false);
    }

    List<IFullPath> timeseriesPaths = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        timeseriesPaths.add(
            new AlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                Collections.singletonList("s" + j),
                Collections.singletonList(new MeasurementSchema("s" + j, TSDataType.INT64))));
      }
    }
    Map<IFullPath, List<TimeValuePair>> sourceData =
        readSourceFiles(timeseriesPaths, Collections.emptyList());

    // inner seq space compact
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        new SizeTieredCompactionSelector(
                COMPACTION_TEST_SG, "0", 0, true, tsFileManager, new CompactionScheduleContext())
            .selectInnerSpaceTask(tsFileManager.getOrCreateSequenceListByTimePartition(0));
    for (InnerSpaceCompactionTask task : innerSpaceCompactionTasks) {
      Assert.assertTrue(task.start());
    }

    // select cross compaction
    ICrossSpaceSelector crossSpaceCompactionSelector =
        IoTDBDescriptor.getInstance()
            .getConfig()
            .getCrossCompactionSelector()
            .createInstance(
                COMPACTION_TEST_SG, "0", 0, tsFileManager, new CompactionScheduleContext());
    CrossCompactionTaskResource sourceFiles =
        crossSpaceCompactionSelector
            .selectCrossSpaceTask(
                tsFileManager.getOrCreateSequenceListByTimePartition(0),
                tsFileManager.getOrCreateUnsequenceListByTimePartition(0))
            .get(0);
    Assert.assertEquals(1, sourceFiles.getSeqFiles().size());
    Assert.assertEquals(2, sourceFiles.getUnseqFiles().size());

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            sourceFiles.getSeqFiles(),
            sourceFiles.getUnseqFiles(),
            new FastCompactionPerformer(true),
            sourceFiles.getTotalMemoryCost(),
            0)
        .start();

    validateSeqFiles(true);
    validateTargetDatas(sourceData, Collections.emptyList());
  }

  @Test
  public void testAlignedUnseqFilesNotOverlapWithSeqFiles2() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(2);
    createFiles(5, 10, 5, 1000, 0, 0, 100, 100, true, true);
    createFiles(2, 5, 10, 500, 6000, 6000, 0, 100, true, false);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // delete d0 ~ d5 in seq files
    Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
    for (int d = 0; d < 5; d++) {
      for (int m = 0; m < 5; m++) {
        deleteMap.put(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (alignDeviceOffset + d)
                + PATH_SEPARATOR
                + "s"
                + m,
            new Pair<>(Long.MIN_VALUE, Long.MAX_VALUE));
      }
    }
    for (TsFileResource resource : seqResources) {
      CompactionFileGeneratorUtils.generateMods(deleteMap, resource, false);
    }

    List<IFullPath> timeseriesPaths = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        timeseriesPaths.add(
            new AlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                Collections.singletonList("s" + j),
                Collections.singletonList(new MeasurementSchema("s" + j, TSDataType.INT64))));
      }
    }
    Map<IFullPath, List<TimeValuePair>> sourceData =
        readSourceFiles(timeseriesPaths, Collections.emptyList());

    // inner seq space compact
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        new SizeTieredCompactionSelector(
                COMPACTION_TEST_SG, "0", 0, true, tsFileManager, new CompactionScheduleContext())
            .selectInnerSpaceTask(tsFileManager.getOrCreateSequenceListByTimePartition(0));
    for (InnerSpaceCompactionTask task : innerSpaceCompactionTasks) {
      Assert.assertTrue(task.start());
    }

    // select cross compaction
    ICrossSpaceSelector crossSpaceCompactionSelector =
        IoTDBDescriptor.getInstance()
            .getConfig()
            .getCrossCompactionSelector()
            .createInstance(
                COMPACTION_TEST_SG, "0", 0, tsFileManager, new CompactionScheduleContext());
    CrossCompactionTaskResource sourceFiles =
        crossSpaceCompactionSelector
            .selectCrossSpaceTask(
                tsFileManager.getOrCreateSequenceListByTimePartition(0),
                tsFileManager.getOrCreateUnsequenceListByTimePartition(0))
            .get(0);
    Assert.assertEquals(1, sourceFiles.getSeqFiles().size());
    Assert.assertEquals(2, sourceFiles.getUnseqFiles().size());

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            sourceFiles.getSeqFiles(),
            sourceFiles.getUnseqFiles(),
            new FastCompactionPerformer(true),
            sourceFiles.getTotalMemoryCost(),
            0)
        .start();

    validateSeqFiles(true);
    validateTargetDatas(sourceData, Collections.emptyList());
  }

  @Test
  public void testAlignedUnseqFilesNotOverlapWithSeqFiles3() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(2);
    createFiles(4, 10, 5, 1000, 0, 0, 100, 100, true, true);
    createFiles(2, 5, 10, 500, 6000, 6000, 0, 100, true, false);
    createFiles(1, 10, 5, 1000, 7500, 7500, 100, 100, true, true);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // delete d0 ~ d5 in seq files
    Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
    for (int d = 0; d < 5; d++) {
      for (int m = 0; m < 5; m++) {
        deleteMap.put(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (alignDeviceOffset + d)
                + PATH_SEPARATOR
                + "s"
                + m,
            new Pair<>(Long.MIN_VALUE, Long.MAX_VALUE));
      }
    }
    for (TsFileResource resource : seqResources) {
      CompactionFileGeneratorUtils.generateMods(deleteMap, resource, false);
    }

    List<IFullPath> timeseriesPaths = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        timeseriesPaths.add(
            new AlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                Collections.singletonList("s" + j),
                Collections.singletonList(new MeasurementSchema("s" + j, TSDataType.INT64))));
      }
    }
    Map<IFullPath, List<TimeValuePair>> sourceData =
        readSourceFiles(timeseriesPaths, Collections.emptyList());

    // inner seq space compact
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        new SizeTieredCompactionSelector(
                COMPACTION_TEST_SG, "0", 0, true, tsFileManager, new CompactionScheduleContext())
            .selectInnerSpaceTask(tsFileManager.getOrCreateSequenceListByTimePartition(0));
    for (InnerSpaceCompactionTask task : innerSpaceCompactionTasks) {
      Assert.assertTrue(task.start());
    }

    // select cross compaction
    ICrossSpaceSelector crossSpaceCompactionSelector =
        IoTDBDescriptor.getInstance()
            .getConfig()
            .getCrossCompactionSelector()
            .createInstance(
                COMPACTION_TEST_SG, "0", 0, tsFileManager, new CompactionScheduleContext());
    CrossCompactionTaskResource sourceFiles =
        crossSpaceCompactionSelector
            .selectCrossSpaceTask(
                tsFileManager.getOrCreateSequenceListByTimePartition(0),
                tsFileManager.getOrCreateUnsequenceListByTimePartition(0))
            .get(0);
    Assert.assertEquals(1, sourceFiles.getSeqFiles().size());
    Assert.assertEquals(2, sourceFiles.getUnseqFiles().size());

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            sourceFiles.getSeqFiles(),
            sourceFiles.getUnseqFiles(),
            new FastCompactionPerformer(true),
            sourceFiles.getTotalMemoryCost(),
            0)
        .start();

    validateSeqFiles(true);
    validateTargetDatas(sourceData, Collections.emptyList());
  }

  @Test
  public void testAlignedUnseqFilesNotOverlapWithSeqFiles4() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(2);
    createFiles(5, 10, 5, 1000, 0, 0, 100, 100, true, true);
    createFiles(1, 9, 10, 500, 100, 100, 0, 100, true, false);
    createFiles(2, 5, 10, 500, 6000, 6000, 0, 100, true, false);
    createFiles(3, 10, 5, 1000, 7500, 7500, 100, 100, true, true);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // delete d0 ~ d5 in seq files
    Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
    for (int d = 0; d < 5; d++) {
      for (int m = 0; m < 5; m++) {
        deleteMap.put(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (alignDeviceOffset + d)
                + PATH_SEPARATOR
                + "s"
                + m,
            new Pair<>(Long.MIN_VALUE, Long.MAX_VALUE));
      }
    }
    for (TsFileResource resource : seqResources) {
      CompactionFileGeneratorUtils.generateMods(deleteMap, resource, false);
    }

    List<IFullPath> timeseriesPaths = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        timeseriesPaths.add(
            new AlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                Collections.singletonList("s" + j),
                Collections.singletonList(new MeasurementSchema("s" + j, TSDataType.INT64))));
      }
    }
    Map<IFullPath, List<TimeValuePair>> sourceData =
        readSourceFiles(timeseriesPaths, Collections.emptyList());

    // inner seq space compact
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        new SizeTieredCompactionSelector(
                COMPACTION_TEST_SG, "0", 0, true, tsFileManager, new CompactionScheduleContext())
            .selectInnerSpaceTask(tsFileManager.getOrCreateSequenceListByTimePartition(0));
    for (InnerSpaceCompactionTask task : innerSpaceCompactionTasks) {
      Assert.assertTrue(task.start());
    }

    // select cross compaction
    ICrossSpaceSelector crossSpaceCompactionSelector =
        IoTDBDescriptor.getInstance()
            .getConfig()
            .getCrossCompactionSelector()
            .createInstance(
                COMPACTION_TEST_SG, "0", 0, tsFileManager, new CompactionScheduleContext());
    CrossCompactionTaskResource sourceFiles =
        crossSpaceCompactionSelector
            .selectCrossSpaceTask(
                tsFileManager.getOrCreateSequenceListByTimePartition(0),
                tsFileManager.getOrCreateUnsequenceListByTimePartition(0))
            .get(0);
    Assert.assertEquals(2, sourceFiles.getSeqFiles().size());
    Assert.assertEquals(3, sourceFiles.getUnseqFiles().size());

    new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            sourceFiles.getSeqFiles(),
            sourceFiles.getUnseqFiles(),
            new FastCompactionPerformer(true),
            sourceFiles.getTotalMemoryCost(),
            0)
        .start();

    validateSeqFiles(true);
    validateTargetDatas(sourceData, Collections.emptyList());
  }

  public void generateModsFile(
      List<PartialPath> seriesPaths, TsFileResource resource, long startValue, long endValue)
      throws IllegalPathException, IOException {
    Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
    for (PartialPath path : seriesPaths) {
      String fullPath =
          (path instanceof AlignedPath)
              ? path.getFullPath()
                  + TsFileConstant.PATH_SEPARATOR
                  + ((AlignedPath) path).getMeasurementList().get(0)
              : path.getFullPath();
      deleteMap.put(fullPath, new Pair<>(startValue, endValue));
    }
    CompactionFileGeneratorUtils.generateMods(deleteMap, resource, false);
  }
}
