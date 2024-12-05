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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleContext;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.RewriteCrossSpaceCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.CrossCompactionTaskResource;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.CrossSpaceCompactionCandidate;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;

public class RewriteCompactionFileSelectorTest extends MergeTest {
  private static final Logger logger =
      LoggerFactory.getLogger(RewriteCompactionFileSelectorTest.class);

  private int oldMinCrossCompactionUnseqLevel =
      IoTDBDescriptor.getInstance().getConfig().getMinCrossCompactionUnseqFileLevel();
  private long compactionMemory = SystemInfo.getInstance().getMemorySizeForCompaction();

  @Before
  public void setUp() throws IOException, MetadataException, WriteProcessException {
    super.setUp();
    IoTDBDescriptor.getInstance().getConfig().setMinCrossCompactionUnseqFileLevel(0);
    IoTDBDescriptor.getInstance().getConfig().setCompactionThreadCount(1);
    SystemInfo.getInstance().setMemorySizeForCompaction(1000 * 1024 * 1024);
  }

  @After
  public void tearDown() throws StorageEngineException, IOException {
    super.tearDown();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMinCrossCompactionUnseqFileLevel(oldMinCrossCompactionUnseqLevel);
    SystemInfo.getInstance().setMemorySizeForCompaction(compactionMemory);
  }

  @Test
  public void testFullSelection() throws MergeException, IOException {
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);
    List<TsFileResource> seqSelected = selected.get(0).getSeqFiles();
    List<TsFileResource> unseqSelected = selected.get(0).getUnseqFiles();
    assertEquals(seqResources, seqSelected);
    assertEquals(unseqResources, unseqSelected);

    selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
    selected = selector.selectCrossSpaceTask(seqResources.subList(0, 1), unseqResources);
    seqSelected = selected.get(0).getSeqFiles();
    unseqSelected = selected.get(0).getUnseqFiles();
    assertEquals(seqResources.subList(0, 1), seqSelected);
    assertEquals(unseqResources, unseqSelected);

    selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
    selected = selector.selectCrossSpaceTask(seqResources, unseqResources.subList(0, 1));
    seqSelected = selected.get(0).getSeqFiles();
    unseqSelected = selected.get(0).getUnseqFiles();
    assertEquals(seqResources.subList(0, 1), seqSelected);
    assertEquals(unseqResources.subList(0, 1), unseqSelected);
  }

  @Test
  public void testWithFewMemoryBudgeSelection() throws MergeException, IOException {
    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);
    assertEquals(1, selected.size());
  }

  @Test
  public void testRestrictedSelection() throws MergeException, IOException {
    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);
    List<TsFileResource> seqSelected = selected.get(0).getSeqFiles();
    List<TsFileResource> unseqSelected = selected.get(0).getUnseqFiles();
    assertEquals(seqResources.subList(0, 5), seqSelected);
    assertEquals(unseqResources.subList(0, 6), unseqSelected);
  }

  /**
   * test unseq merge select with the following files: {0seq-0-0-0.tsfile 0-100 1seq-1-1-0.tsfile
   * 100-200 2seq-2-2-0.tsfile 200-300 3seq-3-3-0.tsfile 300-400 4seq-4-4-0.tsfile 400-500}
   * {10unseq-10-10-0.tsfile 0-500}
   */
  @Test
  public void testFileOpenSelection()
      throws MergeException,
          IOException,
          WriteProcessException,
          NoSuchFieldException,
          IllegalAccessException {
    File file =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                System.currentTimeMillis()
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 10
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 10
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource largeUnseqTsFileResource = new TsFileResource(file);
    unseqResources.add(largeUnseqTsFileResource);
    largeUnseqTsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
    largeUnseqTsFileResource.setMinPlanIndex(10);
    largeUnseqTsFileResource.setMaxPlanIndex(10);
    largeUnseqTsFileResource.setVersion(10);
    prepareFile(largeUnseqTsFileResource, 0, seqFileNum * ptNum, 0);

    // update the second file's status to open
    TsFileResource secondTsFileResource = seqResources.get(1);
    secondTsFileResource.setStatusForTest(TsFileResourceStatus.UNCLOSED);
    Set<IDeviceID> devices = secondTsFileResource.getDevices();
    // update the end time of the file to Long.MIN_VALUE, so we can simulate a real open file
    Field timeIndexField = TsFileResource.class.getDeclaredField("timeIndex");
    timeIndexField.setAccessible(true);
    ITimeIndex timeIndex = (ITimeIndex) timeIndexField.get(secondTsFileResource);
    ITimeIndex newTimeIndex =
        IoTDBDescriptor.getInstance().getConfig().getTimeIndexLevel().getTimeIndex();
    for (IDeviceID device : devices) {
      newTimeIndex.updateStartTime(device, timeIndex.getStartTime(device));
    }
    secondTsFileResource.setTimeIndex(newTimeIndex);

    List<TsFileResource> newUnseqResources = new ArrayList<>();
    newUnseqResources.add(largeUnseqTsFileResource);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, newUnseqResources);
    assertEquals(0, selected.size());
  }

  /**
   * test unseq merge select with the following files: {0seq-0-0-0.tsfile 0-100 1seq-1-1-0.tsfile
   * 100-200 2seq-2-2-0.tsfile 200-300 3seq-3-3-0.tsfile 300-400 4seq-4-4-0.tsfile 400-500}
   * {10unseq-10-10-0.tsfile 0-500}
   */
  @Test
  public void testFileOpenSelectionFromCompaction()
      throws IOException, WriteProcessException, NoSuchFieldException, IllegalAccessException {
    File file =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                System.currentTimeMillis()
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 10
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 10
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource largeUnseqTsFileResource = new TsFileResource(file);
    unseqResources.add(largeUnseqTsFileResource);
    largeUnseqTsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
    largeUnseqTsFileResource.setMinPlanIndex(10);
    largeUnseqTsFileResource.setMaxPlanIndex(10);
    largeUnseqTsFileResource.setVersion(10);
    prepareFile(largeUnseqTsFileResource, 0, seqFileNum * ptNum, 0);

    // update the second file's status to open
    TsFileResource secondTsFileResource = seqResources.get(1);
    secondTsFileResource.setStatusForTest(TsFileResourceStatus.UNCLOSED);
    Set<IDeviceID> devices = secondTsFileResource.getDevices();
    // update the end time of the file to Long.MIN_VALUE, so we can simulate a real open file
    Field timeIndexField = TsFileResource.class.getDeclaredField("timeIndex");
    timeIndexField.setAccessible(true);
    ITimeIndex timeIndex = (ITimeIndex) timeIndexField.get(secondTsFileResource);
    ITimeIndex newTimeIndex =
        IoTDBDescriptor.getInstance().getConfig().getTimeIndexLevel().getTimeIndex();
    for (IDeviceID device : devices) {
      newTimeIndex.updateStartTime(device, timeIndex.getStartTime(device));
    }
    secondTsFileResource.setTimeIndex(newTimeIndex);
    List<TsFileResource> newUnseqResources = new ArrayList<>();
    newUnseqResources.add(largeUnseqTsFileResource);

    long ttlLowerBound = System.currentTimeMillis() - Long.MAX_VALUE;
    CrossSpaceCompactionCandidate mergeResource =
        new CrossSpaceCompactionCandidate(seqResources, newUnseqResources, ttlLowerBound);
    assertEquals(5, mergeResource.getSeqFiles().size());
    assertEquals(1, mergeResource.getUnseqFiles().size());
  }

  /**
   * test unseq merge select with the following files: {0seq-0-0-0.tsfile 0-100 1seq-1-1-0.tsfile
   * 100-200 2seq-2-2-0.tsfile 200-300 3seq-3-3-0.tsfile 300-400 4seq-4-4-0.tsfile 400-500}
   * {10unseq-10-10-0.tsfile 0-101}
   */
  @Test
  public void testFileSelectionAboutLastSeqFile()
      throws MergeException, IOException, WriteProcessException {
    File file =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                System.currentTimeMillis()
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 10
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 10
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource largeUnseqTsFileResource = new TsFileResource(file);
    largeUnseqTsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
    largeUnseqTsFileResource.setMinPlanIndex(10);
    largeUnseqTsFileResource.setMaxPlanIndex(10);
    largeUnseqTsFileResource.setVersion(10);
    prepareFile(largeUnseqTsFileResource, 0, ptNum + 1, 0);

    unseqResources.clear();
    unseqResources.add(largeUnseqTsFileResource);

    CrossSpaceCompactionCandidate resource =
        new CrossSpaceCompactionCandidate(seqResources, unseqResources);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);
    assertEquals(2, selected.get(0).getSeqFiles().size());
  }

  @Test
  public void testSelectContinuousUnseqFile()
      throws IOException, WriteProcessException, MergeException {
    List<TsFileResource> seqList = new ArrayList<>();
    List<TsFileResource> unseqList = new ArrayList<>();
    try {
      // seq files [0,0] [1,1] [2,2] ... [99,99]
      int seqFileNum = 99;
      for (int i = 0; i < seqFileNum; i++) {
        File file =
            new File(
                TestConstant.BASE_OUTPUT_PATH.concat(
                    System.currentTimeMillis()
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + i
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 10
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + ".tsfile"));
        TsFileResource fileResource = new TsFileResource(file);
        fileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
        prepareFile(fileResource, i, 1, 0);
        seqList.add(fileResource);
      }
      int unseqFileNum = 3;
      // 3 unseq files [0,0] [0,99] [99,99]
      for (int i = 0; i < unseqFileNum; i++) {
        File file =
            new File(
                TestConstant.BASE_OUTPUT_PATH.concat(
                    System.currentTimeMillis()
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + i
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 10
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + ".tsfile"));
        TsFileResource fileResource = new TsFileResource(file);
        fileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
        unseqList.add(fileResource);
      }
      prepareFile(unseqList.get(0), 0, 1, 10);
      prepareFile(unseqList.get(1), 0, 100, 20);
      prepareFile(unseqList.get(2), 99, 1, 30);

      CrossSpaceCompactionCandidate resource =
          new CrossSpaceCompactionCandidate(seqList, unseqList);
      // the budget is enough to select unseq0 and unseq2, but not unseq1
      // the first selection should only contain seq0 and unseq0
      long originMemoryBudget = SystemInfo.getInstance().getMemorySizeForCompaction();
      SystemInfo.getInstance()
          .setMemorySizeForCompaction(
              29000L * IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount());
      try {
        RewriteCrossSpaceCompactionSelector selector =
            new RewriteCrossSpaceCompactionSelector(
                "", "", 0, null, new CompactionScheduleContext());
        List<CrossCompactionTaskResource> selected =
            selector.selectCrossSpaceTask(seqList, unseqList);
        assertEquals(1, selected.get(0).getSeqFiles().size());
        assertEquals(1, selected.get(0).getUnseqFiles().size());
        assertEquals(seqList.get(0), selected.get(0).getSeqFiles().get(0));
        assertEquals(unseqList.get(0), selected.get(0).getUnseqFiles().get(0));

        selected =
            selector.selectCrossSpaceTask(
                seqList.subList(1, seqList.size()), unseqList.subList(1, unseqList.size()));
        assertEquals(1, selected.size());
      } finally {
        SystemInfo.getInstance().setMemorySizeForCompaction(originMemoryBudget);
      }

    } finally {
      removeFiles(seqList, unseqList);
    }
  }

  /**
   * 5 source seq files: [11,11] [12,12] [13,13] [14,14] [15,15]<br>
   * 10 source unseq files: [0,0] [1,1] ... [9,9]<br>
   * selected seq file index: 1<br>
   * selected unseq file index: 1 ~ 10
   */
  @Test
  public void testUnseqFilesOverlappedWithOneSeqFile()
      throws IOException, WriteProcessException, MergeException {
    List<TsFileResource> seqList = new ArrayList<>();
    List<TsFileResource> unseqList = new ArrayList<>();
    // 5 seq files [11,11] [12,12] [13,13] ... [15,15]
    int seqFileNum = 5;
    for (int i = 11; i < seqFileNum + 11; i++) {
      File file =
          new File(
              TestConstant.OUTPUT_DATA_DIR.concat(
                  System.currentTimeMillis()
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 10
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile"));
      TsFileResource fileResource = new TsFileResource(file);
      fileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
      prepareFile(fileResource, i, 1, i);
      seqList.add(fileResource);
    }
    int unseqFileNum = 10;
    // 10 unseq files [0,0] [1,1] ... [9,9]
    for (int i = 0; i < unseqFileNum; i++) {
      File file =
          new File(
              TestConstant.OUTPUT_DATA_DIR.concat(
                  System.currentTimeMillis()
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 10
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile"));
      TsFileResource fileResource = new TsFileResource(file);
      fileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
      prepareFile(fileResource, i, 1, i);
      unseqList.add(fileResource);
    }

    CrossSpaceCompactionCandidate resource = new CrossSpaceCompactionCandidate(seqList, unseqList);
    Assert.assertEquals(5, resource.getSeqFiles().size());
    Assert.assertEquals(10, resource.getUnseqFiles().size());
    long origin = SystemInfo.getInstance().getMemorySizeForCompaction();
    SystemInfo.getInstance()
        .setMemorySizeForCompaction(
            500L
                * 1024
                * 1024
                * IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount());
    try {
      RewriteCrossSpaceCompactionSelector selector =
          new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
      List<CrossCompactionTaskResource> selected =
          selector.selectCrossSpaceTask(seqList, unseqList);
      Assert.assertEquals(1, selected.size());
      Assert.assertEquals(1, selected.get(0).getSeqFiles().size());
      Assert.assertEquals(10, selected.get(0).getUnseqFiles().size());
    } finally {
      SystemInfo.getInstance().setMemorySizeForCompaction(origin);
    }
  }

  /**
   * 5 source seq files: [11,11] [12,12] [13,13] [14,14] [15,15]<br>
   * 1 source unseq files: [0 ~ 9]<br>
   * selected seq file index: 1<br>
   * selected unseq file index: 1
   */
  @Test
  public void testOneUnseqFileOverlappedWithOneSeqFile()
      throws IOException, WriteProcessException, MergeException {
    List<TsFileResource> seqList = new ArrayList<>();
    List<TsFileResource> unseqList = new ArrayList<>();
    // 5 seq files [11,11] [12,12] [13,13] ... [15,15]
    int seqFileNum = 5;
    for (int i = 11; i < seqFileNum + 11; i++) {
      File file =
          new File(
              TestConstant.OUTPUT_DATA_DIR.concat(
                  System.currentTimeMillis()
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 10
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile"));
      TsFileResource fileResource = new TsFileResource(file);
      fileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
      prepareFile(fileResource, i, 1, i);
      seqList.add(fileResource);
    }
    int unseqFileNum = 1;
    // 1 unseq files [0~9]
    for (int i = 0; i < unseqFileNum; i++) {
      File file =
          new File(
              TestConstant.OUTPUT_DATA_DIR.concat(
                  System.currentTimeMillis()
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 10
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile"));
      TsFileResource fileResource = new TsFileResource(file);
      fileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
      prepareFile(fileResource, i, 10, i);
      unseqList.add(fileResource);
    }

    CrossSpaceCompactionCandidate resource = new CrossSpaceCompactionCandidate(seqList, unseqList);
    Assert.assertEquals(5, resource.getSeqFiles().size());
    Assert.assertEquals(1, resource.getUnseqFiles().size());
    long origin = SystemInfo.getInstance().getMemorySizeForCompaction();
    SystemInfo.getInstance()
        .setMemorySizeForCompaction(
            500L
                * 1024
                * 1024
                * IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount());
    try {
      RewriteCrossSpaceCompactionSelector selector =
          new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
      List<CrossCompactionTaskResource> selected =
          selector.selectCrossSpaceTask(seqList, unseqList);
      Assert.assertEquals(1, selected.size());
      Assert.assertEquals(1, selected.get(0).getSeqFiles().size());
      Assert.assertEquals(1, selected.get(0).getUnseqFiles().size());
    } finally {
      SystemInfo.getInstance().setMemorySizeForCompaction(origin);
    }
  }

  /**
   * 5 source seq files: [11,11] [12,12] [13,13] [14,14] [15,15]<br>
   * 2 source unseq files: [7~9] [10~13]<br>
   * selected seq file index: 1 2 3 <br>
   * selected unseq file index: 1 2
   */
  @Test
  public void testUnseqFilesOverlapped() throws IOException, WriteProcessException, MergeException {
    List<TsFileResource> seqList = new ArrayList<>();
    List<TsFileResource> unseqList = new ArrayList<>();
    // 5 seq files [11,11] [12,12] [13,13] ... [15,15]
    int seqFileNum = 5;
    for (int i = 11; i < seqFileNum + 11; i++) {
      File file =
          new File(
              TestConstant.OUTPUT_DATA_DIR.concat(
                  System.currentTimeMillis()
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 10
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile"));
      TsFileResource fileResource = new TsFileResource(file);
      fileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
      prepareFile(fileResource, i, 1, i);
      seqList.add(fileResource);
    }
    int unseqFileNum = 2;
    // 2 unseq files [7~9] [10~13]
    for (int i = 0; i < unseqFileNum; i++) {
      File file =
          new File(
              TestConstant.OUTPUT_DATA_DIR.concat(
                  System.currentTimeMillis()
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 10
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile"));
      TsFileResource fileResource = new TsFileResource(file);
      fileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
      unseqList.add(fileResource);
    }
    prepareFile(unseqList.get(0), 7, 3, 7);
    prepareFile(unseqList.get(1), 10, 4, 10);

    CrossSpaceCompactionCandidate resource = new CrossSpaceCompactionCandidate(seqList, unseqList);
    Assert.assertEquals(5, resource.getSeqFiles().size());
    Assert.assertEquals(2, resource.getUnseqFiles().size());
    long origin = SystemInfo.getInstance().getMemorySizeForCompaction();
    SystemInfo.getInstance()
        .setMemorySizeForCompaction(
            500L
                * 1024
                * 1024
                * IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount());
    try {
      RewriteCrossSpaceCompactionSelector selector =
          new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
      List<CrossCompactionTaskResource> selected =
          selector.selectCrossSpaceTask(seqList, unseqList);
      Assert.assertEquals(1, selected.size());
      Assert.assertEquals(3, selected.get(0).getSeqFiles().size());
      Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());
    } finally {
      SystemInfo.getInstance().setMemorySizeForCompaction(origin);
    }
  }

  /**
   * 5 source seq files: [11,11] [12,12] [13,13] [14,14] [15,15]<br>
   * 4 source unseq files: [7~9] [10~13] [14~16] [17~18]<br>
   * selected seq file index: 1 2 3 4 5<br>
   * selected unseq file index: 1 2 3 4
   */
  @Test
  public void testAllUnseqFilesOverlapped()
      throws IOException, WriteProcessException, MergeException {
    List<TsFileResource> seqList = new ArrayList<>();
    List<TsFileResource> unseqList = new ArrayList<>();
    // 5 seq files [11,11] [12,12] [13,13] ... [15,15]
    int seqFileNum = 5;
    for (int i = 11; i < seqFileNum + 11; i++) {
      File file =
          new File(
              TestConstant.OUTPUT_DATA_DIR.concat(
                  System.currentTimeMillis()
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 10
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile"));
      TsFileResource fileResource = new TsFileResource(file);
      fileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
      prepareFile(fileResource, i, 1, i);
      seqList.add(fileResource);
    }
    int unseqFileNum = 4;
    // 4 unseq files [7~9] [10~13] [14~16] [17~18]
    for (int i = 0; i < unseqFileNum; i++) {
      File file =
          new File(
              TestConstant.OUTPUT_DATA_DIR.concat(
                  System.currentTimeMillis()
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 10
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile"));
      TsFileResource fileResource = new TsFileResource(file);
      fileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
      unseqList.add(fileResource);
    }
    prepareFile(unseqList.get(0), 7, 3, 7);
    prepareFile(unseqList.get(1), 10, 4, 10);
    prepareFile(unseqList.get(2), 14, 3, 14);
    prepareFile(unseqList.get(3), 17, 2, 17);

    CrossSpaceCompactionCandidate resource = new CrossSpaceCompactionCandidate(seqList, unseqList);
    Assert.assertEquals(5, resource.getSeqFiles().size());
    Assert.assertEquals(4, resource.getUnseqFiles().size());
    long origin = SystemInfo.getInstance().getMemorySizeForCompaction();
    SystemInfo.getInstance()
        .setMemorySizeForCompaction(
            500L
                * 1024
                * 1024
                * IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount());
    try {
      RewriteCrossSpaceCompactionSelector selector =
          new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
      List<CrossCompactionTaskResource> selected =
          selector.selectCrossSpaceTask(seqList, unseqList);
      Assert.assertEquals(1, selected.size());
      Assert.assertEquals(5, selected.get(0).getSeqFiles().size());
      Assert.assertEquals(4, selected.get(0).getUnseqFiles().size());
    } finally {
      SystemInfo.getInstance().setMemorySizeForCompaction(origin);
    }
  }

  /**
   * 5 source seq files: [11,11] [12,12] [13,13] [14,14] [15,15]<br>
   * 4 source unseq files: [7~9] [10~13] [14~16] [17~18]<br>
   * while the forth seq file is not close.<br>
   * selected seq file index: 1 2 3 <br>
   * selected unseq file index: 1 2
   */
  @Test
  public void testAllUnseqFilesOverlappedWithSeqFileOpen()
      throws IOException, WriteProcessException, MergeException {
    List<TsFileResource> seqList = new ArrayList<>();
    List<TsFileResource> unseqList = new ArrayList<>();
    // 5 seq files [11,11] [12,12] [13,13] ... [15,15]
    int seqFileNum = 5;
    for (int i = 11; i < seqFileNum + 11; i++) {
      File file =
          new File(
              TestConstant.OUTPUT_DATA_DIR.concat(
                  System.currentTimeMillis()
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 10
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile"));
      TsFileResource fileResource = new TsFileResource(file);
      if (i - 11 != 3) {
        fileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
      }
      prepareFile(fileResource, i, 1, i);
      seqList.add(fileResource);
    }
    int unseqFileNum = 4;
    // 4 unseq files [7~9] [10~13] [14~16] [17~18]
    for (int i = 0; i < unseqFileNum; i++) {
      File file =
          new File(
              TestConstant.OUTPUT_DATA_DIR.concat(
                  System.currentTimeMillis()
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 10
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile"));
      TsFileResource fileResource = new TsFileResource(file);
      fileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
      unseqList.add(fileResource);
    }
    prepareFile(unseqList.get(0), 7, 3, 7);
    prepareFile(unseqList.get(1), 10, 4, 10);
    prepareFile(unseqList.get(2), 14, 3, 14);
    prepareFile(unseqList.get(3), 17, 2, 17);

    CrossSpaceCompactionCandidate resource = new CrossSpaceCompactionCandidate(seqList, unseqList);
    Assert.assertEquals(5, resource.getSeqFiles().size());
    Assert.assertEquals(4, resource.getUnseqFiles().size());
    long origin = SystemInfo.getInstance().getMemorySizeForCompaction();
    SystemInfo.getInstance()
        .setMemorySizeForCompaction(
            500L
                * 1024
                * 1024
                * IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount());
    try {
      RewriteCrossSpaceCompactionSelector selector =
          new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
      List<CrossCompactionTaskResource> selected =
          selector.selectCrossSpaceTask(seqList, unseqList);
      Assert.assertEquals(1, selected.size());
      Assert.assertEquals(3, selected.get(0).getSeqFiles().size());
      Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());
    } finally {
      SystemInfo.getInstance().setMemorySizeForCompaction(origin);
    }
  }

  @Test
  public void testMultiFileOverlapWithOneFile()
      throws IOException, WriteProcessException, MergeException {
    List<TsFileResource> seqList = new ArrayList<>();
    List<TsFileResource> unseqList = new ArrayList<>();
    // first file [0, 10]
    // first device [0, 5]
    // second device [0, 10]
    File firstFile =
        new File(
            TestConstant.OUTPUT_DATA_DIR.concat(
                1
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 1
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource firstTsFileResource = new TsFileResource(firstFile);
    firstTsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
    firstTsFileResource.setMinPlanIndex(1);
    firstTsFileResource.setMaxPlanIndex(1);
    firstTsFileResource.setVersion(1);
    seqList.add(firstTsFileResource);
    if (!firstFile.getParentFile().exists()) {
      Assert.assertTrue(firstFile.getParentFile().mkdirs());
    }

    TsFileWriter fileWriter = new TsFileWriter(firstFile);
    for (IDeviceID deviceId : deviceIds) {
      for (IMeasurementSchema measurementSchema : measurementSchemas) {
        fileWriter.registerTimeseries(new Path(deviceId), measurementSchema);
      }
    }

    for (long i = 0; i < 10; ++i) {
      for (int j = 0; j < deviceNum; j++) {
        if (j == 3 && i > 5) {
          continue;
        }
        TSRecord record = new TSRecord(deviceIds[j], i);
        for (int k = 0; k < measurementNum; ++k) {
          record.addTuple(
              DataPoint.getDataPoint(
                  measurementSchemas[k].getType(),
                  measurementSchemas[k].getMeasurementName(),
                  String.valueOf(i)));
        }
        fileWriter.writeRecord(record);
        firstTsFileResource.updateStartTime(deviceIds[j], i);
        firstTsFileResource.updateEndTime(deviceIds[j], i);
      }
    }

    fileWriter.flush();
    fileWriter.close();

    // second file time range: [11, 20]
    // first measurement: [11, 20]
    // second measurement: [11, 20]
    File secondFile =
        new File(
            TestConstant.OUTPUT_DATA_DIR.concat(
                2
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 2
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource secondTsFileResource = new TsFileResource(secondFile);
    secondTsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
    secondTsFileResource.setMinPlanIndex(2);
    secondTsFileResource.setMaxPlanIndex(2);
    secondTsFileResource.setVersion(2);
    seqList.add(secondTsFileResource);

    if (!secondFile.getParentFile().exists()) {
      Assert.assertTrue(secondFile.getParentFile().mkdirs());
    }
    fileWriter = new TsFileWriter(secondFile);
    for (IDeviceID deviceId : deviceIds) {
      for (IMeasurementSchema measurementSchema : measurementSchemas) {
        fileWriter.registerTimeseries(new Path(deviceId), measurementSchema);
      }
    }
    for (long i = 11; i < 21; ++i) {
      for (int j = 0; j < deviceNum; j++) {
        TSRecord record = new TSRecord(deviceIds[j], i);
        for (int k = 0; k < measurementNum; k++) {
          record.addTuple(
              DataPoint.getDataPoint(
                  measurementSchemas[k].getType(),
                  measurementSchemas[k].getMeasurementName(),
                  String.valueOf(i)));
        }
        fileWriter.writeRecord(record);
        secondTsFileResource.updateStartTime(deviceIds[j], i);
        secondTsFileResource.updateEndTime(deviceIds[j], i);
      }
    }
    fileWriter.flush();
    fileWriter.close();

    // unseq file: [0, 1]
    File thirdFile =
        new File(
            TestConstant.OUTPUT_DATA_DIR.concat(
                3
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 3
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource thirdTsFileResource = new TsFileResource(thirdFile);
    thirdTsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
    thirdTsFileResource.setMinPlanIndex(3);
    thirdTsFileResource.setMaxPlanIndex(3);
    thirdTsFileResource.setVersion(3);
    unseqList.add(thirdTsFileResource);

    if (!secondFile.getParentFile().exists()) {
      Assert.assertTrue(thirdFile.getParentFile().mkdirs());
    }
    fileWriter = new TsFileWriter(thirdFile);
    for (IDeviceID deviceId : deviceIds) {
      for (IMeasurementSchema measurementSchema : measurementSchemas) {
        fileWriter.registerTimeseries(new Path(deviceId), measurementSchema);
      }
    }
    for (long i = 0; i < 2; ++i) {
      for (int j = 0; j < deviceNum; j++) {
        TSRecord record = new TSRecord(deviceIds[j], i);
        for (int k = 0; k < measurementNum; k++) {
          record.addTuple(
              DataPoint.getDataPoint(
                  measurementSchemas[k].getType(),
                  measurementSchemas[k].getMeasurementName(),
                  String.valueOf(i)));
        }
        fileWriter.writeRecord(record);
        thirdTsFileResource.updateStartTime(deviceIds[j], i);
        thirdTsFileResource.updateEndTime(deviceIds[j], i);
      }
    }
    fileWriter.flush();
    fileWriter.close();

    // unseq file: [6, 14]
    File fourthFile =
        new File(
            TestConstant.OUTPUT_DATA_DIR.concat(
                4
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 4
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource fourthTsFileResource = new TsFileResource(fourthFile);
    fourthTsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
    fourthTsFileResource.setMinPlanIndex(4);
    fourthTsFileResource.setMaxPlanIndex(4);
    fourthTsFileResource.setVersion(4);
    unseqList.add(fourthTsFileResource);

    if (!fourthFile.getParentFile().exists()) {
      Assert.assertTrue(fourthFile.getParentFile().mkdirs());
    }
    fileWriter = new TsFileWriter(fourthFile);
    for (IDeviceID deviceId : deviceIds) {
      for (IMeasurementSchema measurementSchema : measurementSchemas) {
        fileWriter.registerTimeseries(new Path(deviceId), measurementSchema);
      }
    }
    for (long i = 6; i < 15; ++i) {
      for (int j = 0; j < deviceNum; j++) {
        if (j == 3) {
          continue;
        }
        TSRecord record = new TSRecord(deviceIds[j], i);
        for (int k = 0; k < measurementNum; k++) {
          record.addTuple(
              DataPoint.getDataPoint(
                  measurementSchemas[k].getType(),
                  measurementSchemas[k].getMeasurementName(),
                  String.valueOf(i)));
        }
        fileWriter.writeRecord(record);
        fourthTsFileResource.updateStartTime(deviceIds[j], i);
        fourthTsFileResource.updateEndTime(deviceIds[j], i);
      }
    }
    TSRecord record = new TSRecord(deviceIds[3], 1);
    for (int k = 0; k < measurementNum; k++) {
      record.addTuple(
          DataPoint.getDataPoint(
              measurementSchemas[k].getType(),
              measurementSchemas[k].getMeasurementName(),
              String.valueOf(1)));
    }
    fileWriter.writeRecord(record);
    fourthTsFileResource.updateStartTime(deviceIds[3], 1);
    fourthTsFileResource.updateEndTime(deviceIds[3], 1);
    fileWriter.flush();
    fileWriter.close();

    long origin = SystemInfo.getInstance().getMemorySizeForCompaction();
    SystemInfo.getInstance()
        .setMemorySizeForCompaction(
            500L
                * 1024
                * 1024
                * IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount());
    try {
      RewriteCrossSpaceCompactionSelector selector =
          new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
      List<CrossCompactionTaskResource> selected =
          selector.selectCrossSpaceTask(seqList, unseqList);
      Assert.assertEquals(1, selected.size());
      Assert.assertEquals(2, selected.get(0).getSeqFiles().size());
      Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());
    } finally {
      SystemInfo.getInstance().setMemorySizeForCompaction(origin);
    }
  }

  @Test
  public void testMaxFileSelection() throws MergeException, IOException {
    int oldMaxCrossCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getFileLimitPerCrossTask();
    IoTDBDescriptor.getInstance().getConfig().setFileLimitPerCrossTask(5);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);
    assertEquals(1, selected.size());
    List<TsFileResource> seqSelected = selected.get(0).getSeqFiles();
    List<TsFileResource> unseqSelected = selected.get(0).getUnseqFiles();
    assertEquals(2, seqSelected.size());
    assertEquals(2, unseqSelected.size());

    IoTDBDescriptor.getInstance()
        .getConfig()
        .setFileLimitPerCrossTask(oldMaxCrossCompactionCandidateFileNum);
  }

  @Test
  public void testAtLeastOneUnseqFileBeenSelected() throws IOException, MergeException {
    int maxCrossFilesNum = IoTDBDescriptor.getInstance().getConfig().getFileLimitPerCrossTask();
    IoTDBDescriptor.getInstance().getConfig().setFileLimitPerCrossTask(1);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);
    assertEquals(1, selected.size());
    List<TsFileResource> seqSelected = selected.get(0).getSeqFiles();
    List<TsFileResource> unseqSelected = selected.get(0).getUnseqFiles();
    assertEquals(1, seqSelected.size());
    assertEquals(1, unseqSelected.size());

    IoTDBDescriptor.getInstance().getConfig().setFileLimitPerCrossTask(maxCrossFilesNum);
  }

  @Test
  public void testDeleteInSelection() throws Exception {
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
    AtomicBoolean fail = new AtomicBoolean(false);
    Thread thread1 =
        new Thread(
            () -> {
              try {
                Thread.sleep(1000);
                List<CrossCompactionTaskResource> selected =
                    selector.selectCrossSpaceTask(seqResources, unseqResources);
              } catch (Exception e) {
                logger.error("Exception occurs", e);
                fail.set(true);
              }
            });
    Thread thread2 =
        new Thread(
            () -> {
              if (!seqResources.get(0).remove()) {
                fail.set(true);
              }
              if (!unseqResources.get(0).remove()) {
                fail.set(true);
              }
            });
    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();
    if (fail.get()) {
      Assert.fail();
    }
  }

  @Test
  public void testDeleteAndDegradeInSelection() throws Exception {
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
    AtomicBoolean fail = new AtomicBoolean(false);
    Thread thread1 =
        new Thread(
            () -> {
              try {
                Thread.sleep(1000);
                List<CrossCompactionTaskResource> selected =
                    selector.selectCrossSpaceTask(seqResources, unseqResources);
                Assert.assertEquals(1, selected.get(0).getSeqFiles().size());
                Assert.assertEquals(1, selected.get(0).getUnseqFiles().size());
              } catch (Exception e) {
                logger.error("Exception occurs", e);
                fail.set(true);
              }
            });
    Thread thread2 =
        new Thread(
            () -> {
              seqResources.get(1).degradeTimeIndex();
              if (!seqResources.get(1).remove()) {
                fail.set(true);
              }
            });
    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();
    if (fail.get()) {
      Assert.fail();
    }
  }

  @Test
  public void testFirstZeroLevelUnseqFileIsLarge() {
    IoTDBDescriptor.getInstance().getConfig().setMinCrossCompactionUnseqFileLevel(1);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTargetCompactionFileSize(unseqResources.get(5).getTsFileSize());
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources.subList(5, 6));
    Assert.assertEquals(1, selected.size());
  }
}
