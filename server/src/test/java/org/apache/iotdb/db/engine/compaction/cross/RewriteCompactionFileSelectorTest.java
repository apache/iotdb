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

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.manage.CrossSpaceCompactionResource;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.selector.ICrossSpaceMergeFileSelector;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.selector.RewriteCompactionFileSelector;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.db.engine.storagegroup.timeindex.ITimeIndex;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class RewriteCompactionFileSelectorTest extends MergeTest {

  @Test
  public void testFullSelection() throws MergeException, IOException {
    CrossSpaceCompactionResource resource =
        new CrossSpaceCompactionResource(seqResources, unseqResources);
    ICrossSpaceMergeFileSelector mergeFileSelector =
        new RewriteCompactionFileSelector(resource, Long.MAX_VALUE);
    List[] result = mergeFileSelector.select();
    List<TsFileResource> seqSelected = result[0];
    List<TsFileResource> unseqSelected = result[1];
    assertEquals(seqResources, seqSelected);
    assertEquals(unseqResources, unseqSelected);
    resource.clear();

    resource = new CrossSpaceCompactionResource(seqResources.subList(0, 1), unseqResources);
    mergeFileSelector = new RewriteCompactionFileSelector(resource, Long.MAX_VALUE);
    result = mergeFileSelector.select();
    seqSelected = result[0];
    unseqSelected = result[1];
    assertEquals(seqResources.subList(0, 1), seqSelected);
    assertEquals(unseqResources, unseqSelected);
    resource.clear();

    resource = new CrossSpaceCompactionResource(seqResources, unseqResources.subList(0, 1));
    mergeFileSelector = new RewriteCompactionFileSelector(resource, Long.MAX_VALUE);
    result = mergeFileSelector.select();
    seqSelected = result[0];
    unseqSelected = result[1];
    assertEquals(seqResources.subList(0, 1), seqSelected);
    assertEquals(unseqResources.subList(0, 1), unseqSelected);
    resource.clear();
  }

  @Test
  public void testNonSelection() throws MergeException, IOException {
    CrossSpaceCompactionResource resource =
        new CrossSpaceCompactionResource(seqResources, unseqResources);
    ICrossSpaceMergeFileSelector mergeFileSelector = new RewriteCompactionFileSelector(resource, 1);
    List[] result = mergeFileSelector.select();
    assertEquals(2, result.length);
    resource.clear();
  }

  @Test
  public void testRestrictedSelection() throws MergeException, IOException {
    CrossSpaceCompactionResource resource =
        new CrossSpaceCompactionResource(seqResources, unseqResources);
    ICrossSpaceMergeFileSelector mergeFileSelector =
        new RewriteCompactionFileSelector(resource, 100000);
    List[] result = mergeFileSelector.select();
    List<TsFileResource> seqSelected = result[0];
    List<TsFileResource> unseqSelected = result[1];
    assertEquals(seqResources.subList(0, 3), seqSelected);
    assertEquals(unseqResources.subList(0, 3), unseqSelected);
    resource.clear();
  }

  /**
   * test unseq merge select with the following files: {0seq-0-0-0.tsfile 0-100 1seq-1-1-0.tsfile
   * 100-200 2seq-2-2-0.tsfile 200-300 3seq-3-3-0.tsfile 300-400 4seq-4-4-0.tsfile 400-500}
   * {10unseq-10-10-0.tsfile 0-500}
   */
  @Test
  public void testFileOpenSelection()
      throws MergeException, IOException, WriteProcessException, NoSuchFieldException,
          IllegalAccessException {
    File file =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                10
                    + "unseq"
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 10
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 10
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource largeUnseqTsFileResource = new TsFileResource(file);
    unseqResources.add(largeUnseqTsFileResource);
    largeUnseqTsFileResource.setStatus(TsFileResourceStatus.CLOSED);
    largeUnseqTsFileResource.setMinPlanIndex(10);
    largeUnseqTsFileResource.setMaxPlanIndex(10);
    largeUnseqTsFileResource.setVersion(10);
    prepareFile(largeUnseqTsFileResource, 0, seqFileNum * ptNum, 0);

    // update the second file's status to open
    TsFileResource secondTsFileResource = seqResources.get(1);
    secondTsFileResource.setStatus(TsFileResourceStatus.UNCLOSED);
    Set<String> devices = secondTsFileResource.getDevices();
    // update the end time of the file to Long.MIN_VALUE, so we can simulate a real open file
    Field timeIndexField = TsFileResource.class.getDeclaredField("timeIndex");
    timeIndexField.setAccessible(true);
    ITimeIndex timeIndex = (ITimeIndex) timeIndexField.get(secondTsFileResource);
    ITimeIndex newTimeIndex =
        IoTDBDescriptor.getInstance().getConfig().getTimeIndexLevel().getTimeIndex();
    for (String device : devices) {
      newTimeIndex.updateStartTime(device, timeIndex.getStartTime(device));
    }
    secondTsFileResource.setTimeIndex(newTimeIndex);

    List<TsFileResource> newUnseqResources = new ArrayList<>();
    newUnseqResources.add(largeUnseqTsFileResource);
    CrossSpaceCompactionResource resource =
        new CrossSpaceCompactionResource(seqResources, newUnseqResources);
    ICrossSpaceMergeFileSelector mergeFileSelector =
        new RewriteCompactionFileSelector(resource, Long.MAX_VALUE);
    List[] result = mergeFileSelector.select();
    assertEquals(0, result.length);
    resource.clear();
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
                10
                    + "unseq"
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 10
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 10
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource largeUnseqTsFileResource = new TsFileResource(file);
    unseqResources.add(largeUnseqTsFileResource);
    largeUnseqTsFileResource.setStatus(TsFileResourceStatus.CLOSED);
    largeUnseqTsFileResource.setMinPlanIndex(10);
    largeUnseqTsFileResource.setMaxPlanIndex(10);
    largeUnseqTsFileResource.setVersion(10);
    prepareFile(largeUnseqTsFileResource, 0, seqFileNum * ptNum, 0);

    // update the second file's status to open
    TsFileResource secondTsFileResource = seqResources.get(1);
    secondTsFileResource.setStatus(TsFileResourceStatus.UNCLOSED);
    Set<String> devices = secondTsFileResource.getDevices();
    // update the end time of the file to Long.MIN_VALUE, so we can simulate a real open file
    Field timeIndexField = TsFileResource.class.getDeclaredField("timeIndex");
    timeIndexField.setAccessible(true);
    ITimeIndex timeIndex = (ITimeIndex) timeIndexField.get(secondTsFileResource);
    ITimeIndex newTimeIndex =
        IoTDBDescriptor.getInstance().getConfig().getTimeIndexLevel().getTimeIndex();
    for (String device : devices) {
      newTimeIndex.updateStartTime(device, timeIndex.getStartTime(device));
    }
    secondTsFileResource.setTimeIndex(newTimeIndex);
    List<TsFileResource> newUnseqResources = new ArrayList<>();
    newUnseqResources.add(largeUnseqTsFileResource);

    long ttlLowerBound = System.currentTimeMillis() - Long.MAX_VALUE;
    CrossSpaceCompactionResource mergeResource =
        new CrossSpaceCompactionResource(seqResources, newUnseqResources, ttlLowerBound);
    assertEquals(5, mergeResource.getSeqFiles().size());
    assertEquals(1, mergeResource.getUnseqFiles().size());
    mergeResource.clear();
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
                10
                    + "unseq"
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 10
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 10
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource largeUnseqTsFileResource = new TsFileResource(file);
    largeUnseqTsFileResource.setStatus(TsFileResourceStatus.CLOSED);
    largeUnseqTsFileResource.setMinPlanIndex(10);
    largeUnseqTsFileResource.setMaxPlanIndex(10);
    largeUnseqTsFileResource.setVersion(10);
    prepareFile(largeUnseqTsFileResource, 0, ptNum + 1, 0);

    unseqResources.clear();
    unseqResources.add(largeUnseqTsFileResource);

    CrossSpaceCompactionResource resource =
        new CrossSpaceCompactionResource(seqResources, unseqResources);
    ICrossSpaceMergeFileSelector mergeFileSelector =
        new RewriteCompactionFileSelector(resource, Long.MAX_VALUE);
    List[] result = mergeFileSelector.select();
    assertEquals(2, result[0].size());
    resource.clear();
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
        fileResource.setStatus(TsFileResourceStatus.CLOSED);
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
        fileResource.setStatus(TsFileResourceStatus.CLOSED);
        unseqList.add(fileResource);
      }
      prepareFile(unseqList.get(0), 0, 1, 10);
      prepareFile(unseqList.get(1), 0, 100, 20);
      prepareFile(unseqList.get(2), 99, 1, 30);

      CrossSpaceCompactionResource resource = new CrossSpaceCompactionResource(seqList, unseqList);
      // the budget is enough to select unseq0 and unseq2, but not unseq1
      // the first selection should only contain seq0 and unseq0
      ICrossSpaceMergeFileSelector mergeFileSelector =
          new RewriteCompactionFileSelector(resource, 29000);
      List[] result = mergeFileSelector.select();
      assertEquals(1, result[0].size());
      assertEquals(1, result[1].size());
      assertEquals(seqList.get(0), result[0].get(0));
      assertEquals(unseqList.get(0), result[1].get(0));
      resource.clear();

      resource =
          new CrossSpaceCompactionResource(
              seqList.subList(1, seqList.size()), unseqList.subList(1, unseqList.size()));
      // the second selection should be empty
      mergeFileSelector = new RewriteCompactionFileSelector(resource, 29000);
      result = mergeFileSelector.select();
      assertEquals(2, result.length);
      resource.clear();
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
      fileResource.setStatus(TsFileResourceStatus.CLOSED);
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
      fileResource.setStatus(TsFileResourceStatus.CLOSED);
      prepareFile(fileResource, i, 1, i);
      unseqList.add(fileResource);
    }

    CrossSpaceCompactionResource resource = new CrossSpaceCompactionResource(seqList, unseqList);
    Assert.assertEquals(5, resource.getSeqFiles().size());
    Assert.assertEquals(10, resource.getUnseqFiles().size());
    ICrossSpaceMergeFileSelector mergeFileSelector =
        new RewriteCompactionFileSelector(resource, 500 * 1024 * 1024);
    List[] result = mergeFileSelector.select();
    Assert.assertEquals(2, result.length);
    Assert.assertEquals(1, result[0].size());
    Assert.assertEquals(10, result[1].size());
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
      fileResource.setStatus(TsFileResourceStatus.CLOSED);
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
      fileResource.setStatus(TsFileResourceStatus.CLOSED);
      prepareFile(fileResource, i, 10, i);
      unseqList.add(fileResource);
    }

    CrossSpaceCompactionResource resource = new CrossSpaceCompactionResource(seqList, unseqList);
    Assert.assertEquals(5, resource.getSeqFiles().size());
    Assert.assertEquals(1, resource.getUnseqFiles().size());
    ICrossSpaceMergeFileSelector mergeFileSelector =
        new RewriteCompactionFileSelector(resource, 500 * 1024 * 1024);
    List[] result = mergeFileSelector.select();
    Assert.assertEquals(2, result.length);
    Assert.assertEquals(1, result[0].size());
    Assert.assertEquals(1, result[1].size());
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
                  10
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 10
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile"));
      TsFileResource fileResource = new TsFileResource(file);
      fileResource.setStatus(TsFileResourceStatus.CLOSED);
      prepareFile(fileResource, i, 1, i);
      seqList.add(fileResource);
    }
    int unseqFileNum = 2;
    // 2 unseq files [7~9] [10~13]
    for (int i = 0; i < unseqFileNum; i++) {
      File file =
          new File(
              TestConstant.OUTPUT_DATA_DIR.concat(
                  10
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 10
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile"));
      TsFileResource fileResource = new TsFileResource(file);
      fileResource.setStatus(TsFileResourceStatus.CLOSED);
      unseqList.add(fileResource);
    }
    prepareFile(unseqList.get(0), 7, 3, 7);
    prepareFile(unseqList.get(1), 10, 4, 10);

    CrossSpaceCompactionResource resource = new CrossSpaceCompactionResource(seqList, unseqList);
    Assert.assertEquals(5, resource.getSeqFiles().size());
    Assert.assertEquals(2, resource.getUnseqFiles().size());
    ICrossSpaceMergeFileSelector mergeFileSelector =
        new RewriteCompactionFileSelector(resource, 500 * 1024 * 1024);
    List[] result = mergeFileSelector.select();
    Assert.assertEquals(2, result.length);
    Assert.assertEquals(3, result[0].size());
    Assert.assertEquals(2, result[1].size());
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
      fileResource.setStatus(TsFileResourceStatus.CLOSED);
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
      fileResource.setStatus(TsFileResourceStatus.CLOSED);
      unseqList.add(fileResource);
    }
    prepareFile(unseqList.get(0), 7, 3, 7);
    prepareFile(unseqList.get(1), 10, 4, 10);
    prepareFile(unseqList.get(2), 14, 3, 14);
    prepareFile(unseqList.get(3), 17, 2, 17);

    CrossSpaceCompactionResource resource = new CrossSpaceCompactionResource(seqList, unseqList);
    Assert.assertEquals(5, resource.getSeqFiles().size());
    Assert.assertEquals(4, resource.getUnseqFiles().size());
    ICrossSpaceMergeFileSelector mergeFileSelector =
        new RewriteCompactionFileSelector(resource, 500 * 1024 * 1024);
    List[] result = mergeFileSelector.select();
    Assert.assertEquals(2, result.length);
    Assert.assertEquals(5, result[0].size());
    Assert.assertEquals(4, result[1].size());
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
        fileResource.setStatus(TsFileResourceStatus.CLOSED);
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
      fileResource.setStatus(TsFileResourceStatus.CLOSED);
      unseqList.add(fileResource);
    }
    prepareFile(unseqList.get(0), 7, 3, 7);
    prepareFile(unseqList.get(1), 10, 4, 10);
    prepareFile(unseqList.get(2), 14, 3, 14);
    prepareFile(unseqList.get(3), 17, 2, 17);

    CrossSpaceCompactionResource resource = new CrossSpaceCompactionResource(seqList, unseqList);
    Assert.assertEquals(5, resource.getSeqFiles().size());
    Assert.assertEquals(4, resource.getUnseqFiles().size());
    ICrossSpaceMergeFileSelector mergeFileSelector =
        new RewriteCompactionFileSelector(resource, 500 * 1024 * 1024);
    List[] result = mergeFileSelector.select();
    Assert.assertEquals(2, result.length);
    Assert.assertEquals(3, result[0].size());
    Assert.assertEquals(2, result[1].size());
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
    firstTsFileResource.setStatus(TsFileResourceStatus.CLOSED);
    firstTsFileResource.setMinPlanIndex(1);
    firstTsFileResource.setMaxPlanIndex(1);
    firstTsFileResource.setVersion(1);
    seqList.add(firstTsFileResource);
    if (!firstFile.getParentFile().exists()) {
      Assert.assertTrue(firstFile.getParentFile().mkdirs());
    }

    TsFileWriter fileWriter = new TsFileWriter(firstFile);
    for (String deviceId : deviceIds) {
      for (MeasurementSchema measurementSchema : measurementSchemas) {
        fileWriter.registerTimeseries(new Path(deviceId), measurementSchema);
      }
    }

    for (long i = 0; i < 10; ++i) {
      for (int j = 0; j < deviceNum; j++) {
        if (j == 3 && i > 5) {
          continue;
        }
        TSRecord record = new TSRecord(i, deviceIds[j]);
        for (int k = 0; k < measurementNum; ++k) {
          record.addTuple(
              DataPoint.getDataPoint(
                  measurementSchemas[k].getType(),
                  measurementSchemas[k].getMeasurementId(),
                  String.valueOf(i)));
        }
        fileWriter.write(record);
        firstTsFileResource.updateStartTime(deviceIds[j], i);
        firstTsFileResource.updateEndTime(deviceIds[j], i);
      }
    }

    fileWriter.flushAllChunkGroups();
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
    secondTsFileResource.setStatus(TsFileResourceStatus.CLOSED);
    secondTsFileResource.setMinPlanIndex(2);
    secondTsFileResource.setMaxPlanIndex(2);
    secondTsFileResource.setVersion(2);
    seqList.add(secondTsFileResource);

    if (!secondFile.getParentFile().exists()) {
      Assert.assertTrue(secondFile.getParentFile().mkdirs());
    }
    fileWriter = new TsFileWriter(secondFile);
    for (String deviceId : deviceIds) {
      for (MeasurementSchema measurementSchema : measurementSchemas) {
        fileWriter.registerTimeseries(new Path(deviceId), measurementSchema);
      }
    }
    for (long i = 11; i < 21; ++i) {
      for (int j = 0; j < deviceNum; j++) {
        TSRecord record = new TSRecord(i, deviceIds[j]);
        for (int k = 0; k < measurementNum; k++) {
          record.addTuple(
              DataPoint.getDataPoint(
                  measurementSchemas[k].getType(),
                  measurementSchemas[k].getMeasurementId(),
                  String.valueOf(i)));
        }
        fileWriter.write(record);
        secondTsFileResource.updateStartTime(deviceIds[j], i);
        secondTsFileResource.updateEndTime(deviceIds[j], i);
      }
    }
    fileWriter.flushAllChunkGroups();
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
    thirdTsFileResource.setStatus(TsFileResourceStatus.CLOSED);
    thirdTsFileResource.setMinPlanIndex(3);
    thirdTsFileResource.setMaxPlanIndex(3);
    thirdTsFileResource.setVersion(3);
    unseqList.add(thirdTsFileResource);

    if (!secondFile.getParentFile().exists()) {
      Assert.assertTrue(thirdFile.getParentFile().mkdirs());
    }
    fileWriter = new TsFileWriter(thirdFile);
    for (String deviceId : deviceIds) {
      for (MeasurementSchema measurementSchema : measurementSchemas) {
        fileWriter.registerTimeseries(new Path(deviceId), measurementSchema);
      }
    }
    for (long i = 0; i < 2; ++i) {
      for (int j = 0; j < deviceNum; j++) {
        TSRecord record = new TSRecord(i, deviceIds[j]);
        for (int k = 0; k < measurementNum; k++) {
          record.addTuple(
              DataPoint.getDataPoint(
                  measurementSchemas[k].getType(),
                  measurementSchemas[k].getMeasurementId(),
                  String.valueOf(i)));
        }
        fileWriter.write(record);
        thirdTsFileResource.updateStartTime(deviceIds[j], i);
        thirdTsFileResource.updateEndTime(deviceIds[j], i);
      }
    }
    fileWriter.flushAllChunkGroups();
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
    fourthTsFileResource.setStatus(TsFileResourceStatus.CLOSED);
    fourthTsFileResource.setMinPlanIndex(4);
    fourthTsFileResource.setMaxPlanIndex(4);
    fourthTsFileResource.setVersion(4);
    unseqList.add(fourthTsFileResource);

    if (!fourthFile.getParentFile().exists()) {
      Assert.assertTrue(fourthFile.getParentFile().mkdirs());
    }
    fileWriter = new TsFileWriter(fourthFile);
    for (String deviceId : deviceIds) {
      for (MeasurementSchema measurementSchema : measurementSchemas) {
        fileWriter.registerTimeseries(new Path(deviceId), measurementSchema);
      }
    }
    for (long i = 6; i < 15; ++i) {
      for (int j = 0; j < deviceNum; j++) {
        if (j == 3) {
          continue;
        }
        TSRecord record = new TSRecord(i, deviceIds[j]);
        for (int k = 0; k < measurementNum; k++) {
          record.addTuple(
              DataPoint.getDataPoint(
                  measurementSchemas[k].getType(),
                  measurementSchemas[k].getMeasurementId(),
                  String.valueOf(i)));
        }
        fileWriter.write(record);
        fourthTsFileResource.updateStartTime(deviceIds[j], i);
        fourthTsFileResource.updateEndTime(deviceIds[j], i);
      }
    }
    TSRecord record = new TSRecord(1, deviceIds[3]);
    for (int k = 0; k < measurementNum; k++) {
      record.addTuple(
          DataPoint.getDataPoint(
              measurementSchemas[k].getType(),
              measurementSchemas[k].getMeasurementId(),
              String.valueOf(1)));
    }
    fileWriter.write(record);
    fourthTsFileResource.updateStartTime(deviceIds[3], 1);
    fourthTsFileResource.updateEndTime(deviceIds[3], 1);
    fileWriter.flushAllChunkGroups();
    fileWriter.close();

    CrossSpaceCompactionResource compactionResource =
        new CrossSpaceCompactionResource(seqList, unseqList);
    RewriteCompactionFileSelector selector =
        new RewriteCompactionFileSelector(compactionResource, 500 * 1024 * 1024);
    List[] result = selector.select();
    Assert.assertEquals(2, result[0].size());
    Assert.assertEquals(2, result[1].size());
  }

  @Test
  public void testMaxFileSelection() throws MergeException, IOException {
    int oldMaxCrossCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxCrossCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCrossCompactionCandidateFileNum(5);
    CrossSpaceCompactionResource resource =
        new CrossSpaceCompactionResource(seqResources, unseqResources);
    ICrossSpaceMergeFileSelector mergeFileSelector =
        new RewriteCompactionFileSelector(resource, Long.MAX_VALUE);
    List[] result = mergeFileSelector.select();
    assertEquals(2, result.length);
    List<TsFileResource> seqSelected = result[0];
    List<TsFileResource> unseqSelected = result[1];
    assertEquals(2, seqSelected.size());
    assertEquals(2, unseqSelected.size());
    resource.clear();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMaxCrossCompactionCandidateFileNum(oldMaxCrossCompactionCandidateFileNum);
  }

  @Test
  public void testAtLeastOneUnseqFileBeenSelected() throws IOException, MergeException {
    int maxCrossFilesNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxCrossCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCrossCompactionCandidateFileNum(1);

    CrossSpaceCompactionResource resource =
        new CrossSpaceCompactionResource(seqResources, unseqResources);
    ICrossSpaceMergeFileSelector mergeFileSelector =
        new RewriteCompactionFileSelector(resource, Long.MAX_VALUE);
    List[] result = mergeFileSelector.select();
    List<TsFileResource> seqSelected = result[0];
    List<TsFileResource> unseqSelected = result[1];
    assertEquals(1, seqSelected.size());
    assertEquals(1, unseqSelected.size());
    resource.clear();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMaxCrossCompactionCandidateFileNum(maxCrossFilesNum);
  }
}
