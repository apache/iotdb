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

package org.apache.iotdb.db.engine.compaction.crossSpaceCompaction;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.compaction.crossSpaceCompaction.inplace.manage.MergeResource;
import org.apache.iotdb.db.engine.compaction.crossSpaceCompaction.inplace.selector.IMergeFileSelector;
import org.apache.iotdb.db.engine.compaction.crossSpaceCompaction.inplace.selector.MaxFileMergeFileSelector;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.timeindex.ITimeIndex;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class MaxFileMergeFileSelectorTest extends MergeTest {

  @Test
  public void testFullSelection() throws MergeException, IOException {
    MergeResource resource = new MergeResource(seqResources, unseqResources);
    IMergeFileSelector mergeFileSelector = new MaxFileMergeFileSelector(resource, Long.MAX_VALUE);
    List[] result = mergeFileSelector.select();
    List<TsFileResource> seqSelected = result[0];
    List<TsFileResource> unseqSelected = result[1];
    assertEquals(seqResources, seqSelected);
    assertEquals(unseqResources, unseqSelected);
    resource.clear();

    resource = new MergeResource(seqResources.subList(0, 1), unseqResources);
    mergeFileSelector = new MaxFileMergeFileSelector(resource, Long.MAX_VALUE);
    result = mergeFileSelector.select();
    seqSelected = result[0];
    unseqSelected = result[1];
    assertEquals(seqResources.subList(0, 1), seqSelected);
    assertEquals(unseqResources, unseqSelected);
    resource.clear();

    resource = new MergeResource(seqResources, unseqResources.subList(0, 1));
    mergeFileSelector = new MaxFileMergeFileSelector(resource, Long.MAX_VALUE);
    result = mergeFileSelector.select();
    seqSelected = result[0];
    unseqSelected = result[1];
    assertEquals(seqResources.subList(0, 1), seqSelected);
    assertEquals(unseqResources.subList(0, 1), unseqSelected);
    resource.clear();
  }

  @Test
  public void testNonSelection() throws MergeException, IOException {
    MergeResource resource = new MergeResource(seqResources, unseqResources);
    IMergeFileSelector mergeFileSelector = new MaxFileMergeFileSelector(resource, 1);
    List[] result = mergeFileSelector.select();
    assertEquals(0, result.length);
    resource.clear();
  }

  @Test
  public void testRestrictedSelection() throws MergeException, IOException {
    MergeResource resource = new MergeResource(seqResources, unseqResources);
    IMergeFileSelector mergeFileSelector = new MaxFileMergeFileSelector(resource, 400000);
    List[] result = mergeFileSelector.select();
    List<TsFileResource> seqSelected = result[0];
    List<TsFileResource> unseqSelected = result[1];
    assertEquals(seqResources.subList(0, 4), seqSelected);
    assertEquals(unseqResources.subList(0, 4), unseqSelected);
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
    largeUnseqTsFileResource.setClosed(true);
    largeUnseqTsFileResource.setMinPlanIndex(10);
    largeUnseqTsFileResource.setMaxPlanIndex(10);
    largeUnseqTsFileResource.setVersion(10);
    prepareFile(largeUnseqTsFileResource, 0, seqFileNum * ptNum, 0);

    // update the second file's status to open
    TsFileResource secondTsFileResource = seqResources.get(1);
    secondTsFileResource.setClosed(false);
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
    MergeResource resource = new MergeResource(seqResources, newUnseqResources);
    IMergeFileSelector mergeFileSelector = new MaxFileMergeFileSelector(resource, Long.MAX_VALUE);
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
    largeUnseqTsFileResource.setClosed(true);
    largeUnseqTsFileResource.setMinPlanIndex(10);
    largeUnseqTsFileResource.setMaxPlanIndex(10);
    largeUnseqTsFileResource.setVersion(10);
    prepareFile(largeUnseqTsFileResource, 0, seqFileNum * ptNum, 0);

    // update the second file's status to open
    TsFileResource secondTsFileResource = seqResources.get(1);
    secondTsFileResource.setClosed(false);
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

    long timeLowerBound = System.currentTimeMillis() - Long.MAX_VALUE;
    MergeResource mergeResource =
        new MergeResource(seqResources, newUnseqResources, timeLowerBound);
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
    largeUnseqTsFileResource.setClosed(true);
    largeUnseqTsFileResource.setMinPlanIndex(10);
    largeUnseqTsFileResource.setMaxPlanIndex(10);
    largeUnseqTsFileResource.setVersion(10);
    prepareFile(largeUnseqTsFileResource, 0, ptNum + 1, 0);

    unseqResources.clear();
    unseqResources.add(largeUnseqTsFileResource);

    MergeResource resource = new MergeResource(seqResources, unseqResources);
    IMergeFileSelector mergeFileSelector = new MaxFileMergeFileSelector(resource, Long.MAX_VALUE);
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
                    10
                        + "seq"
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + i
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 10
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + ".tsfile"));
        TsFileResource fileResource = new TsFileResource(file);
        fileResource.setClosed(true);
        prepareFile(fileResource, i, 1, 0);
        seqList.add(fileResource);
      }
      int unseqFileNum = 3;
      // 3 unseq files [0,0] [0,99] [99,99]
      for (int i = 0; i < unseqFileNum; i++) {
        File file =
            new File(
                TestConstant.BASE_OUTPUT_PATH.concat(
                    10
                        + "unseq"
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + i
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 10
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + ".tsfile"));
        TsFileResource fileResource = new TsFileResource(file);
        fileResource.setClosed(true);
        unseqList.add(fileResource);
      }
      prepareFile(unseqList.get(0), 0, 1, 10);
      prepareFile(unseqList.get(1), 0, 100, 20);
      prepareFile(unseqList.get(2), 99, 1, 30);

      MergeResource resource = new MergeResource(seqList, unseqList);
      // the budget is enough to select unseq0 and unseq2, but not unseq1
      // the first selection should only contain seq0 and unseq0
      IMergeFileSelector mergeFileSelector = new MaxFileMergeFileSelector(resource, 29000);
      List[] result = mergeFileSelector.select();
      assertEquals(1, result[0].size());
      assertEquals(1, result[1].size());
      assertEquals(seqList.get(0), result[0].get(0));
      assertEquals(unseqList.get(0), result[1].get(0));
      resource.clear();

      resource =
          new MergeResource(
              seqList.subList(1, seqList.size()), unseqList.subList(1, unseqList.size()));
      // the second selection should be empty
      mergeFileSelector = new MaxFileMergeFileSelector(resource, 29000);
      result = mergeFileSelector.select();
      assertEquals(0, result.length);
      resource.clear();
    } finally {
      removeFiles(seqList, unseqList);
    }
  }
}
