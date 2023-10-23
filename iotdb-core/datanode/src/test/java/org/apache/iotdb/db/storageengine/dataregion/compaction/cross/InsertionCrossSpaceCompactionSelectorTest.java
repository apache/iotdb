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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.RewriteCrossSpaceCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.CrossSpaceCompactionCandidate;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.XXXXCrossCompactionTaskResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class InsertionCrossSpaceCompactionSelectorTest extends AbstractCompactionTest {

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
  public void testSimpleInsertionCompaction() throws IOException {
    String d1 = "root.testsg.d1";
    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    TsFileResource seqResource2 = createTsFileResource("3-3-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 40);
    seqResource2.updateEndTime(d1, 50);
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    TsFileResource unseqResource1 = createTsFileResource("5-5-1-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 30);
    unseqResource1.updateEndTime(d1, 35);
    unseqResources.add(unseqResource1);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    XXXXCrossCompactionTaskResource result =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, result.toInsertUnSeqFile);
    Assert.assertEquals(seqResource1, result.prevSeqFile);
    Assert.assertEquals(seqResource2, result.nextSeqFile);
  }

  @Test
  public void testSimpleInsertionCompactionWithMultiUnseqFiles() throws IOException {
    String d1 = "root.testsg.d1";
    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    TsFileResource seqResource2 = createTsFileResource("3-3-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 40);
    seqResource2.updateEndTime(d1, 50);
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    TsFileResource unseqResource1 = createTsFileResource("5-5-1-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 30);
    unseqResource1.updateEndTime(d1, 35);
    unseqResources.add(unseqResource1);
    TsFileResource unseqResource2 = createTsFileResource("6-6-1-0.tsfile", false);
    unseqResource2.updateStartTime(d1, 32);
    unseqResource2.updateEndTime(d1, 37);
    unseqResources.add(unseqResource2);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    XXXXCrossCompactionTaskResource result =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, result.toInsertUnSeqFile);
    Assert.assertEquals(seqResource1, result.prevSeqFile);
    Assert.assertEquals(seqResource2, result.nextSeqFile);
  }

  @Test
  public void testSimpleInsertionCompactionWithFirstUnseqFileCannotSelect() throws IOException {
    String d1 = "root.testsg.d1";
    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    TsFileResource seqResource2 = createTsFileResource("3-3-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 40);
    seqResource2.updateEndTime(d1, 50);
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    // overlap
    TsFileResource unseqResource1 = createTsFileResource("5-5-1-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 10);
    unseqResource1.updateEndTime(d1, 35);
    unseqResources.add(unseqResource1);
    TsFileResource unseqResource2 = createTsFileResource("6-6-1-0.tsfile", false);
    unseqResource2.updateStartTime(d1, 32);
    unseqResource2.updateEndTime(d1, 37);
    unseqResources.add(unseqResource2);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    XXXXCrossCompactionTaskResource result =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource2, result.toInsertUnSeqFile);
    Assert.assertEquals(seqResource1, result.prevSeqFile);
    Assert.assertEquals(seqResource2, result.nextSeqFile);
    Assert.assertEquals(unseqResource1, result.firstUnSeqFileInParitition);
  }

  @Test
  public void testSimpleInsertionCompactionWithFirstUnseqFileInvalid() throws IOException {
    String d1 = "root.testsg.d1";
    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    TsFileResource seqResource2 = createTsFileResource("3-3-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 40);
    seqResource2.updateEndTime(d1, 50);
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    // overlap
    TsFileResource unseqResource1 = createTsFileResource("5-5-1-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 10);
    unseqResource1.updateEndTime(d1, 35);
    unseqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);
    unseqResources.add(unseqResource1);
    TsFileResource unseqResource2 = createTsFileResource("6-6-1-0.tsfile", false);
    unseqResource2.updateStartTime(d1, 32);
    unseqResource2.updateEndTime(d1, 37);
    unseqResources.add(unseqResource2);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    XXXXCrossCompactionTaskResource result =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertNull(result);
  }

  @Test
  public void testSimpleInsertionCompactionWithFirstTwoUnseqFileCannotSelect() throws IOException {
    String d1 = "root.testsg.d1";
    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    TsFileResource seqResource2 = createTsFileResource("3-3-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 40);
    seqResource2.updateEndTime(d1, 50);
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    // overlap
    TsFileResource unseqResource1 = createTsFileResource("5-5-1-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 10);
    unseqResource1.updateEndTime(d1, 35);
    unseqResources.add(unseqResource1);
    TsFileResource unseqResource2 = createTsFileResource("6-6-1-0.tsfile", false);
    unseqResource2.updateStartTime(d1, 12);
    unseqResource2.updateEndTime(d1, 37);
    unseqResources.add(unseqResource2);
    TsFileResource unseqResource3 = createTsFileResource("7-7-1-0.tsfile", false);
    unseqResource3.updateStartTime(d1, 32);
    unseqResource3.updateEndTime(d1, 37);
    unseqResources.add(unseqResource3);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    XXXXCrossCompactionTaskResource result =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource3, result.toInsertUnSeqFile);
    Assert.assertEquals(seqResource1, result.prevSeqFile);
    Assert.assertEquals(seqResource2, result.nextSeqFile);
    Assert.assertEquals(unseqResource1, result.firstUnSeqFileInParitition);
  }

  @Test
  public void testSimpleInsertionCompactionWithUnseqDeviceNotExistInSeqSpace() throws IOException {
    String d1 = "root.testsg.d1", d2 = "root.testsg.d2";
    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    TsFileResource seqResource2 = createTsFileResource("3-3-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 40);
    seqResource2.updateEndTime(d1, 50);
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    TsFileResource unseqResource1 = createTsFileResource("5-5-1-0.tsfile", false);
    unseqResource1.updateStartTime(d2, 30);
    unseqResource1.updateEndTime(d2, 35);
    unseqResources.add(unseqResource1);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    XXXXCrossCompactionTaskResource result =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, result.toInsertUnSeqFile);
    Assert.assertNull(result.prevSeqFile);
    Assert.assertEquals(seqResource1, result.nextSeqFile);
  }

  @Test
  public void testSimpleInsertionCompactionWithUnseqFileInsertFirstInSeqSpace() throws IOException {
    String d1 = "root.testsg.d1";
    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    TsFileResource seqResource2 = createTsFileResource("3-3-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 40);
    seqResource2.updateEndTime(d1, 50);
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    TsFileResource unseqResource1 = createTsFileResource("5-5-1-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 3);
    unseqResource1.updateEndTime(d1, 5);
    unseqResources.add(unseqResource1);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    XXXXCrossCompactionTaskResource result =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, result.toInsertUnSeqFile);
    Assert.assertNull(result.prevSeqFile);
    Assert.assertEquals(seqResource1, result.nextSeqFile);
  }

  @Test
  public void testSimpleInsertionCompactionWithUnseqFileInsertLastInSeqSpace() throws IOException {
    String d1 = "root.testsg.d1";
    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    TsFileResource seqResource2 = createTsFileResource("3-3-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 40);
    seqResource2.updateEndTime(d1, 50);
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    TsFileResource unseqResource1 = createTsFileResource("5-5-1-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 60);
    unseqResource1.updateEndTime(d1, 65);
    unseqResources.add(unseqResource1);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    XXXXCrossCompactionTaskResource result =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, result.toInsertUnSeqFile);
    Assert.assertEquals(seqResource2, result.prevSeqFile);
    Assert.assertNull(result.nextSeqFile);
  }

  @Test
  public void testSimpleInsertionCompactionWithCloseTimestamp() throws IOException {
    String d1 = "root.testsg.d1";
    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    TsFileResource seqResource2 = createTsFileResource("2-3-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 40);
    seqResource2.updateEndTime(d1, 50);
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    TsFileResource unseqResource1 = createTsFileResource("5-5-1-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 30);
    unseqResource1.updateEndTime(d1, 35);
    unseqResources.add(unseqResource1);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    XXXXCrossCompactionTaskResource result =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertNull(result);
  }

  @Test
  public void testSimpleInsertionCompactionWithOverlap() throws IOException {
    String d1 = "root.testsg.d1";
    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    TsFileResource seqResource2 = createTsFileResource("2-3-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 40);
    seqResource2.updateEndTime(d1, 50);
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    TsFileResource unseqResource1 = createTsFileResource("5-5-1-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 20);
    unseqResource1.updateEndTime(d1, 45);
    unseqResources.add(unseqResource1);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    XXXXCrossCompactionTaskResource result =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertNull(result);
  }

  @Test
  public void testSimpleInsertionCompactionWithPrevSeqFileInvalidCompactionCandidate()
      throws IOException {
    String d1 = "root.testsg.d1";
    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    seqResource1.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);
    TsFileResource seqResource2 = createTsFileResource("3-3-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 40);
    seqResource2.updateEndTime(d1, 50);
    TsFileResource seqResource3 = createTsFileResource("5-5-0-0.tsfile", true);
    seqResource3.updateStartTime(d1, 60);
    seqResource3.updateEndTime(d1, 70);
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    TsFileResource unseqResource1 = createTsFileResource("4-4-1-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 30);
    unseqResource1.updateEndTime(d1, 35);
    unseqResources.add(unseqResource1);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    XXXXCrossCompactionTaskResource result =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertNull(result);
  }

  @Test
  public void testSimpleInsertionCompactionWithNextSeqFileInvalidCompactionCandidate()
      throws IOException {
    String d1 = "root.testsg.d1";
    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    TsFileResource seqResource2 = createTsFileResource("3-3-0-0.tsfile", true);
    seqResource2.setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);
    seqResource2.updateStartTime(d1, 40);
    seqResource2.updateEndTime(d1, 50);
    TsFileResource seqResource3 = createTsFileResource("5-5-0-0.tsfile", true);
    seqResource3.updateStartTime(d1, 60);
    seqResource3.updateEndTime(d1, 70);
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    TsFileResource unseqResource1 = createTsFileResource("4-4-1-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 30);
    unseqResource1.updateEndTime(d1, 35);
    unseqResources.add(unseqResource1);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    XXXXCrossCompactionTaskResource result =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertNull(result);
  }

  @Test
  public void testSimpleInsertionCompactionWithManySeqFiles() throws IOException {
    String d1 = "root.testsg.d1";
    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    TsFileResource seqResource2 = createTsFileResource("3-3-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 30);
    seqResource2.updateEndTime(d1, 40);
    TsFileResource seqResource3 = createTsFileResource("5-5-0-0.tsfile", true);
    seqResource3.updateStartTime(d1, 50);
    seqResource3.updateEndTime(d1, 60);
    TsFileResource seqResource4 = createTsFileResource("7-7-0-0.tsfile", true);
    seqResource4.updateStartTime(d1, 70);
    seqResource4.updateEndTime(d1, 80);
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    seqResources.add(seqResource3);
    seqResources.add(seqResource4);
    TsFileResource unseqResource1 = createTsFileResource("9-9-0-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 42);
    unseqResource1.updateEndTime(d1, 45);
    unseqResources.add(unseqResource1);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    XXXXCrossCompactionTaskResource task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, task.toInsertUnSeqFile);
    Assert.assertEquals(seqResource2, task.prevSeqFile);
    Assert.assertEquals(seqResource3, task.nextSeqFile);
  }

  @Test
  public void testInsertionCompactionWithManySeqFilesManyDevices() throws IOException {
    String d1 = "root.testsg.d1";
    String d2 = "root.testsg.d2";
    String d3 = "root.testsg.d3";
    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    seqResource1.updateStartTime(d2, 15);
    seqResource1.updateEndTime(d2, 30);
    seqResource1.updateStartTime(d3, 1);
    seqResource1.updateEndTime(d3, 3);
    TsFileResource seqResource2 = createTsFileResource("3-3-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 30);
    seqResource2.updateEndTime(d1, 40);
    TsFileResource seqResource3 = createTsFileResource("5-5-0-0.tsfile", true);
    seqResource3.updateStartTime(d3, 21);
    seqResource3.updateEndTime(d3, 23);
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    seqResources.add(seqResource3);
    TsFileResource unseqResource1 = createTsFileResource("9-9-0-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 42);
    unseqResource1.updateEndTime(d1, 45);
    unseqResource1.updateStartTime(d2, 31);
    unseqResource1.updateEndTime(d2, 37);
    unseqResource1.updateStartTime(d3, 10);
    unseqResource1.updateEndTime(d3, 20);
    unseqResources.add(unseqResource1);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    XXXXCrossCompactionTaskResource task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, task.toInsertUnSeqFile);
    Assert.assertEquals(seqResource2, task.prevSeqFile);
    Assert.assertEquals(seqResource3, task.nextSeqFile);
  }

  @Test
  public void testInsertionCompactionWithManySeqFilesManyDevices2() throws IOException {
    String d1 = "root.testsg.d1";
    String d2 = "root.testsg.d2";
    String d3 = "root.testsg.d3";
    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    seqResource1.updateStartTime(d2, 15);
    seqResource1.updateEndTime(d2, 30);
    seqResource1.updateStartTime(d3, 1);
    seqResource1.updateEndTime(d3, 3);
    TsFileResource seqResource2 = createTsFileResource("3-3-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 30);
    seqResource2.updateEndTime(d1, 40);
    TsFileResource seqResource3 = createTsFileResource("5-5-0-0.tsfile", true);
    seqResource3.updateStartTime(d3, 21);
    seqResource3.updateEndTime(d3, 23);
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    seqResources.add(seqResource3);
    TsFileResource unseqResource1 = createTsFileResource("9-9-0-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 56);
    unseqResource1.updateEndTime(d1, 75);
    unseqResource1.updateStartTime(d2, 31);
    unseqResource1.updateEndTime(d2, 37);
    unseqResource1.updateStartTime(d3, 40);
    unseqResource1.updateEndTime(d3, 50);
    unseqResources.add(unseqResource1);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    XXXXCrossCompactionTaskResource task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, task.toInsertUnSeqFile);
    Assert.assertEquals(seqResource3, task.prevSeqFile);
    Assert.assertNull(task.nextSeqFile);
  }

  @Test
  public void testInsertionCompactionWithManySeqFilesManyDevices3() throws IOException {
    String d1 = "root.testsg.d1";
    String d2 = "root.testsg.d2";
    String d3 = "root.testsg.d3";
    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    seqResource1.updateStartTime(d2, 15);
    seqResource1.updateEndTime(d2, 30);
    seqResource1.updateStartTime(d3, 3);
    seqResource1.updateEndTime(d3, 5);
    TsFileResource seqResource2 = createTsFileResource("3-3-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 30);
    seqResource2.updateEndTime(d1, 40);
    TsFileResource seqResource3 = createTsFileResource("5-5-0-0.tsfile", true);
    seqResource3.updateStartTime(d3, 21);
    seqResource3.updateEndTime(d3, 23);
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    seqResources.add(seqResource3);
    TsFileResource unseqResource1 = createTsFileResource("9-9-0-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 1);
    unseqResource1.updateEndTime(d1, 2);
    unseqResource1.updateStartTime(d2, 1);
    unseqResource1.updateEndTime(d2, 2);
    unseqResource1.updateStartTime(d3, 1);
    unseqResource1.updateEndTime(d3, 2);
    unseqResources.add(unseqResource1);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    XXXXCrossCompactionTaskResource task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, task.toInsertUnSeqFile);
    Assert.assertNull(task.prevSeqFile);
    Assert.assertEquals(seqResource1, task.nextSeqFile);
  }

  @Test
  public void testInsertionCompactionWithManySeqFilesManyDevices4() throws IOException {
    String d1 = "root.testsg.d1";
    String d2 = "root.testsg.d2";
    String d3 = "root.testsg.d3";
    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    seqResource1.updateStartTime(d2, 10);
    seqResource1.updateEndTime(d2, 20);
    seqResource1.updateStartTime(d3, 10);
    seqResource1.updateEndTime(d3, 20);
    TsFileResource seqResource2 = createTsFileResource("3-3-0-0.tsfile", true);
    seqResource2.updateStartTime(d2, 30);
    seqResource2.updateEndTime(d2, 40);
    TsFileResource seqResource3 = createTsFileResource("5-5-0-0.tsfile", true);
    seqResource3.updateStartTime(d2, 51);
    seqResource3.updateEndTime(d2, 63);
    TsFileResource seqResource4 = createTsFileResource("7-7-0-0.tsfile", true);
    seqResource4.updateStartTime(d3, 60);
    seqResource4.updateEndTime(d3, 70);
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    seqResources.add(seqResource3);
    seqResources.add(seqResource4);
    TsFileResource unseqResource1 = createTsFileResource("9-9-0-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 30);
    unseqResource1.updateEndTime(d1, 40);
    unseqResource1.updateStartTime(d3, 30);
    unseqResource1.updateEndTime(d3, 40);
    unseqResources.add(unseqResource1);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    XXXXCrossCompactionTaskResource task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, task.toInsertUnSeqFile);
    Assert.assertEquals(seqResource1, task.prevSeqFile);
    Assert.assertEquals(seqResource2, task.nextSeqFile);
  }

  @Test
  public void testInsertionCompactionWithManySeqFilesManyDevices5() throws IOException {
    String d1 = "root.testsg.d1";
    String d2 = "root.testsg.d2";
    String d3 = "root.testsg.d3";
    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    seqResource1.updateStartTime(d2, 10);
    seqResource1.updateEndTime(d2, 20);
    seqResource1.updateStartTime(d3, 10);
    seqResource1.updateEndTime(d3, 20);
    TsFileResource seqResource2 = createTsFileResource("2-2-0-0.tsfile", true);
    seqResource2.updateStartTime(d2, 30);
    seqResource2.updateEndTime(d2, 40);
    TsFileResource seqResource3 = createTsFileResource("5-5-0-0.tsfile", true);
    seqResource3.updateStartTime(d2, 51);
    seqResource3.updateEndTime(d2, 63);
    TsFileResource seqResource4 = createTsFileResource("7-7-0-0.tsfile", true);
    seqResource4.updateStartTime(d3, 60);
    seqResource4.updateEndTime(d3, 70);
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    seqResources.add(seqResource3);
    seqResources.add(seqResource4);
    TsFileResource unseqResource1 = createTsFileResource("9-9-0-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 30);
    unseqResource1.updateEndTime(d1, 40);
    unseqResource1.updateStartTime(d3, 30);
    unseqResource1.updateEndTime(d3, 40);
    unseqResources.add(unseqResource1);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    XXXXCrossCompactionTaskResource task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, task.toInsertUnSeqFile);
    Assert.assertEquals(seqResource2, task.prevSeqFile);
    Assert.assertEquals(seqResource3, task.nextSeqFile);
  }

  @Test
  public void testInsertionCompactionWithManySeqFilesManyDevices6() throws IOException {
    String d1 = "root.testsg.d1";
    String d2 = "root.testsg.d2";
    String d3 = "root.testsg.d3";
    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    seqResource1.updateStartTime(d2, 10);
    seqResource1.updateEndTime(d2, 20);
    TsFileResource seqResource2 = createTsFileResource("2-2-0-0.tsfile", true);
    seqResource2.updateStartTime(d2, 30);
    seqResource2.updateEndTime(d2, 40);
    TsFileResource seqResource3 = createTsFileResource("5-5-0-0.tsfile", true);
    seqResource3.updateStartTime(d2, 51);
    seqResource3.updateEndTime(d2, 63);
    TsFileResource seqResource4 = createTsFileResource("7-7-0-0.tsfile", true);
    seqResource4.updateStartTime(d3, 60);
    seqResource4.updateEndTime(d3, 70);
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    seqResources.add(seqResource3);
    seqResources.add(seqResource4);
    TsFileResource unseqResource1 = createTsFileResource("9-9-0-0.tsfile", false);
    unseqResource1.updateStartTime(d3, 30);
    unseqResource1.updateEndTime(d3, 40);
    unseqResources.add(unseqResource1);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    XXXXCrossCompactionTaskResource task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, task.toInsertUnSeqFile);
    Assert.assertNull(task.prevSeqFile);
    Assert.assertEquals(seqResource1, task.nextSeqFile);
  }

  @Test
  public void testInsertionCompactionWithManySeqFilesManyDevicesWithOverlap() throws IOException {
    String d1 = "root.testsg.d1";
    String d2 = "root.testsg.d2";
    String d3 = "root.testsg.d3";
    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    seqResource1.updateStartTime(d2, 15);
    seqResource1.updateEndTime(d2, 30);
    seqResource1.updateStartTime(d3, 1);
    seqResource1.updateEndTime(d3, 3);
    TsFileResource seqResource2 = createTsFileResource("3-3-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 30);
    seqResource2.updateEndTime(d1, 40);
    TsFileResource seqResource3 = createTsFileResource("5-5-0-0.tsfile", true);
    seqResource3.updateStartTime(d3, 21);
    seqResource3.updateEndTime(d3, 23);
    TsFileResource seqResource4 = createTsFileResource("7-7-0-0.tsfile", true);
    seqResource4.updateStartTime(d1, 41);
    seqResource4.updateEndTime(d1, 41);
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    seqResources.add(seqResource3);
    seqResources.add(seqResource4);
    TsFileResource unseqResource1 = createTsFileResource("9-9-0-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 42);
    unseqResource1.updateEndTime(d1, 45);
    unseqResource1.updateStartTime(d2, 31);
    unseqResource1.updateEndTime(d2, 37);
    unseqResource1.updateStartTime(d3, 10);
    unseqResource1.updateEndTime(d3, 20);
    unseqResources.add(unseqResource1);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    XXXXCrossCompactionTaskResource task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertNull(task);
  }

  private TsFileResource createTsFileResource(String name, boolean seq) {
    String filePath = (seq ? SEQ_DIRS : UNSEQ_DIRS) + File.separator + name;
    TsFileResource resource = new TsFileResource();
    resource.setTimeIndex(new DeviceTimeIndex());
    resource.setFile(new File(filePath));
    resource.setStatusForTest(TsFileResourceStatus.NORMAL);
    resource.setSeq(seq);
    return resource;
  }
}
