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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InsertionCrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.RewriteCrossSpaceCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.CrossSpaceCompactionCandidate;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.InsertionCrossCompactionTaskResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Phaser;

import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.writeNonAlignedChunk;

public class InsertionCrossSpaceCompactionSelectorTest extends AbstractCompactionTest {

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    IoTDBDescriptor.getInstance().getConfig().setEnableCrossSpaceCompaction(true);
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    IoTDBDescriptor.getInstance().getConfig().setEnableCrossSpaceCompaction(false);
    super.tearDown();
  }

  @Test
  public void testSimpleInsertionCompaction() throws IOException, MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
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
    InsertionCrossCompactionTaskResource result =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, result.toInsertUnSeqFile);
    Assert.assertEquals(seqResource1, result.prevSeqFile);
    Assert.assertEquals(seqResource2, result.nextSeqFile);
  }

  @Test
  public void testSimpleInsertionCompactionWithMultiUnseqFiles()
      throws IOException, MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
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
    InsertionCrossCompactionTaskResource result =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, result.toInsertUnSeqFile);
    Assert.assertEquals(seqResource1, result.prevSeqFile);
    Assert.assertEquals(seqResource2, result.nextSeqFile);
  }

  @Test
  public void testSimpleInsertionCompactionWithFirstUnseqFileCannotSelect()
      throws IOException, MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
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
    InsertionCrossCompactionTaskResource result =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertFalse(result.isValid());
  }

  @Test
  public void testSimpleInsertionCompactionWithFirstUnseqFileInvalid()
      throws IOException, MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
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
    InsertionCrossCompactionTaskResource result =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertFalse(result.isValid());
  }

  @Test
  public void testSimpleInsertionCompactionWithFirstTwoUnseqFileCannotSelect()
      throws IOException, MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
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
    InsertionCrossCompactionTaskResource result =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertFalse(result.isValid());
  }

  @Test
  public void testSimpleInsertionCompactionWithUnseqDeviceNotExistInSeqSpace()
      throws IOException, MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1"), d2 = new PlainDeviceID("root.testsg.d2");
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
    InsertionCrossCompactionTaskResource result =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, result.toInsertUnSeqFile);
    Assert.assertNull(result.prevSeqFile);
    Assert.assertEquals(seqResource1, result.nextSeqFile);
  }

  @Test
  public void testSimpleInsertionCompactionWithUnseqFileInsertFirstInSeqSpace()
      throws IOException, MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
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
    InsertionCrossCompactionTaskResource result =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, result.toInsertUnSeqFile);
    Assert.assertNull(result.prevSeqFile);
    Assert.assertEquals(seqResource1, result.nextSeqFile);
  }

  @Test
  public void testSimpleInsertionCompactionWithUnseqFileInsertLastInSeqSpace()
      throws IOException, MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
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
    InsertionCrossCompactionTaskResource result =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, result.toInsertUnSeqFile);
    Assert.assertEquals(seqResource2, result.prevSeqFile);
    Assert.assertNull(result.nextSeqFile);
  }

  @Test
  public void testSimpleInsertionCompactionWithCloseTimestamp() throws IOException, MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
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
    InsertionCrossCompactionTaskResource result =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertFalse(result.isValid());
  }

  @Test
  public void testSimpleInsertionCompactionWithOverlap() throws IOException, MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
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
    InsertionCrossCompactionTaskResource result =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertFalse(result.isValid());
  }

  @Test
  public void testSimpleInsertionCompactionWithPrevSeqFileInvalidCompactionCandidate()
      throws IOException, MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
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
    InsertionCrossCompactionTaskResource result =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertFalse(result.isValid());
  }

  @Test
  public void testSimpleInsertionCompactionWithNextSeqFileInvalidCompactionCandidate()
      throws IOException, MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
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
    InsertionCrossCompactionTaskResource result =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertFalse(result.isValid());
  }

  @Test
  public void testSimpleInsertionCompactionWithManySeqFiles() throws IOException, MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
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
    InsertionCrossCompactionTaskResource task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, task.toInsertUnSeqFile);
    Assert.assertEquals(seqResource2, task.prevSeqFile);
    Assert.assertEquals(seqResource3, task.nextSeqFile);
  }

  @Test
  public void testInsertionCompactionWithManySeqFilesManyDevices()
      throws IOException, MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
    IDeviceID d2 = new PlainDeviceID("root.testsg.d2");
    IDeviceID d3 = new PlainDeviceID("root.testsg.d3");
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
    InsertionCrossCompactionTaskResource task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, task.toInsertUnSeqFile);
    Assert.assertEquals(seqResource2, task.prevSeqFile);
    Assert.assertEquals(seqResource3, task.nextSeqFile);
  }

  @Test
  public void testInsertionCompactionWithManySeqFilesManyDevices2()
      throws IOException, MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
    IDeviceID d2 = new PlainDeviceID("root.testsg.d2");
    IDeviceID d3 = new PlainDeviceID("root.testsg.d3");
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
    InsertionCrossCompactionTaskResource task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, task.toInsertUnSeqFile);
    Assert.assertEquals(seqResource3, task.prevSeqFile);
    Assert.assertNull(task.nextSeqFile);
  }

  @Test
  public void testInsertLast1() throws IOException, MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
    IDeviceID d2 = new PlainDeviceID("root.testsg.d2");
    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    seqResource1.updateStartTime(d2, 20);
    seqResource1.updateEndTime(d2, 30);
    TsFileResource seqResource2 = createTsFileResource("3-3-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 30);
    seqResource2.updateEndTime(d1, 40);
    seqResource2.updateStartTime(d2, 40);
    seqResource2.updateEndTime(d2, 50);
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    TsFileResource unseqResource1 = createTsFileResource("4-4-1-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 42);
    unseqResource1.updateEndTime(d1, 45);
    unseqResource1.updateStartTime(d2, 31);
    unseqResource1.updateEndTime(d2, 37);
    unseqResources.add(unseqResource1);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    InsertionCrossCompactionTaskResource result =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertFalse(result.isValid());
  }

  @Test
  public void testInsertLast2() throws IOException, MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
    IDeviceID d2 = new PlainDeviceID("root.testsg.d2");
    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    seqResource1.updateStartTime(d2, 20);
    seqResource1.updateEndTime(d2, 30);
    TsFileResource seqResource2 = createTsFileResource("3-3-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 30);
    seqResource2.updateEndTime(d1, 40);
    seqResource2.updateStartTime(d2, 40);
    seqResource2.updateEndTime(d2, 50);
    TsFileResource seqResource3 = createTsFileResource("5-5-0-0.tsfile", true);
    seqResource3.updateStartTime(d1, 50);
    seqResource3.updateEndTime(d1, 60);
    seqResource3.updateStartTime(d2, 60);
    seqResource3.updateEndTime(d2, 70);
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    seqResources.add(seqResource3);
    TsFileResource unseqResource1 = createTsFileResource("4-4-1-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 42);
    unseqResource1.updateEndTime(d1, 45);
    unseqResource1.updateStartTime(d2, 31);
    unseqResource1.updateEndTime(d2, 37);
    unseqResources.add(unseqResource1);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    InsertionCrossCompactionTaskResource result =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertFalse(result.isValid());
  }

  @Test
  public void testInsertFirst() throws IOException, MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
    IDeviceID d2 = new PlainDeviceID("root.testsg.d2");
    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    seqResource1.updateStartTime(d2, 20);
    seqResource1.updateEndTime(d2, 30);
    seqResources.add(seqResource1);
    TsFileResource unseqResource1 = createTsFileResource("4-4-1-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 30);
    unseqResource1.updateEndTime(d1, 40);
    unseqResource1.updateStartTime(d2, 10);
    unseqResource1.updateEndTime(d2, 15);
    unseqResources.add(unseqResource1);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    InsertionCrossCompactionTaskResource result =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertFalse(result.isValid());
  }

  @Test
  public void testInsertionCompactionWithManySeqFilesManyDevices3()
      throws IOException, MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
    IDeviceID d2 = new PlainDeviceID("root.testsg.d2");
    IDeviceID d3 = new PlainDeviceID("root.testsg.d3");
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
    InsertionCrossCompactionTaskResource task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, task.toInsertUnSeqFile);
    Assert.assertNull(task.prevSeqFile);
    Assert.assertEquals(seqResource1, task.nextSeqFile);
  }

  @Test
  public void testInsertionCompactionWithManySeqFilesManyDevices4()
      throws IOException, MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
    IDeviceID d2 = new PlainDeviceID("root.testsg.d2");
    IDeviceID d3 = new PlainDeviceID("root.testsg.d3");
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
    InsertionCrossCompactionTaskResource task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, task.toInsertUnSeqFile);
    Assert.assertEquals(seqResource1, task.prevSeqFile);
    Assert.assertEquals(seqResource2, task.nextSeqFile);
  }

  @Test
  public void testInsertionCompactionWithManySeqFilesManyDevices5()
      throws IOException, MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
    IDeviceID d2 = new PlainDeviceID("root.testsg.d2");
    IDeviceID d3 = new PlainDeviceID("root.testsg.d3");
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
    InsertionCrossCompactionTaskResource task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, task.toInsertUnSeqFile);
    Assert.assertEquals(seqResource2, task.prevSeqFile);
    Assert.assertEquals(seqResource3, task.nextSeqFile);
  }

  @Test
  public void testInsertionCompactionWithManySeqFilesManyDevices6()
      throws IOException, MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
    IDeviceID d2 = new PlainDeviceID("root.testsg.d2");
    IDeviceID d3 = new PlainDeviceID("root.testsg.d3");
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
    InsertionCrossCompactionTaskResource task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(unseqResource1, task.toInsertUnSeqFile);
    Assert.assertNull(task.prevSeqFile);
    Assert.assertEquals(seqResource1, task.nextSeqFile);
  }

  @Test
  public void testInsertionCompactionWithManySeqFilesManyDevicesWithOverlap()
      throws IOException, MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
    IDeviceID d2 = new PlainDeviceID("root.testsg.d2");
    IDeviceID d3 = new PlainDeviceID("root.testsg.d3");
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
    InsertionCrossCompactionTaskResource task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertFalse(task.isValid());
  }

  @Test
  public void testInsertionSelectorWithNoSeqFiles() throws MergeException, IOException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
    IDeviceID d2 = new PlainDeviceID("root.testsg.d2");

    TsFileResource unseqResource1 = createTsFileResource("1-1-0-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 10);
    unseqResource1.updateEndTime(d1, 20);
    unseqResource1.updateStartTime(d2, 20);
    unseqResource1.updateEndTime(d2, 30);
    unseqResource1.serialize();
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(unseqResource1.getTsFile())) {
      // write d1
      tsFileIOWriter.startChunkGroup(d1);
      MeasurementSchema schema =
          new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY);
      ChunkWriterImpl iChunkWriter = new ChunkWriterImpl(schema);
      List<TimeRange> pages = new ArrayList<>();
      pages.add(new TimeRange(10, 20));
      writeNonAlignedChunk(iChunkWriter, tsFileIOWriter, pages, false);
      tsFileIOWriter.endChunkGroup();

      // write d2
      tsFileIOWriter.startChunkGroup(d2);
      pages.clear();
      pages.add(new TimeRange(20, 30));
      writeNonAlignedChunk(iChunkWriter, tsFileIOWriter, pages, false);
      tsFileIOWriter.endChunkGroup();
      tsFileIOWriter.endFile();
    }

    TsFileResource unseqResource2 = createTsFileResource("2-2-0-0.tsfile", false);
    unseqResource2.updateStartTime(d1, 30);
    unseqResource2.updateEndTime(d1, 40);
    unseqResource2.updateStartTime(d2, 40);
    unseqResource2.updateEndTime(d2, 50);
    unseqResource2.serialize();
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(unseqResource2.getTsFile())) {
      // write d1
      tsFileIOWriter.startChunkGroup(d1);
      MeasurementSchema schema =
          new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY);
      ChunkWriterImpl iChunkWriter = new ChunkWriterImpl(schema);
      List<TimeRange> pages = new ArrayList<>();
      pages.add(new TimeRange(30, 40));
      writeNonAlignedChunk(iChunkWriter, tsFileIOWriter, pages, false);
      tsFileIOWriter.endChunkGroup();

      // write d2
      tsFileIOWriter.startChunkGroup(d2);
      pages.clear();
      pages.add(new TimeRange(40, 50));
      writeNonAlignedChunk(iChunkWriter, tsFileIOWriter, pages, false);
      tsFileIOWriter.endChunkGroup();
      tsFileIOWriter.endFile();
    }

    TsFileResource unseqResource3 = createTsFileResource("3-3-0-0.tsfile", false);
    unseqResource3.updateStartTime(d1, 50);
    unseqResource3.updateEndTime(d1, 60);
    unseqResource3.updateStartTime(d2, 60);
    unseqResource3.updateEndTime(d2, 70);
    unseqResource3.serialize();
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(unseqResource3.getTsFile())) {
      // write d1
      tsFileIOWriter.startChunkGroup(d1);
      MeasurementSchema schema =
          new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY);
      ChunkWriterImpl iChunkWriter = new ChunkWriterImpl(schema);
      List<TimeRange> pages = new ArrayList<>();
      pages.add(new TimeRange(50, 60));
      writeNonAlignedChunk(iChunkWriter, tsFileIOWriter, pages, false);
      tsFileIOWriter.endChunkGroup();

      // write d2
      tsFileIOWriter.startChunkGroup(d2);
      pages.clear();
      pages.add(new TimeRange(60, 70));
      writeNonAlignedChunk(iChunkWriter, tsFileIOWriter, pages, false);
      tsFileIOWriter.endChunkGroup();
      tsFileIOWriter.endFile();
    }

    unseqResources.add(unseqResource1);
    unseqResources.add(unseqResource2);
    unseqResources.add(unseqResource3);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    InsertionCrossCompactionTaskResource taskResource;

    int i = 1;
    while (tsFileManager.getTsFileList(false).size() > 0) {
      taskResource =
          selector.selectOneInsertionTask(
              new CrossSpaceCompactionCandidate(
                  tsFileManager.getTsFileList(true), tsFileManager.getTsFileList(false)));
      Assert.assertTrue(taskResource.isValid());
      Assert.assertEquals(
          tsFileManager.getTsFileList(false).get(0), taskResource.firstUnSeqFileInParitition);
      Assert.assertEquals(
          tsFileManager.getTsFileList(false).get(0), taskResource.toInsertUnSeqFile);
      Assert.assertEquals(null, taskResource.nextSeqFile);
      if (i == 1) {
        Assert.assertEquals(null, taskResource.prevSeqFile);
      } else if (i == 2) {
        Assert.assertEquals(tsFileManager.getTsFileList(true).get(0), taskResource.prevSeqFile);
      } else {
        Assert.assertEquals(tsFileManager.getTsFileList(true).get(1), taskResource.prevSeqFile);
      }
      InsertionCrossSpaceCompactionTask task =
          new InsertionCrossSpaceCompactionTask(
              new Phaser(1),
              0,
              tsFileManager,
              taskResource,
              tsFileManager.getNextCompactionTaskId());
      task.start();
      i++;
    }
    Assert.assertEquals(3, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(0, tsFileManager.getTsFileList(false).size());
  }

  @Test
  public void testInsertionSelectorWithNoUnseqFiles() throws MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
    IDeviceID d2 = new PlainDeviceID("root.testsg.d2");

    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    seqResource1.updateStartTime(d2, 20);
    seqResource1.updateEndTime(d2, 30);
    TsFileResource seqResource2 = createTsFileResource("2-2-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 30);
    seqResource2.updateEndTime(d1, 40);
    seqResource2.updateStartTime(d2, 40);
    seqResource2.updateEndTime(d2, 50);
    TsFileResource seqResource3 = createTsFileResource("3-3-0-0.tsfile", true);
    seqResource3.updateStartTime(d1, 50);
    seqResource3.updateEndTime(d1, 60);
    seqResource3.updateStartTime(d2, 60);
    seqResource3.updateEndTime(d2, 70);

    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    seqResources.add(seqResource3);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    InsertionCrossCompactionTaskResource task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertNull(task.toInsertUnSeqFile);
    Assert.assertNull(task.prevSeqFile);
    Assert.assertNull(task.nextSeqFile);
    Assert.assertNull(task.firstUnSeqFileInParitition);
  }

  // Multiple unseq files with multiple devices overlap with different seq files. None unseq file
  // can be inserted into seq file list.
  @Test
  public void testInsertionSelectorWithOverlapUnseqFile() throws MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
    IDeviceID d2 = new PlainDeviceID("root.testsg.d2");

    // 1. prevSeqFileIndex == nextSeqFileIndex
    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    seqResource1.updateStartTime(d2, 20);
    seqResource1.updateEndTime(d2, 30);
    TsFileResource seqResource2 = createTsFileResource("2-2-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 30);
    seqResource2.updateEndTime(d1, 40);
    seqResource2.updateStartTime(d2, 40);
    seqResource2.updateEndTime(d2, 50);
    TsFileResource seqResource3 = createTsFileResource("3-3-0-0.tsfile", true);
    seqResource3.updateStartTime(d1, 50);
    seqResource3.updateEndTime(d1, 60);
    seqResource3.updateStartTime(d2, 60);
    seqResource3.updateEndTime(d2, 70);

    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    seqResources.add(seqResource3);

    TsFileResource unseqResource1 = createTsFileResource("9-9-0-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 42);
    unseqResource1.updateEndTime(d1, 45);
    unseqResource1.updateStartTime(d2, 31);
    unseqResource1.updateEndTime(d2, 37);
    unseqResources.add(unseqResource1);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    InsertionCrossCompactionTaskResource task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(null, task.toInsertUnSeqFile);
    Assert.assertEquals(null, task.prevSeqFile);
    Assert.assertEquals(null, task.nextSeqFile);
    Assert.assertEquals(null, task.firstUnSeqFileInParitition);

    // 2. prevSeqFileIndex > nextSeqFileIndex
    unseqResources.remove(unseqResource1);
    unseqResource1 = createTsFileResource("9-9-0-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 62);
    unseqResource1.updateEndTime(d1, 65);
    unseqResource1.updateStartTime(d2, 31);
    unseqResource1.updateEndTime(d2, 37);
    unseqResources.add(unseqResource1);

    task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(null, task.toInsertUnSeqFile);
    Assert.assertEquals(null, task.prevSeqFile);
    Assert.assertEquals(null, task.nextSeqFile);
    Assert.assertEquals(null, task.firstUnSeqFileInParitition);
  }

  @Test
  public void testInsertionIntoCompactingSeqFiles() throws IOException, MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
    IDeviceID d2 = new PlainDeviceID("root.testsg.d2");
    IDeviceID d3 = new PlainDeviceID("root.testsg.d3");

    TsFileResource seqResource1 = createTsFileResource("100-100-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    seqResource1.updateStartTime(d2, 20);
    seqResource1.updateEndTime(d2, 30);
    seqResource1.serialize();
    createTsFileByResource(seqResource1);

    TsFileResource seqResource2 = createTsFileResource("200-200-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 30);
    seqResource2.updateEndTime(d1, 40);
    seqResource2.updateStartTime(d2, 40);
    seqResource2.updateEndTime(d2, 50);
    seqResource2.serialize();
    createTsFileByResource(seqResource2);

    TsFileResource seqResource3 = createTsFileResource("300-300-0-0.tsfile", true);
    seqResource3.updateStartTime(d3, 50);
    seqResource3.updateEndTime(d3, 60);
    seqResource3.serialize();
    createTsFileByResource(seqResource3);

    TsFileResource seqResource4 = createTsFileResource("400-400-0-0.tsfile", true);
    seqResource4.updateStartTime(d3, 70);
    seqResource4.updateEndTime(d3, 80);
    seqResource4.serialize();
    createTsFileByResource(seqResource4);

    TsFileResource seqResource5 = createTsFileResource("500-500-0-0.tsfile", true);
    seqResource5.updateStartTime(d2, 100);
    seqResource5.updateEndTime(d2, 110);
    seqResource5.serialize();
    createTsFileByResource(seqResource5);

    TsFileResource seqResource6 = createTsFileResource("600-600-0-0.tsfile", true);
    seqResource6.updateStartTime(d1, 110);
    seqResource6.updateEndTime(d1, 120);
    seqResource6.updateStartTime(d2, 120);
    seqResource6.updateEndTime(d2, 130);
    seqResource6.serialize();
    createTsFileByResource(seqResource6);

    TsFileResource seqResource7 = createTsFileResource("700-700-0-0.tsfile", true);
    seqResource7.updateStartTime(d1, 130);
    seqResource7.updateEndTime(d1, 140);
    seqResource7.updateStartTime(d2, 140);
    seqResource7.updateEndTime(d2, 150);
    seqResource7.serialize();
    createTsFileByResource(seqResource7);

    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    seqResources.add(seqResource3);
    seqResources.add(seqResource4);
    seqResources.add(seqResource5);
    seqResources.add(seqResource6);
    seqResources.add(seqResource7);

    TsFileResource unseqResource1 = createTsFileResource("9-9-0-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 42);
    unseqResource1.updateEndTime(d1, 45);
    unseqResource1.updateStartTime(d2, 61);
    unseqResource1.updateEndTime(d2, 97);
    unseqResource1.serialize();
    createTsFileByResource(unseqResource1);
    unseqResources.add(unseqResource1);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    InsertionCrossCompactionTaskResource task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(tsFileManager.getTsFileList(false).get(0), task.toInsertUnSeqFile);
    Assert.assertEquals(tsFileManager.getTsFileList(true).get(1), task.prevSeqFile);
    Assert.assertEquals(tsFileManager.getTsFileList(true).get(2), task.nextSeqFile);
    Assert.assertEquals(tsFileManager.getTsFileList(false).get(0), task.firstUnSeqFileInParitition);

    // seq file 1 ~ 3 is compaction candidate
    for (int i = 0; i < 3; i++) {
      seqResources.get(i).setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);
    }
    task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(tsFileManager.getTsFileList(false).get(0), task.toInsertUnSeqFile);
    Assert.assertEquals(tsFileManager.getTsFileList(true).get(3), task.prevSeqFile);
    Assert.assertEquals(tsFileManager.getTsFileList(true).get(4), task.nextSeqFile);
    Assert.assertEquals(tsFileManager.getTsFileList(false).get(0), task.firstUnSeqFileInParitition);

    // all seq file are compacting
    for (TsFileResource resource : seqResources) {
      resource.setStatusForTest(TsFileResourceStatus.COMPACTING);
    }
    task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(null, task.toInsertUnSeqFile);
    Assert.assertEquals(null, task.prevSeqFile);
    Assert.assertEquals(null, task.nextSeqFile);
    Assert.assertEquals(null, task.firstUnSeqFileInParitition);
  }

  @Test
  public void testInsertionSelectorWithUnclosedSeqFile() throws MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
    IDeviceID d2 = new PlainDeviceID("root.testsg.d2");

    TsFileResource seqResource1 = createTsFileResource("100-100-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    seqResource1.updateStartTime(d2, 20);
    seqResource1.updateEndTime(d2, 30);

    TsFileResource seqResource2 = createTsFileResource("200-200-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 30);
    seqResource2.updateEndTime(d1, 40);
    seqResource2.updateStartTime(d2, 40);
    seqResource2.updateEndTime(d2, 50);

    TsFileResource seqResource3 = createTsFileResource("300-300-0-0.tsfile", true);
    seqResource3.updateStartTime(d1, 50);
    seqResource3.updateEndTime(d1, 60);
    seqResource3.updateStartTime(d2, 60);
    seqResource3.updateEndTime(d2, 70);
    seqResource3.setStatusForTest(TsFileResourceStatus.UNCLOSED);

    TsFileResource seqResource4 = createTsFileResource("600-600-0-0.tsfile", true);
    seqResource4.updateStartTime(d1, 110);
    seqResource4.updateEndTime(d1, 120);
    seqResource4.updateStartTime(d2, 120);
    seqResource4.updateEndTime(d2, 130);
    seqResource4.setStatusForTest(TsFileResourceStatus.UNCLOSED);

    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    seqResources.add(seqResource3);
    seqResources.add(seqResource4);

    // overlap with unclosed seq file
    TsFileResource unseqResource1 = createTsFileResource("9-9-0-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 42);
    unseqResource1.updateEndTime(d1, 55);
    unseqResource1.updateStartTime(d2, 61);
    unseqResource1.updateEndTime(d2, 97);

    // nonOverlap with unclosed seq file
    TsFileResource unseqResource2 = createTsFileResource("10-10-0-0.tsfile", false);
    unseqResource2.updateStartTime(d1, 42);
    unseqResource2.updateEndTime(d1, 45);
    unseqResource2.updateStartTime(d2, 51);
    unseqResource2.updateEndTime(d2, 57);

    unseqResources.add(unseqResource1);
    unseqResources.add(unseqResource2);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    InsertionCrossCompactionTaskResource task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertNull(task.toInsertUnSeqFile);
    Assert.assertNull(task.prevSeqFile);
    Assert.assertNull(task.nextSeqFile);
    Assert.assertNull(task.firstUnSeqFileInParitition);

    // nonOverlap with unclosed seq file, in the gap of the two unclosed seq file
    unseqResources.remove(unseqResource2);
    TsFileResource unseqResource3 = createTsFileResource("11-11-0-0.tsfile", false);
    unseqResource3.updateStartTime(d1, 80);
    unseqResource3.updateEndTime(d1, 90);
    unseqResource3.updateStartTime(d2, 80);
    unseqResource3.updateEndTime(d2, 90);

    unseqResources.add(unseqResource1);
    unseqResources.add(unseqResource3);

    task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertNull(task.toInsertUnSeqFile);
    Assert.assertNull(task.prevSeqFile);
    Assert.assertNull(task.nextSeqFile);
    Assert.assertNull(task.firstUnSeqFileInParitition);
  }

  @Test
  public void testInsertionSelectorWithUnclosedUnSeqFile() throws MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
    IDeviceID d2 = new PlainDeviceID("root.testsg.d2");

    TsFileResource seqResource1 = createTsFileResource("100-100-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    seqResource1.updateStartTime(d2, 20);
    seqResource1.updateEndTime(d2, 30);

    TsFileResource seqResource2 = createTsFileResource("200-200-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 30);
    seqResource2.updateEndTime(d1, 40);
    seqResource2.updateStartTime(d2, 40);
    seqResource2.updateEndTime(d2, 50);

    TsFileResource seqResource3 = createTsFileResource("300-300-0-0.tsfile", true);
    seqResource3.updateStartTime(d1, 50);
    seqResource3.updateEndTime(d1, 60);
    seqResource3.updateStartTime(d2, 60);
    seqResource3.updateEndTime(d2, 70);

    TsFileResource seqResource4 = createTsFileResource("600-600-0-0.tsfile", true);
    seqResource4.updateStartTime(d1, 110);
    seqResource4.updateEndTime(d1, 120);
    seqResource4.updateStartTime(d2, 120);
    seqResource4.updateEndTime(d2, 130);

    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    seqResources.add(seqResource3);
    seqResources.add(seqResource4);

    // overlap with seq file
    TsFileResource unseqResource1 = createTsFileResource("9-9-0-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 42);
    unseqResource1.updateEndTime(d1, 55);
    unseqResource1.updateStartTime(d2, 61);
    unseqResource1.updateEndTime(d2, 97);
    unseqResource1.setStatusForTest(TsFileResourceStatus.UNCLOSED);

    // nonOverlap with seq file
    TsFileResource unseqResource2 = createTsFileResource("10-10-0-0.tsfile", false);
    unseqResource2.updateStartTime(d1, 42);
    unseqResource2.updateEndTime(d1, 45);
    unseqResource2.updateStartTime(d2, 51);
    unseqResource2.updateEndTime(d2, 57);
    unseqResource1.setStatusForTest(TsFileResourceStatus.UNCLOSED);

    unseqResources.add(unseqResource1);
    unseqResources.add(unseqResource2);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    InsertionCrossCompactionTaskResource task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertNull(task.toInsertUnSeqFile);
    Assert.assertNull(task.prevSeqFile);
    Assert.assertNull(task.nextSeqFile);
    Assert.assertNull(task.firstUnSeqFileInParitition);
  }

  @Test
  public void testInsertionSelectorWithNoSeqFilesAndFileTimeIndex()
      throws MergeException, IOException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
    IDeviceID d2 = new PlainDeviceID("root.testsg.d2");

    TsFileResource unseqResource1 = createTsFileResource("1-1-0-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 10);
    unseqResource1.updateEndTime(d1, 20);
    unseqResource1.updateStartTime(d2, 20);
    unseqResource1.updateEndTime(d2, 30);
    unseqResource1.serialize();
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(unseqResource1.getTsFile())) {
      // write d1
      tsFileIOWriter.startChunkGroup(d1);
      MeasurementSchema schema =
          new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY);
      ChunkWriterImpl iChunkWriter = new ChunkWriterImpl(schema);
      List<TimeRange> pages = new ArrayList<>();
      pages.add(new TimeRange(10, 20));
      writeNonAlignedChunk(iChunkWriter, tsFileIOWriter, pages, false);
      tsFileIOWriter.endChunkGroup();

      // write d2
      tsFileIOWriter.startChunkGroup(d2);
      pages.clear();
      pages.add(new TimeRange(20, 30));
      writeNonAlignedChunk(iChunkWriter, tsFileIOWriter, pages, false);
      tsFileIOWriter.endChunkGroup();
      tsFileIOWriter.endFile();
    }

    TsFileResource unseqResource2 = createTsFileResource("2-2-0-0.tsfile", false);
    unseqResource2.updateStartTime(d1, 30);
    unseqResource2.updateEndTime(d1, 40);
    unseqResource2.updateStartTime(d2, 40);
    unseqResource2.updateEndTime(d2, 50);
    unseqResource2.serialize();
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(unseqResource2.getTsFile())) {
      // write d1
      tsFileIOWriter.startChunkGroup(d1);
      MeasurementSchema schema =
          new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY);
      ChunkWriterImpl iChunkWriter = new ChunkWriterImpl(schema);
      List<TimeRange> pages = new ArrayList<>();
      pages.add(new TimeRange(30, 40));
      writeNonAlignedChunk(iChunkWriter, tsFileIOWriter, pages, false);
      tsFileIOWriter.endChunkGroup();

      // write d2
      tsFileIOWriter.startChunkGroup(d2);
      pages.clear();
      pages.add(new TimeRange(40, 50));
      writeNonAlignedChunk(iChunkWriter, tsFileIOWriter, pages, false);
      tsFileIOWriter.endChunkGroup();
      tsFileIOWriter.endFile();
    }

    TsFileResource unseqResource3 = createTsFileResource("3-3-0-0.tsfile", false);
    unseqResource3.updateStartTime(d1, 50);
    unseqResource3.updateEndTime(d1, 60);
    unseqResource3.updateStartTime(d2, 60);
    unseqResource3.updateEndTime(d2, 70);
    unseqResource3.serialize();
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(unseqResource3.getTsFile())) {
      // write d1
      tsFileIOWriter.startChunkGroup(d1);
      MeasurementSchema schema =
          new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY);
      ChunkWriterImpl iChunkWriter = new ChunkWriterImpl(schema);
      List<TimeRange> pages = new ArrayList<>();
      pages.add(new TimeRange(50, 60));
      writeNonAlignedChunk(iChunkWriter, tsFileIOWriter, pages, false);
      tsFileIOWriter.endChunkGroup();

      // write d2
      tsFileIOWriter.startChunkGroup(d2);
      pages.clear();
      pages.add(new TimeRange(60, 70));
      writeNonAlignedChunk(iChunkWriter, tsFileIOWriter, pages, false);
      tsFileIOWriter.endChunkGroup();
      tsFileIOWriter.endFile();
    }

    unseqResources.add(unseqResource1);
    unseqResources.add(unseqResource2);
    unseqResources.add(unseqResource3);
    degradeTimeIndex();
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    InsertionCrossCompactionTaskResource taskResource;

    int i = 1;
    while (tsFileManager.getTsFileList(false).size() > 0) {
      taskResource =
          selector.selectOneInsertionTask(
              new CrossSpaceCompactionCandidate(
                  tsFileManager.getTsFileList(true), tsFileManager.getTsFileList(false)));
      Assert.assertTrue(taskResource.isValid());
      Assert.assertEquals(
          tsFileManager.getTsFileList(false).get(0), taskResource.firstUnSeqFileInParitition);
      Assert.assertEquals(
          tsFileManager.getTsFileList(false).get(0), taskResource.toInsertUnSeqFile);
      Assert.assertEquals(null, taskResource.nextSeqFile);
      if (i == 1) {
        Assert.assertEquals(null, taskResource.prevSeqFile);
      } else if (i == 2) {
        Assert.assertEquals(tsFileManager.getTsFileList(true).get(0), taskResource.prevSeqFile);
      } else {
        Assert.assertEquals(tsFileManager.getTsFileList(true).get(1), taskResource.prevSeqFile);
      }
      InsertionCrossSpaceCompactionTask task =
          new InsertionCrossSpaceCompactionTask(
              new Phaser(1),
              0,
              tsFileManager,
              taskResource,
              tsFileManager.getNextCompactionTaskId());
      task.start();
      i++;
    }
    Assert.assertEquals(3, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(0, tsFileManager.getTsFileList(false).size());
  }

  @Test
  public void testInsertionSelectorWithNoUnseqFilesAndFileTimeIndex()
      throws MergeException, IOException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
    IDeviceID d2 = new PlainDeviceID("root.testsg.d2");

    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    seqResource1.updateStartTime(d2, 20);
    seqResource1.updateEndTime(d2, 30);
    seqResource1.serialize();
    TsFileResource seqResource2 = createTsFileResource("2-2-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 30);
    seqResource2.updateEndTime(d1, 40);
    seqResource2.updateStartTime(d2, 40);
    seqResource2.updateEndTime(d2, 50);
    seqResource2.serialize();
    TsFileResource seqResource3 = createTsFileResource("3-3-0-0.tsfile", true);
    seqResource3.updateStartTime(d1, 50);
    seqResource3.updateEndTime(d1, 60);
    seqResource3.updateStartTime(d2, 60);
    seqResource3.updateEndTime(d2, 70);
    seqResource3.serialize();

    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    seqResources.add(seqResource3);
    degradeTimeIndex();

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    InsertionCrossCompactionTaskResource task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertNull(task.toInsertUnSeqFile);
    Assert.assertNull(task.prevSeqFile);
    Assert.assertNull(task.nextSeqFile);
    Assert.assertNull(task.firstUnSeqFileInParitition);
  }

  // Multiple unseq files with multiple devices overlap with different seq files. None unseq file
  // can be inserted into seq file list.
  @Test
  public void testInsertionSelectorWithOverlapUnseqFileAndFileTimeIndex()
      throws MergeException, IOException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
    IDeviceID d2 = new PlainDeviceID("root.testsg.d2");

    // 1. prevSeqFileIndex == nextSeqFileIndex
    TsFileResource seqResource1 = createTsFileResource("1-1-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    seqResource1.updateStartTime(d2, 20);
    seqResource1.updateEndTime(d2, 30);
    seqResource1.serialize();
    TsFileResource seqResource2 = createTsFileResource("2-2-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 30);
    seqResource2.updateEndTime(d1, 40);
    seqResource2.updateStartTime(d2, 40);
    seqResource2.updateEndTime(d2, 50);
    seqResource2.serialize();
    TsFileResource seqResource3 = createTsFileResource("3-3-0-0.tsfile", true);
    seqResource3.updateStartTime(d1, 50);
    seqResource3.updateEndTime(d1, 60);
    seqResource3.updateStartTime(d2, 60);
    seqResource3.updateEndTime(d2, 70);
    seqResource3.serialize();

    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    seqResources.add(seqResource3);

    TsFileResource unseqResource1 = createTsFileResource("9-9-0-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 42);
    unseqResource1.updateEndTime(d1, 45);
    unseqResource1.updateStartTime(d2, 31);
    unseqResource1.updateEndTime(d2, 37);
    unseqResource1.serialize();
    unseqResources.add(unseqResource1);

    degradeTimeIndex();

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    InsertionCrossCompactionTaskResource task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(null, task.toInsertUnSeqFile);
    Assert.assertEquals(null, task.prevSeqFile);
    Assert.assertEquals(null, task.nextSeqFile);
    Assert.assertEquals(null, task.firstUnSeqFileInParitition);

    // 2. prevSeqFileIndex > nextSeqFileIndex
    unseqResources.remove(unseqResource1);
    unseqResource1.remove();
    unseqResource1 = createTsFileResource("9-9-0-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 62);
    unseqResource1.updateEndTime(d1, 65);
    unseqResource1.updateStartTime(d2, 31);
    unseqResource1.updateEndTime(d2, 37);
    unseqResource1.serialize();

    unseqResources.add(unseqResource1);

    degradeTimeIndex();

    task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(null, task.toInsertUnSeqFile);
    Assert.assertEquals(null, task.prevSeqFile);
    Assert.assertEquals(null, task.nextSeqFile);
    Assert.assertEquals(null, task.firstUnSeqFileInParitition);
  }

  @Test
  public void testInsertionIntoCompactingSeqFilesAndFileTimeIndex()
      throws IOException, MergeException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
    IDeviceID d2 = new PlainDeviceID("root.testsg.d2");
    IDeviceID d3 = new PlainDeviceID("root.testsg.d3");

    TsFileResource seqResource1 = createTsFileResource("100-100-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    seqResource1.updateStartTime(d2, 20);
    seqResource1.updateEndTime(d2, 30);
    seqResource1.serialize();
    createTsFileByResource(seqResource1);

    TsFileResource seqResource2 = createTsFileResource("200-200-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 30);
    seqResource2.updateEndTime(d1, 40);
    seqResource2.updateStartTime(d2, 40);
    seqResource2.updateEndTime(d2, 50);
    seqResource2.serialize();
    createTsFileByResource(seqResource2);

    TsFileResource seqResource3 = createTsFileResource("300-300-0-0.tsfile", true);
    seqResource3.updateStartTime(d3, 50);
    seqResource3.updateEndTime(d3, 60);
    seqResource3.serialize();
    createTsFileByResource(seqResource3);

    TsFileResource seqResource4 = createTsFileResource("400-400-0-0.tsfile", true);
    seqResource4.updateStartTime(d3, 70);
    seqResource4.updateEndTime(d3, 80);
    seqResource4.serialize();
    createTsFileByResource(seqResource4);

    TsFileResource seqResource5 = createTsFileResource("500-500-0-0.tsfile", true);
    seqResource5.updateStartTime(d2, 100);
    seqResource5.updateEndTime(d2, 110);
    seqResource5.serialize();
    createTsFileByResource(seqResource5);

    TsFileResource seqResource6 = createTsFileResource("600-600-0-0.tsfile", true);
    seqResource6.updateStartTime(d1, 110);
    seqResource6.updateEndTime(d1, 120);
    seqResource6.updateStartTime(d2, 120);
    seqResource6.updateEndTime(d2, 130);
    seqResource6.serialize();
    createTsFileByResource(seqResource6);

    TsFileResource seqResource7 = createTsFileResource("700-700-0-0.tsfile", true);
    seqResource7.updateStartTime(d1, 130);
    seqResource7.updateEndTime(d1, 140);
    seqResource7.updateStartTime(d2, 140);
    seqResource7.updateEndTime(d2, 150);
    seqResource7.serialize();
    createTsFileByResource(seqResource7);

    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    seqResources.add(seqResource3);
    seqResources.add(seqResource4);
    seqResources.add(seqResource5);
    seqResources.add(seqResource6);
    seqResources.add(seqResource7);

    TsFileResource unseqResource1 = createTsFileResource("9-9-0-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 42);
    unseqResource1.updateEndTime(d1, 45);
    unseqResource1.updateStartTime(d2, 61);
    unseqResource1.updateEndTime(d2, 97);
    unseqResource1.serialize();
    createTsFileByResource(unseqResource1);
    unseqResources.add(unseqResource1);
    degradeTimeIndex();
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    InsertionCrossCompactionTaskResource task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(tsFileManager.getTsFileList(false).get(0), task.toInsertUnSeqFile);
    Assert.assertEquals(tsFileManager.getTsFileList(true).get(1), task.prevSeqFile);
    Assert.assertEquals(tsFileManager.getTsFileList(true).get(2), task.nextSeqFile);
    Assert.assertEquals(tsFileManager.getTsFileList(false).get(0), task.firstUnSeqFileInParitition);

    // seq file 1 ~ 3 is compaction candidate
    for (int i = 0; i < 3; i++) {
      seqResources.get(i).setStatusForTest(TsFileResourceStatus.COMPACTION_CANDIDATE);
    }
    task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(tsFileManager.getTsFileList(false).get(0), task.toInsertUnSeqFile);
    Assert.assertEquals(tsFileManager.getTsFileList(true).get(3), task.prevSeqFile);
    Assert.assertEquals(tsFileManager.getTsFileList(true).get(4), task.nextSeqFile);
    Assert.assertEquals(tsFileManager.getTsFileList(false).get(0), task.firstUnSeqFileInParitition);

    // all seq file are compacting
    for (TsFileResource resource : seqResources) {
      resource.setStatusForTest(TsFileResourceStatus.COMPACTING);
    }
    task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertEquals(null, task.toInsertUnSeqFile);
    Assert.assertEquals(null, task.prevSeqFile);
    Assert.assertEquals(null, task.nextSeqFile);
    Assert.assertEquals(null, task.firstUnSeqFileInParitition);
  }

  @Test
  public void testInsertionSelectorWithUnclosedSeqFileAndFileTimeIndex()
      throws MergeException, IOException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
    IDeviceID d2 = new PlainDeviceID("root.testsg.d2");

    TsFileResource seqResource1 = createTsFileResource("100-100-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    seqResource1.updateStartTime(d2, 20);
    seqResource1.updateEndTime(d2, 30);
    seqResource1.serialize();

    TsFileResource seqResource2 = createTsFileResource("200-200-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 30);
    seqResource2.updateEndTime(d1, 40);
    seqResource2.updateStartTime(d2, 40);
    seqResource2.updateEndTime(d2, 50);
    seqResource2.serialize();

    TsFileResource seqResource3 = createTsFileResource("300-300-0-0.tsfile", true);
    seqResource3.updateStartTime(d1, 50);
    seqResource3.updateEndTime(d1, 60);
    seqResource3.updateStartTime(d2, 60);
    seqResource3.updateEndTime(d2, 70);
    seqResource3.serialize();
    seqResource3.setStatusForTest(TsFileResourceStatus.UNCLOSED);

    TsFileResource seqResource4 = createTsFileResource("600-600-0-0.tsfile", true);
    seqResource4.updateStartTime(d1, 110);
    seqResource4.updateEndTime(d1, 120);
    seqResource4.updateStartTime(d2, 120);
    seqResource4.updateEndTime(d2, 130);
    seqResource4.serialize();
    seqResource4.setStatusForTest(TsFileResourceStatus.UNCLOSED);

    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    seqResources.add(seqResource3);
    seqResources.add(seqResource4);

    // overlap with unclosed seq file
    TsFileResource unseqResource1 = createTsFileResource("9-9-0-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 42);
    unseqResource1.updateEndTime(d1, 55);
    unseqResource1.updateStartTime(d2, 61);
    unseqResource1.updateEndTime(d2, 97);
    unseqResource1.serialize();

    // nonOverlap with unclosed seq file
    TsFileResource unseqResource2 = createTsFileResource("10-10-0-0.tsfile", false);
    unseqResource2.updateStartTime(d1, 42);
    unseqResource2.updateEndTime(d1, 45);
    unseqResource2.updateStartTime(d2, 51);
    unseqResource2.updateEndTime(d2, 57);
    unseqResource2.serialize();

    unseqResources.add(unseqResource1);
    unseqResources.add(unseqResource2);

    degradeTimeIndex();

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    InsertionCrossCompactionTaskResource task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertNull(task.toInsertUnSeqFile);
    Assert.assertNull(task.prevSeqFile);
    Assert.assertNull(task.nextSeqFile);
    Assert.assertNull(task.firstUnSeqFileInParitition);

    // nonOverlap with unclosed seq file, in the gap of the two unclosed seq file
    unseqResources.remove(unseqResource2);
    TsFileResource unseqResource3 = createTsFileResource("11-11-0-0.tsfile", false);
    unseqResource3.updateStartTime(d1, 80);
    unseqResource3.updateEndTime(d1, 90);
    unseqResource3.updateStartTime(d2, 80);
    unseqResource3.updateEndTime(d2, 90);
    unseqResource3.serialize();
    unseqResource3.degradeTimeIndex();

    unseqResources.add(unseqResource1);
    unseqResources.add(unseqResource3);

    task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertNull(task.toInsertUnSeqFile);
    Assert.assertNull(task.prevSeqFile);
    Assert.assertNull(task.nextSeqFile);
    Assert.assertNull(task.firstUnSeqFileInParitition);
  }

  @Test
  public void testInsertionSelectorWithUnclosedUnSeqFileAndFileTimeIndex()
      throws MergeException, IOException {
    IDeviceID d1 = new PlainDeviceID("root.testsg.d1");
    IDeviceID d2 = new PlainDeviceID("root.testsg.d2");

    TsFileResource seqResource1 = createTsFileResource("100-100-0-0.tsfile", true);
    seqResource1.updateStartTime(d1, 10);
    seqResource1.updateEndTime(d1, 20);
    seqResource1.updateStartTime(d2, 20);
    seqResource1.updateEndTime(d2, 30);
    seqResource1.serialize();

    TsFileResource seqResource2 = createTsFileResource("200-200-0-0.tsfile", true);
    seqResource2.updateStartTime(d1, 30);
    seqResource2.updateEndTime(d1, 40);
    seqResource2.updateStartTime(d2, 40);
    seqResource2.updateEndTime(d2, 50);
    seqResource2.serialize();

    TsFileResource seqResource3 = createTsFileResource("300-300-0-0.tsfile", true);
    seqResource3.updateStartTime(d1, 50);
    seqResource3.updateEndTime(d1, 60);
    seqResource3.updateStartTime(d2, 60);
    seqResource3.updateEndTime(d2, 70);
    seqResource3.serialize();

    TsFileResource seqResource4 = createTsFileResource("600-600-0-0.tsfile", true);
    seqResource4.updateStartTime(d1, 110);
    seqResource4.updateEndTime(d1, 120);
    seqResource4.updateStartTime(d2, 120);
    seqResource4.updateEndTime(d2, 130);
    seqResource4.serialize();

    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    seqResources.add(seqResource3);
    seqResources.add(seqResource4);

    // overlap with seq file
    TsFileResource unseqResource1 = createTsFileResource("9-9-0-0.tsfile", false);
    unseqResource1.updateStartTime(d1, 42);
    unseqResource1.updateEndTime(d1, 55);
    unseqResource1.updateStartTime(d2, 61);
    unseqResource1.updateEndTime(d2, 97);
    unseqResource1.serialize();
    unseqResource1.setStatusForTest(TsFileResourceStatus.UNCLOSED);

    // nonOverlap with seq file
    TsFileResource unseqResource2 = createTsFileResource("10-10-0-0.tsfile", false);
    unseqResource2.updateStartTime(d1, 42);
    unseqResource2.updateEndTime(d1, 45);
    unseqResource2.updateStartTime(d2, 51);
    unseqResource2.updateEndTime(d2, 57);
    unseqResource2.serialize();
    unseqResource1.setStatusForTest(TsFileResourceStatus.UNCLOSED);

    unseqResources.add(unseqResource1);
    unseqResources.add(unseqResource2);

    degradeTimeIndex();

    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("root.testsg", "0", 0, tsFileManager);
    InsertionCrossCompactionTaskResource task =
        selector.selectOneInsertionTask(
            new CrossSpaceCompactionCandidate(seqResources, unseqResources));
    Assert.assertNull(task.toInsertUnSeqFile);
    Assert.assertNull(task.prevSeqFile);
    Assert.assertNull(task.nextSeqFile);
    Assert.assertNull(task.firstUnSeqFileInParitition);
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

  private void degradeTimeIndex() {
    for (TsFileResource resource : seqResources) {
      resource.degradeTimeIndex();
    }
    for (TsFileResource resource : unseqResources) {
      resource.degradeTimeIndex();
    }
  }
}
