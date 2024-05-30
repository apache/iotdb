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

package org.apache.iotdb.db.storageengine.dataregion.compaction;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class CompactionOverlapCheckTest extends AbstractCompactionTest {

  long initChunkSizeLowerBound =
      IoTDBDescriptor.getInstance().getConfig().getChunkSizeLowerBoundInCompaction();
  long initChunkPointNumLowerBound =
      IoTDBDescriptor.getInstance().getConfig().getChunkPointNumLowerBoundInCompaction();
  boolean isEnableUnseqSpaceCompaction =
      IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    IoTDBDescriptor.getInstance().getConfig().setChunkSizeLowerBoundInCompaction(10240);
    IoTDBDescriptor.getInstance().getConfig().setChunkPointNumLowerBoundInCompaction(1000);
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setChunkSizeLowerBoundInCompaction(initChunkSizeLowerBound);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setChunkPointNumLowerBoundInCompaction(initChunkPointNumLowerBound);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(isEnableUnseqSpaceCompaction);
  }

  @Test
  public void
      testOverlapFilesInnerSequenceSpaceCompactionWithReadChunkCompactionPerformerWithNonAlignedSeries()
          throws IOException {
    prepareOverlapSequenceFilesWithNonAlignedSeries();
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, new ReadChunkCompactionPerformer(), 0);
    Assert.assertFalse(task.start());
  }

  @Test
  public void
      testOverlapFilesInnerSequenceSpaceCompactionWithReadChunkCompactionPerformerWithAlignedSeries()
          throws IOException {
    prepareOverlapSequenceFilesWithAlignedSeries();
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, new ReadChunkCompactionPerformer(), 0);
    Assert.assertFalse(task.start());
  }

  @Test
  public void
      testOverlapFilesInnerSequenceSpaceCompactionWithFastCompactionPerformerWithNonAlignedSeries()
          throws IOException {
    prepareOverlapSequenceFilesWithNonAlignedSeries();
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, new FastCompactionPerformer(false), 0);
    Assert.assertFalse(task.start());
  }

  @Test
  public void
      testOverlapFilesInnerSequenceSpaceCompactionWithFastCompactionPerformerWithAlignedSeries()
          throws IOException {
    prepareOverlapSequenceFilesWithAlignedSeries();
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, new FastCompactionPerformer(false), 0);
    Assert.assertFalse(task.start());
  }

  @Test
  public void
      testOverlapFilesInnerUnSequenceSpaceCompactionWithFastCompactionPerformerWithNonAlignedSeries()
          throws IOException {
    prepareInsideChunkOverlapUnSequenceFilesWithNonAlignedSeries();
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, unseqResources, false, new FastCompactionPerformer(false), 0);
    Assert.assertFalse(task.start());
  }

  @Test
  public void
      testOverlapFilesInnerUnSequenceSpaceCompactionWithFastCompactionPerformerWithAlignedSeries()
          throws IOException {
    prepareInsideChunkOverlapUnSequenceFilesWithAlignedSeries();
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, unseqResources, false, new FastCompactionPerformer(false), 0);
    Assert.assertFalse(task.start());
  }

  @Test
  public void testOverlapFilesCrossSpaceCompactionWithFastCompactionPerformerWithNonAlignedSeries()
      throws IOException {
    prepareOverlapSequenceFilesWithNonAlignedSeries();
    prepareInsideChunkOverlapUnSequenceFilesWithNonAlignedSeries();
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new FastCompactionPerformer(true),
            0,
            0);
    Assert.assertFalse(task.start());
  }

  @Test
  public void testOverlapFilesCrossSpaceCompactionWithFastCompactionPerformerWithAlignedSeries()
      throws IOException {
    prepareOverlapSequenceFilesWithAlignedSeries();
    prepareInsideChunkOverlapUnSequenceFilesWithAlignedSeries();
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new FastCompactionPerformer(true),
            0,
            0);
    Assert.assertFalse(task.start());
  }

  private void prepareOverlapSequenceFilesWithNonAlignedSeries() throws IOException {
    TsFileResource seqResource1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqResource1)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1",
          new TimeRange[][][] {
            new TimeRange[][] {new TimeRange[] {new TimeRange(10, 12), new TimeRange(3, 12)}}
          },
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    TsFileResource seqResource2 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqResource2)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1", new TimeRange[] {new TimeRange(1, 9)}, TSEncoding.PLAIN, CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    tsFileManager.addAll(seqResources, true);
  }

  private void prepareOverlapSequenceFilesWithAlignedSeries() throws IOException {
    TsFileResource seqResource1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqResource1)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1", "s2"),
          new TimeRange[][][] {
            new TimeRange[][] {new TimeRange[] {new TimeRange(10, 12), new TimeRange(3, 12)}}
          },
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    TsFileResource seqResource2 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqResource2)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1", "s2"),
          new TimeRange[] {new TimeRange(1, 9)},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    tsFileManager.addAll(seqResources, true);
  }

  private void prepareInsideChunkOverlapUnSequenceFilesWithNonAlignedSeries() throws IOException {
    TsFileResource unseqResource1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(unseqResource1)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1",
          new TimeRange[][][] {
            new TimeRange[][] {new TimeRange[] {new TimeRange(10, 12), new TimeRange(3, 12)}}
          },
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    TsFileResource unseqResource2 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(unseqResource2)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1", new TimeRange[] {new TimeRange(35, 40)}, TSEncoding.PLAIN, CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    unseqResources.add(unseqResource1);
    unseqResources.add(unseqResource2);
    tsFileManager.addAll(unseqResources, false);
  }

  private void prepareInsideChunkOverlapUnSequenceFilesWithAlignedSeries() throws IOException {
    TsFileResource unseqResource1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(unseqResource1)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1", "s2"),
          new TimeRange[][][] {
            new TimeRange[][] {new TimeRange[] {new TimeRange(10, 12), new TimeRange(3, 12)}}
          },
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    TsFileResource unseqResource2 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(unseqResource2)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1", "s2"),
          new TimeRange[] {new TimeRange(35, 40)},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    unseqResources.add(unseqResource1);
    unseqResources.add(unseqResource2);
    tsFileManager.addAll(unseqResources, false);
  }
}
