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

package org.apache.iotdb.db.storageengine.dataregion.compaction.tablemodel;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.buffer.BloomFilterCache;
import org.apache.iotdb.db.storageengine.buffer.ChunkCache;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.constant.InnerSeqCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.constant.InnerUnseqCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadPointCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.IDPredicate.FullExactMatch;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class CompactionWithAllNullRowsTest extends AbstractCompactionTest {

  private final String performerType;
  private String threadName;
  private final int maxAlignedSeriesNumInOneBatch;
  private int defaultMaxAlignedSeriesNumInOneBatch;

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    this.threadName = Thread.currentThread().getName();
    Thread.currentThread().setName("pool-1-IoTDB-Compaction-Worker-1");
    this.defaultMaxAlignedSeriesNumInOneBatch =
        IoTDBDescriptor.getInstance().getConfig().getCompactionMaxAlignedSeriesNumInOneBatch();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setCompactionMaxAlignedSeriesNumInOneBatch(maxAlignedSeriesNumInOneBatch);
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    BloomFilterCache.getInstance().clear();
    Thread.currentThread().setName(threadName);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setCompactionMaxAlignedSeriesNumInOneBatch(defaultMaxAlignedSeriesNumInOneBatch);
    super.tearDown();
  }

  @Parameterized.Parameters(name = "type={0} batch_size={1}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {"read_chunk", 10}, {"read_chunk", 2}, {"fast", 10}, {"fast", 2}, {"read_point", 10},
        });
  }

  public CompactionWithAllNullRowsTest(String performerType, int maxAlignedSeriesNumInOneBatch) {
    this.performerType = performerType;
    this.maxAlignedSeriesNumInOneBatch = maxAlignedSeriesNumInOneBatch;
  }

  public ICompactionPerformer getPerformer() {
    if (performerType.equalsIgnoreCase(InnerSeqCompactionPerformer.READ_CHUNK.toString())) {
      return new ReadChunkCompactionPerformer();
    } else if (performerType.equalsIgnoreCase(InnerUnseqCompactionPerformer.FAST.toString())) {
      return new FastCompactionPerformer(false);
    } else {
      return new ReadPointCompactionPerformer();
    }
  }

  @Test
  public void testCompactionWithAllNullRows1() throws IOException {
    TsFileResource resource1 = createEmptyFileAndResource(true);
    try (CompactionTableModelTestFileWriter writer =
        new CompactionTableModelTestFileWriter(resource1)) {
      writer.registerTableSchema("t1", Arrays.asList("id1", "id2"));
      writer.startChunkGroup("t1", Arrays.asList("id_field1", "id_field2"));
      writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
          Arrays.asList("s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9"),
          new TimeRange[][][] {new TimeRange[][] {new TimeRange[] {new TimeRange(10, 12)}}},
          TSEncoding.PLAIN,
          CompressionType.LZ4,
          Arrays.asList(true, true, true, true, true, true, true, true, true, true));
      writer.endChunkGroup();
      writer.endFile();
    }
    seqResources.add(resource1);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(0, tsFileManager, seqResources, true, getPerformer(), 0);
    Assert.assertTrue(task.start());
    TsFileResource target = tsFileManager.getTsFileList(true).get(0);
    Assert.assertEquals(10, target.getFileStartTime());
    Assert.assertEquals(12, target.getFileEndTime());
    InnerSpaceCompactionTask task2 =
        new InnerSpaceCompactionTask(
            0, tsFileManager, tsFileManager.getTsFileList(true), true, getPerformer(), 0);
    Assert.assertTrue(task2.start());
    TsFileResource target2 = tsFileManager.getTsFileList(true).get(0);
    Assert.assertEquals(10, target2.getFileStartTime());
    Assert.assertEquals(12, target2.getFileEndTime());
  }

  @Test
  public void testCompactionWithAllNullRows2() throws IOException {
    TsFileResource resource1 = createEmptyFileAndResource(true);
    try (CompactionTableModelTestFileWriter writer =
        new CompactionTableModelTestFileWriter(resource1)) {
      writer.registerTableSchema("t1", Arrays.asList("id1", "id2"));
      writer.startChunkGroup("t1", Arrays.asList("id_field1", "id_field2"));
      writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
          Arrays.asList("s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9"),
          new TimeRange[][][] {
            new TimeRange[][] {new TimeRange[] {new TimeRange(1, 9), new TimeRange(10, 12)}}
          },
          TSEncoding.PLAIN,
          CompressionType.LZ4,
          Arrays.asList(true, true, true, true, true, true, true, true, true, true));

      writer.endChunkGroup();
      writer.endFile();
    }
    seqResources.add(resource1);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(0, tsFileManager, seqResources, true, getPerformer(), 0);
    Assert.assertTrue(task.start());
    TsFileResource target = tsFileManager.getTsFileList(true).get(0);
    Assert.assertEquals(1, target.getFileStartTime());
    Assert.assertEquals(12, target.getFileEndTime());
    InnerSpaceCompactionTask task2 =
        new InnerSpaceCompactionTask(
            0, tsFileManager, tsFileManager.getTsFileList(true), true, getPerformer(), 0);
    Assert.assertTrue(task2.start());
    TsFileResource target2 = tsFileManager.getTsFileList(true).get(0);
    Assert.assertEquals(1, target2.getFileStartTime());
    Assert.assertEquals(12, target2.getFileEndTime());
  }

  @Test
  public void testCompactionWithAllNullRows3() throws IOException {
    TsFileResource resource1 = createEmptyFileAndResource(true);
    try (CompactionTableModelTestFileWriter writer =
        new CompactionTableModelTestFileWriter(resource1)) {
      writer.registerTableSchema("t1", Arrays.asList("id1", "id2"));
      writer.startChunkGroup("t1", Arrays.asList("id_field1", "id_field2"));
      writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
          Arrays.asList("s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9"),
          new TimeRange[][] {new TimeRange[] {new TimeRange(10000, 19999)}},
          TSEncoding.PLAIN,
          CompressionType.LZ4,
          Arrays.asList(true, true, true, true, true, true, true, true, true, true));

      writer.endChunkGroup();
      writer.endFile();
    }
    seqResources.add(resource1);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(0, tsFileManager, seqResources, true, getPerformer(), 0);
    Assert.assertTrue(task.start());
    TsFileResource target = tsFileManager.getTsFileList(true).get(0);
    Assert.assertEquals(10000, target.getFileStartTime());
    Assert.assertEquals(19999, target.getFileEndTime());
    InnerSpaceCompactionTask task2 =
        new InnerSpaceCompactionTask(
            0, tsFileManager, tsFileManager.getTsFileList(true), true, getPerformer(), 0);
    Assert.assertTrue(task2.start());
    TsFileResource target2 = tsFileManager.getTsFileList(true).get(0);
    Assert.assertEquals(10000, target2.getFileStartTime());
    Assert.assertEquals(19999, target2.getFileEndTime());
  }

  @Test
  public void testCompactionWithAllNullRows4() throws IOException {
    TsFileResource resource1 = createEmptyFileAndResource(true);
    try (CompactionTableModelTestFileWriter writer =
        new CompactionTableModelTestFileWriter(resource1)) {
      writer.registerTableSchema("t1", Arrays.asList("id1", "id2"));
      writer.startChunkGroup("t1", Arrays.asList("id_field1", "id_field2"));
      writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
          Arrays.asList("s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9"),
          new TimeRange[] {new TimeRange(100000, 199999)},
          TSEncoding.PLAIN,
          CompressionType.LZ4,
          Arrays.asList(true, true, true, true, true, true, true, true, true, true));

      writer.endChunkGroup();
      writer.endFile();
    }
    seqResources.add(resource1);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(0, tsFileManager, seqResources, true, getPerformer(), 0);
    Assert.assertTrue(task.start());
    TsFileResource target = tsFileManager.getTsFileList(true).get(0);
    Assert.assertEquals(100000, target.getFileStartTime());
    Assert.assertEquals(199999, target.getFileEndTime());
    InnerSpaceCompactionTask task2 =
        new InnerSpaceCompactionTask(
            0, tsFileManager, tsFileManager.getTsFileList(true), true, getPerformer(), 0);
    Assert.assertTrue(task2.start());
    TsFileResource target2 = tsFileManager.getTsFileList(true).get(0);
    Assert.assertEquals(100000, target2.getFileStartTime());
    Assert.assertEquals(199999, target2.getFileEndTime());
  }

  @Test
  @Ignore
  public void testCompactionWithAllDeletion() throws IOException, IllegalPathException {
    TsFileResource resource1 = createEmptyFileAndResource(true);
    IDeviceID deviceID = null;
    try (CompactionTableModelTestFileWriter writer =
        new CompactionTableModelTestFileWriter(resource1)) {
      writer.registerTableSchema("t1", Arrays.asList("id1", "id2"));
      deviceID = writer.startChunkGroup("t1", Arrays.asList("id_field1", "id_field2"));
      writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
          Arrays.asList("s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9"),
          new TimeRange[][][] {new TimeRange[][] {new TimeRange[] {new TimeRange(10, 12)}}},
          TSEncoding.PLAIN,
          CompressionType.LZ4,
          Arrays.asList(true, true, true, true, true, true, true, true, true, true));
      writer.endChunkGroup();
      writer.endFile();
    }
    resource1
        .getModFileForWrite()
        .write(
            new TableDeletionEntry(
                new DeletionPredicate(deviceID.getTableName(), new FullExactMatch(deviceID)),
                new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE)));
    resource1.getModFileForWrite().close();
    seqResources.add(resource1);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(0, tsFileManager, seqResources, true, getPerformer(), 0);
    Assert.assertTrue(task.start());
    Assert.assertTrue(tsFileManager.getTsFileList(true).isEmpty());
  }

  @Test
  public void testCompactionWithAllValueColumnDeletion() throws IOException, IllegalPathException {
    TsFileResource resource1 = createEmptyFileAndResource(true);
    IDeviceID deviceID = null;
    try (CompactionTableModelTestFileWriter writer =
        new CompactionTableModelTestFileWriter(resource1)) {
      writer.registerTableSchema("t1", Arrays.asList("id1", "id2"));
      deviceID = writer.startChunkGroup("t1", Arrays.asList("id_field1", "id_field2"));
      writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
          Arrays.asList("s0", "s1", "s2", "s3"),
          new TimeRange[][][] {new TimeRange[][] {new TimeRange[] {new TimeRange(10, 12)}}},
          TSEncoding.PLAIN,
          CompressionType.LZ4,
          Arrays.asList(true, true, true, true));
      writer.endChunkGroup();
      writer.endFile();
    }
    resource1
        .getModFileForWrite()
        .write(new TreeDeletionEntry(new MeasurementPath(deviceID, "s0"), 11));
    resource1
        .getModFileForWrite()
        .write(new TreeDeletionEntry(new MeasurementPath(deviceID, "s1"), 11));
    resource1
        .getModFileForWrite()
        .write(new TreeDeletionEntry(new MeasurementPath(deviceID, "s2"), 11));
    resource1
        .getModFileForWrite()
        .write(new TreeDeletionEntry(new MeasurementPath(deviceID, "s3"), 11));
    resource1.getModFileForWrite().close();
    seqResources.add(resource1);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(0, tsFileManager, seqResources, true, getPerformer(), 0);
    Assert.assertTrue(task.start());
    TsFileResource target = tsFileManager.getTsFileList(true).get(0);
    Assert.assertEquals(10, target.getFileStartTime());
    Assert.assertEquals(12, target.getFileEndTime());
  }
}
