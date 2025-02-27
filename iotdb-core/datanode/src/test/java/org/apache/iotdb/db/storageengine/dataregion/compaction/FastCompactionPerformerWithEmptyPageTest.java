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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.TimeRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FastCompactionPerformerWithEmptyPageTest extends AbstractCompactionTest {

  int oldAlignedSeriesCompactionBatchSize;

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    oldAlignedSeriesCompactionBatchSize = IoTDBDescriptor.getInstance().getConfig().getBatchSize();
    IoTDBDescriptor.getInstance().getConfig().setCompactionMaxAlignedSeriesNumInOneBatch(10);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setCompactionMaxAlignedSeriesNumInOneBatch(oldAlignedSeriesCompactionBatchSize);
  }

  @Test
  public void test1() throws IOException, IllegalPathException {
    IDeviceID device = IDeviceID.Factory.DEFAULT_FACTORY.create("root.testsg.d1");
    TsFileResource seqFile1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqFile1)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
          Arrays.asList("s1", "s2", "s3"),
          new TimeRange[][] {new TimeRange[] {new TimeRange(10, 30)}},
          TSEncoding.RLE,
          CompressionType.UNCOMPRESSED,
          Arrays.asList(false, true, true));
      writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
          Arrays.asList("s1", "s2", "s3"),
          new TimeRange[][] {new TimeRange[] {new TimeRange(40, 50)}},
          TSEncoding.RLE,
          CompressionType.UNCOMPRESSED,
          Arrays.asList(false, false, false));
      writer.endChunkGroup();
      writer.endFile();
    }
    seqFile1.updateStartTime(device, 10);
    seqFile1.updateEndTime(device, 50);
    seqFile1.serialize();
    generateModsFile(Arrays.asList(new PartialPath("root.testsg.d1.s1")), seqFile1, 0, 31);

    TsFileResource unseqFile1 = createEmptyFileAndResource(false);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(unseqFile1)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1", "s2", "s3"),
          new TimeRange[][] {new TimeRange[] {new TimeRange(20, 34)}},
          TSEncoding.RLE,
          CompressionType.UNCOMPRESSED);
      writer.endChunkGroup();
      writer.endFile();
    }
    unseqFile1.updateStartTime(device, 20);
    unseqFile1.updateEndTime(device, 34);
    unseqFile1.serialize();

    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            Arrays.asList(seqFile1),
            Arrays.asList(unseqFile1),
            new FastCompactionPerformer(true),
            0,
            0);
    try {
      Assert.assertTrue(task.start());
    } catch (Exception e) {
      Assert.fail();
    }
    TsFileResource result = tsFileManager.getTsFileList(true).get(0);
    result.buildDeviceTimeIndex();
    Assert.assertEquals(20, result.getStartTime(device));
    Assert.assertEquals(50, result.getEndTime(device));

    validateSeqFiles(true);

    try (TsFileSequenceReader reader = new TsFileSequenceReader(result.getTsFilePath())) {
      Map<String, List<ChunkMetadata>> chunkMetadataInDevice =
          reader.readChunkMetadataInDevice(device);
      long startTime = Long.MAX_VALUE, endTime = Long.MIN_VALUE;
      List<ChunkMetadata> chunkMetadataList = chunkMetadataInDevice.get("s1");
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        startTime = Math.min(startTime, chunkMetadata.getStartTime());
        endTime = Math.max(endTime, chunkMetadata.getEndTime());
      }
      Assert.assertEquals(20, startTime);
      Assert.assertEquals(50, endTime);
    }
  }
}
