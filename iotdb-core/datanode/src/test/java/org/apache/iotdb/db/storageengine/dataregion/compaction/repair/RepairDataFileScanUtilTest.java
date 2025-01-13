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

package org.apache.iotdb.db.storageengine.dataregion.compaction.repair;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ArrayDeviceTimeIndex;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class RepairDataFileScanUtilTest extends AbstractCompactionTest {

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
  public void testScanNormalFile() throws IOException {
    TsFileResource resource = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
          Arrays.asList("s0", "s1", "s2"),
          new TimeRange[] {new TimeRange(10, 40)},
          TSEncoding.PLAIN,
          CompressionType.LZ4,
          Arrays.asList(false, false, true));
      writer.endChunkGroup();
      writer.startChunkGroup("d2");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s0", new TimeRange[] {new TimeRange(10, 40)}, TSEncoding.PLAIN, CompressionType.LZ4);
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1",
          new TimeRange[] {new TimeRange(40, 40), new TimeRange(50, 70)},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    RepairDataFileScanUtil scanUtil = new RepairDataFileScanUtil(resource);
    scanUtil.scanTsFile();
    Assert.assertFalse(scanUtil.isBrokenFile());
    Assert.assertFalse(scanUtil.hasUnsortedDataOrWrongStatistics());
  }

  @Test
  public void testWrongChunkStatisticsWithNonAlignedSeries() throws IOException {
    TsFileResource resource = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource)) {
      writer.startChunkGroup("d2");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s0", new TimeRange[] {new TimeRange(10, 40)}, TSEncoding.PLAIN, CompressionType.LZ4);
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1",
          new TimeRange[] {new TimeRange(40, 40), new TimeRange(50, 70)},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      List<ChunkMetadata> chunkMetadataListInMemory =
          writer.getFileWriter().getChunkMetadataListOfCurrentDeviceInMemory();
      ChunkMetadata originChunkMetadata = chunkMetadataListInMemory.get(0);
      originChunkMetadata.getStatistics().setStartTime(4);
      writer.endChunkGroup();
      writer.endFile();
    }
    RepairDataFileScanUtil scanUtil = new RepairDataFileScanUtil(resource);
    scanUtil.scanTsFile();
    Assert.assertFalse(scanUtil.isBrokenFile());
    Assert.assertTrue(scanUtil.hasUnsortedDataOrWrongStatistics());
  }

  @Test
  public void testWrongChunkStatisticsWithAlignedSeries() throws IOException {
    TsFileResource resource = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
          Arrays.asList("s0", "s1", "s2"),
          new TimeRange[] {new TimeRange(10, 40)},
          TSEncoding.PLAIN,
          CompressionType.LZ4,
          Arrays.asList(false, false, true));
      List<ChunkMetadata> chunkMetadataListInMemory =
          writer.getFileWriter().getChunkMetadataListOfCurrentDeviceInMemory();
      ChunkMetadata originChunkMetadata = chunkMetadataListInMemory.get(0);
      originChunkMetadata.getStatistics().setStartTime(20);
      writer.endChunkGroup();
      writer.endFile();
    }
    RepairDataFileScanUtil scanUtil = new RepairDataFileScanUtil(resource);
    scanUtil.scanTsFile();
    Assert.assertFalse(scanUtil.isBrokenFile());
    Assert.assertTrue(scanUtil.hasUnsortedDataOrWrongStatistics());
  }

  @Test
  public void testWrongResourceStatistics() throws IOException {
    TsFileResource resource = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
          Arrays.asList("s0", "s1", "s2"),
          new TimeRange[] {new TimeRange(10, 40)},
          TSEncoding.PLAIN,
          CompressionType.LZ4,
          Arrays.asList(false, false, true));
      writer.endChunkGroup();
      writer.endFile();
    }
    resource
        .getTimeIndex()
        .updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create("root.testsg.d1"), 1);
    RepairDataFileScanUtil scanUtil = new RepairDataFileScanUtil(resource);
    scanUtil.scanTsFile(true);
    Assert.assertFalse(scanUtil.isBrokenFile());
    Assert.assertTrue(scanUtil.hasUnsortedDataOrWrongStatistics());
  }

  @Test
  public void testDeviceNotExistsInTsFile() throws IOException {
    TsFileResource resource = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
          Arrays.asList("s0", "s1", "s2"),
          new TimeRange[] {new TimeRange(10, 40)},
          TSEncoding.PLAIN,
          CompressionType.LZ4,
          Arrays.asList(false, false, true));
      writer.endChunkGroup();
      writer.endFile();
    }
    resource
        .getTimeIndex()
        .updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create("root.testsg.d2"), 1);
    RepairDataFileScanUtil scanUtil = new RepairDataFileScanUtil(resource);
    scanUtil.scanTsFile(true);
    Assert.assertFalse(scanUtil.isBrokenFile());
    Assert.assertTrue(scanUtil.hasUnsortedDataOrWrongStatistics());
  }

  @Test
  public void testDeviceDoesNotExistInResourceFile() throws IOException {
    TsFileResource resource = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
          Arrays.asList("s0", "s1", "s2"),
          new TimeRange[] {new TimeRange(10, 40)},
          TSEncoding.PLAIN,
          CompressionType.LZ4,
          Arrays.asList(false, false, true));
      writer.endChunkGroup();
      writer.endFile();
    }
    resource.setTimeIndex(new ArrayDeviceTimeIndex());
    RepairDataFileScanUtil scanUtil = new RepairDataFileScanUtil(resource);
    scanUtil.scanTsFile(true);
    Assert.assertFalse(scanUtil.isBrokenFile());
    Assert.assertTrue(scanUtil.hasUnsortedDataOrWrongStatistics());
  }

  @Test
  public void testScanUnsortedFile() throws IOException {
    TsFileResource resource1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource1)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
          Arrays.asList("s0", "s1", "s2"),
          new TimeRange[] {new TimeRange(10, 40), new TimeRange(20, 50)},
          TSEncoding.PLAIN,
          CompressionType.LZ4,
          Arrays.asList(false, false, true));
      writer.endChunkGroup();
      writer.startChunkGroup("d2");
      writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
          Arrays.asList("s0", "s1", "s2"),
          new TimeRange[] {new TimeRange(10, 40)},
          TSEncoding.PLAIN,
          CompressionType.LZ4,
          Arrays.asList(false, false, true));
      writer.endChunkGroup();
      writer.endFile();
    }
    RepairDataFileScanUtil scanUtil1 = new RepairDataFileScanUtil(resource1);
    scanUtil1.scanTsFile();
    Assert.assertFalse(scanUtil1.isBrokenFile());
    Assert.assertTrue(scanUtil1.hasUnsortedDataOrWrongStatistics());

    TsFileResource resource2 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource2)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
          Arrays.asList("s0", "s1", "s2"),
          new TimeRange[] {new TimeRange(10, 40)},
          TSEncoding.PLAIN,
          CompressionType.LZ4,
          Arrays.asList(false, false, true));
      writer.endChunkGroup();
      writer.startChunkGroup("d2");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s0",
          new TimeRange[] {new TimeRange(10, 40), new TimeRange(50, 60)},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1",
          new TimeRange[] {new TimeRange(10, 40), new TimeRange(20, 50)},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    RepairDataFileScanUtil scanUtil2 = new RepairDataFileScanUtil(resource2);
    scanUtil2.scanTsFile();
    Assert.assertFalse(scanUtil2.isBrokenFile());
    Assert.assertTrue(scanUtil2.hasUnsortedDataOrWrongStatistics());
  }

  @Test
  public void testScanFileWithDifferentCompressionTypes() throws IOException {
    TsFileResource resource = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s0",
          new TimeRange[] {new TimeRange(10, 40), new TimeRange(50, 60)},
          TSEncoding.PLAIN,
          CompressionType.SNAPPY);
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s0",
          new TimeRange[] {new TimeRange(10, 40), new TimeRange(50, 60)},
          TSEncoding.PLAIN,
          CompressionType.GZIP);
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1",
          new TimeRange[] {new TimeRange(10, 40), new TimeRange(20, 50)},
          TSEncoding.PLAIN,
          CompressionType.ZSTD);
      writer.endChunkGroup();
      writer.endFile();
    }
    RepairDataFileScanUtil scanUtil = new RepairDataFileScanUtil(resource);
    scanUtil.scanTsFile();
    Assert.assertFalse(scanUtil.isBrokenFile());
    Assert.assertTrue(scanUtil.hasUnsortedDataOrWrongStatistics());
  }
}
