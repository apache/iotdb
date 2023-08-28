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

package org.apache.iotdb.db.storageengine.dataregion.compaction.utils;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.TimeRange;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class CompactionTestFileWriterTest extends AbstractCompactionTest {
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
  public void testGenerateNonAlignedSeriesWithChunk() {
    TsFileResource seqResource1 = createEmptyFileAndResource(true);
    try {
      CompactionTestFileWriter fileWriter = new CompactionTestFileWriter(seqResource1);
      fileWriter.startChunkGroup("d0");
      fileWriter.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s0",
          new TimeRange[] {new TimeRange(1000, 3000), new TimeRange(4000, 6000)},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      fileWriter.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s0",
          new TimeRange[] {new TimeRange(7000, 9000), new TimeRange(14000, 16000)},
          TSEncoding.PLAIN,
          CompressionType.SNAPPY);
      fileWriter.endChunkGroup();
      fileWriter.endFile();
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testGenerateNonAlignedSeriesWithPage() throws IOException {
    try {
      TsFileResource seqResource1 = createEmptyFileAndResource(true);
      CompactionTestFileWriter fileWriter = new CompactionTestFileWriter(seqResource1);
      fileWriter.startChunkGroup("d0");
      TimeRange[] chunk1PageRanges =
          new TimeRange[] {new TimeRange(1000, 1500), new TimeRange(2000, 2500)};
      TimeRange[] chunk2PageRanges =
          new TimeRange[] {new TimeRange(3000, 3500), new TimeRange(5000, 5500)};

      fileWriter.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s0",
          new TimeRange[][] {chunk1PageRanges, chunk2PageRanges},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      fileWriter.endChunkGroup();
      fileWriter.endFile();
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testGenerateNonAlignedSeriesWithPoints() throws IOException {
    try {
      TsFileResource seqResource1 = createEmptyFileAndResource(true);
      CompactionTestFileWriter fileWriter = new CompactionTestFileWriter(seqResource1);
      fileWriter.startChunkGroup("d0");
      TimeRange[] chunk1Page1Points =
          new TimeRange[] {new TimeRange(1000, 1500), new TimeRange(2000, 2500)};
      TimeRange[] chunk1Page2Points =
          new TimeRange[] {new TimeRange(3000, 3500), new TimeRange(5000, 5500)};
      TimeRange[][] chunk1 = new TimeRange[][] {chunk1Page1Points, chunk1Page2Points};
      TimeRange[] chunk2Page1Points =
          new TimeRange[] {new TimeRange(6000, 6500), new TimeRange(7000, 7500)};
      TimeRange[] chunk2Page2Points =
          new TimeRange[] {new TimeRange(8000, 8500), new TimeRange(9000, 9500)};
      TimeRange[][] chunk2 = new TimeRange[][] {chunk2Page1Points, chunk2Page2Points};

      fileWriter.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s0", new TimeRange[][][] {chunk1, chunk2}, TSEncoding.PLAIN, CompressionType.LZ4);
      fileWriter.endChunkGroup();
      fileWriter.endFile();
      System.out.println(seqResource1.getTsFile().getAbsolutePath());
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testGenerateAlignedSeriesWithChunk() throws IOException {
    try {
      TsFileResource seqResource1 = createEmptyFileAndResource(true);
      CompactionTestFileWriter fileWriter = new CompactionTestFileWriter(seqResource1);
      fileWriter.startChunkGroup("d0");
      fileWriter.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s0",
          new TimeRange[] {new TimeRange(1000, 3000), new TimeRange(4000, 6000)},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      fileWriter.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s0", "s1", "s2"),
          new TimeRange[] {new TimeRange(7000, 9000), new TimeRange(14000, 16000)},
          TSEncoding.PLAIN,
          CompressionType.SNAPPY);
      fileWriter.endChunkGroup();
      fileWriter.endFile();
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testGenerateAlignedSeriesWithPage() throws IOException {
    try {
      TsFileResource seqResource1 = createEmptyFileAndResource(true);
      CompactionTestFileWriter fileWriter = new CompactionTestFileWriter(seqResource1);
      fileWriter.startChunkGroup("d0");
      TimeRange[] chunk1PageRanges =
          new TimeRange[] {new TimeRange(1000, 1500), new TimeRange(2000, 2500)};
      TimeRange[] chunk2PageRanges =
          new TimeRange[] {new TimeRange(3000, 3500), new TimeRange(5000, 5500)};

      fileWriter.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s0", "s1", "s2"),
          new TimeRange[][] {chunk1PageRanges, chunk2PageRanges},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      fileWriter.endChunkGroup();
      fileWriter.endFile();
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testGenerateAlignedSeriesWithPoints() throws IOException {
    try {
      TsFileResource seqResource1 = createEmptyFileAndResource(true);
      CompactionTestFileWriter fileWriter = new CompactionTestFileWriter(seqResource1);
      fileWriter.startChunkGroup("d0");
      TimeRange[] chunk1Page1Points =
          new TimeRange[] {new TimeRange(1000, 1500), new TimeRange(2000, 2500)};
      TimeRange[] chunk1Page2Points =
          new TimeRange[] {new TimeRange(3000, 3500), new TimeRange(5000, 5500)};
      TimeRange[][] chunk1 = new TimeRange[][] {chunk1Page1Points, chunk1Page2Points};
      TimeRange[] chunk2Page1Points =
          new TimeRange[] {new TimeRange(6000, 6500), new TimeRange(7000, 7500)};
      TimeRange[] chunk2Page2Points =
          new TimeRange[] {new TimeRange(8000, 8500), new TimeRange(9000, 9500)};
      TimeRange[][] chunk2 = new TimeRange[][] {chunk2Page1Points, chunk2Page2Points};

      fileWriter.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s0", "s1", "s2"),
          new TimeRange[][][] {chunk1, chunk2},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      fileWriter.endChunkGroup();
      fileWriter.endFile();
      System.out.println(seqResource1.getTsFile().getAbsolutePath());
    } catch (Exception e) {
      Assert.fail();
    }
  }
}
