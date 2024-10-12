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
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.reader.CompactionChunkReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.reader.chunk.AlignedChunkReader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FastCompactionPerformerWithInconsistentCompressionTypeAndEncodingTest
    extends AbstractCompactionTest {

  int oldMinCrossCompactionUnseqFileLevel;

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    oldMinCrossCompactionUnseqFileLevel =
        IoTDBDescriptor.getInstance().getConfig().getMinCrossCompactionUnseqFileLevel();
    IoTDBDescriptor.getInstance().getConfig().setMinCrossCompactionUnseqFileLevel(0);
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMinCrossCompactionUnseqFileLevel(oldMinCrossCompactionUnseqFileLevel);
  }

  @Test
  public void test1() throws MetadataException, IOException, WriteProcessException {

    TsFileResource seqResource1 =
        generateSingleNonAlignedSeriesFile(
            "d0",
            "s0",
            new TimeRange[] {new TimeRange(100000, 200000), new TimeRange(300000, 500000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    seqResources.add(seqResource1);

    TsFileResource seqResource2 =
        generateSingleNonAlignedSeriesFile(
            "d0",
            "s0",
            new TimeRange[] {new TimeRange(600000, 700000), new TimeRange(800000, 900000)},
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            true);
    seqResources.add(seqResource2);

    tsFileManager.addAll(seqResources, true);

    TsFileResource unseqResource =
        generateSingleNonAlignedSeriesFile(
            "d0",
            "s0",
            new TimeRange[] {new TimeRange(210000, 290000), new TimeRange(710000, 890000)},
            TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED,
            true);
    unseqResources.add(unseqResource);

    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new FastCompactionPerformer(true),
            0,
            0);

    Assert.assertTrue(task.start());
    TsFileResource targetFile = tsFileManager.getTsFileList(true).get(0);
    TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getTsFilePath());
    validateSingleTsFileWithNonAlignedSeries(reader);
  }

  @Test
  public void test2() throws MetadataException, IOException, WriteProcessException {

    TimeRange[] file1Chunk1 =
        new TimeRange[] {
          new TimeRange(10000, 15000), new TimeRange(16000, 19000), new TimeRange(20000, 29000)
        };
    TimeRange[] file1Chunk2 =
        new TimeRange[] {
          new TimeRange(30000, 35000), new TimeRange(36000, 39000), new TimeRange(40000, 49000)
        };

    TsFileResource seqResource1 =
        generateSingleNonAlignedSeriesFile(
            "d0",
            "s0",
            new TimeRange[][] {file1Chunk1, file1Chunk2},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    seqResources.add(seqResource1);

    TimeRange[] file2Chunk1 =
        new TimeRange[] {
          new TimeRange(50000, 55000), new TimeRange(56000, 59000), new TimeRange(60000, 69000)
        };
    TimeRange[] file2Chunk2 =
        new TimeRange[] {
          new TimeRange(70000, 75000), new TimeRange(76000, 79000), new TimeRange(180000, 189000)
        };
    TsFileResource seqResource2 =
        generateSingleNonAlignedSeriesFile(
            "d0",
            "s0",
            new TimeRange[][] {file2Chunk1, file2Chunk2},
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            true);
    seqResources.add(seqResource2);

    tsFileManager.addAll(seqResources, true);

    TimeRange[] unseqFileChunk1 =
        new TimeRange[] {
          new TimeRange(1000, 5000), new TimeRange(96000, 99000), new TimeRange(100000, 110000)
        };
    TimeRange[] unseqFileChunk2 =
        new TimeRange[] {
          new TimeRange(120000, 130000),
          new TimeRange(136000, 149000),
          new TimeRange(200000, 210000)
        };
    TsFileResource unseqResource =
        generateSingleNonAlignedSeriesFile(
            "d0",
            "s0",
            new TimeRange[][] {unseqFileChunk1, unseqFileChunk2},
            TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED,
            true);
    unseqResources.add(unseqResource);

    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new FastCompactionPerformer(true),
            0,
            0);

    Assert.assertTrue(task.start());
    TsFileResource targetFile = tsFileManager.getTsFileList(true).get(0);
    TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getTsFilePath());
    validateSingleTsFileWithNonAlignedSeries(reader);
  }

  @Test
  public void test3() throws MetadataException, IOException, WriteProcessException {

    TimeRange[] file1Chunk1Page1 =
        new TimeRange[] {new TimeRange(10000, 12000), new TimeRange(16000, 19000)};
    TimeRange[] file1Chunk1Page2 =
        new TimeRange[] {new TimeRange(30000, 35000), new TimeRange(36000, 39000)};

    TsFileResource seqResource1 =
        generateSingleNonAlignedSeriesFile(
            "d0",
            "s0",
            new TimeRange[][][] {new TimeRange[][] {file1Chunk1Page1, file1Chunk1Page2}},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    seqResources.add(seqResource1);

    TimeRange[] file2Chunk1Page1 =
        new TimeRange[] {
          new TimeRange(50000, 55000), new TimeRange(56000, 59000), new TimeRange(68000, 69000)
        };
    TimeRange[] file2Chunk1Page2 =
        new TimeRange[] {
          new TimeRange(70000, 75000), new TimeRange(76000, 79000), new TimeRange(180000, 181000)
        };
    TsFileResource seqResource2 =
        generateSingleNonAlignedSeriesFile(
            "d0",
            "s0",
            new TimeRange[][][] {new TimeRange[][] {file2Chunk1Page1, file2Chunk1Page2}},
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            true);
    seqResources.add(seqResource2);

    tsFileManager.addAll(seqResources, true);

    TimeRange[] unseqFileChunk1 =
        new TimeRange[] {
          new TimeRange(13000, 15000), new TimeRange(96000, 99000), new TimeRange(100000, 101000)
        };
    TimeRange[] unseqFileChunk2 =
        new TimeRange[] {
          new TimeRange(120000, 123000),
          new TimeRange(136000, 139000),
          new TimeRange(200000, 201000)
        };
    TsFileResource unseqResource =
        generateSingleNonAlignedSeriesFile(
            "d0",
            "s0",
            new TimeRange[][] {unseqFileChunk1, unseqFileChunk2},
            TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED,
            true);
    unseqResources.add(unseqResource);

    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new FastCompactionPerformer(true),
            0,
            0);

    Assert.assertTrue(task.start());
    TsFileResource targetFile = tsFileManager.getTsFileList(true).get(0);
    TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getTsFilePath());
    //    validateSingleTsFileWithNonAlignedSeries(reader);
  }

  @Test
  public void test4() throws MetadataException, IOException, WriteProcessException {

    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(100000, 200000), new TimeRange(300000, 500000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    seqResources.add(seqResource1);

    TsFileResource seqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(600000, 700000), new TimeRange(800000, 900000)},
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            true);
    seqResources.add(seqResource2);

    tsFileManager.addAll(seqResources, true);

    TsFileResource unseqResource =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(210000, 290000), new TimeRange(710000, 890000)},
            TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED,
            true);
    unseqResources.add(unseqResource);

    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new FastCompactionPerformer(true),
            0,
            0);

    Assert.assertTrue(task.start());
    TsFileResource targetFile = tsFileManager.getTsFileList(true).get(0);
    TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getTsFilePath());
    //    validateSingleTsFileWithNonAlignedSeries(reader);
  }

  @Test
  public void test5() throws MetadataException, IOException, WriteProcessException {

    TimeRange[] file1Chunk1 =
        new TimeRange[] {
          new TimeRange(10000, 15000), new TimeRange(16000, 19000), new TimeRange(20000, 29000)
        };
    TimeRange[] file1Chunk2 =
        new TimeRange[] {
          new TimeRange(30000, 35000), new TimeRange(36000, 39000), new TimeRange(40000, 49000)
        };

    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {file1Chunk1, file1Chunk2},
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            true);
    seqResources.add(seqResource1);

    TimeRange[] file2Chunk1 =
        new TimeRange[] {
          new TimeRange(50000, 55000), new TimeRange(56000, 59000), new TimeRange(60000, 69000)
        };
    TimeRange[] file2Chunk2 =
        new TimeRange[] {
          new TimeRange(70000, 75000), new TimeRange(76000, 79000), new TimeRange(180000, 189000)
        };
    TsFileResource seqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {file2Chunk1, file2Chunk2},
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            true);
    seqResources.add(seqResource2);

    tsFileManager.addAll(seqResources, true);

    TimeRange[] unseqFileChunk1 =
        new TimeRange[] {
          new TimeRange(1000, 5000), new TimeRange(96000, 99000), new TimeRange(100000, 110000)
        };
    TimeRange[] unseqFileChunk2 =
        new TimeRange[] {
          new TimeRange(120000, 130000),
          new TimeRange(136000, 149000),
          new TimeRange(200000, 210000)
        };
    TsFileResource unseqResource =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {unseqFileChunk1, unseqFileChunk2},
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            true);
    unseqResources.add(unseqResource);

    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new FastCompactionPerformer(true),
            0,
            0);

    Assert.assertTrue(task.start());
    TsFileResource targetFile = tsFileManager.getTsFileList(true).get(0);
    TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getTsFilePath());
    //    validateSingleTsFileWithNonAlignedSeries(reader);
  }

  @Test
  public void test6() throws MetadataException, IOException, WriteProcessException {

    TimeRange[] file1Chunk1Page1 =
        new TimeRange[] {new TimeRange(10000, 12000), new TimeRange(16000, 19000)};
    TimeRange[] file1Chunk1Page2 =
        new TimeRange[] {new TimeRange(30000, 35000), new TimeRange(36000, 39000)};

    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][][] {new TimeRange[][] {file1Chunk1Page1, file1Chunk1Page2}},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            true);
    seqResources.add(seqResource1);

    TimeRange[] file2Chunk1Page1 =
        new TimeRange[] {
          new TimeRange(50000, 55000), new TimeRange(56000, 59000), new TimeRange(68000, 69000)
        };
    TimeRange[] file2Chunk1Page2 =
        new TimeRange[] {
          new TimeRange(70000, 75000), new TimeRange(76000, 79000), new TimeRange(180000, 181000)
        };
    TsFileResource seqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][][] {new TimeRange[][] {file2Chunk1Page1, file2Chunk1Page2}},
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            true);
    seqResources.add(seqResource2);

    tsFileManager.addAll(seqResources, true);

    TimeRange[] unseqFileChunk1 =
        new TimeRange[] {
          new TimeRange(13000, 15000), new TimeRange(96000, 99000), new TimeRange(100000, 101000)
        };
    TimeRange[] unseqFileChunk2 =
        new TimeRange[] {
          new TimeRange(120000, 123000),
          new TimeRange(136000, 139000),
          new TimeRange(200000, 201000)
        };
    TsFileResource unseqResource =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {unseqFileChunk1, unseqFileChunk2},
            TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED,
            true);
    unseqResources.add(unseqResource);

    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new FastCompactionPerformer(true),
            0,
            0);

    Assert.assertTrue(task.start());
    TsFileResource targetFile = tsFileManager.getTsFileList(true).get(0);
    TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getTsFilePath());
    validateSingleTsFileWithAlignedSeries(reader);
  }

  private TsFileResource generateSingleNonAlignedSeriesFile(
      String device,
      String measurement,
      TimeRange[] chunkTimeRanges,
      TSEncoding encoding,
      CompressionType compressionType,
      boolean isSeq)
      throws IOException {
    TsFileResource seqResource1 = createEmptyFileAndResource(isSeq);
    CompactionTestFileWriter writer1 = new CompactionTestFileWriter(seqResource1);
    writer1.startChunkGroup(device);
    writer1.generateSimpleNonAlignedSeriesToCurrentDevice(
        measurement, chunkTimeRanges, encoding, compressionType);
    writer1.endChunkGroup();
    writer1.endFile();
    writer1.close();
    return seqResource1;
  }

  private TsFileResource generateSingleNonAlignedSeriesFile(
      String device,
      String measurement,
      TimeRange[][] chunkTimeRanges,
      TSEncoding encoding,
      CompressionType compressionType,
      boolean isSeq)
      throws IOException {
    TsFileResource seqResource1 = createEmptyFileAndResource(isSeq);
    CompactionTestFileWriter writer1 = new CompactionTestFileWriter(seqResource1);
    writer1.startChunkGroup(device);
    writer1.generateSimpleNonAlignedSeriesToCurrentDevice(
        measurement, chunkTimeRanges, encoding, compressionType);
    writer1.endChunkGroup();
    writer1.endFile();
    writer1.close();
    return seqResource1;
  }

  private TsFileResource generateSingleNonAlignedSeriesFile(
      String device,
      String measurement,
      TimeRange[][][] chunkTimeRanges,
      TSEncoding encoding,
      CompressionType compressionType,
      boolean isSeq)
      throws IOException {
    TsFileResource seqResource1 = createEmptyFileAndResource(isSeq);
    CompactionTestFileWriter writer1 = new CompactionTestFileWriter(seqResource1);
    writer1.startChunkGroup(device);
    writer1.generateSimpleNonAlignedSeriesToCurrentDevice(
        measurement, chunkTimeRanges, encoding, compressionType);
    writer1.endChunkGroup();
    writer1.endFile();
    writer1.close();
    return seqResource1;
  }

  private TsFileResource generateSingleAlignedSeriesFile(
      String device,
      List<String> measurement,
      TimeRange[] chunkTimeRanges,
      TSEncoding encoding,
      CompressionType compressionType,
      boolean isSeq)
      throws IOException {
    TsFileResource seqResource1 = createEmptyFileAndResource(isSeq);
    CompactionTestFileWriter writer1 = new CompactionTestFileWriter(seqResource1);
    writer1.startChunkGroup(device);
    writer1.generateSimpleAlignedSeriesToCurrentDevice(
        measurement, chunkTimeRanges, encoding, compressionType);
    writer1.endChunkGroup();
    writer1.endFile();
    writer1.close();
    return seqResource1;
  }

  private TsFileResource generateSingleAlignedSeriesFile(
      String device,
      List<String> measurement,
      TimeRange[][] chunkTimeRanges,
      TSEncoding encoding,
      CompressionType compressionType,
      boolean isSeq)
      throws IOException {
    TsFileResource seqResource1 = createEmptyFileAndResource(isSeq);
    CompactionTestFileWriter writer1 = new CompactionTestFileWriter(seqResource1);
    writer1.startChunkGroup(device);
    writer1.generateSimpleAlignedSeriesToCurrentDevice(
        measurement, chunkTimeRanges, encoding, compressionType);
    writer1.endChunkGroup();
    writer1.endFile();
    writer1.close();
    return seqResource1;
  }

  private TsFileResource generateSingleAlignedSeriesFile(
      String device,
      List<String> measurement,
      TimeRange[][][] chunkTimeRanges,
      TSEncoding encoding,
      CompressionType compressionType,
      boolean isSeq)
      throws IOException {
    TsFileResource seqResource1 = createEmptyFileAndResource(isSeq);
    CompactionTestFileWriter writer1 = new CompactionTestFileWriter(seqResource1);
    writer1.startChunkGroup(device);
    writer1.generateSimpleAlignedSeriesToCurrentDevice(
        measurement, chunkTimeRanges, encoding, compressionType);
    writer1.endChunkGroup();
    writer1.endFile();
    writer1.close();
    return seqResource1;
  }

  private void validateSingleTsFileWithNonAlignedSeries(TsFileSequenceReader reader)
      throws IOException {
    Map<String, CompressionType> compressionTypeMap = new HashMap<>();
    for (IDeviceID device : reader.getAllDevices()) {
      Map<String, List<ChunkMetadata>> seriesMetaData = reader.readChunkMetadataInDevice(device);
      for (Map.Entry<String, List<ChunkMetadata>> entry : seriesMetaData.entrySet()) {
        String series = entry.getKey();
        List<ChunkMetadata> chunkMetadataList = entry.getValue();
        for (ChunkMetadata chunkMetadata : chunkMetadataList) {
          Chunk chunk = reader.readMemChunk(chunkMetadata);
          ChunkHeader chunkHeader = chunk.getHeader();
          if (!compressionTypeMap.containsKey(series)) {
            compressionTypeMap.put(series, chunkHeader.getCompressionType());
          }
          validatePages(chunk);
        }
      }
    }
  }

  private void validateSingleTsFileWithAlignedSeries(TsFileSequenceReader reader)
      throws IOException {
    Map<String, CompressionType> compressionTypeMap = new HashMap<>();
    for (IDeviceID device : reader.getAllDevices()) {
      List<AlignedChunkMetadata> alignedChunkMetadataList =
          reader.getAlignedChunkMetadata(device, true);
      for (AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadataList) {
        IChunkMetadata timeChunkMetadata = alignedChunkMetadata.getTimeChunkMetadata();
        List<IChunkMetadata> valueChunkMetadataList =
            alignedChunkMetadata.getValueChunkMetadataList();
        Chunk timeChunk = reader.readMemChunk((ChunkMetadata) timeChunkMetadata);
        if (!compressionTypeMap.containsKey("time")) {
          compressionTypeMap.put("time", timeChunk.getHeader().getCompressionType());
        } else if (!compressionTypeMap
            .get("time")
            .equals(timeChunk.getHeader().getCompressionType())) {
          Assert.fail();
        }

        List<Chunk> valueChunks = new ArrayList<>();
        for (IChunkMetadata chunkMetadata : valueChunkMetadataList) {
          Chunk valueChunk = reader.readMemChunk((ChunkMetadata) chunkMetadata);
          if (!compressionTypeMap.containsKey(valueChunk.getHeader().getMeasurementID())) {
            compressionTypeMap.put(
                valueChunk.getHeader().getMeasurementID(),
                valueChunk.getHeader().getCompressionType());
          }
          valueChunks.add(valueChunk);
        }
        validatePages(timeChunk, valueChunks);
      }
    }
  }

  private void validatePages(Chunk chunk) throws IOException {
    ChunkHeader chunkHeader = chunk.getHeader();
    CompactionChunkReader chunkReader = new CompactionChunkReader(chunk);
    ByteBuffer chunkDataBuffer = chunk.getData();
    while (chunkDataBuffer.remaining() > 0) {
      PageHeader pageHeader;
      if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunk.getChunkStatistic());
      } else {
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
      }
      chunkReader.readPageData(
          pageHeader, chunkReader.readPageDataWithoutUncompressing(pageHeader));
    }
  }

  private void validatePages(Chunk timeChunk, List<Chunk> valueChunks) throws IOException {
    AlignedChunkReader chunkReader = new AlignedChunkReader(timeChunk, valueChunks);
    while (chunkReader.hasNextSatisfiedPage()) {
      BatchData batchData = chunkReader.nextPageData();
    }
  }
}
