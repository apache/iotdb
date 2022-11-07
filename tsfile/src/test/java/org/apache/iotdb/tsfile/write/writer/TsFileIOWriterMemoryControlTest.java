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

package org.apache.iotdb.tsfile.write.writer;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.TsFileIntegrityCheckingTool;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.TimeChunkWriter;
import org.apache.iotdb.tsfile.write.chunk.ValueChunkWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.tsmiterator.TSMIterator;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class TsFileIOWriterMemoryControlTest {
  private static File testFile = new File("target", "1-1-0-0.tsfile");
  private static File emptyFile = new File("target", "temp");
  private long TEST_CHUNK_SIZE = 1000;
  private List<String> sortedSeriesId = new ArrayList<>();
  private List<String> sortedDeviceId = new ArrayList<>();
  private boolean init = false;

  @Before
  public void setUp() throws IOException {
    if (!init) {
      init = true;
      for (int i = 0; i < 2048; ++i) {
        sortedSeriesId.add("s" + i);
        sortedDeviceId.add("root.sg.d" + i);
      }
      sortedSeriesId.sort((String::compareTo));
      sortedDeviceId.sort((String::compareTo));
    }
    TEST_CHUNK_SIZE = 1000;
  }

  @After
  public void tearDown() throws IOException {
    if (testFile.exists()) {
      FileUtils.delete(testFile);
    }
    if (new File(testFile.getPath() + TsFileIOWriter.CHUNK_METADATA_TEMP_FILE_SUFFIX).exists()) {
      FileUtils.delete(
          new File(testFile.getPath() + TsFileIOWriter.CHUNK_METADATA_TEMP_FILE_SUFFIX));
    }
    if (emptyFile.exists()) {
      FileUtils.delete(emptyFile);
    }
  }

  /** The following tests is for ChunkMetadata serialization and deserialization. */
  @Test
  public void testSerializeAndDeserializeChunkMetadata() throws IOException {
    try (TsFileIOWriter writer = new TsFileIOWriter(testFile, true, 1024 * 1024 * 10)) {
      List<ChunkMetadata> originChunkMetadataList = new ArrayList<>();
      for (int i = 0; i < 10; ++i) {
        String deviceId = sortedDeviceId.get(i);
        writer.startChunkGroup(deviceId);
        for (int j = 0; j < 5; ++j) {
          ChunkWriterImpl chunkWriter;
          switch (j) {
            case 0:
              chunkWriter = generateIntData(j, 0L, new ArrayList<>());
              break;
            case 1:
              chunkWriter = generateBooleanData(j, 0, new ArrayList<>());
              break;
            case 2:
              chunkWriter = generateFloatData(j, 0L, new ArrayList<>());
              break;
            case 3:
              chunkWriter = generateDoubleData(j, 0L, new ArrayList<>());
              break;
            case 4:
            default:
              chunkWriter = generateTextData(j, 0L, new ArrayList<>());
              break;
          }
          chunkWriter.writeToFileWriter(writer);
        }
        originChunkMetadataList.addAll(writer.chunkMetadataList);
        writer.endChunkGroup();
      }
      writer.sortAndFlushChunkMetadata();
      writer.tempOutput.flush();

      TSMIterator iterator =
          TSMIterator.getTSMIteratorInDisk(
              writer.chunkMetadataTempFile,
              writer.chunkGroupMetadataList,
              writer.endPosInCMTForDevice);
      for (int i = 0; iterator.hasNext(); ++i) {
        Pair<Path, TimeseriesMetadata> timeseriesMetadataPair = iterator.next();
        TimeseriesMetadata timeseriesMetadata = timeseriesMetadataPair.right;
        Assert.assertEquals(sortedSeriesId.get(i % 5), timeseriesMetadata.getMeasurementId());
        Assert.assertEquals(
            originChunkMetadataList.get(i).getDataType(), timeseriesMetadata.getTSDataType());
        Assert.assertEquals(
            originChunkMetadataList.get(i).getStatistics(), timeseriesMetadata.getStatistics());
      }
    }
  }

  @Test
  public void testSerializeAndDeserializeAlignedChunkMetadata() throws IOException {
    try (TsFileIOWriter writer = new TsFileIOWriter(testFile, true, 1024 * 1024 * 10)) {
      List<ChunkMetadata> originChunkMetadataList = new ArrayList<>();
      for (int i = 0; i < 10; ++i) {
        String deviceId = sortedDeviceId.get(i);
        writer.startChunkGroup(deviceId);
        AlignedChunkWriterImpl chunkWriter = generateVectorData(0L, new ArrayList<>(), 6);
        chunkWriter.writeToFileWriter(writer);
        originChunkMetadataList.addAll(writer.chunkMetadataList);
        writer.endChunkGroup();
      }
      writer.sortAndFlushChunkMetadata();
      writer.tempOutput.flush();

      List<String> measurementIds = new ArrayList<>();
      for (int i = 0; i < 10; ++i) {
        measurementIds.add(sortedDeviceId.get(i) + ".");
        for (int j = 1; j <= 6; ++j) {
          measurementIds.add(sortedDeviceId.get(i) + ".s" + j);
        }
      }
      TSMIterator iterator =
          TSMIterator.getTSMIteratorInDisk(
              writer.chunkMetadataTempFile, new ArrayList<>(), writer.endPosInCMTForDevice);
      for (int i = 0; iterator.hasNext(); ++i) {
        Pair<Path, TimeseriesMetadata> timeseriesMetadataPair = iterator.next();
        String fullPath = timeseriesMetadataPair.left.getFullPath();
        TimeseriesMetadata timeseriesMetadata = timeseriesMetadataPair.right;
        Assert.assertEquals(measurementIds.get(i), fullPath);
        Assert.assertEquals(
            originChunkMetadataList.get(i).getDataType(), timeseriesMetadata.getTSDataType());
        Assert.assertEquals(
            originChunkMetadataList.get(i).getStatistics(), timeseriesMetadata.getStatistics());
      }
    }
  }

  @Test
  public void testSerializeAndDeserializeMixedChunkMetadata() throws IOException {
    try (TsFileIOWriter writer = new TsFileIOWriter(testFile, true, 1024 * 1024 * 10)) {
      List<IChunkMetadata> originChunkMetadataList = new ArrayList<>();
      List<String> seriesIds = new ArrayList<>();
      for (int i = 0; i < 10; ++i) {
        String deviceId = sortedDeviceId.get(i);
        writer.startChunkGroup(deviceId);
        if (i % 2 == 0) {
          // write normal series
          for (int j = 0; j < 5; ++j) {
            ChunkWriterImpl chunkWriter;
            switch (j) {
              case 0:
                chunkWriter = generateIntData(j, 0L, new ArrayList<>());
                break;
              case 1:
                chunkWriter = generateBooleanData(j, 0L, new ArrayList<>());
                break;
              case 2:
                chunkWriter = generateFloatData(j, 0L, new ArrayList<>());
                break;
              case 3:
                chunkWriter = generateDoubleData(j, 0L, new ArrayList<>());
                break;
              case 4:
              default:
                chunkWriter = generateTextData(j, 0L, new ArrayList<>());
                break;
            }
            chunkWriter.writeToFileWriter(writer);
            seriesIds.add(deviceId + "." + sortedSeriesId.get(j));
          }
        } else {
          // write vector
          AlignedChunkWriterImpl chunkWriter = generateVectorData(0L, new ArrayList<>(), 6);
          chunkWriter.writeToFileWriter(writer);
          seriesIds.add(deviceId + ".");
          for (int l = 1; l <= 6; ++l) {
            seriesIds.add(deviceId + ".s" + l);
          }
        }
        originChunkMetadataList.addAll(writer.chunkMetadataList);
        writer.endChunkGroup();
      }
      writer.sortAndFlushChunkMetadata();
      writer.tempOutput.flush();

      TSMIterator iterator =
          TSMIterator.getTSMIteratorInDisk(
              writer.chunkMetadataTempFile, new ArrayList<>(), writer.endPosInCMTForDevice);
      for (int i = 0; i < originChunkMetadataList.size(); ++i) {
        Pair<Path, TimeseriesMetadata> timeseriesMetadataPair = iterator.next();
        Assert.assertEquals(seriesIds.get(i), timeseriesMetadataPair.left.getFullPath());
        Assert.assertEquals(
            originChunkMetadataList.get(i).getDataType(),
            timeseriesMetadataPair.right.getTSDataType());
        Assert.assertEquals(
            originChunkMetadataList.get(i).getStatistics(),
            timeseriesMetadataPair.right.getStatistics());
      }
    }
  }

  /** The following tests is for writing normal series in different nums. */

  /**
   * Write a file with 10 devices and 5 series in each device. For each series, we write one chunk
   * for it. This test make sure that each chunk
   *
   * @throws IOException
   */
  @Test
  public void testWriteCompleteFileWithNormalChunk() throws IOException {
    Map<String, Map<String, List<List<Pair<Long, TsPrimitiveType>>>>> originData = new HashMap<>();
    try (TsFileIOWriter writer = new TsFileIOWriter(testFile, true, 1024)) {
      for (int i = 0; i < 10; ++i) {
        String deviceId = sortedDeviceId.get(i);
        writer.startChunkGroup(deviceId);
        for (int j = 0; j < 5; ++j) {
          List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
          ChunkWriterImpl chunkWriter;
          switch (j) {
            case 0:
              chunkWriter = generateIntData(j, 0L, valList);
              break;
            case 1:
              chunkWriter = generateBooleanData(j, 0L, valList);
              break;
            case 2:
              chunkWriter = generateFloatData(j, 0L, valList);
              break;
            case 3:
              chunkWriter = generateDoubleData(j, 0L, valList);
              break;
            case 4:
            default:
              chunkWriter = generateTextData(j, 0L, valList);
              break;
          }
          chunkWriter.writeToFileWriter(writer);
          writer.checkMetadataSizeAndMayFlush();
          originData
              .computeIfAbsent(deviceId, x -> new HashMap<>())
              .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
              .add(valList);
        }
        writer.endChunkGroup();
      }
      Assert.assertTrue(writer.hasChunkMetadataInDisk);
      writer.endFile();
    }
    Assert.assertFalse(
        new File(testFile.getAbsolutePath() + TsFileIOWriter.CHUNK_METADATA_TEMP_FILE_SUFFIX)
            .exists());
    TsFileIntegrityCheckingTool.checkIntegrityBySequenceRead(testFile.getPath());
    TsFileIntegrityCheckingTool.checkIntegrityByQuery(testFile.getPath(), originData);
  }

  /**
   * Write a file with 10 devices and 5 series in each device. For each series, we write 100 chunks
   * for it. This test make sure that each chunk
   *
   * @throws IOException
   */
  @Test
  public void testWriteCompleteFileWithMultipleNormalChunk() throws IOException {
    Map<String, Map<String, List<List<Pair<Long, TsPrimitiveType>>>>> originData = new HashMap<>();
    try (TsFileIOWriter writer = new TsFileIOWriter(testFile, true, 1024)) {
      for (int i = 0; i < 10; ++i) {
        String deviceId = sortedDeviceId.get(i);
        writer.startChunkGroup(deviceId);
        for (int j = 0; j < 5; ++j) {
          ChunkWriterImpl chunkWriter;
          switch (j) {
            case 0:
              for (int k = 0; k < 10; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateIntData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originData
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
            case 1:
              for (int k = 0; k < 10; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateBooleanData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originData
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
            case 2:
              for (int k = 0; k < 10; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateFloatData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originData
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
            case 3:
              for (int k = 0; k < 10; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateDoubleData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originData
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
            case 4:
            default:
              for (int k = 0; k < 10; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateTextData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originData
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
          }
          writer.checkMetadataSizeAndMayFlush();
        }
        writer.endChunkGroup();
      }
      Assert.assertTrue(writer.hasChunkMetadataInDisk);
      writer.endFile();
    }
    Assert.assertFalse(
        new File(testFile.getAbsolutePath() + TsFileIOWriter.CHUNK_METADATA_TEMP_FILE_SUFFIX)
            .exists());
    TsFileIntegrityCheckingTool.checkIntegrityBySequenceRead(testFile.getPath());
    TsFileIntegrityCheckingTool.checkIntegrityByQuery(testFile.getPath(), originData);
  }

  /**
   * Write a file with 10 devices and 5 series in each device. For each series, we write 100 chunks
   * for it. We maintain some chunk metadata in memory when calling endFile().
   *
   * @throws IOException
   */
  @Test
  public void testWriteCompleteFileWithMetadataRemainsInMemoryWhenEndFile() throws IOException {
    Map<String, Map<String, List<List<Pair<Long, TsPrimitiveType>>>>> originData = new HashMap<>();
    try (TsFileIOWriter writer = new TsFileIOWriter(testFile, true, 1024)) {
      for (int i = 0; i < 10; ++i) {
        String deviceId = sortedDeviceId.get(i);
        writer.startChunkGroup(deviceId);
        for (int j = 0; j < 5; ++j) {
          ChunkWriterImpl chunkWriter;
          switch (j) {
            case 0:
              for (int k = 0; k < 10; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateIntData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originData
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
            case 1:
              for (int k = 0; k < 10; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateBooleanData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originData
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
            case 2:
              for (int k = 0; k < 10; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateFloatData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originData
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
            case 3:
              for (int k = 0; k < 10; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateDoubleData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originData
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
            case 4:
            default:
              for (int k = 0; k < 10; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateTextData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originData
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
          }
          if (i < 9) {
            writer.checkMetadataSizeAndMayFlush();
          }
        }
        writer.endChunkGroup();
      }
      Assert.assertTrue(writer.hasChunkMetadataInDisk);
      Assert.assertFalse(writer.chunkGroupMetadataList.isEmpty());
      writer.endFile();
    }
    Assert.assertFalse(
        new File(testFile.getAbsolutePath() + TsFileIOWriter.CHUNK_METADATA_TEMP_FILE_SUFFIX)
            .exists());
    TsFileIntegrityCheckingTool.checkIntegrityBySequenceRead(testFile.getPath());
    TsFileIntegrityCheckingTool.checkIntegrityByQuery(testFile.getPath(), originData);
  }

  /**
   * Write a file with 2 devices and 5 series in each device. For each series, we write 1024 chunks
   * for it. This test make sure that each chunk
   *
   * @throws IOException
   */
  @Test
  public void testWriteCompleteFileWithEnormousNormalChunk() throws IOException {
    Map<String, Map<String, List<List<Pair<Long, TsPrimitiveType>>>>> originData = new HashMap<>();
    long originTestChunkSize = TEST_CHUNK_SIZE;
    TEST_CHUNK_SIZE = 10;
    try (TsFileIOWriter writer = new TsFileIOWriter(testFile, true, 1024)) {
      for (int i = 0; i < 2; ++i) {
        String deviceId = sortedDeviceId.get(i);
        writer.startChunkGroup(deviceId);
        for (int j = 0; j < 5; ++j) {
          ChunkWriterImpl chunkWriter;
          switch (j) {
            case 0:
              for (int k = 0; k < 1024; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateIntData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originData
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
            case 1:
              for (int k = 0; k < 1024; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateBooleanData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originData
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
            case 2:
              for (int k = 0; k < 1024; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateFloatData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originData
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
            case 3:
              for (int k = 0; k < 1024; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateDoubleData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originData
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
            case 4:
            default:
              for (int k = 0; k < 1024; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateTextData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originData
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
          }
          writer.checkMetadataSizeAndMayFlush();
        }
        writer.endChunkGroup();
      }
      Assert.assertTrue(writer.hasChunkMetadataInDisk);
      writer.endFile();
    } finally {
      TEST_CHUNK_SIZE = originTestChunkSize;
    }
    Assert.assertFalse(
        new File(testFile.getAbsolutePath() + TsFileIOWriter.CHUNK_METADATA_TEMP_FILE_SUFFIX)
            .exists());
    TsFileIntegrityCheckingTool.checkIntegrityBySequenceRead(testFile.getPath());
    TsFileIntegrityCheckingTool.checkIntegrityByQuery(testFile.getPath(), originData);
  }

  /**
   * Write a file with 2 devices and 1024 series in each device. For each series, we write 50 chunks
   * for it. This test make sure that each chunk
   *
   * @throws IOException
   */
  @Test
  public void testWriteCompleteFileWithEnormousSeriesNum() throws IOException {
    Map<String, Map<String, List<List<Pair<Long, TsPrimitiveType>>>>> originTimes = new HashMap<>();
    long originTestChunkSize = TEST_CHUNK_SIZE;
    TEST_CHUNK_SIZE = 1;
    try (TsFileIOWriter writer = new TsFileIOWriter(testFile, true, 1024)) {
      for (int i = 0; i < 2; ++i) {
        String deviceId = sortedDeviceId.get(i);
        writer.startChunkGroup(deviceId);
        for (int j = 0; j < 1024; ++j) {
          ChunkWriterImpl chunkWriter;
          switch (j % 5) {
            case 0:
              for (int k = 0; k < 50; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateIntData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
            case 1:
              for (int k = 0; k < 50; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateBooleanData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
            case 2:
              for (int k = 0; k < 50; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateFloatData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
            case 3:
              for (int k = 0; k < 50; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateDoubleData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
            case 4:
            default:
              for (int k = 0; k < 50; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateTextData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
          }
          writer.checkMetadataSizeAndMayFlush();
        }
        writer.endChunkGroup();
      }
      Assert.assertTrue(writer.hasChunkMetadataInDisk);
      writer.endFile();
    } finally {
      TEST_CHUNK_SIZE = originTestChunkSize;
    }
    Assert.assertFalse(
        new File(testFile.getAbsolutePath() + TsFileIOWriter.CHUNK_METADATA_TEMP_FILE_SUFFIX)
            .exists());
    TsFileIntegrityCheckingTool.checkIntegrityBySequenceRead(testFile.getPath());
    TsFileIntegrityCheckingTool.checkIntegrityByQuery(testFile.getPath(), originTimes);
  }

  /**
   * Write a file with 1024 devices and 5 series in each device. For each series, we write 10 chunks
   * for it. This test make sure that each chunk
   *
   * @throws IOException
   */
  @Test
  public void testWriteCompleteFileWithEnormousDeviceNum() throws IOException {
    Map<String, Map<String, List<List<Pair<Long, TsPrimitiveType>>>>> originTimes = new HashMap<>();
    long originTestChunkSize = TEST_CHUNK_SIZE;
    TEST_CHUNK_SIZE = 10;
    try (TsFileIOWriter writer = new TsFileIOWriter(testFile, true, 1024)) {
      for (int i = 0; i < 1024; ++i) {
        String deviceId = sortedDeviceId.get(i);
        writer.startChunkGroup(deviceId);
        for (int j = 0; j < 5; ++j) {
          ChunkWriterImpl chunkWriter;
          switch (j % 5) {
            case 0:
              for (int k = 0; k < 10; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateIntData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
            case 1:
              for (int k = 0; k < 10; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateBooleanData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
            case 2:
              for (int k = 0; k < 10; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateFloatData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
            case 3:
              for (int k = 0; k < 10; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateDoubleData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
            case 4:
            default:
              for (int k = 0; k < 10; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateTextData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
          }
          writer.checkMetadataSizeAndMayFlush();
        }
        writer.endChunkGroup();
      }
      Assert.assertTrue(writer.hasChunkMetadataInDisk);
      writer.endFile();
    } finally {
      TEST_CHUNK_SIZE = originTestChunkSize;
    }
    Assert.assertFalse(
        new File(testFile.getAbsolutePath() + TsFileIOWriter.CHUNK_METADATA_TEMP_FILE_SUFFIX)
            .exists());
    TsFileIntegrityCheckingTool.checkIntegrityBySequenceRead(testFile.getPath());
    TsFileIntegrityCheckingTool.checkIntegrityByQuery(testFile.getPath(), originTimes);
  }

  /** The following tests is for writing aligned series. */

  /**
   * Test writing 10 align series, 6 in a group.
   *
   * @throws IOException
   */
  @Test
  public void testWriteCompleteFileWithAlignedSeriesWithOneChunk() throws IOException {
    Map<String, Map<String, List<List<Pair<Long, TsPrimitiveType>>>>> originData = new HashMap<>();
    try (TsFileIOWriter writer = new TsFileIOWriter(testFile, true, 1024)) {
      for (int i = 0; i < 10; ++i) {
        String deviceId = sortedDeviceId.get(i);
        writer.startChunkGroup(deviceId);
        List<List<Pair<Long, TsPrimitiveType>>> valList = new ArrayList<>();
        AlignedChunkWriterImpl chunkWriter = generateVectorData(0L, valList, 6);
        for (int j = 1; j <= 6; ++j) {
          originData
              .computeIfAbsent(deviceId, x -> new HashMap<>())
              .computeIfAbsent("s" + j, x -> new ArrayList<>())
              .add(valList.get(j - 1));
        }

        chunkWriter.writeToFileWriter(writer);
        writer.endChunkGroup();
        writer.checkMetadataSizeAndMayFlush();
      }
      writer.endFile();
      Assert.assertTrue(writer.hasChunkMetadataInDisk);
    }
    Assert.assertFalse(
        new File(testFile.getAbsolutePath() + TsFileIOWriter.CHUNK_METADATA_TEMP_FILE_SUFFIX)
            .exists());
    TsFileIntegrityCheckingTool.checkIntegrityBySequenceRead(testFile.getPath());
    TsFileIntegrityCheckingTool.checkIntegrityByQuery(testFile.getPath(), originData);
  }

  /**
   * Test writing 1 aligned series, for each series we write 512 chunks
   *
   * @throws IOException
   */
  @Test
  public void testWriteCompleteFileWithAlignedSeriesWithMultiChunks() throws IOException {
    Map<String, Map<String, List<List<Pair<Long, TsPrimitiveType>>>>> originData = new HashMap<>();
    int chunkNum = 512, seriesNum = 6;
    try (TsFileIOWriter writer = new TsFileIOWriter(testFile, true, 1024)) {
      for (int i = 0; i < 1; ++i) {
        String deviceId = sortedDeviceId.get(i);
        for (int k = 0; k < chunkNum; ++k) {
          writer.startChunkGroup(deviceId);
          List<List<Pair<Long, TsPrimitiveType>>> valList = new ArrayList<>();
          AlignedChunkWriterImpl chunkWriter =
              generateVectorData(k * TEST_CHUNK_SIZE, valList, seriesNum);
          for (int j = 1; j <= seriesNum; ++j) {
            originData
                .computeIfAbsent(deviceId, x -> new HashMap<>())
                .computeIfAbsent("s" + j, x -> new ArrayList<>())
                .add(valList.get(j - 1));
          }

          chunkWriter.writeToFileWriter(writer);
          writer.endChunkGroup();
        }
        writer.checkMetadataSizeAndMayFlush();
      }
      writer.endFile();
      Assert.assertTrue(writer.hasChunkMetadataInDisk);
    }
    Assert.assertFalse(
        new File(testFile.getAbsolutePath() + TsFileIOWriter.CHUNK_METADATA_TEMP_FILE_SUFFIX)
            .exists());
    TsFileIntegrityCheckingTool.checkIntegrityBySequenceRead(testFile.getPath());
    TsFileIntegrityCheckingTool.checkIntegrityByQuery(testFile.getPath(), originData);
  }

  /**
   * Test write aligned chunk metadata, for each aligned series, we write 1024 components.
   *
   * @throws IOException
   */
  @Test
  public void testWriteCompleteFileWithAlignedSeriesWithManyComponents() throws IOException {
    Map<String, Map<String, List<List<Pair<Long, TsPrimitiveType>>>>> originData = new HashMap<>();
    int chunkNum = 5, seriesNum = 1024;
    long originTestPointNum = TEST_CHUNK_SIZE;
    TEST_CHUNK_SIZE = 10;
    try {
      try (TsFileIOWriter writer = new TsFileIOWriter(testFile, true, 1024)) {
        for (int i = 0; i < 10; ++i) {
          String deviceId = sortedDeviceId.get(i);
          for (int k = 0; k < chunkNum; ++k) {
            writer.startChunkGroup(deviceId);
            List<List<Pair<Long, TsPrimitiveType>>> valList = new ArrayList<>();
            AlignedChunkWriterImpl chunkWriter =
                generateVectorData(k * TEST_CHUNK_SIZE, valList, seriesNum);
            for (int j = 1; j <= seriesNum; ++j) {
              originData
                  .computeIfAbsent(deviceId, x -> new HashMap<>())
                  .computeIfAbsent("s" + j, x -> new ArrayList<>())
                  .add(valList.get(j - 1));
            }

            chunkWriter.writeToFileWriter(writer);
            writer.endChunkGroup();
          }
          writer.checkMetadataSizeAndMayFlush();
        }
        writer.endFile();
        Assert.assertTrue(writer.hasChunkMetadataInDisk);
      }
    } finally {
      TEST_CHUNK_SIZE = originTestPointNum;
    }
    Assert.assertFalse(
        new File(testFile.getAbsolutePath() + TsFileIOWriter.CHUNK_METADATA_TEMP_FILE_SUFFIX)
            .exists());
    TsFileIntegrityCheckingTool.checkIntegrityBySequenceRead(testFile.getPath());
    TsFileIntegrityCheckingTool.checkIntegrityByQuery(testFile.getPath(), originData);
  }

  @Test
  public void testWriteCompleteFileWithLotsAlignedSeries() throws IOException {
    Map<String, Map<String, List<List<Pair<Long, TsPrimitiveType>>>>> originData = new HashMap<>();
    int chunkNum = 5, seriesNum = 12;
    long originTestPointNum = TEST_CHUNK_SIZE;
    TEST_CHUNK_SIZE = 10;
    int deviceNum = 1024;
    try {
      try (TsFileIOWriter writer = new TsFileIOWriter(testFile, true, 1024)) {
        for (int i = 0; i < deviceNum; ++i) {
          String deviceId = sortedDeviceId.get(i);
          for (int k = 0; k < chunkNum; ++k) {
            writer.startChunkGroup(deviceId);
            List<List<Pair<Long, TsPrimitiveType>>> valList = new ArrayList<>();
            AlignedChunkWriterImpl chunkWriter =
                generateVectorData(k * TEST_CHUNK_SIZE, valList, seriesNum);
            for (int j = 1; j <= seriesNum; ++j) {
              originData
                  .computeIfAbsent(deviceId, x -> new HashMap<>())
                  .computeIfAbsent("s" + j, x -> new ArrayList<>())
                  .add(valList.get(j - 1));
            }

            chunkWriter.writeToFileWriter(writer);
            writer.endChunkGroup();
          }
          writer.checkMetadataSizeAndMayFlush();
        }
        writer.endFile();
        Assert.assertTrue(writer.hasChunkMetadataInDisk);
      }
    } finally {
      TEST_CHUNK_SIZE = originTestPointNum;
    }
    Assert.assertFalse(
        new File(testFile.getAbsolutePath() + TsFileIOWriter.CHUNK_METADATA_TEMP_FILE_SUFFIX)
            .exists());
    TsFileIntegrityCheckingTool.checkIntegrityBySequenceRead(testFile.getPath());
    TsFileIntegrityCheckingTool.checkIntegrityByQuery(testFile.getPath(), originData);
  }

  @Test
  public void testWritingAlignedSeriesByColumnWithMultiComponents() throws IOException {
    Map<String, Map<String, List<List<Pair<Long, TsPrimitiveType>>>>> originValue = new HashMap<>();
    TEST_CHUNK_SIZE = 10;
    try (TsFileIOWriter writer = new TsFileIOWriter(testFile, true, 1024)) {
      for (int i = 0; i < 5; i++) {
        String deviceId = sortedDeviceId.get(i);
        writer.startChunkGroup(deviceId);
        TSEncoding timeEncoding =
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder());
        TSDataType timeType = TSFileDescriptor.getInstance().getConfig().getTimeSeriesDataType();
        Encoder encoder = TSEncodingBuilder.getEncodingBuilder(timeEncoding).getEncoder(timeType);
        for (int chunkIdx = 0; chunkIdx < 10; ++chunkIdx) {
          TimeChunkWriter timeChunkWriter =
              new TimeChunkWriter("", CompressionType.SNAPPY, TSEncoding.PLAIN, encoder);
          for (long j = TEST_CHUNK_SIZE * chunkIdx; j < TEST_CHUNK_SIZE * (chunkIdx + 1); ++j) {
            timeChunkWriter.write(j);
          }
          timeChunkWriter.writeToFileWriter(writer);
        }
        writer.sortAndFlushChunkMetadata();
        Assert.assertTrue(writer.hasChunkMetadataInDisk);
        for (int k = 0; k < 1024; ++k) {
          TSEncodingBuilder builder = TSEncodingBuilder.getEncodingBuilder(TSEncoding.PLAIN);
          builder.initFromProps(null);
          for (int chunkIdx = 0; chunkIdx < 10; ++chunkIdx) {
            ValueChunkWriter chunkWriter =
                new ValueChunkWriter(
                    sortedSeriesId.get(k),
                    CompressionType.SNAPPY,
                    TSDataType.DOUBLE,
                    TSEncoding.PLAIN,
                    builder.getEncoder(TSDataType.DOUBLE));
            Random random = new Random();
            List<Pair<Long, TsPrimitiveType>> valueList = new ArrayList<>();
            for (long j = TEST_CHUNK_SIZE * chunkIdx; j < TEST_CHUNK_SIZE * (chunkIdx + 1); ++j) {
              double val = random.nextDouble();
              chunkWriter.write(j, val, false);
              valueList.add(new Pair<>((long) j, new TsPrimitiveType.TsDouble(val)));
            }
            chunkWriter.writeToFileWriter(writer);
            originValue
                .computeIfAbsent(deviceId, x -> new HashMap<>())
                .computeIfAbsent(sortedSeriesId.get(k), x -> new ArrayList<>())
                .add(valueList);
          }
          writer.sortAndFlushChunkMetadata();
        }
        writer.endChunkGroup();
      }
      writer.endFile();
    }
    Assert.assertFalse(
        new File(testFile.getAbsolutePath() + TsFileIOWriter.CHUNK_METADATA_TEMP_FILE_SUFFIX)
            .exists());
    TsFileIntegrityCheckingTool.checkIntegrityBySequenceRead(testFile.getPath());
    TsFileIntegrityCheckingTool.checkIntegrityByQuery(testFile.getPath(), originValue);
  }

  @Test
  public void testWritingCompleteMixedFiles() throws IOException {
    Map<String, Map<String, List<List<Pair<Long, TsPrimitiveType>>>>> originData = new HashMap<>();
    try (TsFileIOWriter writer = new TsFileIOWriter(testFile, true, 1024)) {
      for (int i = 0; i < 5; ++i) {
        String deviceId = sortedDeviceId.get(i);
        for (int k = 0; k < 10; ++k) {
          writer.startChunkGroup(deviceId);
          List<List<Pair<Long, TsPrimitiveType>>> valList = new ArrayList<>();
          AlignedChunkWriterImpl chunkWriter = generateVectorData(k * TEST_CHUNK_SIZE, valList, 6);
          for (int j = 1; j <= 6; ++j) {
            originData
                .computeIfAbsent(deviceId, x -> new HashMap<>())
                .computeIfAbsent("s" + j, x -> new ArrayList<>())
                .add(valList.get(j - 1));
          }

          chunkWriter.writeToFileWriter(writer);
          writer.endChunkGroup();
        }
        writer.checkMetadataSizeAndMayFlush();
      }
      for (int i = 5; i < 10; ++i) {
        String deviceId = sortedDeviceId.get(i);
        writer.startChunkGroup(deviceId);
        for (int j = 0; j < 5; ++j) {
          ChunkWriterImpl chunkWriter;
          switch (j) {
            case 0:
              for (int k = 0; k < 10; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateIntData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originData
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
            case 1:
              for (int k = 0; k < 10; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateBooleanData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originData
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
            case 2:
              for (int k = 0; k < 10; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateFloatData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originData
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
            case 3:
              for (int k = 0; k < 10; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateDoubleData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originData
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
            case 4:
            default:
              for (int k = 0; k < 10; ++k) {
                List<Pair<Long, TsPrimitiveType>> valList = new ArrayList<>();
                chunkWriter = generateTextData(j, (long) TEST_CHUNK_SIZE * k, valList);
                chunkWriter.writeToFileWriter(writer);
                originData
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(sortedSeriesId.get(j), x -> new ArrayList<>())
                    .add(valList);
              }
              break;
          }
          writer.checkMetadataSizeAndMayFlush();
        }
        writer.endChunkGroup();
      }
      writer.endFile();
      Assert.assertTrue(writer.hasChunkMetadataInDisk);
    }
    Assert.assertFalse(
        new File(testFile.getAbsolutePath() + TsFileIOWriter.CHUNK_METADATA_TEMP_FILE_SUFFIX)
            .exists());
    TsFileIntegrityCheckingTool.checkIntegrityBySequenceRead(testFile.getPath());
    TsFileIntegrityCheckingTool.checkIntegrityByQuery(testFile.getPath(), originData);
  }

  @Test
  public void testWritingAlignedSeriesByColumn() throws IOException {
    Map<String, Map<String, List<List<Pair<Long, TsPrimitiveType>>>>> originValue = new HashMap<>();
    try (TsFileIOWriter writer = new TsFileIOWriter(testFile, true, 1024)) {
      for (int i = 0; i < 5; i++) {
        String deviceId = sortedDeviceId.get(i);
        writer.startChunkGroup(deviceId);
        TSEncoding timeEncoding =
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder());
        TSDataType timeType = TSFileDescriptor.getInstance().getConfig().getTimeSeriesDataType();
        Encoder encoder = TSEncodingBuilder.getEncodingBuilder(timeEncoding).getEncoder(timeType);
        TimeChunkWriter timeChunkWriter =
            new TimeChunkWriter("", CompressionType.SNAPPY, TSEncoding.PLAIN, encoder);
        for (int j = 0; j < TEST_CHUNK_SIZE; ++j) {
          timeChunkWriter.write(j);
        }
        timeChunkWriter.writeToFileWriter(writer);
        writer.sortAndFlushChunkMetadata();
        Assert.assertTrue(writer.hasChunkMetadataInDisk);
        for (int k = 0; k < 5; ++k) {
          TSEncodingBuilder builder = TSEncodingBuilder.getEncodingBuilder(TSEncoding.PLAIN);
          builder.initFromProps(null);
          ValueChunkWriter chunkWriter =
              new ValueChunkWriter(
                  sortedSeriesId.get(k),
                  CompressionType.SNAPPY,
                  TSDataType.DOUBLE,
                  TSEncoding.PLAIN,
                  builder.getEncoder(TSDataType.DOUBLE));
          Random random = new Random();
          List<Pair<Long, TsPrimitiveType>> valueList = new ArrayList<>();
          for (int j = 0; j < TEST_CHUNK_SIZE; ++j) {
            double val = random.nextDouble();
            chunkWriter.write(j, val, false);
            valueList.add(new Pair<>((long) j, new TsPrimitiveType.TsDouble(val)));
          }
          chunkWriter.writeToFileWriter(writer);
          originValue
              .computeIfAbsent(deviceId, x -> new HashMap<>())
              .computeIfAbsent(sortedSeriesId.get(k), x -> new ArrayList<>())
              .add(valueList);
          writer.sortAndFlushChunkMetadata();
        }
        writer.endChunkGroup();
      }
      writer.endFile();
    }
    Assert.assertFalse(
        new File(testFile.getAbsolutePath() + TsFileIOWriter.CHUNK_METADATA_TEMP_FILE_SUFFIX)
            .exists());
    TsFileIntegrityCheckingTool.checkIntegrityBySequenceRead(testFile.getPath());
    TsFileIntegrityCheckingTool.checkIntegrityByQuery(testFile.getPath(), originValue);
  }

  @Test
  public void testWritingAlignedSeriesByColumnWithMultiChunks() throws IOException {
    Map<String, Map<String, List<List<Pair<Long, TsPrimitiveType>>>>> originValue = new HashMap<>();
    try (TsFileIOWriter writer = new TsFileIOWriter(testFile, true, 1024)) {
      for (int i = 0; i < 5; i++) {
        String deviceId = sortedDeviceId.get(i);
        writer.startChunkGroup(deviceId);
        TSEncoding timeEncoding =
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder());
        TSDataType timeType = TSFileDescriptor.getInstance().getConfig().getTimeSeriesDataType();
        Encoder encoder = TSEncodingBuilder.getEncodingBuilder(timeEncoding).getEncoder(timeType);
        for (int chunkIdx = 0; chunkIdx < 10; ++chunkIdx) {
          TimeChunkWriter timeChunkWriter =
              new TimeChunkWriter("", CompressionType.SNAPPY, TSEncoding.PLAIN, encoder);
          for (long j = TEST_CHUNK_SIZE * chunkIdx; j < TEST_CHUNK_SIZE * (chunkIdx + 1); ++j) {
            timeChunkWriter.write(j);
          }
          timeChunkWriter.writeToFileWriter(writer);
        }
        writer.sortAndFlushChunkMetadata();
        Assert.assertTrue(writer.hasChunkMetadataInDisk);
        for (int k = 0; k < 5; ++k) {
          TSEncodingBuilder builder = TSEncodingBuilder.getEncodingBuilder(TSEncoding.PLAIN);
          builder.initFromProps(null);
          for (int chunkIdx = 0; chunkIdx < 10; ++chunkIdx) {
            ValueChunkWriter chunkWriter =
                new ValueChunkWriter(
                    sortedSeriesId.get(k),
                    CompressionType.SNAPPY,
                    TSDataType.DOUBLE,
                    TSEncoding.PLAIN,
                    builder.getEncoder(TSDataType.DOUBLE));
            Random random = new Random();
            List<Pair<Long, TsPrimitiveType>> valueList = new ArrayList<>();
            for (long j = TEST_CHUNK_SIZE * chunkIdx; j < TEST_CHUNK_SIZE * (chunkIdx + 1); ++j) {
              double val = random.nextDouble();
              chunkWriter.write(j, val, false);
              valueList.add(new Pair<>((long) j, new TsPrimitiveType.TsDouble(val)));
            }
            chunkWriter.writeToFileWriter(writer);
            originValue
                .computeIfAbsent(deviceId, x -> new HashMap<>())
                .computeIfAbsent(sortedSeriesId.get(k), x -> new ArrayList<>())
                .add(valueList);
          }
          writer.sortAndFlushChunkMetadata();
        }
        writer.endChunkGroup();
      }
      writer.endFile();
    }
    Assert.assertFalse(
        new File(testFile.getAbsolutePath() + TsFileIOWriter.CHUNK_METADATA_TEMP_FILE_SUFFIX)
            .exists());
    TsFileIntegrityCheckingTool.checkIntegrityBySequenceRead(testFile.getPath());
    TsFileIntegrityCheckingTool.checkIntegrityByQuery(testFile.getPath(), originValue);
  }

  /** The following tests is for writing mixed of normal series and aligned series */
  private ChunkWriterImpl generateIntData(
      int idx, long startTime, List<Pair<Long, TsPrimitiveType>> record) {
    ChunkWriterImpl chunkWriter =
        new ChunkWriterImpl(new MeasurementSchema(sortedSeriesId.get(idx), TSDataType.INT64));
    Random random = new Random();
    for (long i = startTime; i < startTime + TEST_CHUNK_SIZE; ++i) {
      long val = random.nextLong();
      chunkWriter.write(i, val);
      record.add(new Pair<>(i, new TsPrimitiveType.TsLong(val)));
    }
    return chunkWriter;
  }

  private ChunkWriterImpl generateFloatData(
      int idx, long startTime, List<Pair<Long, TsPrimitiveType>> record) {
    ChunkWriterImpl chunkWriter =
        new ChunkWriterImpl(new MeasurementSchema(sortedSeriesId.get(idx), TSDataType.FLOAT));
    Random random = new Random();
    for (long i = startTime; i < startTime + TEST_CHUNK_SIZE; ++i) {
      float val = random.nextFloat();
      chunkWriter.write(i, val);
      record.add(new Pair<>(i, new TsPrimitiveType.TsFloat(val)));
    }
    return chunkWriter;
  }

  private ChunkWriterImpl generateDoubleData(
      int idx, long startTime, List<Pair<Long, TsPrimitiveType>> record) {
    ChunkWriterImpl chunkWriter =
        new ChunkWriterImpl(new MeasurementSchema(sortedSeriesId.get(idx), TSDataType.DOUBLE));
    Random random = new Random();
    for (long i = startTime; i < startTime + TEST_CHUNK_SIZE; ++i) {
      double val = random.nextDouble();
      chunkWriter.write(i, val);
      record.add(new Pair<>(i, new TsPrimitiveType.TsDouble(val)));
    }
    return chunkWriter;
  }

  private ChunkWriterImpl generateBooleanData(
      int idx, long startTime, List<Pair<Long, TsPrimitiveType>> record) {
    ChunkWriterImpl chunkWriter =
        new ChunkWriterImpl(new MeasurementSchema(sortedSeriesId.get(idx), TSDataType.BOOLEAN));
    Random random = new Random();
    for (long i = startTime; i < startTime + TEST_CHUNK_SIZE; ++i) {
      boolean val = random.nextBoolean();
      chunkWriter.write(i, val);
      record.add(new Pair<>(i, new TsPrimitiveType.TsBoolean(val)));
    }
    return chunkWriter;
  }

  private AlignedChunkWriterImpl generateVectorData(
      long startTime, List<List<Pair<Long, TsPrimitiveType>>> record, int seriesNum) {
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.INT32,
          TSDataType.INT64,
          TSDataType.FLOAT,
          TSDataType.DOUBLE,
          TSDataType.BOOLEAN,
          TSDataType.TEXT
        };
    for (int i = 0; i < seriesNum; ++i) {
      measurementSchemas.add(new MeasurementSchema("s" + (i + 1), dataTypes[i % 6]));
    }
    AlignedChunkWriterImpl chunkWriter = new AlignedChunkWriterImpl(measurementSchemas);
    Random random = new Random();
    for (int i = 0; i < seriesNum; ++i) {
      record.add(new ArrayList<>());
    }
    for (long i = startTime; i < startTime + TEST_CHUNK_SIZE; ++i) {
      TsPrimitiveType[] points = new TsPrimitiveType[seriesNum];
      for (int j = 0; j < seriesNum; ++j) {
        switch (j % 6) {
          case 0:
            points[j] = new TsPrimitiveType.TsInt(random.nextInt());
            break;
          case 1:
            points[j] = new TsPrimitiveType.TsLong(random.nextLong());
            break;
          case 2:
            points[j] = new TsPrimitiveType.TsFloat(random.nextFloat());
            break;
          case 3:
            points[j] = new TsPrimitiveType.TsDouble(random.nextDouble());
            break;
          case 4:
            points[j] = new TsPrimitiveType.TsBoolean(random.nextBoolean());
            break;
          case 5:
            points[j] =
                new TsPrimitiveType.TsBinary(new Binary(String.valueOf(random.nextDouble())));
            break;
        }
      }
      for (int j = 0; j < seriesNum; ++j) {
        record.get(j).add(new Pair<>(i, points[j]));
      }
      chunkWriter.write(i, points);
    }
    return chunkWriter;
  }

  private ChunkWriterImpl generateTextData(
      int idx, long startTime, List<Pair<Long, TsPrimitiveType>> record) {
    ChunkWriterImpl chunkWriter =
        new ChunkWriterImpl(new MeasurementSchema(sortedSeriesId.get(idx), TSDataType.TEXT));
    Random random = new Random();
    for (long i = startTime; i < startTime + TEST_CHUNK_SIZE; ++i) {
      Binary val = new Binary(String.valueOf(random.nextDouble()));
      chunkWriter.write(i, val);
      record.add(new Pair<>(i, new TsPrimitiveType.TsBinary(val)));
    }
    return chunkWriter;
  }
}
