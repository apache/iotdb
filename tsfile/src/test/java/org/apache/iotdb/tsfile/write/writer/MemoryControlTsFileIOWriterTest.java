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

import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.reader.LocalTsFileInput;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.TsFileIntegrityCheckingTool;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

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

public class MemoryControlTsFileIOWriterTest extends MemoryControlTsFileIOWriter {
  private static File testFile = new File("target", "1-1-0-0.tsfile");
  private static File emptyFile = new File("target", "temp");
  private long TEST_CHUNK_SIZE = 1000;
  private List<String> measurementDictInOrder = new ArrayList<>();
  private List<String> deviceDictInOrder = new ArrayList<>();
  private boolean init = false;

  @Before
  public void setUp() throws IOException {
    if (!init) {
      init = true;
      for (int i = 0; i < 2048; ++i) {
        measurementDictInOrder.add("s" + i);
        deviceDictInOrder.add("root.sg.d" + i);
      }
      measurementDictInOrder.sort((String::compareTo));
      deviceDictInOrder.sort((String::compareTo));
    }
  }

  @After
  public void tearDown() throws IOException {
    this.close();
    if (testFile.exists()) {
      FileUtils.delete(testFile);
    }
    if (new File(testFile.getPath() + MemoryControlTsFileIOWriter.CHUNK_METADATA_TEMP_FILE_PREFIX)
        .exists()) {
      FileUtils.delete(
          new File(
              testFile.getPath() + MemoryControlTsFileIOWriter.CHUNK_METADATA_TEMP_FILE_PREFIX));
    }
    if (emptyFile.exists()) {
      FileUtils.delete(emptyFile);
    }
  }

  public MemoryControlTsFileIOWriterTest() throws IOException {
    super(emptyFile, 1024, true);
  }

  @Test
  public void testSerializeAndDeserializeChunkMetadata() throws IOException {
    try (MemoryControlTsFileIOWriter writer =
        new MemoryControlTsFileIOWriter(testFile, 1024 * 1024 * 10, true)) {
      List<ChunkMetadata> originChunkMetadataList = new ArrayList<>();
      for (int i = 0; i < 10; ++i) {
        String deviceId = deviceDictInOrder.get(i);
        writer.startChunkGroup(deviceId);
        for (int j = 0; j < 5; ++j) {
          ChunkWriterImpl chunkWriter;
          switch (j) {
            case 0:
              chunkWriter = generateIntData(j, 0L);
              break;
            case 1:
              chunkWriter = generateBooleanData(j, 0);
              break;
            case 2:
              chunkWriter = generateFloatData(j, 0L);
              break;
            case 3:
              chunkWriter = generateDoubleData(j, 0L);
              break;
            case 4:
            default:
              chunkWriter = generateTextData(j, 0L);
              break;
          }
          chunkWriter.writeToFileWriter(writer);
        }
        originChunkMetadataList.addAll(writer.chunkMetadataList);
        writer.endChunkGroup();
      }
      writer.sortAndFlushChunkMetadata();
      writer.tempOutput.flush();

      ChunkMetadataReadIterator window =
          writer
          .new ChunkMetadataReadIterator(
              0,
              writer.chunkMetadataTempFile.length(),
              new LocalTsFileInput(writer.chunkMetadataTempFile.toPath()));
      for (int i = 0; i < originChunkMetadataList.size(); ++i) {
        Pair<String, IChunkMetadata> chunkMetadataPair = window.getNextSeriesNameAndChunkMetadata();
        Assert.assertEquals(
            deviceDictInOrder.get(i / 5) + "." + measurementDictInOrder.get(i % 5),
            chunkMetadataPair.left);
        Assert.assertEquals(
            originChunkMetadataList.get(i).getStartTime(), chunkMetadataPair.right.getStartTime());
        Assert.assertEquals(
            originChunkMetadataList.get(i).getEndTime(), chunkMetadataPair.right.getEndTime());
        Assert.assertEquals(
            originChunkMetadataList.get(i).getDataType(), chunkMetadataPair.right.getDataType());
        Assert.assertEquals(
            originChunkMetadataList.get(i).getStatistics(),
            chunkMetadataPair.right.getStatistics());
      }
    }
  }

  @Test
  public void testSerializeAndDeserializeAlignedChunkMetadata() throws IOException {
    try (MemoryControlTsFileIOWriter writer =
        new MemoryControlTsFileIOWriter(testFile, 1024 * 1024 * 10, true)) {
      List<ChunkMetadata> originChunkMetadataList = new ArrayList<>();
      for (int i = 0; i < 10; ++i) {
        String deviceId = deviceDictInOrder.get(i);
        writer.startChunkGroup(deviceId);
        AlignedChunkWriterImpl chunkWriter = generateVectorData(i, 0L);
        chunkWriter.writeToFileWriter(writer);
        originChunkMetadataList.addAll(writer.chunkMetadataList);
        writer.endChunkGroup();
      }
      writer.sortAndFlushChunkMetadata();
      writer.tempOutput.flush();

      List<IChunkMetadata> alignedChunkMetadata = new ArrayList<>();
      IChunkMetadata currentTimeChunkMetadata = originChunkMetadataList.get(0);
      List<IChunkMetadata> currentValueChunkMetadata = new ArrayList<>();
      for (int i = 1; i < originChunkMetadataList.size(); ++i) {
        if (originChunkMetadataList.get(i).getDataType() == TSDataType.VECTOR) {
          alignedChunkMetadata.add(
              new AlignedChunkMetadata(currentTimeChunkMetadata, currentValueChunkMetadata));
          currentTimeChunkMetadata = originChunkMetadataList.get(i);
          currentValueChunkMetadata = new ArrayList<>();
        } else {
          currentValueChunkMetadata.add(originChunkMetadataList.get(i));
        }
      }
      if (currentValueChunkMetadata.size() > 0) {
        alignedChunkMetadata.add(
            new AlignedChunkMetadata(currentTimeChunkMetadata, currentValueChunkMetadata));
      }

      ChunkMetadataReadIterator window =
          writer
          .new ChunkMetadataReadIterator(
              0,
              writer.chunkMetadataTempFile.length(),
              new LocalTsFileInput(writer.chunkMetadataTempFile.toPath()));
      for (int i = 0; i < alignedChunkMetadata.size(); ++i) {
        Pair<String, IChunkMetadata> chunkMetadataPair = window.getNextSeriesNameAndChunkMetadata();
        Assert.assertEquals(deviceDictInOrder.get(i), chunkMetadataPair.left);
        Assert.assertEquals(
            alignedChunkMetadata.get(i).getStartTime(), chunkMetadataPair.right.getStartTime());
        Assert.assertEquals(
            alignedChunkMetadata.get(i).getEndTime(), chunkMetadataPair.right.getEndTime());
        Assert.assertEquals(
            alignedChunkMetadata.get(i).getDataType(), chunkMetadataPair.right.getDataType());
        Assert.assertEquals(
            alignedChunkMetadata.get(i).getStatistics(), chunkMetadataPair.right.getStatistics());
      }
    }
  }

  @Test
  public void testSerializeAndDeserializeMixedChunkMetadata() throws IOException {
    try (MemoryControlTsFileIOWriter writer =
        new MemoryControlTsFileIOWriter(testFile, 1024 * 1024 * 10, true)) {
      List<IChunkMetadata> originChunkMetadataList = new ArrayList<>();
      for (int i = 0; i < 10; ++i) {
        String deviceId = deviceDictInOrder.get(i);
        writer.startChunkGroup(deviceId);
        if (i % 2 == 0) {
          // write normal series
          for (int j = 0; j < 5; ++j) {
            ChunkWriterImpl chunkWriter;
            switch (j) {
              case 0:
                chunkWriter = generateIntData(j, 0L);
                break;
              case 1:
                chunkWriter = generateBooleanData(j, 0L);
                break;
              case 2:
                chunkWriter = generateFloatData(j, 0L);
                break;
              case 3:
                chunkWriter = generateDoubleData(j, 0L);
                break;
              case 4:
              default:
                chunkWriter = generateTextData(j, 0L);
                break;
            }
            chunkWriter.writeToFileWriter(writer);
          }
          originChunkMetadataList.addAll(writer.chunkMetadataList);
        } else {
          // write vector
          AlignedChunkWriterImpl chunkWriter = generateVectorData(i, 0L);
          chunkWriter.writeToFileWriter(writer);
          originChunkMetadataList.add(
              new AlignedChunkMetadata(
                  writer.chunkMetadataList.get(0),
                  new ArrayList<>(
                      writer.chunkMetadataList.subList(1, writer.chunkMetadataList.size()))));
        }
        writer.endChunkGroup();
      }
      writer.sortAndFlushChunkMetadata();
      writer.tempOutput.flush();

      ChunkMetadataReadIterator window =
          writer
          .new ChunkMetadataReadIterator(
              0,
              writer.chunkMetadataTempFile.length(),
              new LocalTsFileInput(writer.chunkMetadataTempFile.toPath()));
      for (int i = 0, deviceCnt = 0; i < originChunkMetadataList.size(); ++i) {
        Pair<String, IChunkMetadata> chunkMetadataPair = window.getNextSeriesNameAndChunkMetadata();
        if (originChunkMetadataList.get(i) instanceof ChunkMetadata) {
          Assert.assertEquals(
              deviceDictInOrder.get(deviceCnt)
                  + "."
                  + originChunkMetadataList.get(i).getMeasurementUid(),
              chunkMetadataPair.left);
        } else {
          deviceCnt++;
          Assert.assertEquals(deviceDictInOrder.get(deviceCnt++), chunkMetadataPair.left);
        }
        Assert.assertEquals(
            originChunkMetadataList.get(i).getStartTime(), chunkMetadataPair.right.getStartTime());
        Assert.assertEquals(
            originChunkMetadataList.get(i).getEndTime(), chunkMetadataPair.right.getEndTime());
        Assert.assertEquals(
            originChunkMetadataList.get(i).getDataType(), chunkMetadataPair.right.getDataType());
        Assert.assertEquals(
            originChunkMetadataList.get(i).getStatistics(),
            chunkMetadataPair.right.getStatistics());
      }
    }
  }

  /**
   * Write a file with 10 devices and 5 series in each device. For each series, we write one chunk
   * for it. This test make sure that each chunk
   *
   * @throws IOException
   */
  @Test
  public void testWriteCompleteFileWithNormalChunk() throws IOException {
    Map<String, Map<String, List<List<Long>>>> originTimes = new HashMap<>();
    try (MemoryControlTsFileIOWriter writer =
        new MemoryControlTsFileIOWriter(testFile, 1024, true)) {
      List<IChunkMetadata> originChunkMetadataList = new ArrayList<>();
      for (int i = 0; i < 10; ++i) {
        String deviceId = deviceDictInOrder.get(i);
        writer.startChunkGroup(deviceId);
        for (int j = 0; j < 5; ++j) {
          ChunkWriterImpl chunkWriter;
          switch (j) {
            case 0:
              chunkWriter = generateIntData(j, 0L);
              break;
            case 1:
              chunkWriter = generateBooleanData(j, 0L);
              break;
            case 2:
              chunkWriter = generateFloatData(j, 0L);
              break;
            case 3:
              chunkWriter = generateDoubleData(j, 0L);
              break;
            case 4:
            default:
              chunkWriter = generateTextData(j, 0L);
              break;
          }
          chunkWriter.writeToFileWriter(writer);
          List<Long> times = new ArrayList<>();
          for (long t = 0; t < TEST_CHUNK_SIZE; ++t) {
            times.add(t);
          }
          originTimes
              .computeIfAbsent(deviceId, x -> new HashMap<>())
              .computeIfAbsent(measurementDictInOrder.get(j), x -> new ArrayList<>())
              .add(times);
        }
        originChunkMetadataList.addAll(writer.chunkMetadataList);
        writer.endChunkGroup();
      }
      Assert.assertTrue(writer.hasChunkMetadataInDisk);
      writer.endFile();
    }
    TsFileIntegrityCheckingTool.checkIntegrityBySequenceRead(testFile.getPath());
    TsFileIntegrityCheckingTool.checkIntegrityByQuery(testFile.getPath(), originTimes);
  }

  /**
   * Write a file with 10 devices and 5 series in each device. For each series, we write 100 chunks
   * for it. This test make sure that each chunk
   *
   * @throws IOException
   */
  @Test
  public void testWriteCompleteFileWithMultipleNormalChunk() throws IOException {
    Map<String, Map<String, List<List<Long>>>> originTimes = new HashMap<>();
    try (MemoryControlTsFileIOWriter writer =
        new MemoryControlTsFileIOWriter(testFile, 1024, true)) {
      for (int i = 0; i < 10; ++i) {
        String deviceId = deviceDictInOrder.get(i);
        writer.startChunkGroup(deviceId);
        for (int j = 0; j < 5; ++j) {
          ChunkWriterImpl chunkWriter;
          switch (j) {
            case 0:
              for (int k = 0; k < 10; ++k) {
                chunkWriter = generateIntData(j, (long) TEST_CHUNK_SIZE * k);
                chunkWriter.writeToFileWriter(writer);
                List<Long> times = new ArrayList<>();
                for (long t = (long) TEST_CHUNK_SIZE * k;
                    t < (long) TEST_CHUNK_SIZE * (k + 1);
                    ++t) {
                  times.add(t);
                }
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(measurementDictInOrder.get(j), x -> new ArrayList<>())
                    .add(times);
              }
              break;
            case 1:
              for (int k = 0; k < 10; ++k) {
                chunkWriter = generateBooleanData(j, (long) TEST_CHUNK_SIZE * k);
                chunkWriter.writeToFileWriter(writer);
                List<Long> times = new ArrayList<>();
                for (long t = (long) TEST_CHUNK_SIZE * k;
                    t < (long) TEST_CHUNK_SIZE * (k + 1);
                    ++t) {
                  times.add(t);
                }
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(measurementDictInOrder.get(j), x -> new ArrayList<>())
                    .add(times);
              }
              break;
            case 2:
              for (int k = 0; k < 10; ++k) {
                chunkWriter = generateFloatData(j, (long) TEST_CHUNK_SIZE * k);
                chunkWriter.writeToFileWriter(writer);
                List<Long> times = new ArrayList<>();
                for (long t = (long) TEST_CHUNK_SIZE * k;
                    t < (long) TEST_CHUNK_SIZE * (k + 1);
                    ++t) {
                  times.add(t);
                }
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(measurementDictInOrder.get(j), x -> new ArrayList<>())
                    .add(times);
              }
              break;
            case 3:
              for (int k = 0; k < 10; ++k) {
                chunkWriter = generateDoubleData(j, (long) TEST_CHUNK_SIZE * k);
                chunkWriter.writeToFileWriter(writer);
                List<Long> times = new ArrayList<>();
                for (long t = (long) TEST_CHUNK_SIZE * k;
                    t < (long) TEST_CHUNK_SIZE * (k + 1);
                    ++t) {
                  times.add(t);
                }
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(measurementDictInOrder.get(j), x -> new ArrayList<>())
                    .add(times);
              }
              break;
            case 4:
            default:
              for (int k = 0; k < 10; ++k) {
                chunkWriter = generateTextData(j, (long) TEST_CHUNK_SIZE * k);
                chunkWriter.writeToFileWriter(writer);
                List<Long> times = new ArrayList<>();
                for (long t = (long) TEST_CHUNK_SIZE * k;
                    t < (long) TEST_CHUNK_SIZE * (k + 1);
                    ++t) {
                  times.add(t);
                }
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(measurementDictInOrder.get(j), x -> new ArrayList<>())
                    .add(times);
              }
              break;
          }
        }
        writer.endChunkGroup();
      }
      Assert.assertTrue(writer.hasChunkMetadataInDisk);
      writer.endFile();
    }
    TsFileIntegrityCheckingTool.checkIntegrityBySequenceRead(testFile.getPath());
    TsFileIntegrityCheckingTool.checkIntegrityByQuery(testFile.getPath(), originTimes);
  }

  /**
   * Write a file with 10 devices and 5 series in each device. For each series, we write 1024 chunks
   * for it. This test make sure that each chunk
   *
   * @throws IOException
   */
  @Test
  public void testWriteCompleteFileWithEnormousNormalChunk() throws IOException {
    Map<String, Map<String, List<List<Long>>>> originTimes = new HashMap<>();
    long originTestChunkSize = TEST_CHUNK_SIZE;
    TEST_CHUNK_SIZE = 10;
    try (MemoryControlTsFileIOWriter writer =
        new MemoryControlTsFileIOWriter(testFile, 1024, true)) {
      for (int i = 0; i < 10; ++i) {
        String deviceId = deviceDictInOrder.get(i);
        writer.startChunkGroup(deviceId);
        for (int j = 0; j < 5; ++j) {
          ChunkWriterImpl chunkWriter;
          switch (j) {
            case 0:
              for (int k = 0; k < 1024; ++k) {
                chunkWriter = generateIntData(j, (long) TEST_CHUNK_SIZE * k);
                chunkWriter.writeToFileWriter(writer);
                List<Long> times = new ArrayList<>();
                for (long t = (long) TEST_CHUNK_SIZE * k;
                    t < (long) TEST_CHUNK_SIZE * (k + 1);
                    ++t) {
                  times.add(t);
                }
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(measurementDictInOrder.get(j), x -> new ArrayList<>())
                    .add(times);
              }
              break;
            case 1:
              for (int k = 0; k < 1024; ++k) {
                chunkWriter = generateBooleanData(j, (long) TEST_CHUNK_SIZE * k);
                chunkWriter.writeToFileWriter(writer);
                List<Long> times = new ArrayList<>();
                for (long t = (long) TEST_CHUNK_SIZE * k;
                    t < (long) TEST_CHUNK_SIZE * (k + 1);
                    ++t) {
                  times.add(t);
                }
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(measurementDictInOrder.get(j), x -> new ArrayList<>())
                    .add(times);
              }
              break;
            case 2:
              for (int k = 0; k < 1024; ++k) {
                chunkWriter = generateFloatData(j, (long) TEST_CHUNK_SIZE * k);
                chunkWriter.writeToFileWriter(writer);
                List<Long> times = new ArrayList<>();
                for (long t = (long) TEST_CHUNK_SIZE * k;
                    t < (long) TEST_CHUNK_SIZE * (k + 1);
                    ++t) {
                  times.add(t);
                }
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(measurementDictInOrder.get(j), x -> new ArrayList<>())
                    .add(times);
              }
              break;
            case 3:
              for (int k = 0; k < 1024; ++k) {
                chunkWriter = generateDoubleData(j, (long) TEST_CHUNK_SIZE * k);
                chunkWriter.writeToFileWriter(writer);
                List<Long> times = new ArrayList<>();
                for (long t = (long) TEST_CHUNK_SIZE * k;
                    t < (long) TEST_CHUNK_SIZE * (k + 1);
                    ++t) {
                  times.add(t);
                }
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(measurementDictInOrder.get(j), x -> new ArrayList<>())
                    .add(times);
              }
              break;
            case 4:
            default:
              for (int k = 0; k < 1024; ++k) {
                chunkWriter = generateTextData(j, (long) TEST_CHUNK_SIZE * k);
                chunkWriter.writeToFileWriter(writer);
                List<Long> times = new ArrayList<>();
                for (long t = (long) TEST_CHUNK_SIZE * k;
                    t < (long) TEST_CHUNK_SIZE * (k + 1);
                    ++t) {
                  times.add(t);
                }
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(measurementDictInOrder.get(j), x -> new ArrayList<>())
                    .add(times);
              }
              break;
          }
        }
        writer.endChunkGroup();
      }
      Assert.assertTrue(writer.hasChunkMetadataInDisk);
      writer.endFile();
    } finally {
      TEST_CHUNK_SIZE = originTestChunkSize;
    }
    TsFileIntegrityCheckingTool.checkIntegrityBySequenceRead(testFile.getPath());
    TsFileIntegrityCheckingTool.checkIntegrityByQuery(testFile.getPath(), originTimes);
  }

  /**
   * Write a file with 10 devices and 1024 series in each device. For each series, we write 100
   * chunks for it. This test make sure that each chunk
   *
   * @throws IOException
   */
  @Test
  public void testWriteCompleteFileWithEnormousSeriesNum() throws IOException {
    Map<String, Map<String, List<List<Long>>>> originTimes = new HashMap<>();
    long originTestChunkSize = TEST_CHUNK_SIZE;
    TEST_CHUNK_SIZE = 10;
    try (MemoryControlTsFileIOWriter writer =
        new MemoryControlTsFileIOWriter(testFile, 1024, true)) {
      for (int i = 0; i < 10; ++i) {
        String deviceId = deviceDictInOrder.get(i);
        writer.startChunkGroup(deviceId);
        for (int j = 0; j < 1024; ++j) {
          ChunkWriterImpl chunkWriter;
          switch (j % 5) {
            case 0:
              for (int k = 0; k < 100; ++k) {
                chunkWriter = generateIntData(j, (long) TEST_CHUNK_SIZE * k);
                chunkWriter.writeToFileWriter(writer);
                List<Long> times = new ArrayList<>();
                for (long t = (long) TEST_CHUNK_SIZE * k;
                    t < (long) TEST_CHUNK_SIZE * (k + 1);
                    ++t) {
                  times.add(t);
                }
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(measurementDictInOrder.get(j), x -> new ArrayList<>())
                    .add(times);
              }
              break;
            case 1:
              for (int k = 0; k < 100; ++k) {
                chunkWriter = generateBooleanData(j, (long) TEST_CHUNK_SIZE * k);
                chunkWriter.writeToFileWriter(writer);
                List<Long> times = new ArrayList<>();
                for (long t = (long) TEST_CHUNK_SIZE * k;
                    t < (long) TEST_CHUNK_SIZE * (k + 1);
                    ++t) {
                  times.add(t);
                }
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(measurementDictInOrder.get(j), x -> new ArrayList<>())
                    .add(times);
              }
              break;
            case 2:
              for (int k = 0; k < 100; ++k) {
                chunkWriter = generateFloatData(j, (long) TEST_CHUNK_SIZE * k);
                chunkWriter.writeToFileWriter(writer);
                List<Long> times = new ArrayList<>();
                for (long t = (long) TEST_CHUNK_SIZE * k;
                    t < (long) TEST_CHUNK_SIZE * (k + 1);
                    ++t) {
                  times.add(t);
                }
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(measurementDictInOrder.get(j), x -> new ArrayList<>())
                    .add(times);
              }
              break;
            case 3:
              for (int k = 0; k < 100; ++k) {
                chunkWriter = generateDoubleData(j, (long) TEST_CHUNK_SIZE * k);
                chunkWriter.writeToFileWriter(writer);
                List<Long> times = new ArrayList<>();
                for (long t = (long) TEST_CHUNK_SIZE * k;
                    t < (long) TEST_CHUNK_SIZE * (k + 1);
                    ++t) {
                  times.add(t);
                }
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(measurementDictInOrder.get(j), x -> new ArrayList<>())
                    .add(times);
              }
              break;
            case 4:
            default:
              for (int k = 0; k < 100; ++k) {
                chunkWriter = generateTextData(j, (long) TEST_CHUNK_SIZE * k);
                chunkWriter.writeToFileWriter(writer);
                List<Long> times = new ArrayList<>();
                for (long t = (long) TEST_CHUNK_SIZE * k;
                    t < (long) TEST_CHUNK_SIZE * (k + 1);
                    ++t) {
                  times.add(t);
                }
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(measurementDictInOrder.get(j), x -> new ArrayList<>())
                    .add(times);
              }
              break;
          }
        }
        writer.endChunkGroup();
      }
      Assert.assertTrue(writer.hasChunkMetadataInDisk);
      writer.endFile();
    } finally {
      TEST_CHUNK_SIZE = originTestChunkSize;
    }
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
    Map<String, Map<String, List<List<Long>>>> originTimes = new HashMap<>();
    long originTestChunkSize = TEST_CHUNK_SIZE;
    TEST_CHUNK_SIZE = 10;
    try (MemoryControlTsFileIOWriter writer =
        new MemoryControlTsFileIOWriter(testFile, 1024, true)) {
      for (int i = 0; i < 1024; ++i) {
        String deviceId = deviceDictInOrder.get(i);
        writer.startChunkGroup(deviceId);
        for (int j = 0; j < 5; ++j) {
          ChunkWriterImpl chunkWriter;
          switch (j % 5) {
            case 0:
              for (int k = 0; k < 10; ++k) {
                chunkWriter = generateIntData(j, (long) TEST_CHUNK_SIZE * k);
                chunkWriter.writeToFileWriter(writer);
                List<Long> times = new ArrayList<>();
                for (long t = (long) TEST_CHUNK_SIZE * k;
                    t < (long) TEST_CHUNK_SIZE * (k + 1);
                    ++t) {
                  times.add(t);
                }
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(measurementDictInOrder.get(j), x -> new ArrayList<>())
                    .add(times);
              }
              break;
            case 1:
              for (int k = 0; k < 10; ++k) {
                chunkWriter = generateBooleanData(j, (long) TEST_CHUNK_SIZE * k);
                chunkWriter.writeToFileWriter(writer);
                List<Long> times = new ArrayList<>();
                for (long t = (long) TEST_CHUNK_SIZE * k;
                    t < (long) TEST_CHUNK_SIZE * (k + 1);
                    ++t) {
                  times.add(t);
                }
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(measurementDictInOrder.get(j), x -> new ArrayList<>())
                    .add(times);
              }
              break;
            case 2:
              for (int k = 0; k < 10; ++k) {
                chunkWriter = generateFloatData(j, (long) TEST_CHUNK_SIZE * k);
                chunkWriter.writeToFileWriter(writer);
                List<Long> times = new ArrayList<>();
                for (long t = (long) TEST_CHUNK_SIZE * k;
                    t < (long) TEST_CHUNK_SIZE * (k + 1);
                    ++t) {
                  times.add(t);
                }
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(measurementDictInOrder.get(j), x -> new ArrayList<>())
                    .add(times);
              }
              break;
            case 3:
              for (int k = 0; k < 10; ++k) {
                chunkWriter = generateDoubleData(j, (long) TEST_CHUNK_SIZE * k);
                chunkWriter.writeToFileWriter(writer);
                List<Long> times = new ArrayList<>();
                for (long t = (long) TEST_CHUNK_SIZE * k;
                    t < (long) TEST_CHUNK_SIZE * (k + 1);
                    ++t) {
                  times.add(t);
                }
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(measurementDictInOrder.get(j), x -> new ArrayList<>())
                    .add(times);
              }
              break;
            case 4:
            default:
              for (int k = 0; k < 10; ++k) {
                chunkWriter = generateTextData(j, (long) TEST_CHUNK_SIZE * k);
                chunkWriter.writeToFileWriter(writer);
                List<Long> times = new ArrayList<>();
                for (long t = (long) TEST_CHUNK_SIZE * k;
                    t < (long) TEST_CHUNK_SIZE * (k + 1);
                    ++t) {
                  times.add(t);
                }
                originTimes
                    .computeIfAbsent(deviceId, x -> new HashMap<>())
                    .computeIfAbsent(measurementDictInOrder.get(j), x -> new ArrayList<>())
                    .add(times);
              }
              break;
          }
        }
        writer.endChunkGroup();
      }
      Assert.assertTrue(writer.hasChunkMetadataInDisk);
      writer.endFile();
    } finally {
      TEST_CHUNK_SIZE = originTestChunkSize;
    }
    TsFileIntegrityCheckingTool.checkIntegrityBySequenceRead(testFile.getPath());
    TsFileIntegrityCheckingTool.checkIntegrityByQuery(testFile.getPath(), originTimes);
  }

  private ChunkWriterImpl generateIntData(int idx, long startTime) {
    ChunkWriterImpl chunkWriter =
        new ChunkWriterImpl(
            new MeasurementSchema(measurementDictInOrder.get(idx), TSDataType.INT64));
    Random random = new Random();
    for (long i = startTime; i < startTime + TEST_CHUNK_SIZE; ++i) {
      chunkWriter.write(i, random.nextLong());
    }
    return chunkWriter;
  }

  private ChunkWriterImpl generateFloatData(int idx, long startTime) {
    ChunkWriterImpl chunkWriter =
        new ChunkWriterImpl(
            new MeasurementSchema(measurementDictInOrder.get(idx), TSDataType.FLOAT));
    Random random = new Random();
    for (long i = startTime; i < startTime + TEST_CHUNK_SIZE; ++i) {
      chunkWriter.write(i, random.nextFloat());
    }
    return chunkWriter;
  }

  private ChunkWriterImpl generateDoubleData(int idx, long startTime) {
    ChunkWriterImpl chunkWriter =
        new ChunkWriterImpl(
            new MeasurementSchema(measurementDictInOrder.get(idx), TSDataType.DOUBLE));
    Random random = new Random();
    for (long i = startTime; i < startTime + TEST_CHUNK_SIZE; ++i) {
      chunkWriter.write(i, random.nextDouble());
    }
    return chunkWriter;
  }

  private ChunkWriterImpl generateBooleanData(int idx, long startTime) {
    ChunkWriterImpl chunkWriter =
        new ChunkWriterImpl(
            new MeasurementSchema(measurementDictInOrder.get(idx), TSDataType.BOOLEAN));
    Random random = new Random();
    for (long i = startTime; i < startTime + TEST_CHUNK_SIZE; ++i) {
      chunkWriter.write(i, random.nextBoolean());
    }
    return chunkWriter;
  }

  private AlignedChunkWriterImpl generateVectorData(int idx, long startTime) {
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    measurementSchemas.add(new MeasurementSchema("", TSDataType.INT32));
    measurementSchemas.add(new MeasurementSchema("", TSDataType.INT64));
    measurementSchemas.add(new MeasurementSchema("", TSDataType.FLOAT));
    measurementSchemas.add(new MeasurementSchema("", TSDataType.DOUBLE));
    measurementSchemas.add(new MeasurementSchema("", TSDataType.BOOLEAN));
    measurementSchemas.add(new MeasurementSchema("", TSDataType.TEXT));
    AlignedChunkWriterImpl chunkWriter = new AlignedChunkWriterImpl(measurementSchemas);
    Random random = new Random();
    for (long i = startTime; i < startTime + TEST_CHUNK_SIZE; ++i) {
      TsPrimitiveType[] points = new TsPrimitiveType[6];
      points[0] = new TsPrimitiveType.TsInt(random.nextInt());
      points[1] = new TsPrimitiveType.TsLong(random.nextLong());
      points[2] = new TsPrimitiveType.TsFloat(random.nextFloat());
      points[3] = new TsPrimitiveType.TsDouble(random.nextDouble());
      points[4] = new TsPrimitiveType.TsBoolean(random.nextBoolean());
      points[5] = new TsPrimitiveType.TsBinary(new Binary(String.valueOf(random.nextDouble())));
      chunkWriter.write(i, points);
    }
    return chunkWriter;
  }

  private ChunkWriterImpl generateTextData(int idx, long startTime) {
    ChunkWriterImpl chunkWriter =
        new ChunkWriterImpl(
            new MeasurementSchema(measurementDictInOrder.get(idx), TSDataType.TEXT));
    Random random = new Random();
    for (long i = startTime; i < startTime + TEST_CHUNK_SIZE; ++i) {
      chunkWriter.write(i, new Binary(String.valueOf(random.nextDouble())));
    }
    return chunkWriter;
  }
}
