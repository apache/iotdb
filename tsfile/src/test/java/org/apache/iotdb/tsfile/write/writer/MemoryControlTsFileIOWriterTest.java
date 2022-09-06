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
import java.util.List;
import java.util.Random;

public class MemoryControlTsFileIOWriterTest extends MemoryControlTsFileIOWriter {
  private static File testFile = new File("target", "1-1-0-0.tsfile");
  private static File emptyFile = new File("target", "temp");
  private static final int TEST_CHUNK_SIZE = 1000;

  @Before
  public void setUp() throws IOException {}

  @After
  public void tearDown() throws IOException {
    this.close();
    FileUtils.delete(testFile);
    FileUtils.delete(
        new File(testFile.getPath() + MemoryControlTsFileIOWriter.CHUNK_METADATA_TEMP_FILE_PREFIX));
    FileUtils.delete(emptyFile);
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
        String deviceId = "root.sg.d" + i;
        writer.startChunkGroup(deviceId);
        for (int j = 0; j < 5; ++j) {
          ChunkWriterImpl chunkWriter;
          switch (j) {
            case 0:
              chunkWriter = generateIntData(j);
              break;
            case 1:
              chunkWriter = generateBooleanData(j);
              break;
            case 2:
              chunkWriter = generateFloatData(j);
              break;
            case 3:
              chunkWriter = generateDoubleData(j);
              break;
            case 4:
            default:
              chunkWriter = generateTextData(j);
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
        Assert.assertEquals("root.sg.d" + i / 5 + ".s" + i % 5, chunkMetadataPair.left);
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
        String deviceId = "root.sg.d" + i;
        writer.startChunkGroup(deviceId);
        AlignedChunkWriterImpl chunkWriter = generateVectorData(i);
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
        Assert.assertEquals("root.sg.d" + i, chunkMetadataPair.left);
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
        String deviceId = "root.sg.d" + i;
        writer.startChunkGroup(deviceId);
        if (i % 2 == 0) {
          // write normal series
          for (int j = 0; j < 5; ++j) {
            ChunkWriterImpl chunkWriter;
            switch (j) {
              case 0:
                chunkWriter = generateIntData(j);
                break;
              case 1:
                chunkWriter = generateBooleanData(j);
                break;
              case 2:
                chunkWriter = generateFloatData(j);
                break;
              case 3:
                chunkWriter = generateDoubleData(j);
                break;
              case 4:
              default:
                chunkWriter = generateTextData(j);
                break;
            }
            chunkWriter.writeToFileWriter(writer);
          }
          originChunkMetadataList.addAll(writer.chunkMetadataList);
        } else {
          // write vector
          AlignedChunkWriterImpl chunkWriter = generateVectorData(i);
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
              "root.sg.d" + deviceCnt + "." + originChunkMetadataList.get(i).getMeasurementUid(),
              chunkMetadataPair.left);
        } else {
          deviceCnt++;
          Assert.assertEquals("root.sg.d" + deviceCnt++, chunkMetadataPair.left);
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

  private ChunkWriterImpl generateIntData(int idx) {
    ChunkWriterImpl chunkWriter =
        new ChunkWriterImpl(new MeasurementSchema("s" + idx, TSDataType.INT64));
    Random random = new Random();
    for (int i = 0; i < TEST_CHUNK_SIZE; ++i) {
      chunkWriter.write(i, random.nextLong());
    }
    return chunkWriter;
  }

  private ChunkWriterImpl generateFloatData(int idx) {
    ChunkWriterImpl chunkWriter =
        new ChunkWriterImpl(new MeasurementSchema("s" + idx, TSDataType.FLOAT));
    Random random = new Random();
    for (int i = 0; i < TEST_CHUNK_SIZE; ++i) {
      chunkWriter.write(i, random.nextFloat());
    }
    return chunkWriter;
  }

  private ChunkWriterImpl generateDoubleData(int idx) {
    ChunkWriterImpl chunkWriter =
        new ChunkWriterImpl(new MeasurementSchema("s" + idx, TSDataType.DOUBLE));
    Random random = new Random();
    for (int i = 0; i < TEST_CHUNK_SIZE; ++i) {
      chunkWriter.write(i, random.nextDouble());
    }
    return chunkWriter;
  }

  private ChunkWriterImpl generateBooleanData(int idx) {
    ChunkWriterImpl chunkWriter =
        new ChunkWriterImpl(new MeasurementSchema("s" + idx, TSDataType.BOOLEAN));
    Random random = new Random();
    for (int i = 0; i < TEST_CHUNK_SIZE; ++i) {
      chunkWriter.write(i, random.nextBoolean());
    }
    return chunkWriter;
  }

  private AlignedChunkWriterImpl generateVectorData(int idx) {
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    measurementSchemas.add(new MeasurementSchema("", TSDataType.INT32));
    measurementSchemas.add(new MeasurementSchema("", TSDataType.INT64));
    measurementSchemas.add(new MeasurementSchema("", TSDataType.FLOAT));
    measurementSchemas.add(new MeasurementSchema("", TSDataType.DOUBLE));
    measurementSchemas.add(new MeasurementSchema("", TSDataType.BOOLEAN));
    measurementSchemas.add(new MeasurementSchema("", TSDataType.TEXT));
    AlignedChunkWriterImpl chunkWriter = new AlignedChunkWriterImpl(measurementSchemas);
    Random random = new Random();
    for (int i = 0; i < TEST_CHUNK_SIZE; ++i) {
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

  private ChunkWriterImpl generateTextData(int idx) {
    ChunkWriterImpl chunkWriter =
        new ChunkWriterImpl(new MeasurementSchema("s" + idx, TSDataType.TEXT));
    Random random = new Random();
    for (int i = 0; i < TEST_CHUNK_SIZE; ++i) {
      chunkWriter.write(i, new Binary(String.valueOf(random.nextDouble())));
    }
    return chunkWriter;
  }
}
