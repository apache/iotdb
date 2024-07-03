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
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.BatchedCompactionAlignedPagePointReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.CompactChunkPlan;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.FirstBatchCompactionAlignedChunkWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.IntegerStatistics;
import org.apache.tsfile.file.metadata.statistics.TimeStatistics;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.tsfile.read.reader.page.AlignedPageReader;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BatchCompactionUtilsTest extends AbstractCompactionTest {

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
  public void testBatchCompactionPointReader() throws IOException {
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(1000, 2000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, true, true),
            true);
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(seqResource1.getTsFile().getAbsolutePath())) {
      AlignedChunkMetadata alignedChunkMetadata =
          reader.getAlignedChunkMetadata(new PlainDeviceID("root.testsg.d0")).get(0);
      ChunkMetadata timeChunkMetadata = (ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata();
      List<IChunkMetadata> valueChunkMetadataList =
          alignedChunkMetadata.getValueChunkMetadataList();
      Chunk timeChunk = reader.readMemChunk(timeChunkMetadata);
      List<Chunk> valueChunks = new ArrayList<>();
      for (IChunkMetadata valueChunkMetadata : valueChunkMetadataList) {
        if (valueChunkMetadata == null) {
          valueChunks.add(null);
          continue;
        }
        valueChunks.add(reader.readMemChunk((ChunkMetadata) valueChunkMetadata));
      }

      AlignedChunkReader alignedChunkReader = new AlignedChunkReader(timeChunk, valueChunks);
      AlignedPageReader iPageReader =
          (AlignedPageReader) alignedChunkReader.loadPageReaderList().get(0);
      BatchedCompactionAlignedPagePointReader batchCompactionPointReader =
          new BatchedCompactionAlignedPagePointReader(
              iPageReader.getTimePageReader(), iPageReader.getValuePageReaderList().subList(1, 2));
      int readPointNum = 0;
      while (batchCompactionPointReader.hasNextTimeValuePair()) {
        TimeValuePair timeValuePair = batchCompactionPointReader.nextTimeValuePair();
        for (Object value : timeValuePair.getValues()) {
          Assert.assertNull(value);
        }
        readPointNum++;
      }

      Assert.assertEquals(readPointNum, 1001);
    }
  }

  @Test
  public void testFirstBatchChunkWriter() throws IOException, PageException {
    TsFileResource resource = createEmptyFileAndResource(true);
    CompactChunkPlan compactChunkPlan = null;
    try (TsFileIOWriter writer = new TsFileIOWriter(resource.getTsFile())) {
      IMeasurementSchema timeSchema = new MeasurementSchema("", TSDataType.TIMESTAMP);
      List<IMeasurementSchema> valueSchemas = new ArrayList<>();
      valueSchemas.add(new MeasurementSchema("s0", TSDataType.INT32));
      FirstBatchCompactionAlignedChunkWriter chunkWriter =
          new FirstBatchCompactionAlignedChunkWriter(timeSchema, valueSchemas);
      // write data point
      chunkWriter.write(1, new TsPrimitiveType[] {new TsPrimitiveType.TsInt(10)});

      // flush page
      TimeStatistics timeStatistics = new TimeStatistics();
      timeStatistics.update(10);
      timeStatistics.update(20);
      IntegerStatistics statistics = new IntegerStatistics();
      statistics.update(10, 10);
      statistics.update(20, 20);
      chunkWriter.sealCurrentPage();
      // repeat call seal current page
      chunkWriter.sealCurrentPage();
      chunkWriter.writePageHeaderAndDataIntoTimeBuff(
          ByteBuffer.allocate(1), new PageHeader(1, 1, timeStatistics));
      chunkWriter.writePageHeaderAndDataIntoValueBuff(
          ByteBuffer.allocate(1), new PageHeader(1, 1, statistics), 0);
      compactChunkPlan = chunkWriter.getCompactedChunkRecord();
      chunkWriter.writeToFileWriter(writer);
    }
    Assert.assertFalse(compactChunkPlan.isCompactedByDirectlyFlush());
    Assert.assertEquals(1, compactChunkPlan.getTimeRange().getMin());
    Assert.assertEquals(20, compactChunkPlan.getTimeRange().getMax());
    Assert.assertEquals(2, compactChunkPlan.getPageRecords().size());
  }
}
