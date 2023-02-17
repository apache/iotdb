/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.compaction.execute.utils.executor.readchunk;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.TsFileMetricManager;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.schedule.constant.CompactionType;
import org.apache.iotdb.db.engine.compaction.schedule.constant.ProcessChunkType;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.service.metrics.recorder.CompactionMetricsManager;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileAlignedSeriesReaderIterator;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import com.google.common.util.concurrent.RateLimiter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class AlignedSeriesCompactionExecutor {
  private final String device;
  private final LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>>
      readerAndChunkMetadataList;
  private final TsFileResource targetResource;
  private final TsFileIOWriter writer;

  private final AlignedChunkWriterImpl chunkWriter;
  private final List<IMeasurementSchema> schemaList;
  private long remainingPointInChunkWriter = 0L;
  private final CompactionTaskSummary summary;
  private final RateLimiter rateLimiter =
      CompactionTaskManager.getInstance().getMergeWriteRateLimiter();

  private final long chunkSizeThreshold =
      IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();
  private final long chunkPointNumThreshold =
      IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum();

  public AlignedSeriesCompactionExecutor(
      String device,
      TsFileResource targetResource,
      LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>> readerAndChunkMetadataList,
      TsFileIOWriter writer,
      CompactionTaskSummary summary)
      throws IOException {
    this.device = device;
    this.readerAndChunkMetadataList = readerAndChunkMetadataList;
    this.writer = writer;
    this.targetResource = targetResource;
    schemaList = collectSchemaFromAlignedChunkMetadataList(readerAndChunkMetadataList);
    chunkWriter = new AlignedChunkWriterImpl(schemaList);
    this.summary = summary;
  }

  /**
   * collect the measurement schema from list of alignedChunkMetadata list, and sort them in
   * dictionary order.
   *
   * @param readerAndChunkMetadataList
   * @return
   */
  private List<IMeasurementSchema> collectSchemaFromAlignedChunkMetadataList(
      LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>> readerAndChunkMetadataList)
      throws IOException {
    Set<MeasurementSchema> schemaSet = new HashSet<>();
    Set<String> measurementSet = new HashSet<>();
    for (Pair<TsFileSequenceReader, List<AlignedChunkMetadata>> readerListPair :
        readerAndChunkMetadataList) {
      TsFileSequenceReader reader = readerListPair.left;
      List<AlignedChunkMetadata> alignedChunkMetadataList = readerListPair.right;
      for (AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadataList) {
        List<IChunkMetadata> valueChunkMetadataList =
            alignedChunkMetadata.getValueChunkMetadataList();
        for (IChunkMetadata chunkMetadata : valueChunkMetadataList) {
          if (chunkMetadata == null) {
            continue;
          }
          if (measurementSet.contains(chunkMetadata.getMeasurementUid())) {
            continue;
          }
          measurementSet.add(chunkMetadata.getMeasurementUid());
          Chunk chunk = ChunkCache.getInstance().get((ChunkMetadata) chunkMetadata);
          ChunkHeader header = chunk.getHeader();
          schemaSet.add(
              new MeasurementSchema(
                  header.getMeasurementID(),
                  header.getDataType(),
                  header.getEncodingType(),
                  header.getCompressionType()));
        }
      }
    }
    List<IMeasurementSchema> schemaList = new ArrayList<>(schemaSet);
    schemaList.sort(Comparator.comparing(IMeasurementSchema::getMeasurementId));
    return schemaList;
  }

  public void execute() throws IOException {
    long originTempFileSize = writer.getPos();
    while (readerAndChunkMetadataList.size() > 0) {
      Pair<TsFileSequenceReader, List<AlignedChunkMetadata>> readerListPair =
          readerAndChunkMetadataList.removeFirst();
      TsFileSequenceReader reader = readerListPair.left;
      List<AlignedChunkMetadata> alignedChunkMetadataList = readerListPair.right;
      TsFileAlignedSeriesReaderIterator readerIterator =
          new TsFileAlignedSeriesReaderIterator(reader, alignedChunkMetadataList, schemaList);
      while (readerIterator.hasNext()) {
        TsFileAlignedSeriesReaderIterator.NextAlignedChunkInfo nextAlignedChunkInfo =
            readerIterator.nextReader();
        summary.increaseProcessChunkNum(nextAlignedChunkInfo.getNotNullChunkNum());
        summary.increaseProcessPointNum(nextAlignedChunkInfo.getTotalPointNum());
        CompactionMetricsManager.getInstance().recordReadInfo(nextAlignedChunkInfo.getTotalSize());
        compactOneAlignedChunk(
            nextAlignedChunkInfo.getReader(), nextAlignedChunkInfo.getNotNullChunkNum());
      }
    }

    if (remainingPointInChunkWriter != 0L) {
      CompactionTaskManager.mergeRateLimiterAcquire(
          rateLimiter, chunkWriter.estimateMaxSeriesMemSize());
      CompactionMetricsManager.getInstance()
          .recordWriteInfo(
              CompactionType.INNER_SEQ_COMPACTION,
              ProcessChunkType.DESERIALIZE_CHUNK,
              true,
              chunkWriter.estimateMaxSeriesMemSize());
      chunkWriter.writeToFileWriter(writer);
    }
    writer.checkMetadataSizeAndMayFlush();

    // update temporal file metrics
    TsFileMetricManager.getInstance()
        .addCompactionTempFileSize(true, true, writer.getPos() - originTempFileSize);
  }

  private void compactOneAlignedChunk(AlignedChunkReader chunkReader, int notNullChunkNum)
      throws IOException {
    while (chunkReader.hasNextSatisfiedPage()) {
      // including value chunk and time chunk, thus we should plus one
      IBatchDataIterator batchDataIterator = chunkReader.nextPageData().getBatchDataIterator();
      while (batchDataIterator.hasNext()) {
        TsPrimitiveType[] pointsData = (TsPrimitiveType[]) batchDataIterator.currentValue();
        long time = batchDataIterator.currentTime();
        chunkWriter.write(time, pointsData);
        ++remainingPointInChunkWriter;

        targetResource.updateStartTime(device, time);
        targetResource.updateEndTime(device, time);

        batchDataIterator.next();
      }
    }
    flushChunkWriterIfLargeEnough();
  }

  /**
   * if the avg size of each chunk is larger than the threshold, or the chunk point num is larger
   * than the threshold, flush it
   *
   * @throws IOException
   */
  private void flushChunkWriterIfLargeEnough() throws IOException {
    if (remainingPointInChunkWriter >= chunkPointNumThreshold
        || chunkWriter.estimateMaxSeriesMemSize() >= chunkSizeThreshold * schemaList.size()) {
      CompactionTaskManager.mergeRateLimiterAcquire(
          rateLimiter, chunkWriter.estimateMaxSeriesMemSize());
      CompactionMetricsManager.getInstance()
          .recordWriteInfo(
              CompactionType.INNER_SEQ_COMPACTION,
              ProcessChunkType.DESERIALIZE_CHUNK,
              true,
              chunkWriter.estimateMaxSeriesMemSize());
      chunkWriter.writeToFileWriter(writer);
      remainingPointInChunkWriter = 0L;
    }
  }
}
