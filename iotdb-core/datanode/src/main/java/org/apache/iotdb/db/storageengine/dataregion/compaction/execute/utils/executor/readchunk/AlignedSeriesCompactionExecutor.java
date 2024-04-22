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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionLastTimeCheckFailedException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileAlignedSeriesReaderIterator;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.IBatchDataIterator;
import org.apache.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class AlignedSeriesCompactionExecutor {
  private final IDeviceID device;
  private final LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>>
      readerAndChunkMetadataList;
  private final TsFileResource targetResource;
  private final CompactionTsFileWriter writer;

  private final AlignedChunkWriterImpl chunkWriter;
  private final List<IMeasurementSchema> schemaList;
  private long remainingPointInChunkWriter = 0L;
  private final CompactionTaskSummary summary;
  private long lastWriteTimestamp = Long.MIN_VALUE;

  private final long chunkSizeThreshold =
      IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();
  private final long chunkPointNumThreshold =
      IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum();

  @SuppressWarnings("squid:S1319")
  public AlignedSeriesCompactionExecutor(
      IDeviceID device,
      TsFileResource targetResource,
      LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>> readerAndChunkMetadataList,
      CompactionTsFileWriter writer,
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
   * collect the measurement metadata from list of alignedChunkMetadata list, and sort them in
   * dictionary order.
   *
   * @param readerAndChunkMetadataList list of reader and alignedChunkMetadata list
   * @return schema list sorted by dictionary order
   * @throws IOException if io errors occurred
   */
  private List<IMeasurementSchema> collectSchemaFromAlignedChunkMetadataList(
      LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>> readerAndChunkMetadataList)
      throws IOException {
    Set<MeasurementSchema> schemaSet = new HashSet<>();
    Set<String> measurementSet = new HashSet<>();
    for (Pair<TsFileSequenceReader, List<AlignedChunkMetadata>> readerListPair :
        readerAndChunkMetadataList) {
      TsFileSequenceReader reader = readerListPair.left;
      if (reader instanceof CompactionTsFileReader) {
        ((CompactionTsFileReader) reader).markStartOfAlignedSeries();
      }
      List<AlignedChunkMetadata> alignedChunkMetadataList = readerListPair.right;
      collectSchemaFromOneFile(alignedChunkMetadataList, reader, schemaSet, measurementSet);
      if (reader instanceof CompactionTsFileReader) {
        ((CompactionTsFileReader) reader).markEndOfAlignedSeries();
      }
    }
    List<IMeasurementSchema> collectedSchemaList = new ArrayList<>(schemaSet);
    collectedSchemaList.sort(Comparator.comparing(IMeasurementSchema::getMeasurementId));
    return collectedSchemaList;
  }

  private void collectSchemaFromOneFile(
      List<AlignedChunkMetadata> alignedChunkMetadataList,
      TsFileSequenceReader reader,
      Set<MeasurementSchema> schemaSet,
      Set<String> measurementSet)
      throws IOException {
    for (AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadataList) {
      List<IChunkMetadata> valueChunkMetadataList =
          alignedChunkMetadata.getValueChunkMetadataList();
      for (IChunkMetadata chunkMetadata : valueChunkMetadataList) {
        if (chunkMetadata == null || measurementSet.contains(chunkMetadata.getMeasurementUid())) {
          continue;
        }
        measurementSet.add(chunkMetadata.getMeasurementUid());
        Chunk chunk = reader.readMemChunk((ChunkMetadata) chunkMetadata);
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

  public void execute() throws IOException {
    while (!readerAndChunkMetadataList.isEmpty()) {
      Pair<TsFileSequenceReader, List<AlignedChunkMetadata>> readerListPair =
          readerAndChunkMetadataList.removeFirst();
      TsFileSequenceReader reader = readerListPair.left;
      List<AlignedChunkMetadata> alignedChunkMetadataList = readerListPair.right;

      if (reader instanceof CompactionTsFileReader) {
        ((CompactionTsFileReader) reader).markStartOfAlignedSeries();
      }

      TsFileAlignedSeriesReaderIterator readerIterator =
          new TsFileAlignedSeriesReaderIterator(reader, alignedChunkMetadataList, schemaList);
      while (readerIterator.hasNext()) {
        TsFileAlignedSeriesReaderIterator.NextAlignedChunkInfo nextAlignedChunkInfo =
            readerIterator.nextReader();
        summary.increaseProcessChunkNum(nextAlignedChunkInfo.getNotNullChunkNum());
        summary.increaseProcessPointNum(nextAlignedChunkInfo.getTotalPointNum());
        compactOneAlignedChunk(
            nextAlignedChunkInfo.getReader(), nextAlignedChunkInfo.getNotNullChunkNum());
      }
      if (reader instanceof CompactionTsFileReader) {
        ((CompactionTsFileReader) reader).markEndOfAlignedSeries();
      }
    }

    if (remainingPointInChunkWriter != 0L) {
      writer.writeChunk(chunkWriter);
    }
    writer.checkMetadataSizeAndMayFlush();
  }

  @SuppressWarnings("squid:S1172")
  private void compactOneAlignedChunk(AlignedChunkReader chunkReader, int notNullChunkNum)
      throws IOException {
    while (chunkReader.hasNextSatisfiedPage()) {
      // including value chunk and time chunk, thus we should plus one
      IBatchDataIterator batchDataIterator = chunkReader.nextPageData().getBatchDataIterator();
      while (batchDataIterator.hasNext()) {
        TsPrimitiveType[] pointsData = (TsPrimitiveType[]) batchDataIterator.currentValue();
        long time = batchDataIterator.currentTime();
        checkAndUpdatePreviousTimestamp(time);
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
   * than the threshold, flush it.
   *
   * @throws IOException if io errors occurred
   */
  private void flushChunkWriterIfLargeEnough() throws IOException {
    if (remainingPointInChunkWriter >= chunkPointNumThreshold
        || chunkWriter.estimateMaxSeriesMemSize() >= chunkSizeThreshold * schemaList.size()) {
      writer.writeChunk(chunkWriter);
      remainingPointInChunkWriter = 0L;
    }
  }

  private void checkAndUpdatePreviousTimestamp(long currentWritingTimestamp) {
    if (currentWritingTimestamp <= lastWriteTimestamp) {
      throw new CompactionLastTimeCheckFailedException(
          device.toString(), currentWritingTimestamp, lastWriteTimestamp);
    } else {
      lastWriteTimestamp = currentWritingTimestamp;
    }
  }
}
