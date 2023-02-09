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
package org.apache.iotdb.db.engine.compaction.execute.utils.writer;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.schedule.constant.CompactionType;
import org.apache.iotdb.db.engine.compaction.schedule.constant.ProcessChunkType;
import org.apache.iotdb.db.service.metrics.recorder.CompactionMetricsManager;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.chunk.ValueChunkWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import com.google.common.util.concurrent.RateLimiter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public abstract class AbstractCompactionWriter implements AutoCloseable {
  protected int subTaskNum = IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum();

  private RateLimiter compactionRateLimiter =
      CompactionTaskManager.getInstance().getMergeWriteRateLimiter();

  // check if there is unseq error point during writing
  protected long[] lastTime = new long[subTaskNum];

  // Each sub task has its own chunk writer.
  // The index of the array corresponds to subTaskId.
  protected IChunkWriter[] chunkWriters = new IChunkWriter[subTaskNum];

  // Each sub task has point count in current measurment, which is used to check size.
  // The index of the array corresponds to subTaskId.
  protected int[] chunkPointNumArray = new int[subTaskNum];

  // used to control the target chunk size
  protected long targetChunkSize = IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();

  // used to control the point num of target chunk
  protected long targetChunkPointNum =
      IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum();

  // When num of points writing into target files reaches check point, then check chunk size
  private final long checkPoint = (targetChunkPointNum >= 10 ? targetChunkPointNum : 10) / 10;

  private long lastCheckIndex = 0;

  // if unsealed chunk size is lower then this, then deserialize next chunk no matter it is
  // overlapped or not
  protected long chunkSizeLowerBoundInCompaction =
      IoTDBDescriptor.getInstance().getConfig().getChunkSizeLowerBoundInCompaction();

  // if point num of unsealed chunk is lower then this, then deserialize next chunk no matter it is
  // overlapped or not
  protected long chunkPointNumLowerBoundInCompaction =
      IoTDBDescriptor.getInstance().getConfig().getChunkPointNumLowerBoundInCompaction();

  // if unsealed page size is lower then this, then deserialize next page no matter it is
  // overlapped or not
  protected long pageSizeLowerBoundInCompaction = chunkSizeLowerBoundInCompaction / 10;

  // if point num of unsealed page is lower then this, then deserialize next page no matter it is
  // overlapped or not
  protected long pagePointNumLowerBoundInCompaction = chunkPointNumLowerBoundInCompaction / 10;

  protected boolean isAlign;

  protected String deviceId;

  protected String[] measurementId = new String[subTaskNum];

  public abstract void startChunkGroup(String deviceId, boolean isAlign) throws IOException;

  public abstract void endChunkGroup() throws IOException;

  public void startMeasurement(List<IMeasurementSchema> measurementSchemaList, int subTaskId) {
    lastCheckIndex = 0;
    lastTime[subTaskId] = Long.MIN_VALUE;
    if (isAlign) {
      chunkWriters[subTaskId] = new AlignedChunkWriterImpl(measurementSchemaList);
      measurementId[subTaskId] = "";
    } else {
      chunkWriters[subTaskId] = new ChunkWriterImpl(measurementSchemaList.get(0), true);
      measurementId[subTaskId] = measurementSchemaList.get(0).getMeasurementId();
    }
  }

  public abstract void endMeasurement(int subTaskId) throws IOException;

  public abstract void write(TimeValuePair timeValuePair, int subTaskId) throws IOException;

  public abstract void write(TimeColumn timestamps, Column[] columns, int subTaskId, int batchSize)
      throws IOException;

  public abstract void endFile() throws IOException;

  public abstract long getWriterSize() throws IOException;

  /**
   * Update startTime and endTime of the current device in each target resources, and check whether
   * to flush chunk metadatas or not.
   */
  public abstract void checkAndMayFlushChunkMetadata() throws IOException;

  protected void writeDataPoint(long timestamp, TsPrimitiveType value, IChunkWriter iChunkWriter) {
    if (iChunkWriter instanceof ChunkWriterImpl) {
      ChunkWriterImpl chunkWriter = (ChunkWriterImpl) iChunkWriter;
      switch (chunkWriter.getDataType()) {
        case TEXT:
          chunkWriter.write(timestamp, value.getBinary());
          break;
        case DOUBLE:
          chunkWriter.write(timestamp, value.getDouble());
          break;
        case BOOLEAN:
          chunkWriter.write(timestamp, value.getBoolean());
          break;
        case INT64:
          chunkWriter.write(timestamp, value.getLong());
          break;
        case INT32:
          chunkWriter.write(timestamp, value.getInt());
          break;
        case FLOAT:
          chunkWriter.write(timestamp, value.getFloat());
          break;
        default:
          throw new UnsupportedOperationException("Unknown data type " + chunkWriter.getDataType());
      }
    } else {
      AlignedChunkWriterImpl alignedChunkWriter = (AlignedChunkWriterImpl) iChunkWriter;
      alignedChunkWriter.write(timestamp, value.getVector());
    }
  }

  protected void sealChunk(TsFileIOWriter targetWriter, IChunkWriter iChunkWriter, int subTaskId)
      throws IOException {
    CompactionTaskManager.mergeRateLimiterAcquire(
        compactionRateLimiter, iChunkWriter.estimateMaxSeriesMemSize());
    synchronized (targetWriter) {
      iChunkWriter.writeToFileWriter(targetWriter);
    }
    chunkPointNumArray[subTaskId] = 0;
  }

  public abstract boolean flushNonAlignedChunk(
      Chunk chunk, ChunkMetadata chunkMetadata, int subTaskId) throws IOException;

  public abstract boolean flushAlignedChunk(
      Chunk timeChunk,
      IChunkMetadata timeChunkMetadata,
      List<Chunk> valueChunks,
      List<IChunkMetadata> valueChunkMetadatas,
      int subTaskId)
      throws IOException;

  protected void flushNonAlignedChunkToFileWriter(
      TsFileIOWriter targetWriter, Chunk chunk, ChunkMetadata chunkMetadata, int subTaskId)
      throws IOException {
    CompactionTaskManager.mergeRateLimiterAcquire(compactionRateLimiter, getChunkSize(chunk));
    synchronized (targetWriter) {
      // seal last chunk to file writer
      chunkWriters[subTaskId].writeToFileWriter(targetWriter);
      chunkPointNumArray[subTaskId] = 0;
      targetWriter.writeChunk(chunk, chunkMetadata);
    }
  }

  protected void flushAlignedChunkToFileWriter(
      TsFileIOWriter targetWriter,
      Chunk timeChunk,
      IChunkMetadata timeChunkMetadata,
      List<Chunk> valueChunks,
      List<IChunkMetadata> valueChunkMetadatas,
      int subTaskId)
      throws IOException {
    synchronized (targetWriter) {
      AlignedChunkWriterImpl alignedChunkWriter = (AlignedChunkWriterImpl) chunkWriters[subTaskId];
      // seal last chunk to file writer
      alignedChunkWriter.writeToFileWriter(targetWriter);
      chunkPointNumArray[subTaskId] = 0;

      // flush time chunk
      CompactionTaskManager.mergeRateLimiterAcquire(compactionRateLimiter, getChunkSize(timeChunk));
      targetWriter.writeChunk(timeChunk, (ChunkMetadata) timeChunkMetadata);

      // flush value chunks
      for (int i = 0; i < valueChunks.size(); i++) {
        Chunk valueChunk = valueChunks.get(i);
        if (valueChunk == null) {
          // sub sensor does not exist in current file or value chunk has been deleted completely
          ValueChunkWriter valueChunkWriter = alignedChunkWriter.getValueChunkWriterByIndex(i);
          targetWriter.writeEmptyValueChunk(
              valueChunkWriter.getMeasurementId(),
              valueChunkWriter.getCompressionType(),
              valueChunkWriter.getDataType(),
              valueChunkWriter.getEncodingType(),
              valueChunkWriter.getStatistics());
          continue;
        }
        CompactionTaskManager.mergeRateLimiterAcquire(
            compactionRateLimiter, getChunkSize(valueChunk));
        targetWriter.writeChunk(valueChunk, (ChunkMetadata) valueChunkMetadatas.get(i));
      }
    }
  }

  public abstract boolean flushNonAlignedPage(
      ByteBuffer compressedPageData, PageHeader pageHeader, int subTaskId)
      throws IOException, PageException;

  protected void flushNonAlignedPageToChunkWriter(
      ChunkWriterImpl chunkWriter,
      ByteBuffer compressedPageData,
      PageHeader pageHeader,
      int subTaskId)
      throws PageException {
    // seal current page
    chunkWriter.sealCurrentPage();
    // flush new page to chunk writer directly
    chunkWriter.writePageHeaderAndDataIntoBuff(compressedPageData, pageHeader);

    chunkPointNumArray[subTaskId] += pageHeader.getStatistics().getCount();
  }

  public abstract boolean flushAlignedPage(
      ByteBuffer compressedTimePageData,
      PageHeader timePageHeader,
      List<ByteBuffer> compressedValuePageDatas,
      List<PageHeader> valuePageHeaders,
      int subTaskId)
      throws IOException, PageException;

  protected void flushAlignedPageToChunkWriter(
      AlignedChunkWriterImpl alignedChunkWriter,
      ByteBuffer compressedTimePageData,
      PageHeader timePageHeader,
      List<ByteBuffer> compressedValuePageDatas,
      List<PageHeader> valuePageHeaders,
      int subTaskId)
      throws IOException, PageException {
    // seal current page
    alignedChunkWriter.sealCurrentPage();
    // flush new time page to chunk writer directly
    alignedChunkWriter.writePageHeaderAndDataIntoTimeBuff(compressedTimePageData, timePageHeader);

    // flush new value pages to chunk writer directly
    for (int i = 0; i < valuePageHeaders.size(); i++) {
      if (valuePageHeaders.get(i) == null) {
        // sub sensor does not exist in current file or value page has been deleted completely
        alignedChunkWriter.getValueChunkWriterByIndex(i).writeEmptyPageToPageBuffer();
        continue;
      }
      alignedChunkWriter.writePageHeaderAndDataIntoValueBuff(
          compressedValuePageDatas.get(i), valuePageHeaders.get(i), i);
    }

    chunkPointNumArray[subTaskId] += timePageHeader.getStatistics().getCount();
  }

  protected void checkChunkSizeAndMayOpenANewChunk(
      TsFileIOWriter fileWriter, IChunkWriter iChunkWriter, int subTaskId, boolean isCrossSpace)
      throws IOException {
    if (chunkPointNumArray[subTaskId] >= (lastCheckIndex + 1) * checkPoint) {
      // if chunk point num reaches the check point, then check if the chunk size over threshold
      lastCheckIndex = chunkPointNumArray[subTaskId] / checkPoint;
      if (iChunkWriter.checkIsChunkSizeOverThreshold(targetChunkSize, targetChunkPointNum, false)) {
        sealChunk(fileWriter, iChunkWriter, subTaskId);
        lastCheckIndex = 0;
        CompactionMetricsManager.getInstance()
            .recordWriteInfo(
                isCrossSpace
                    ? CompactionType.CROSS_COMPACTION
                    : CompactionType.INNER_UNSEQ_COMPACTION,
                ProcessChunkType.DESERIALIZE_CHUNK,
                isAlign,
                iChunkWriter.estimateMaxSeriesMemSize());
      }
    }
  }

  protected long getChunkSize(Chunk chunk) {
    return chunk.getHeader().getSerializedSize() + chunk.getHeader().getDataSize();
  }
}
