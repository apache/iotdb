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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionLastTimeCheckFailedException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.element.AlignedPageElement;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.element.ChunkMetadataElement;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.flushcontroller.AbstractCompactionFlushController;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileWriter;

import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.IChunkWriter;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.schema.Schema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public abstract class AbstractCompactionWriter implements AutoCloseable {
  protected int subTaskNum = IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum();

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
  @SuppressWarnings("squid:S1170")
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

  protected IDeviceID deviceId;

  protected String[] measurementId = new String[subTaskNum];

  public abstract void startChunkGroup(IDeviceID deviceId, boolean isAlign) throws IOException;

  public abstract void endChunkGroup() throws IOException;

  public void startMeasurement(String measurement, IChunkWriter chunkWriter, int subTaskId) {
    lastCheckIndex = 0;
    lastTime[subTaskId] = Long.MIN_VALUE;
    chunkWriters[subTaskId] = chunkWriter;
    measurementId[subTaskId] = measurement;
  }

  public abstract void endMeasurement(int subTaskId) throws IOException;

  public abstract void write(TimeValuePair timeValuePair, int subTaskId) throws IOException;

  public abstract void write(TsBlock tsBlock, int subTaskId) throws IOException;

  public abstract void endFile() throws IOException;

  public abstract long getWriterSize() throws IOException;

  /**
   * Update startTime and endTime of the current device in each target resources, and check whether
   * to flush chunk metadatas or not.
   *
   * @throws IOException if io errors occurred
   */
  public abstract void checkAndMayFlushChunkMetadata() throws IOException;

  protected void writeDataPoint(long timestamp, TsPrimitiveType value, IChunkWriter chunkWriter) {
    if (chunkWriter instanceof ChunkWriterImpl) {
      ChunkWriterImpl chunkWriterImpl = (ChunkWriterImpl) chunkWriter;
      switch (chunkWriterImpl.getDataType()) {
        case TEXT:
        case STRING:
        case BLOB:
          chunkWriterImpl.write(timestamp, value.getBinary());
          break;
        case DOUBLE:
          chunkWriterImpl.write(timestamp, value.getDouble());
          break;
        case BOOLEAN:
          chunkWriterImpl.write(timestamp, value.getBoolean());
          break;
        case INT64:
        case TIMESTAMP:
          chunkWriterImpl.write(timestamp, value.getLong());
          break;
        case INT32:
        case DATE:
          chunkWriterImpl.write(timestamp, value.getInt());
          break;
        case FLOAT:
          chunkWriterImpl.write(timestamp, value.getFloat());
          break;
        default:
          throw new UnsupportedOperationException(
              "Unknown data type " + chunkWriterImpl.getDataType());
      }
    } else {
      AlignedChunkWriterImpl alignedChunkWriter = (AlignedChunkWriterImpl) chunkWriter;
      alignedChunkWriter.write(timestamp, value.getVector());
    }
  }

  @SuppressWarnings("squid:S2445")
  protected void sealChunk(
      CompactionTsFileWriter targetWriter, IChunkWriter chunkWriter, int subTaskId)
      throws IOException {
    synchronized (targetWriter) {
      targetWriter.writeChunk(chunkWriter);
    }
    chunkPointNumArray[subTaskId] = 0;
  }

  public abstract boolean flushNonAlignedChunk(
      Chunk chunk, ChunkMetadata chunkMetadata, int subTaskId) throws IOException;

  public abstract boolean flushAlignedChunk(
      ChunkMetadataElement chunkMetadataElement, int subTaskId) throws IOException;

  public abstract boolean flushBatchedValueChunk(
      ChunkMetadataElement chunkMetadataElement,
      int subTaskId,
      AbstractCompactionFlushController flushController)
      throws IOException;

  @SuppressWarnings("squid:S2445")
  protected void flushNonAlignedChunkToFileWriter(
      CompactionTsFileWriter targetWriter, Chunk chunk, ChunkMetadata chunkMetadata, int subTaskId)
      throws IOException {
    synchronized (targetWriter) {
      // seal last chunk to file writer
      targetWriter.writeChunk(chunkWriters[subTaskId]);
      chunkPointNumArray[subTaskId] = 0;
      targetWriter.writeChunk(chunk, chunkMetadata);
    }
  }

  @SuppressWarnings("squid:S2445")
  protected void flushAlignedChunkToFileWriter(
      CompactionTsFileWriter targetWriter,
      Chunk timeChunk,
      IChunkMetadata timeChunkMetadata,
      List<Chunk> valueChunks,
      List<IChunkMetadata> valueChunkMetadatas,
      int subTaskId)
      throws IOException {
    synchronized (targetWriter) {
      AlignedChunkWriterImpl alignedChunkWriter = (AlignedChunkWriterImpl) chunkWriters[subTaskId];
      // seal last chunk to file writer
      targetWriter.writeChunk(alignedChunkWriter);
      chunkPointNumArray[subTaskId] = 0;

      targetWriter.markStartingWritingAligned();

      // flush time chunk
      if (timeChunk != null) {
        // time chunk may be null when compact following series batch
        targetWriter.writeChunk(timeChunk, (ChunkMetadata) timeChunkMetadata);
      }

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
              Statistics.getStatsByType(valueChunkWriter.getDataType()));
          continue;
        }
        targetWriter.writeChunk(valueChunk, (ChunkMetadata) valueChunkMetadatas.get(i));
      }

      targetWriter.markEndingWritingAligned();
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

  public abstract boolean flushAlignedPage(AlignedPageElement alignedPageElement, int subTaskId)
      throws IOException, PageException;

  public abstract boolean flushBatchedValuePage(
      AlignedPageElement alignedPageElement,
      int subTaskId,
      AbstractCompactionFlushController flushController)
      throws PageException, IOException;

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
      CompactionTsFileWriter fileWriter, IChunkWriter chunkWriter, int subTaskId)
      throws IOException {
    if (chunkPointNumArray[subTaskId] >= (lastCheckIndex + 1) * checkPoint) {
      // if chunk point num reaches the check point, then check if the chunk size over threshold
      lastCheckIndex = chunkPointNumArray[subTaskId] / checkPoint;
      if (chunkWriter.checkIsChunkSizeOverThreshold(targetChunkSize, targetChunkPointNum, false)) {
        sealChunk(fileWriter, chunkWriter, subTaskId);
        lastCheckIndex = 0;
      }
    }
  }

  protected long getChunkSize(Chunk chunk) {
    return (long) chunk.getHeader().getSerializedSize() + chunk.getHeader().getDataSize();
  }

  protected void checkPreviousTimestamp(long currentWritingTimestamp, int subTaskId) {
    if (currentWritingTimestamp <= lastTime[subTaskId]) {
      throw new CompactionLastTimeCheckFailedException(
          deviceId.toString() + IoTDBConstant.PATH_SEPARATOR + measurementId[subTaskId],
          currentWritingTimestamp,
          lastTime[subTaskId]);
    }
  }

  public abstract void setSchemaForAllTargetFile(List<Schema> schemas);
}
