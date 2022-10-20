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
package org.apache.iotdb.db.engine.compaction.inner.utils;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.constant.CompactionType;
import org.apache.iotdb.db.engine.compaction.constant.ProcessChunkType;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.service.metrics.recorder.CompactionMetricsRecorder;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import com.google.common.util.concurrent.RateLimiter;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/** This class is used to compact one series during inner space compaction. */
public class SingleSeriesCompactionExecutor {
  private String device;
  private PartialPath series;
  private LinkedList<Pair<TsFileSequenceReader, List<ChunkMetadata>>> readerAndChunkMetadataList;
  private TsFileIOWriter fileWriter;
  private TsFileResource targetResource;

  private IMeasurementSchema schema;
  private ChunkWriterImpl chunkWriter;
  private Chunk cachedChunk;
  private ChunkMetadata cachedChunkMetadata;
  private RateLimiter compactionRateLimiter =
      CompactionTaskManager.getInstance().getMergeWriteRateLimiter();
  // record the min time and max time to update the target resource
  private long minStartTimestamp = Long.MAX_VALUE;
  private long maxEndTimestamp = Long.MIN_VALUE;
  private long pointCountInChunkWriter = 0;

  private final long targetChunkSize =
      IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();
  private final long targetChunkPointNum =
      IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum();
  private final long chunkSizeLowerBound =
      IoTDBDescriptor.getInstance().getConfig().getChunkSizeLowerBoundInCompaction();
  private final long chunkPointNumLowerBound =
      IoTDBDescriptor.getInstance().getConfig().getChunkPointNumLowerBoundInCompaction();

  public SingleSeriesCompactionExecutor(
      PartialPath series,
      IMeasurementSchema measurementSchema,
      LinkedList<Pair<TsFileSequenceReader, List<ChunkMetadata>>> readerAndChunkMetadataList,
      TsFileIOWriter fileWriter,
      TsFileResource targetResource) {
    this.device = series.getDevice();
    this.series = series;
    this.readerAndChunkMetadataList = readerAndChunkMetadataList;
    this.fileWriter = fileWriter;
    this.schema = measurementSchema;
    this.chunkWriter = new ChunkWriterImpl(this.schema);
    this.cachedChunk = null;
    this.cachedChunkMetadata = null;
    this.targetResource = targetResource;
  }

  public SingleSeriesCompactionExecutor(
      PartialPath series,
      LinkedList<Pair<TsFileSequenceReader, List<ChunkMetadata>>> readerAndChunkMetadataList,
      TsFileIOWriter fileWriter,
      TsFileResource targetResource) {
    this.device = series.getDevice();
    this.series = series;
    this.readerAndChunkMetadataList = readerAndChunkMetadataList;
    this.fileWriter = fileWriter;
    this.schema = null;
    this.chunkWriter = null;
    this.cachedChunk = null;
    this.cachedChunkMetadata = null;
    this.targetResource = targetResource;
  }

  /**
   * This function execute the compaction of a single time series. Notice, the result of single
   * series compaction may contain more than one chunk.
   */
  public void execute() throws IOException {
    while (readerAndChunkMetadataList.size() > 0) {
      Pair<TsFileSequenceReader, List<ChunkMetadata>> readerListPair =
          readerAndChunkMetadataList.removeFirst();
      TsFileSequenceReader reader = readerListPair.left;
      List<ChunkMetadata> chunkMetadataList = readerListPair.right;
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        Chunk currentChunk = reader.readMemChunk(chunkMetadata);
        if (this.chunkWriter == null) {
          constructChunkWriterFromReadChunk(currentChunk);
        }
        CompactionMetricsRecorder.recordReadInfo(
            currentChunk.getHeader().getSerializedSize() + currentChunk.getHeader().getDataSize());

        // if this chunk is modified, deserialize it into points
        if (chunkMetadata.getDeleteIntervalList() != null) {
          processModifiedChunk(currentChunk);
          continue;
        }

        long chunkSize = getChunkSize(currentChunk);
        long chunkPointNum = currentChunk.getChunkStatistic().getCount();
        // we process this chunk in three different way according to the size of it
        if (chunkSize >= targetChunkSize || chunkPointNum >= targetChunkPointNum) {
          processLargeChunk(currentChunk, chunkMetadata);
        } else if (chunkSize < chunkSizeLowerBound && chunkPointNum < chunkPointNumLowerBound) {
          processSmallChunk(currentChunk);
        } else {
          processMiddleChunk(currentChunk, chunkMetadata);
        }
      }
    }

    // after all the chunk of this sensor is read, flush the remaining data
    if (cachedChunk != null) {
      flushChunkToFileWriter(cachedChunk, cachedChunkMetadata, true);
      cachedChunk = null;
      cachedChunkMetadata = null;
    } else if (pointCountInChunkWriter != 0L) {
      flushChunkWriter();
    }
    fileWriter.checkMetadataSizeAndMayFlush();
    targetResource.updateStartTime(device, minStartTimestamp);
    targetResource.updateEndTime(device, maxEndTimestamp);
  }

  private void constructChunkWriterFromReadChunk(Chunk chunk) {
    ChunkHeader chunkHeader = chunk.getHeader();
    this.schema =
        new MeasurementSchema(
            series.getMeasurement(),
            chunkHeader.getDataType(),
            chunkHeader.getEncodingType(),
            chunkHeader.getCompressionType());
    this.chunkWriter = new ChunkWriterImpl(this.schema);
  }

  private long getChunkSize(Chunk chunk) {
    return chunk.getHeader().getSerializedSize() + chunk.getHeader().getDataSize();
  }

  private void processModifiedChunk(Chunk chunk) throws IOException {
    if (cachedChunk != null) {
      // if there is a cached chunk, deserialize it and write it to ChunkWriter
      writeCachedChunkIntoChunkWriter();
    }
    // write this chunk to ChunkWriter
    writeChunkIntoChunkWriter(chunk);
    flushChunkWriterIfLargeEnough();
  }

  private void processLargeChunk(Chunk chunk, ChunkMetadata chunkMetadata) throws IOException {
    if (pointCountInChunkWriter != 0L) {
      // if there are points remaining in ChunkWriter
      // deserialize current chunk and write to ChunkWriter, then flush the ChunkWriter
      writeChunkIntoChunkWriter(chunk);
      flushChunkWriterIfLargeEnough();
    } else if (cachedChunk != null) {
      // if there is a cached chunk, merge it with current chunk, then flush it
      mergeWithCachedChunk(chunk, chunkMetadata);
      flushCachedChunkIfLargeEnough();
    } else {
      // there is no points remaining in ChunkWriter and no cached chunk
      // flush it to file directly
      flushChunkToFileWriter(chunk, chunkMetadata, false);
    }
  }

  private void processMiddleChunk(Chunk chunk, ChunkMetadata chunkMetadata) throws IOException {
    // the chunk is not too large either too small
    if (pointCountInChunkWriter != 0L) {
      // if there are points remaining in ChunkWriter
      // deserialize current chunk and write to ChunkWriter
      writeChunkIntoChunkWriter(chunk);
      flushChunkWriterIfLargeEnough();
    } else if (cachedChunk != null) {
      // if there is a cached chunk, merge it with current chunk
      mergeWithCachedChunk(chunk, chunkMetadata);
      flushCachedChunkIfLargeEnough();
    } else {
      // there is no points remaining in ChunkWriter and no cached chunk
      // cached current chunk
      cachedChunk = chunk;
      cachedChunkMetadata = chunkMetadata;
    }
  }

  private void processSmallChunk(Chunk chunk) throws IOException {
    // this chunk is too small
    // to ensure the flushed chunk is large enough
    // it should be deserialized and written to ChunkWriter
    if (cachedChunk != null) {
      // if there is a cached chunk, write the cached chunk to ChunkWriter
      writeCachedChunkIntoChunkWriter();
    }
    writeChunkIntoChunkWriter(chunk);
    flushChunkWriterIfLargeEnough();
  }

  /** Deserialize a chunk into points and write it to the chunkWriter */
  private void writeChunkIntoChunkWriter(Chunk chunk) throws IOException {
    IChunkReader chunkReader = new ChunkReader(chunk, null);
    while (chunkReader.hasNextSatisfiedPage()) {
      IPointReader batchIterator = chunkReader.nextPageData().getBatchDataIterator();
      while (batchIterator.hasNextTimeValuePair()) {
        TimeValuePair timeValuePair = batchIterator.nextTimeValuePair();
        writeTimeAndValueToChunkWriter(timeValuePair);
        if (timeValuePair.getTimestamp() > maxEndTimestamp) {
          maxEndTimestamp = timeValuePair.getTimestamp();
        }
        if (timeValuePair.getTimestamp() < minStartTimestamp) {
          minStartTimestamp = timeValuePair.getTimestamp();
        }
      }
    }
    pointCountInChunkWriter += chunk.getChunkStatistic().getCount();
  }

  private void writeCachedChunkIntoChunkWriter() throws IOException {
    if (cachedChunk.getData().position() != 0) {
      // If the position of cache chunk data buffer is 0,
      // it means that the cache chunk is the first chunk cached,
      // and it hasn't merged with any chunk yet.
      // If we flip it, both the position and limit in the buffer will be 0,
      // which leads to the lost of data.
      cachedChunk.getData().flip();
    }
    writeChunkIntoChunkWriter(cachedChunk);
    cachedChunk = null;
    cachedChunkMetadata = null;
  }

  private void mergeWithCachedChunk(Chunk currentChunk, ChunkMetadata currentChunkMetadata)
      throws IOException {
    // Notice!!!
    // We must execute mergeChunkByAppendPage before mergeChunkMetadata
    // otherwise the statistic of data may be wrong.
    cachedChunk.mergeChunkByAppendPage(currentChunk);
    cachedChunkMetadata.mergeChunkMetadata(currentChunkMetadata);
  }

  private void writeTimeAndValueToChunkWriter(TimeValuePair timeValuePair) {
    switch (chunkWriter.getDataType()) {
      case TEXT:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBinary());
        break;
      case FLOAT:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getFloat());
        break;
      case DOUBLE:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getDouble());
        break;
      case BOOLEAN:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBoolean());
        break;
      case INT64:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getLong());
        break;
      case INT32:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getInt());
        break;
      default:
        throw new UnsupportedOperationException("Unknown data type " + chunkWriter.getDataType());
    }
  }

  private void flushChunkToFileWriter(
      Chunk chunk, ChunkMetadata chunkMetadata, boolean isCachedChunk) throws IOException {
    CompactionTaskManager.mergeRateLimiterAcquire(compactionRateLimiter, getChunkSize(chunk));
    if (chunkMetadata.getStartTime() < minStartTimestamp) {
      minStartTimestamp = chunkMetadata.getStartTime();
    }
    if (chunkMetadata.getEndTime() > maxEndTimestamp) {
      maxEndTimestamp = chunkMetadata.getEndTime();
    }
    CompactionMetricsRecorder.recordWriteInfo(
        CompactionType.INNER_SEQ_COMPACTION,
        isCachedChunk ? ProcessChunkType.MERGE_CHUNK : ProcessChunkType.FLUSH_CHUNK,
        false,
        getChunkSize(chunk));
    fileWriter.writeChunk(chunk, chunkMetadata);
  }

  private void flushChunkWriterIfLargeEnough() throws IOException {
    if (pointCountInChunkWriter >= targetChunkPointNum
        || chunkWriter.estimateMaxSeriesMemSize() >= targetChunkSize) {
      CompactionTaskManager.mergeRateLimiterAcquire(
          compactionRateLimiter, chunkWriter.estimateMaxSeriesMemSize());
      CompactionMetricsRecorder.recordWriteInfo(
          CompactionType.INNER_SEQ_COMPACTION,
          ProcessChunkType.DESERIALIZE_CHUNK,
          false,
          chunkWriter.estimateMaxSeriesMemSize());
      chunkWriter.writeToFileWriter(fileWriter);
      pointCountInChunkWriter = 0L;
    }
  }

  private void flushCachedChunkIfLargeEnough() throws IOException {
    if (cachedChunk.getChunkStatistic().getCount() >= targetChunkPointNum
        || getChunkSize(cachedChunk) >= targetChunkSize) {
      flushChunkToFileWriter(cachedChunk, cachedChunkMetadata, true);
      cachedChunk = null;
      cachedChunkMetadata = null;
    }
  }

  private void flushChunkWriter() throws IOException {
    CompactionTaskManager.mergeRateLimiterAcquire(
        compactionRateLimiter, chunkWriter.estimateMaxSeriesMemSize());
    CompactionMetricsRecorder.recordWriteInfo(
        CompactionType.INNER_SEQ_COMPACTION,
        ProcessChunkType.DESERIALIZE_CHUNK,
        false,
        chunkWriter.estimateMaxSeriesMemSize());
    chunkWriter.writeToFileWriter(fileWriter);
    pointCountInChunkWriter = 0L;
  }
}
