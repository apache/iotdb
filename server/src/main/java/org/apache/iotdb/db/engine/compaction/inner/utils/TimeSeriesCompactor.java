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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.MergeManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderByTimestamp;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/** This class is used to compact one series during sequence space inner compaction. */
public class TimeSeriesCompactor {
  private static final Logger LOGGER = LoggerFactory.getLogger("COMPACTION");
  private String storageGroup;
  private String device;
  private String timeSeries;
  private LinkedList<Pair<TsFileSequenceReader, List<ChunkMetadata>>> readerAndChunkMetadataList;
  private TsFileIOWriter fileWriter;
  private TsFileResource targetResource;

  private IMeasurementSchema schema;
  private ChunkWriterImpl chunkWriter;
  private Chunk cachedChunk;
  private ChunkMetadata cachedChunkMetadata;
  private boolean dataRemainsInChunkWriter = false;
  private RateLimiter compactionRateLimiter = MergeManager.getINSTANCE().getMergeWriteRateLimiter();
  // record the min time and max time to update the target resource
  private long minStartTimestamp = Long.MAX_VALUE;
  private long maxEndTimestamp = Long.MIN_VALUE;

  private final long targetChunkSize =
      IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();
  private final long chunkSizeLowerBound =
      IoTDBDescriptor.getInstance().getConfig().getChunkSizeLowerBoundInCompaction();

  public TimeSeriesCompactor(
      String storageGroup,
      String device,
      String timeSeries,
      LinkedList<Pair<TsFileSequenceReader, List<ChunkMetadata>>> readerAndChunkMetadataList,
      TsFileIOWriter fileWriter,
      TsFileResource targetResource)
      throws MetadataException {
    this.storageGroup = storageGroup;
    this.device = device;
    this.timeSeries = timeSeries;
    this.readerAndChunkMetadataList = readerAndChunkMetadataList;
    this.fileWriter = fileWriter;
    this.schema = IoTDB.metaManager.getSeriesSchema(new PartialPath(device, timeSeries));
    this.chunkWriter = new ChunkWriterImpl(this.schema);
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

        // if this chunk is modified, deserialize it into points
        if (chunkMetadata.getDeleteIntervalList() != null) {
          if (cachedChunk != null) {
            // if there is a cached chunk, deserialize it and write it to ChunkWriter
            writeChunkIntoChunkWriter(cachedChunk);
            cachedChunk = null;
            cachedChunkMetadata = null;
          }
          // write this chunk to ChunkWriter
          writeChunkIntoChunkWriter(currentChunk);
          flushChunkWriterIfLargeEnough();
          continue;
        }

        long chunkSize = getChunkSize(currentChunk);
        // we process this chunk in three different way according to the size of it
        if (chunkSize >= targetChunkSize) {
          if (dataRemainsInChunkWriter) {
            // if there are points remaining in ChunkWriter
            // deserialize current chunk and write to ChunkWriter, then flush the ChunkWriter
            writeChunkIntoChunkWriter(currentChunk);
            flushChunkWriterIfLargeEnough();
          } else if (cachedChunk != null) {
            // if there is a cached chunk, merge it with current chunk, then flush it
            cachedChunkMetadata.mergeChunkMetadata(chunkMetadata);
            cachedChunk.mergeChunk(cachedChunk);
            flushChunkToFileWriter(cachedChunk, cachedChunkMetadata);
            cachedChunk = null;
            cachedChunkMetadata = null;
          } else {
            // there is no points remaining in ChunkWriter and no cached chunk
            // flush it to file directly
            flushChunkToFileWriter(currentChunk, chunkMetadata);
          }
        } else if (chunkSize < chunkSizeLowerBound) {
          // this chunk is too small
          // to ensure the flushed chunk is large enough
          // it should be deserialized and written to ChunkWriter
          if (cachedChunk != null) {
            // if there is a cached chunk, write the cached chunk to ChunkWriter
            writeChunkIntoChunkWriter(cachedChunk);
            cachedChunk = null;
            cachedChunkMetadata = null;
          }
          writeChunkIntoChunkWriter(currentChunk);
          flushChunkWriterIfLargeEnough();
        } else {
          // the chunk is not too large either too small
          if (dataRemainsInChunkWriter) {
            // if there are points remaining in ChunkWriter
            // deserialize current chunk and write to ChunkWriter
            writeChunkIntoChunkWriter(currentChunk);
            flushChunkWriterIfLargeEnough();
          } else if (cachedChunk != null) {
            // if there is a cached chunk, merge it with current chunk
            cachedChunkMetadata.mergeChunkMetadata(chunkMetadata);
            cachedChunk.mergeChunk(currentChunk);
            if (getChunkSize(cachedChunk) >= targetChunkSize) {
              flushChunkToFileWriter(cachedChunk, cachedChunkMetadata);
              cachedChunk = null;
              cachedChunkMetadata = null;
            }
          } else {
            // there is no points remaining in ChunkWriter and no cached chunk
            // cached current chunk
            cachedChunk = currentChunk;
            cachedChunkMetadata = chunkMetadata;
          }
        }
      }
    }

    // after all the chunk of this sensor is read, flush the remaining data
    if (cachedChunk != null) {
      flushChunkToFileWriter(cachedChunk, cachedChunkMetadata);
      cachedChunk = null;
      cachedChunkMetadata = null;
    } else if (dataRemainsInChunkWriter) {
      flushChunkWriter();
    }
  }

  private long getChunkSize(Chunk chunk) {
    return chunk.getHeader().getSerializedSize() + chunk.getHeader().getDataSize();
  }

  /** Deserialize a chunk into points and write it to the chunkWriter */
  private void writeChunkIntoChunkWriter(Chunk chunk) throws IOException {
    IChunkReader chunkReader = new ChunkReaderByTimestamp(chunk);
    while (chunkReader.hasNextSatisfiedPage()) {
      IPointReader batchIterator = chunkReader.nextPageData().getBatchDataIterator();
      while (batchIterator.hasNextTimeValuePair()) {
        TimeValuePair timeValuePair = batchIterator.nextTimeValuePair();
        writeTimeAndValueToChunkWriter(timeValuePair);
        if (timeValuePair.getTimestamp() > maxEndTimestamp) {
          maxEndTimestamp = timeValuePair.getTimestamp();
        } else if (timeValuePair.getTimestamp() < minStartTimestamp) {
          minStartTimestamp = timeValuePair.getTimestamp();
        }
      }
    }
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

  private void flushChunkToFileWriter(Chunk chunk, ChunkMetadata chunkMetadata) throws IOException {
    MergeManager.mergeRateLimiterAcquire(compactionRateLimiter, getChunkSize(chunk));
    if (chunkMetadata.getStartTime() < minStartTimestamp) {
      minStartTimestamp = chunkMetadata.getStartTime();
    }
    if (chunkMetadata.getEndTime() > maxEndTimestamp) {
      maxEndTimestamp = chunkMetadata.getEndTime();
    }
    fileWriter.writeChunk(chunk, chunkMetadata);
  }

  private void flushChunkWriterIfLargeEnough() throws IOException {
    if (chunkWriter.estimateMaxSeriesMemSize() >= targetChunkSize) {
      MergeManager.mergeRateLimiterAcquire(
          compactionRateLimiter, chunkWriter.estimateMaxSeriesMemSize());
      chunkWriter.writeToFileWriter(fileWriter);
    }
  }

  private void flushChunkWriter() throws IOException {
    MergeManager.mergeRateLimiterAcquire(
        compactionRateLimiter, chunkWriter.estimateMaxSeriesMemSize());
    chunkWriter.writeToFileWriter(fileWriter);
  }
}
