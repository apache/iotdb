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
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

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

  private IMeasurementSchema schema;
  private IChunkWriter chunkWriter;
  private Chunk cachedChunk;
  private ChunkMetadata cachedChunkMetadata;
  private boolean dataRemainsInChunkWriter = false;

  private final long targetChunkSize =
      IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();
  private final long chunkSizeLowerBound =
      IoTDBDescriptor.getInstance().getConfig().getChunkSizeLowerBoundInCompaction();

  public TimeSeriesCompactor(
      String storageGroup,
      String device,
      String timeSeries,
      LinkedList<Pair<TsFileSequenceReader, List<ChunkMetadata>>> readerAndChunkMetadataList,
      TsFileIOWriter fileWriter)
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
  }

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
            writeChunkIntoChunkWriter(cachedChunk, cachedChunkMetadata);
            cachedChunk = null;
            cachedChunkMetadata = null;
          }
          // write this chunk to ChunkWriter
          writeChunkIntoChunkWriter(currentChunk, chunkMetadata);
          flushChunkWriterIfLargeEnough();
          continue;
        }

        long chunkSize = getChunkSize(currentChunk);
        // we process this chunk in three different way according to the size of it
        if (chunkSize >= targetChunkSize) {
          if (dataRemainsInChunkWriter) {
            // if there are points remaining in ChunkWriter
            // deserialize current chunk and write to ChunkWriter, then flush the ChunkWriter
            writeChunkIntoChunkWriter(currentChunk, chunkMetadata);
            flushChunkWriterIfLargeEnough();
          } else if (cachedChunk != null) {
            // if there is a cached chunk, merge it with current chunk, then flush it
            cachedChunkMetadata.mergeChunkMetadata(chunkMetadata);
            cachedChunk.mergeChunk(cachedChunk);
            flushChunkToWriter(cachedChunk, cachedChunkMetadata);
            cachedChunk = null;
            cachedChunkMetadata = null;
          } else {
            // there is no points remaining in ChunkWriter and no cached chunk
            // flush it to file directly
            flushChunkToWriter(currentChunk, chunkMetadata);
          }
        } else if (chunkSize < chunkSizeLowerBound) {
          // this chunk is too small
          // to ensure the flushed chunk is large enough
          // it should be deserialized and written to ChunkWriter
          if (cachedChunk != null) {
            // if there is a cached chunk, write the cached chunk to ChunkWriter
            writeChunkIntoChunkWriter(cachedChunk, cachedChunkMetadata);
            cachedChunk = null;
            cachedChunkMetadata = null;
          }
          writeChunkIntoChunkWriter(currentChunk, chunkMetadata);
          flushChunkWriterIfLargeEnough();
        } else {
          // the chunk is not too large either too small
          if (dataRemainsInChunkWriter) {
            // if there are points remaining in ChunkWriter
            // deserialize current chunk and write to ChunkWriter
            writeChunkIntoChunkWriter(currentChunk, chunkMetadata);
            flushChunkWriterIfLargeEnough();
          } else if (cachedChunk != null) {
            // if there is a cached chunk, merge it with current chunk
            cachedChunkMetadata.mergeChunkMetadata(chunkMetadata);
            cachedChunk.mergeChunk(currentChunk);
            if (getChunkSize(cachedChunk) >= targetChunkSize) {
              flushChunkToWriter(cachedChunk, cachedChunkMetadata);
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
      flushChunkToWriter(cachedChunk, cachedChunkMetadata);
      cachedChunk = null;
      cachedChunkMetadata = null;
    } else if (dataRemainsInChunkWriter) {
      flushChunkWriter();
    }
  }

  private void writeChunkIntoChunkWriter(Chunk chunk, ChunkMetadata chunkMetadata) {}

  private long getChunkSize(Chunk chunk) {
    return chunk.getHeader().getSerializedSize() + chunk.getHeader().getDataSize();
  }

  private void flushChunkToWriter(Chunk chunk, ChunkMetadata chunkMetadata) {}

  private void flushChunkWriterIfLargeEnough() {}

  private void flushChunkWriter() {}
}
