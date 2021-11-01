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
package org.apache.iotdb.tsfile.read.reader.series;

import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderByTimestamp;

import java.io.IOException;
import java.util.List;

/**
 * Series reader is used to query one series of one tsfile, using this reader to query the value of
 * a series with given timestamps.
 */
public class FileSeriesReaderByTimestamp {

  protected IChunkLoader chunkLoader;
  protected List<IChunkMetadata> chunkMetadataList;
  private int currentChunkIndex = 0;

  private ChunkReader chunkReader;
  private long currentTimestamp;
  private BatchData data = null; // current batch data

  /** init with chunkLoader and chunkMetaDataList. */
  public FileSeriesReaderByTimestamp(
      IChunkLoader chunkLoader, List<IChunkMetadata> chunkMetadataList) {
    this.chunkLoader = chunkLoader;
    this.chunkMetadataList = chunkMetadataList;
    currentTimestamp = Long.MIN_VALUE;
  }

  public TSDataType getDataType() {
    return chunkMetadataList.get(0).getDataType();
  }

  /** get value with time equals timestamp. If there is no such point, return null. */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public Object getValueInTimestamp(long timestamp) throws IOException {
    this.currentTimestamp = timestamp;

    // first initialization, only invoked in the first time
    if (chunkReader == null) {
      if (!constructNextSatisfiedChunkReader()) {
        return null;
      }

      if (chunkReader.hasNextSatisfiedPage()) {
        data = chunkReader.nextPageData();
      } else {
        return null;
      }
    }

    while (data != null) {
      while (data.hasCurrent()) {
        if (data.currentTime() < timestamp) {
          data.next();
        } else {
          break;
        }
      }

      if (data.hasCurrent()) {
        if (data.currentTime() == timestamp) {
          Object value = data.currentValue();
          data.next();
          return value;
        }
        return null;
      } else {
        if (chunkReader.hasNextSatisfiedPage()) {
          data = chunkReader.nextPageData();
        } else if (!constructNextSatisfiedChunkReader()) {
          return null;
        }
      }
    }

    return null;
  }

  /**
   * Judge if the series reader has next time-value pair.
   *
   * @return true if has next, false if not.
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public boolean hasNext() throws IOException {

    if (chunkReader != null) {
      if (data != null && data.hasCurrent()) {
        return true;
      }
      while (chunkReader.hasNextSatisfiedPage()) {
        data = chunkReader.nextPageData();
        if (data != null && data.hasCurrent()) {
          return true;
        }
      }
    }
    while (constructNextSatisfiedChunkReader()) {
      while (chunkReader.hasNextSatisfiedPage()) {
        data = chunkReader.nextPageData();
        if (data != null && data.hasCurrent()) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean constructNextSatisfiedChunkReader() throws IOException {
    while (currentChunkIndex < chunkMetadataList.size()) {
      IChunkMetadata chunkMetaData = chunkMetadataList.get(currentChunkIndex++);
      if (chunkSatisfied(chunkMetaData)) {
        initChunkReader(chunkMetaData);
        ((ChunkReaderByTimestamp) chunkReader).setCurrentTimestamp(currentTimestamp);
        return true;
      }
    }
    return false;
  }

  private void initChunkReader(IChunkMetadata chunkMetaData) throws IOException {
    Chunk chunk = chunkLoader.loadChunk((ChunkMetadata) chunkMetaData);
    this.chunkReader = new ChunkReaderByTimestamp(chunk);
  }

  private boolean chunkSatisfied(IChunkMetadata chunkMetaData) {
    return chunkMetaData.getEndTime() >= currentTimestamp;
  }
}
