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

package org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk;

import org.apache.iotdb.db.storageengine.buffer.ChunkCache;
import org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.AlignedChunkReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.CONSTRUCT_CHUNK_READER_ALIGNED_DISK;
import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.INIT_CHUNK_READER_ALIGNED_DISK;

public class DiskAlignedChunkLoader implements IChunkLoader {

  private final boolean debug;
  private static final SeriesScanCostMetricSet SERIES_SCAN_COST_METRIC_SET =
      SeriesScanCostMetricSet.getInstance();

  public DiskAlignedChunkLoader(boolean debug) {
    this.debug = debug;
  }

  @Override
  public Chunk loadChunk(ChunkMetadata chunkMetaData) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    // there is no resource need to be closed
  }

  @Override
  public IChunkReader getChunkReader(IChunkMetadata chunkMetaData, Filter timeFilter)
      throws IOException {
    long t1 = System.nanoTime();
    try {
      AlignedChunkMetadata alignedChunkMetadata = (AlignedChunkMetadata) chunkMetaData;
      Chunk timeChunk =
          ChunkCache.getInstance()
              .get((ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata(), debug);
      List<Chunk> valueChunkList = new ArrayList<>();
      for (IChunkMetadata valueChunkMetadata : alignedChunkMetadata.getValueChunkMetadataList()) {
        valueChunkList.add(
            valueChunkMetadata == null
                ? null
                : ChunkCache.getInstance().get((ChunkMetadata) valueChunkMetadata, debug));
      }

      long t2 = System.nanoTime();
      IChunkReader chunkReader = new AlignedChunkReader(timeChunk, valueChunkList, timeFilter);
      SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(
          INIT_CHUNK_READER_ALIGNED_DISK, System.nanoTime() - t2);

      return chunkReader;
    } finally {
      SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(
          CONSTRUCT_CHUNK_READER_ALIGNED_DISK, System.nanoTime() - t1);
    }
  }
}
