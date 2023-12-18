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

import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet;
import org.apache.iotdb.db.storageengine.buffer.ChunkCache;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;

import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.INIT_CHUNK_READER_NONALIGNED_DISK;

/** To read one chunk from disk, and only used in iotdb server module. */
public class DiskChunkLoader implements IChunkLoader {

  private final QueryContext context;

  private final TsFileResource resource;

  private final boolean debug;

  public DiskChunkLoader(QueryContext context, TsFileResource resource) {
    this.context = context;
    this.debug = context.isDebug();
    this.resource = resource;
  }

  @Override
  public Chunk loadChunk(ChunkMetadata chunkMetaData) throws IOException {
    return ChunkCache.getInstance()
        .get(
            new ChunkCache.ChunkCacheKey(
                resource.getTsFilePath(),
                resource.getTsFileID(),
                chunkMetaData.getOffsetOfChunkHeader(),
                resource.isClosed()),
            chunkMetaData.getDeleteIntervalList(),
            chunkMetaData.getStatistics(),
            debug);
  }

  @Override
  public void close() {
    // do nothing
  }

  @Override
  public IChunkReader getChunkReader(IChunkMetadata chunkMetaData, Filter globalTimeFilter)
      throws IOException {
    long t1 = System.nanoTime();
    try {
      Chunk chunk =
          ChunkCache.getInstance()
              .get(
                  new ChunkCache.ChunkCacheKey(
                      resource.getTsFilePath(),
                      resource.getTsFileID(),
                      chunkMetaData.getOffsetOfChunkHeader(),
                      resource.isClosed()),
                  chunkMetaData.getDeleteIntervalList(),
                  chunkMetaData.getStatistics(),
                  debug);

      long t2 = System.nanoTime();
      IChunkReader chunkReader = new ChunkReader(chunk, globalTimeFilter);
      SeriesScanCostMetricSet.getInstance()
          .recordSeriesScanCost(INIT_CHUNK_READER_NONALIGNED_DISK, System.nanoTime() - t2);

      return chunkReader;
    } finally {
      long time = System.nanoTime() - t1;
      context.getQueryStatistics().constructNonAlignedChunkReadersDiskCount.getAndAdd(1);
      context.getQueryStatistics().constructNonAlignedChunkReadersDiskTime.getAndAdd(time);
    }
  }
}
