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
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.controller.IChunkLoader;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.IChunkReader;
import org.apache.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.tsfile.read.reader.chunk.TableChunkReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.INIT_CHUNK_READER_ALIGNED_DISK;

public class DiskAlignedChunkLoader implements IChunkLoader {

  private final QueryContext context;
  private final boolean debug;

  private final TsFileResource resource;

  // for table model, it will be false
  // for tree model, it will be true
  private final boolean ignoreAllNullRows;

  private static final SeriesScanCostMetricSet SERIES_SCAN_COST_METRIC_SET =
      SeriesScanCostMetricSet.getInstance();

  public DiskAlignedChunkLoader(
      QueryContext context, TsFileResource resource, boolean ignoreAllNullRows) {
    this.context = context;
    this.debug = context.isDebug();
    this.resource = resource;
    this.ignoreAllNullRows = ignoreAllNullRows;
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
  public IChunkReader getChunkReader(IChunkMetadata chunkMetaData, Filter globalTimeFilter)
      throws IOException {
    long t1 = System.nanoTime();
    try {
      AlignedChunkMetadata alignedChunkMetadata = (AlignedChunkMetadata) chunkMetaData;
      ChunkMetadata timeChunkMetadata = (ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata();
      Chunk timeChunk =
          ChunkCache.getInstance()
              .get(
                  new ChunkCache.ChunkCacheKey(
                      resource.getTsFilePath(),
                      resource.getTsFileID(),
                      timeChunkMetadata.getOffsetOfChunkHeader(),
                      resource.isClosed()),
                  timeChunkMetadata.getDeleteIntervalList(),
                  timeChunkMetadata.getStatistics(),
                  debug);
      List<Chunk> valueChunkList = new ArrayList<>();
      for (IChunkMetadata valueChunkMetadata : alignedChunkMetadata.getValueChunkMetadataList()) {
        valueChunkList.add(
            valueChunkMetadata == null
                ? null
                : ChunkCache.getInstance()
                    .get(
                        new ChunkCache.ChunkCacheKey(
                            resource.getTsFilePath(),
                            resource.getTsFileID(),
                            valueChunkMetadata.getOffsetOfChunkHeader(),
                            resource.isClosed()),
                        valueChunkMetadata.getDeleteIntervalList(),
                        valueChunkMetadata.getStatistics(),
                        debug));
      }

      long t2 = System.nanoTime();
      IChunkReader chunkReader =
          ignoreAllNullRows
              ? new AlignedChunkReader(timeChunk, valueChunkList, globalTimeFilter)
              : new TableChunkReader(timeChunk, valueChunkList, globalTimeFilter);
      SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(
          INIT_CHUNK_READER_ALIGNED_DISK, System.nanoTime() - t2);

      return chunkReader;
    } finally {
      long time = System.nanoTime() - t1;
      context.getQueryStatistics().getConstructAlignedChunkReadersDiskCount().getAndAdd(1);
      context.getQueryStatistics().getConstructAlignedChunkReadersDiskTime().getAndAdd(time);
    }
  }

  public TsFileID getTsFileID() {
    return resource.getTsFileID();
  }
}
