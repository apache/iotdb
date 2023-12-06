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
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.AlignedChunkReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.CONSTRUCT_CHUNK_READER_ALIGNED_DISK;
import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.INIT_CHUNK_READER_ALIGNED_DISK;

public class DiskAlignedChunkLoader implements IChunkLoader {
  private static final Logger LOGGER = LoggerFactory.getLogger(DiskAlignedChunkLoader.class);
  private QueryContext context;
  private final boolean debug;

  // only used for limit and offset push down optimizer, if we select all columns from aligned
  // device, we
  // can use statistics to skip.
  // it's only exact while using limit & offset push down
  private final boolean queryAllSensors;

  private final TsFileResource resource;

  private static final SeriesScanCostMetricSet SERIES_SCAN_COST_METRIC_SET =
      SeriesScanCostMetricSet.getInstance();

  public DiskAlignedChunkLoader(
      QueryContext context, boolean queryAllSensors, TsFileResource resource) {
    this.context = context;
    this.debug = context.isDebug();
    this.queryAllSensors = queryAllSensors;
    this.resource = resource;
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
          new AlignedChunkReader(timeChunk, valueChunkList, timeFilter, queryAllSensors);
      long time = System.nanoTime() - t2;
      context.pageReadersDiskDeserializationTime.getAndAdd(time);
      SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(INIT_CHUNK_READER_ALIGNED_DISK, time);

      return chunkReader;
    } finally {
      long time = System.nanoTime() - t1;
      context.constructChunkReaderCountFromDisk.getAndAdd(1);
      context.constructChunkReaderTimeFromDisk.getAndAdd(time);

      SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(CONSTRUCT_CHUNK_READER_ALIGNED_DISK, time);
    }
  }
}
