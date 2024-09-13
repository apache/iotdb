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
import org.apache.iotdb.db.storageengine.dataregion.memtable.ReadOnlyMemChunk;

import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.controller.IChunkLoader;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.IChunkReader;

import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.INIT_CHUNK_READER_NONALIGNED_MEM;

/** To read one chunk from memory, and only used in iotdb server module. */
public class MemChunkLoader implements IChunkLoader {
  private final QueryContext context;
  private final ReadOnlyMemChunk chunk;

  private static final SeriesScanCostMetricSet SERIES_SCAN_COST_METRIC_SET =
      SeriesScanCostMetricSet.getInstance();

  public MemChunkLoader(QueryContext context, ReadOnlyMemChunk chunk) {
    this.context = context;
    this.chunk = chunk;
  }

  @Override
  public Chunk loadChunk(ChunkMetadata chunkMetaData) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    // no resources need to close
  }

  @Override
  public IChunkReader getChunkReader(IChunkMetadata chunkMetaData, Filter globalTimeFilter) {
    long startTime = System.nanoTime();
    try {
      return new MemChunkReader(chunk, globalTimeFilter);
    } finally {
      long duration = System.nanoTime() - startTime;
      context.getQueryStatistics().getConstructNonAlignedChunkReadersMemCount().getAndAdd(1);
      context.getQueryStatistics().getConstructNonAlignedChunkReadersMemTime().getAndAdd(duration);
      SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(INIT_CHUNK_READER_NONALIGNED_MEM, duration);
    }
  }
}
