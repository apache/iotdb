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

package org.apache.iotdb.db.query.reader.chunk;

import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.mpp.metric.QueryMetricsManager;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;

import static org.apache.iotdb.db.mpp.metric.SeriesScanCostMetricSet.CONSTRUCT_CHUNK_READER_NONALIGNED_MEM;
import static org.apache.iotdb.db.mpp.metric.SeriesScanCostMetricSet.INIT_CHUNK_READER_NONALIGNED_MEM;

/** To read one chunk from memory, and only used in iotdb server module */
public class MemChunkLoader implements IChunkLoader {

  private final ReadOnlyMemChunk chunk;

  private static final QueryMetricsManager QUERY_METRICS = QueryMetricsManager.getInstance();

  public MemChunkLoader(ReadOnlyMemChunk chunk) {
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
  public IChunkReader getChunkReader(IChunkMetadata chunkMetaData, Filter timeFilter) {
    long startTime = System.nanoTime();
    try {
      return new MemChunkReader(chunk, timeFilter);
    } finally {
      long duration = System.nanoTime() - startTime;
      QUERY_METRICS.recordSeriesScanCost(CONSTRUCT_CHUNK_READER_NONALIGNED_MEM, duration);
      QUERY_METRICS.recordSeriesScanCost(INIT_CHUNK_READER_NONALIGNED_MEM, duration);
    }
  }
}
