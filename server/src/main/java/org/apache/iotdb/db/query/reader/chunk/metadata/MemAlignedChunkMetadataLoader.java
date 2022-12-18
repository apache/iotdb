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
package org.apache.iotdb.db.query.reader.chunk.metadata;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.mpp.metric.QueryMetricsManager;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.chunk.DiskAlignedChunkLoader;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.iotdb.tsfile.read.controller.IChunkMetadataLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.util.List;

import static org.apache.iotdb.db.mpp.metric.SeriesScanCostMetricSet.CHUNK_METADATA_FILTER_ALIGNED_MEM;
import static org.apache.iotdb.db.mpp.metric.SeriesScanCostMetricSet.LOAD_CHUNK_METADATA_LIST_ALIGNED_MEM;

public class MemAlignedChunkMetadataLoader implements IChunkMetadataLoader {

  private final TsFileResource resource;
  private final PartialPath seriesPath;
  private final QueryContext context;
  private final Filter timeFilter;

  private static final QueryMetricsManager QUERY_METRICS = QueryMetricsManager.getInstance();

  public MemAlignedChunkMetadataLoader(
      TsFileResource resource, PartialPath seriesPath, QueryContext context, Filter timeFilter) {
    this.resource = resource;
    this.seriesPath = seriesPath;
    this.context = context;
    this.timeFilter = timeFilter;
  }

  @Override
  public List<IChunkMetadata> loadChunkMetadataList(ITimeSeriesMetadata timeSeriesMetadata) {
    long t1 = System.nanoTime();
    try {
      // There is no need to apply modifications to these, because we already do that while
      // generating it in TSP
      List<IChunkMetadata> chunkMetadataList = resource.getChunkMetadataList(seriesPath);

      chunkMetadataList.forEach(
          chunkMetadata -> {
            if (chunkMetadata.needSetChunkLoader()) {
              chunkMetadata.setFilePath(resource.getTsFilePath());
              chunkMetadata.setClosed(resource.isClosed());
              chunkMetadata.setChunkLoader(new DiskAlignedChunkLoader(context.isDebug()));
            }
          });

      // There is no need to set IChunkLoader for it, because the MemChunkLoader has already been
      // set
      // while creating ReadOnlyMemChunk
      List<ReadOnlyMemChunk> memChunks = resource.getReadOnlyMemChunk(seriesPath);
      if (memChunks != null) {
        for (ReadOnlyMemChunk readOnlyMemChunk : memChunks) {
          if (!memChunks.isEmpty()) {
            chunkMetadataList.add(readOnlyMemChunk.getChunkMetaData());
          }
        }
      }

      // remove not satisfied ChunkMetaData
      long t2 = System.nanoTime();
      chunkMetadataList.removeIf(
          chunkMetaData ->
              (timeFilter != null
                      && !timeFilter.satisfyStartEndTime(
                          chunkMetaData.getStartTime(), chunkMetaData.getEndTime()))
                  || chunkMetaData.getStartTime() > chunkMetaData.getEndTime());
      QUERY_METRICS.recordSeriesScanCost(CHUNK_METADATA_FILTER_ALIGNED_MEM, System.nanoTime() - t2);

      for (IChunkMetadata metadata : chunkMetadataList) {
        metadata.setVersion(resource.getVersion());
      }
      return chunkMetadataList;
    } finally {
      QUERY_METRICS.recordSeriesScanCost(
          LOAD_CHUNK_METADATA_LIST_ALIGNED_MEM, System.nanoTime() - t1);
    }
  }
}
