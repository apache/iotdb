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

package org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.metadata;

import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet;
import org.apache.iotdb.db.storageengine.dataregion.memtable.ReadOnlyMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.DiskChunkLoader;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.tsfile.read.controller.IChunkMetadataLoader;
import org.apache.tsfile.read.filter.basic.Filter;

import java.util.List;

import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.CHUNK_METADATA_FILTER_NONALIGNED_MEM;
import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.LOAD_CHUNK_METADATA_LIST_NONALIGNED_MEM;

public class MemChunkMetadataLoader implements IChunkMetadataLoader {

  private final TsFileResource resource;
  private final NonAlignedFullPath seriesPath;
  private final QueryContext context;
  private final Filter globalTimeFilter;

  private static final SeriesScanCostMetricSet SERIES_SCAN_COST_METRIC_SET =
      SeriesScanCostMetricSet.getInstance();

  public MemChunkMetadataLoader(
      TsFileResource resource,
      NonAlignedFullPath seriesPath,
      QueryContext context,
      Filter globalTimeFilter) {
    this.resource = resource;
    this.seriesPath = seriesPath;
    this.context = context;
    this.globalTimeFilter = globalTimeFilter;
  }

  @Override
  public List<IChunkMetadata> loadChunkMetadataList(ITimeSeriesMetadata timeSeriesMetadata) {
    long t1 = System.nanoTime();
    try {
      // There is no need to apply modifications to these, because we already do that while
      // generating it in TSP
      List<IChunkMetadata> chunkMetadataList = resource.getChunkMetadataList(seriesPath);

      // it is ok, even if it is not thread safe, because the cost of creating a DiskChunkLoader is
      // very cheap.
      chunkMetadataList.forEach(
          chunkMetadata -> {
            if (chunkMetadata.needSetChunkLoader()) {
              chunkMetadata.setVersion(resource.getVersion());
              chunkMetadata.setClosed(resource.isClosed());
              chunkMetadata.setChunkLoader(new DiskChunkLoader(context, resource));
            }
          });

      List<ReadOnlyMemChunk> memChunks = resource.getReadOnlyMemChunk(seriesPath);
      if (memChunks != null) {
        for (ReadOnlyMemChunk readOnlyMemChunk : memChunks) {
          if (!readOnlyMemChunk.isEmpty()) {
            chunkMetadataList.add(readOnlyMemChunk.getChunkMetaData());
          }
        }
      }

      // when chunkMetadataList.size() == 1, it means that the chunk statistics is same as
      // the time series metadata, so we don't need to filter it again.
      if (chunkMetadataList.size() > 1) {
        // remove not satisfied ChunkMetaData
        long t2 = System.nanoTime();
        chunkMetadataList.removeIf(
            chunkMetaData ->
                (globalTimeFilter != null && globalTimeFilter.canSkip(chunkMetaData))
                    || chunkMetaData.getStartTime() > chunkMetaData.getEndTime());
        SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(
            CHUNK_METADATA_FILTER_NONALIGNED_MEM, System.nanoTime() - t2);
      }

      for (IChunkMetadata metadata : chunkMetadataList) {
        metadata.setVersion(resource.getVersion());
      }
      return chunkMetadataList;
    } finally {
      SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(
          LOAD_CHUNK_METADATA_LIST_NONALIGNED_MEM, System.nanoTime() - t1);
    }
  }
}
