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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet;
import org.apache.iotdb.db.storageengine.dataregion.memtable.AlignedReadOnlyMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.memtable.ReadOnlyMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.DiskAlignedChunkLoader;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.iotdb.tsfile.read.controller.IChunkMetadataLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.util.List;

import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.CHUNK_METADATA_FILTER_ALIGNED_MEM;
import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.LOAD_CHUNK_METADATA_LIST_ALIGNED_MEM;

public class MemAlignedChunkMetadataLoader implements IChunkMetadataLoader {

  private final TsFileResource resource;
  private final PartialPath seriesPath;
  private final QueryContext context;
  private final Filter timeFilter;
  // only used for limit and offset push down optimizer, if we select all columns from aligned
  // device, we
  // can use statistics to skip.
  // it's only exact while using limit & offset push down
  private final boolean queryAllSensors;

  private static final SeriesScanCostMetricSet SERIES_SCAN_COST_METRIC_SET =
      SeriesScanCostMetricSet.getInstance();

  public MemAlignedChunkMetadataLoader(
      TsFileResource resource,
      PartialPath seriesPath,
      QueryContext context,
      Filter timeFilter,
      boolean queryAllSensors) {
    this.resource = resource;
    this.seriesPath = seriesPath;
    this.context = context;
    this.timeFilter = timeFilter;
    this.queryAllSensors = queryAllSensors;
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
              chunkMetadata.setChunkLoader(
                  new DiskAlignedChunkLoader(context.isDebug(), queryAllSensors));
            }
          });

      // There is no need to set IChunkLoader for it, because the MemChunkLoader has already been
      // set
      // while creating ReadOnlyMemChunk
      List<ReadOnlyMemChunk> memChunks = resource.getReadOnlyMemChunk(seriesPath);
      if (memChunks != null) {
        for (ReadOnlyMemChunk readOnlyMemChunk : memChunks) {
          if (!readOnlyMemChunk.isEmpty()) {
            ((AlignedReadOnlyMemChunk) readOnlyMemChunk).setQueryAllSensors(queryAllSensors);
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
      SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(
          CHUNK_METADATA_FILTER_ALIGNED_MEM, System.nanoTime() - t2);

      for (IChunkMetadata metadata : chunkMetadataList) {
        metadata.setVersion(resource.getVersion());
      }
      return chunkMetadataList;
    } finally {
      SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(
          LOAD_CHUNK_METADATA_LIST_ALIGNED_MEM, System.nanoTime() - t1);
    }
  }
}
