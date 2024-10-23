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

import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.DiskChunkLoader;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.ModificationUtils;

import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.controller.IChunkMetadataLoader;
import org.apache.tsfile.read.filter.basic.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.CHUNK_METADATA_FILTER_NONALIGNED_DISK;
import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.CHUNK_METADATA_MODIFICATION_NONALIGNED_DISK;
import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.LOAD_CHUNK_METADATA_LIST_NONALIGNED_DISK;

public class DiskChunkMetadataLoader implements IChunkMetadataLoader {

  private final TsFileResource resource;
  private final QueryContext context;

  // global time filter, only used to check time range
  private final Filter globalTimeFilter;

  private final List<ModEntry> pathModifications;

  private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("QUERY_DEBUG");
  private static final SeriesScanCostMetricSet SERIES_SCAN_COST_METRIC_SET =
      SeriesScanCostMetricSet.getInstance();

  public DiskChunkMetadataLoader(
      TsFileResource resource,
      QueryContext context,
      Filter globalTimeFilter,
      List<ModEntry> pathModifications) {
    this.resource = resource;
    this.context = context;
    this.globalTimeFilter = globalTimeFilter;
    this.pathModifications = pathModifications;
  }

  @Override
  public List<IChunkMetadata> loadChunkMetadataList(ITimeSeriesMetadata timeSeriesMetadata) {
    final long t1 = System.nanoTime();
    try {
      List<IChunkMetadata> chunkMetadataList =
          ((TimeseriesMetadata) timeSeriesMetadata).getCopiedChunkMetadataList();

      // when chunkMetadataList.size() == 1, it means that the chunk statistics is same as
      // the time series metadata, so we don't need to filter it again.
      if (chunkMetadataList.size() > 1) {
        // remove not satisfied ChunkMetaData
        final long t2 = System.nanoTime();
        chunkMetadataList.removeIf(
            chunkMetaData ->
                (globalTimeFilter != null && globalTimeFilter.canSkip(chunkMetaData))
                    || chunkMetaData.getStartTime() > chunkMetaData.getEndTime());

        if (context.isDebug()) {
          DEBUG_LOGGER.info("After removed by filter Chunk meta data list is: ");
          chunkMetadataList.forEach(c -> DEBUG_LOGGER.info(c.toString()));
        }

        SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(
            CHUNK_METADATA_FILTER_NONALIGNED_DISK, System.nanoTime() - t2);
      }

      final long t3 = System.nanoTime();

      if (context.isDebug()) {
        DEBUG_LOGGER.info(
            "Modifications size is {} for file Path: {} ",
            pathModifications.size(),
            resource.getTsFilePath());
        pathModifications.forEach(c -> DEBUG_LOGGER.info(c.toString()));
      }

      if (!pathModifications.isEmpty()) {
        ModificationUtils.modifyChunkMetaData(chunkMetadataList, pathModifications);
      }

      if (context.isDebug()) {
        DEBUG_LOGGER.info("After modification Chunk meta data list is: ");
        chunkMetadataList.forEach(c -> DEBUG_LOGGER.info(c.toString()));
      }

      SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(
          CHUNK_METADATA_MODIFICATION_NONALIGNED_DISK, System.nanoTime() - t3);

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

      return chunkMetadataList;
    } finally {
      SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(
          LOAD_CHUNK_METADATA_LIST_NONALIGNED_DISK, System.nanoTime() - t1);
    }
  }
}
