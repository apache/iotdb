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
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.DiskAlignedChunkLoader;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.AlignedTimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.iotdb.tsfile.read.controller.IChunkMetadataLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.CHUNK_METADATA_FILTER_ALIGNED_DISK;
import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.CHUNK_METADATA_MODIFICATION_ALIGNED_DISK;
import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.LOAD_CHUNK_METADATA_LIST_ALIGNED_DISK;

public class DiskAlignedChunkMetadataLoader implements IChunkMetadataLoader {

  private final TsFileResource resource;
  private final QueryContext context;

  // global time filter, only used to check time range
  private final Filter globalTimeFilter;

  // all sub sensors' modifications
  private final List<List<Modification>> pathModifications;

  private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("QUERY_DEBUG");
  private static final SeriesScanCostMetricSet SERIES_SCAN_COST_METRIC_SET =
      SeriesScanCostMetricSet.getInstance();

  public DiskAlignedChunkMetadataLoader(
      TsFileResource resource,
      QueryContext context,
      Filter globalTimeFilter,
      List<List<Modification>> pathModifications) {
    this.resource = resource;
    this.context = context;
    this.globalTimeFilter = globalTimeFilter;
    this.pathModifications = pathModifications;
  }

  @Override
  public List<IChunkMetadata> loadChunkMetadataList(ITimeSeriesMetadata timeSeriesMetadata) {
    final long t1 = System.nanoTime();
    try {
      List<AlignedChunkMetadata> alignedChunkMetadataList =
          ((AlignedTimeSeriesMetadata) timeSeriesMetadata).getCopiedChunkMetadataList();

      // when alignedChunkMetadataList.size() == 1, it means that the chunk statistics is same as
      // the time series metadata, so we don't need to filter it again.
      if (alignedChunkMetadataList.size() > 1) {
        // remove not satisfied ChunkMetaData
        final long t2 = System.nanoTime();
        alignedChunkMetadataList.removeIf(
            alignedChunkMetaData ->
                (globalTimeFilter != null && globalTimeFilter.canSkip(alignedChunkMetaData))
                    || alignedChunkMetaData.getStartTime() > alignedChunkMetaData.getEndTime());

        if (context.isDebug()) {
          DEBUG_LOGGER.info("After removed by filter Chunk meta data list is: ");
          alignedChunkMetadataList.forEach(c -> DEBUG_LOGGER.info(c.toString()));
        }

        SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(
            CHUNK_METADATA_FILTER_ALIGNED_DISK, System.nanoTime() - t2);
      }

      final long t3 = System.nanoTime();

      if (context.isDebug()) {
        DEBUG_LOGGER.info(
            "Modifications size is {} for file Path: {} ",
            pathModifications.size(),
            resource.getTsFilePath());
        pathModifications.forEach(c -> DEBUG_LOGGER.info(c.toString()));
      }

      // remove ChunkMetadata that have been deleted
      ModificationUtils.modifyAlignedChunkMetaData(alignedChunkMetadataList, pathModifications);

      if (context.isDebug()) {
        DEBUG_LOGGER.info("After modification Chunk meta data list is: ");
        alignedChunkMetadataList.forEach(c -> DEBUG_LOGGER.info(c.toString()));
      }
      SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(
          CHUNK_METADATA_MODIFICATION_ALIGNED_DISK, System.nanoTime() - t3);

      // it is ok, even if it is not thread safe, because the cost of creating a DiskChunkLoader is
      // very cheap.
      alignedChunkMetadataList.forEach(
          chunkMetadata -> {
            if (chunkMetadata.needSetChunkLoader()) {
              chunkMetadata.setVersion(resource.getVersion());
              chunkMetadata.setClosed(resource.isClosed());
              chunkMetadata.setChunkLoader(new DiskAlignedChunkLoader(context, resource));
            }
          });

      return new ArrayList<>(alignedChunkMetadataList);
    } finally {
      SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(
          LOAD_CHUNK_METADATA_LIST_ALIGNED_DISK, System.nanoTime() - t1);
    }
  }
}
