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

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.mpp.metric.QueryMetricsManager;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.chunk.DiskAlignedChunkLoader;
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

import static org.apache.iotdb.db.mpp.metric.SeriesScanCostMetricSet.CHUNK_METADATA_FILTER_ALIGNED_DISK;
import static org.apache.iotdb.db.mpp.metric.SeriesScanCostMetricSet.CHUNK_METADATA_MODIFICATION_ALIGNED_DISK;
import static org.apache.iotdb.db.mpp.metric.SeriesScanCostMetricSet.LOAD_CHUNK_METADATA_LIST_ALIGNED_DISK;

public class DiskAlignedChunkMetadataLoader implements IChunkMetadataLoader {

  private final TsFileResource resource;
  private final AlignedPath seriesPath;
  private final QueryContext context;
  // time filter or value filter, only used to check time range
  private final Filter filter;

  private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("QUERY_DEBUG");
  private static final QueryMetricsManager QUERY_METRICS = QueryMetricsManager.getInstance();

  public DiskAlignedChunkMetadataLoader(
      TsFileResource resource, AlignedPath seriesPath, QueryContext context, Filter filter) {
    this.resource = resource;
    this.seriesPath = seriesPath;
    this.context = context;
    this.filter = filter;
  }

  @Override
  public List<IChunkMetadata> loadChunkMetadataList(ITimeSeriesMetadata timeSeriesMetadata) {
    long t1 = System.nanoTime();
    try {
      List<AlignedChunkMetadata> alignedChunkMetadataList =
          ((AlignedTimeSeriesMetadata) timeSeriesMetadata).getCopiedChunkMetadataList();

      long t2 = System.nanoTime();
      // get all sub sensors' modifications
      List<List<Modification>> pathModifications =
          context.getPathModifications(resource.getModFile(), seriesPath);

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
      QUERY_METRICS.recordSeriesScanCost(
          CHUNK_METADATA_MODIFICATION_ALIGNED_DISK, System.nanoTime() - t2);

      // remove not satisfied ChunkMetaData
      long t3 = System.nanoTime();
      alignedChunkMetadataList.removeIf(
          alignedChunkMetaData ->
              (filter != null
                      && !filter.satisfyStartEndTime(
                          alignedChunkMetaData.getStartTime(), alignedChunkMetaData.getEndTime()))
                  || alignedChunkMetaData.getStartTime() > alignedChunkMetaData.getEndTime());
      QUERY_METRICS.recordSeriesScanCost(
          CHUNK_METADATA_FILTER_ALIGNED_DISK, System.nanoTime() - t3);

      // it is ok, even if it is not thread safe, because the cost of creating a DiskChunkLoader is
      // very cheap.
      alignedChunkMetadataList.forEach(
          chunkMetadata -> {
            if (chunkMetadata.needSetChunkLoader()) {
              chunkMetadata.setFilePath(resource.getTsFilePath());
              chunkMetadata.setClosed(resource.isClosed());
              chunkMetadata.setChunkLoader(new DiskAlignedChunkLoader(context.isDebug()));
            }
          });

      if (context.isDebug()) {
        DEBUG_LOGGER.info("After removed by filter Chunk meta data list is: ");
        alignedChunkMetadataList.forEach(c -> DEBUG_LOGGER.info(c.toString()));
      }

      return new ArrayList<>(alignedChunkMetadataList);
    } finally {
      QUERY_METRICS.recordSeriesScanCost(
          LOAD_CHUNK_METADATA_LIST_ALIGNED_DISK, System.nanoTime() - t1);
    }
  }
}
