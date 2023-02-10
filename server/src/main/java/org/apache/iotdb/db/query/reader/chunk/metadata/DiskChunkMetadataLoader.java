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
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.mpp.metric.QueryMetricsManager;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.chunk.DiskChunkLoader;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.read.controller.IChunkMetadataLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.iotdb.db.mpp.metric.SeriesScanCostMetricSet.CHUNK_METADATA_FILTER_NONALIGNED_DISK;
import static org.apache.iotdb.db.mpp.metric.SeriesScanCostMetricSet.CHUNK_METADATA_MODIFICATION_NONALIGNED_DISK;
import static org.apache.iotdb.db.mpp.metric.SeriesScanCostMetricSet.LOAD_CHUNK_METADATA_LIST_NONALIGNED_DISK;

public class DiskChunkMetadataLoader implements IChunkMetadataLoader {

  private final TsFileResource resource;
  private final PartialPath seriesPath;
  private final QueryContext context;
  // time filter or value filter, only used to check time range
  private final Filter filter;

  private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("QUERY_DEBUG");
  private static final QueryMetricsManager QUERY_METRICS = QueryMetricsManager.getInstance();

  public DiskChunkMetadataLoader(
      TsFileResource resource, PartialPath seriesPath, QueryContext context, Filter filter) {
    this.resource = resource;
    this.seriesPath = seriesPath;
    this.context = context;
    this.filter = filter;
  }

  @Override
  public List<IChunkMetadata> loadChunkMetadataList(ITimeSeriesMetadata timeSeriesMetadata) {
    long t1 = System.nanoTime();
    try {
      List<IChunkMetadata> chunkMetadataList =
          ((TimeseriesMetadata) timeSeriesMetadata).getCopiedChunkMetadataList();

      long t2 = System.nanoTime();
      List<Modification> pathModifications =
          context.getPathModifications(resource.getModFile(), seriesPath);

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

      QUERY_METRICS.recordSeriesScanCost(
          CHUNK_METADATA_MODIFICATION_NONALIGNED_DISK, System.nanoTime() - t2);

      // it is ok, even if it is not thread safe, because the cost of creating a DiskChunkLoader is
      // very cheap.
      chunkMetadataList.forEach(
          chunkMetadata -> {
            if (chunkMetadata.needSetChunkLoader()) {
              chunkMetadata.setFilePath(resource.getTsFilePath());
              chunkMetadata.setClosed(resource.isClosed());
              chunkMetadata.setChunkLoader(new DiskChunkLoader(context.isDebug()));
            }
          });

      // remove not satisfied ChunkMetaData
      long t3 = System.nanoTime();
      chunkMetadataList.removeIf(
          chunkMetaData ->
              (filter != null
                      && !filter.satisfyStartEndTime(
                          chunkMetaData.getStartTime(), chunkMetaData.getEndTime()))
                  || chunkMetaData.getStartTime() > chunkMetaData.getEndTime());
      QUERY_METRICS.recordSeriesScanCost(
          CHUNK_METADATA_FILTER_NONALIGNED_DISK, System.nanoTime() - t3);

      // For chunkMetadata from old TsFile, do not set version
      for (IChunkMetadata metadata : chunkMetadataList) {
        if (!metadata.isFromOldTsFile()) {
          metadata.setVersion(resource.getVersion());
        }
      }

      if (context.isDebug()) {
        DEBUG_LOGGER.info("After removed by filter Chunk meta data list is: ");
        chunkMetadataList.forEach(c -> DEBUG_LOGGER.info(c.toString()));
      }

      return chunkMetadataList;
    } finally {
      QUERY_METRICS.recordSeriesScanCost(
          LOAD_CHUNK_METADATA_LIST_NONALIGNED_DISK, System.nanoTime() - t1);
    }
  }
}
