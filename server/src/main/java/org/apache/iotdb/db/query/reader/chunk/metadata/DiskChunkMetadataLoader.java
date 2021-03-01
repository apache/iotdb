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

import org.apache.iotdb.db.engine.cache.ChunkMetadataCache;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.chunk.DiskChunkLoader;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.read.controller.IChunkMetadataLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.List;

public class DiskChunkMetadataLoader implements IChunkMetadataLoader {

  private TsFileResource resource;
  private PartialPath seriesPath;
  private QueryContext context;
  // time filter or value filter, only used to check time range
  private Filter filter;

  public DiskChunkMetadataLoader(
      TsFileResource resource, PartialPath seriesPath, QueryContext context, Filter filter) {
    this.resource = resource;
    this.seriesPath = seriesPath;
    this.context = context;
    this.filter = filter;
  }

  @Override
  public List<ChunkMetadata> loadChunkMetadataList(TimeseriesMetadata timeseriesMetadata)
      throws IOException {
    List<ChunkMetadata> chunkMetadataList =
        ChunkMetadataCache.getInstance()
            .get(resource.getTsFilePath(), seriesPath, timeseriesMetadata);

    setDiskChunkLoader(chunkMetadataList, resource, seriesPath, context);

    /*
     * remove not satisfied ChunkMetaData
     */
    chunkMetadataList.removeIf(
        chunkMetaData ->
            (filter != null
                    && !filter.satisfyStartEndTime(
                        chunkMetaData.getStartTime(), chunkMetaData.getEndTime()))
                || chunkMetaData.getStartTime() > chunkMetaData.getEndTime());

    // For chunkMetadata from old TsFile, do not set version
    for (ChunkMetadata metadata : chunkMetadataList) {
      if (!metadata.isFromOldTsFile()) {
        metadata.setVersion(resource.getVersion());
      }
    }
    return chunkMetadataList;
  }

  /**
   * For query v0.9/v1 tsfile only When generate temporary timeseriesMetadata set DiskChunkLoader to
   * each chunkMetadata in the List
   *
   * @param chunkMetadataList
   * @throws IOException
   */
  @Override
  public void setDiskChunkLoader(List<ChunkMetadata> chunkMetadataList) {
    setDiskChunkLoader(chunkMetadataList, resource, seriesPath, context);
  }

  public static void setDiskChunkLoader(
      List<ChunkMetadata> chunkMetadataList,
      TsFileResource resource,
      PartialPath seriesPath,
      QueryContext context) {
    List<Modification> pathModifications =
        context.getPathModifications(resource.getModFile(), seriesPath);

    if (!pathModifications.isEmpty()) {
      QueryUtils.modifyChunkMetaData(chunkMetadataList, pathModifications);
    }

    for (ChunkMetadata data : chunkMetadataList) {
      data.setChunkLoader(new DiskChunkLoader(resource));
    }
  }
}
