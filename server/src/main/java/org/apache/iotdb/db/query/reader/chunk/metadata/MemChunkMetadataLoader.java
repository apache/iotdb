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

import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.iotdb.tsfile.read.controller.IChunkMetadataLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.util.List;

public class MemChunkMetadataLoader implements IChunkMetadataLoader {

  private TsFileResource resource;
  private PartialPath seriesPath;
  private QueryContext context;
  private Filter timeFilter;

  public MemChunkMetadataLoader(
      TsFileResource resource, PartialPath seriesPath, QueryContext context, Filter timeFilter) {
    this.resource = resource;
    this.seriesPath = seriesPath;
    this.context = context;
    this.timeFilter = timeFilter;
  }

  @Override
  public List<IChunkMetadata> loadChunkMetadataList(ITimeSeriesMetadata timeseriesMetadata) {
    List<IChunkMetadata> chunkMetadataList = resource.getChunkMetadataList();

    DiskChunkMetadataLoader.setDiskChunkLoader(chunkMetadataList, resource, seriesPath, context);

    List<ReadOnlyMemChunk> memChunks = resource.getReadOnlyMemChunk();
    if (memChunks != null) {
      for (ReadOnlyMemChunk readOnlyMemChunk : memChunks) {
        if (!memChunks.isEmpty()) {
          chunkMetadataList.add(readOnlyMemChunk.getChunkMetaData());
        }
      }
    }
    /*
     * remove not satisfied ChunkMetaData
     */
    chunkMetadataList.removeIf(
        chunkMetaData ->
            (timeFilter != null
                    && !timeFilter.satisfyStartEndTime(
                        chunkMetaData.getStartTime(), chunkMetaData.getEndTime()))
                || chunkMetaData.getStartTime() > chunkMetaData.getEndTime());

    for (IChunkMetadata metadata : chunkMetadataList) {
      metadata.setVersion(resource.getVersion());
    }
    return chunkMetadataList;
  }

  @Override
  public boolean isMemChunkMetadataLoader() {
    return true;
  }
}
