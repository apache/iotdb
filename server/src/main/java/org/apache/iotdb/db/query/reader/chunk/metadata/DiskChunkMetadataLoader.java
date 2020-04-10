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
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.chunk.DiskChunkLoader;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.IChunkMetadataLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import java.io.IOException;
import java.util.List;

public class DiskChunkMetadataLoader implements IChunkMetadataLoader {

  private TsFileResource resource;
  private Path seriesPath;
  private QueryContext context;
  private Filter timeFilter;

  public DiskChunkMetadataLoader(TsFileResource resource, Path seriesPath, QueryContext context, Filter timeFilter) {
    this.resource = resource;
    this.seriesPath = seriesPath;
    this.context = context;
    this.timeFilter = timeFilter;
  }

  @Override
  public List<ChunkMetadata> loadChunkMetadataList() throws IOException {
    List<ChunkMetadata> chunkMetadataList = ChunkMetadataCache
        .getInstance().get(resource.getPath(), seriesPath);

    setDiskChunkLoader(chunkMetadataList, resource, seriesPath, context);

    /*
     * remove not satisfied ChunkMetaData
     */
    chunkMetadataList.removeIf(chunkMetaData -> (timeFilter != null && !timeFilter
            .satisfyStartEndTime(chunkMetaData.getStartTime(), chunkMetaData.getEndTime()))
            || chunkMetaData.getStartTime() > chunkMetaData.getEndTime());
    return chunkMetadataList;
  }

  public static void setDiskChunkLoader(List<ChunkMetadata> chunkMetadataList, TsFileResource resource, Path seriesPath, QueryContext context) throws IOException {
    List<Modification> pathModifications =
            context.getPathModifications(resource.getModFile(), seriesPath.getFullPath());

    if (!pathModifications.isEmpty()) {
      QueryUtils.modifyChunkMetaData(chunkMetadataList, pathModifications);
    }

    TsFileSequenceReader tsFileSequenceReader =
            FileReaderManager.getInstance().get(resource.getPath(), resource.isClosed());
    for (ChunkMetadata data : chunkMetadataList) {
      data.setChunkLoader(new DiskChunkLoader(tsFileSequenceReader));
    }
  }

}
