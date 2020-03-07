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
package org.apache.iotdb.db.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.engine.cache.DeviceMetaDataCache;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.chunk.DiskChunkLoader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public class FileLoaderUtils {

  private FileLoaderUtils() {
  }

  public static void checkTsFileResource(TsFileResource tsFileResource) throws IOException {
    if (!tsFileResource.fileExists()) {
      // .resource file does not exist, read file metadata and recover tsfile resource
      try (TsFileSequenceReader reader = new TsFileSequenceReader(
          tsFileResource.getFile().getAbsolutePath())) {
        TsFileMetaData metaData = reader.readFileMetadata();
        updateTsFileResource(metaData, reader, tsFileResource);
      }
      // write .resource file
      tsFileResource.serialize();
    } else {
      tsFileResource.deserialize();
    }
  }

  public static void updateTsFileResource(TsFileMetaData metaData, TsFileSequenceReader reader,
      TsFileResource tsFileResource) throws IOException {
    for (TsDeviceMetadataIndex index : metaData.getDeviceMap().values()) {
      TsDeviceMetadata deviceMetadata = reader.readTsDeviceMetaData(index);
      List<ChunkGroupMetaData> chunkGroupMetaDataList = deviceMetadata
          .getChunkGroupMetaDataList();
      for (ChunkGroupMetaData chunkGroupMetaData : chunkGroupMetaDataList) {
        for (ChunkMetaData chunkMetaData : chunkGroupMetaData.getChunkMetaDataList()) {
          tsFileResource.updateStartTime(chunkGroupMetaData.getDeviceID(),
              chunkMetaData.getStartTime());
          tsFileResource
              .updateEndTime(chunkGroupMetaData.getDeviceID(), chunkMetaData.getEndTime());
        }
      }
    }
  }

  /**
   * load all ChunkMetadatas belong to the seriesPath and satisfy filter
   */
  public static List<ChunkMetaData> loadChunkMetadataFromTsFileResource(
      TsFileResource resource, Path seriesPath, QueryContext context, Filter timeFilter) throws IOException {
    List<ChunkMetaData> chunkMetaDataList = loadChunkMetadataFromTsFileResource(resource, seriesPath, context);

    /*
     * remove not satisfied ChunkMetaData
     */
    chunkMetaDataList.removeIf(chunkMetaData -> (timeFilter != null && !timeFilter
        .satisfyStartEndTime(chunkMetaData.getStartTime(), chunkMetaData.getEndTime()))
        || chunkMetaData.getStartTime() > chunkMetaData.getEndTime());
    return chunkMetaDataList;
  }

  /**
   * load all ChunkMetadatas belong to the seriesPath
   */
  public static List<ChunkMetaData> loadChunkMetadataFromTsFileResource(
      TsFileResource resource, Path seriesPath, QueryContext context) throws IOException {
    List<ChunkMetaData> chunkMetaDataList;
    if (resource == null) {
      return new ArrayList<>();
    }
    if (resource.isClosed()) {
      chunkMetaDataList = DeviceMetaDataCache.getInstance().get(resource, seriesPath);
    } else {
      chunkMetaDataList = resource.getChunkMetaDataList();
    }
    List<Modification> pathModifications =
        context.getPathModifications(resource.getModFile(), seriesPath.getFullPath());

    if (!pathModifications.isEmpty()) {
      QueryUtils.modifyChunkMetaData(chunkMetaDataList, pathModifications);
    }

    for (ChunkMetaData data : chunkMetaDataList) {
      TsFileSequenceReader tsFileSequenceReader =
          FileReaderManager.getInstance().get(resource, resource.isClosed());
      data.setChunkLoader(new DiskChunkLoader(tsFileSequenceReader));
    }
    List<ReadOnlyMemChunk> memChunks = resource.getReadOnlyMemChunk();
    if (memChunks != null) {
      for (ReadOnlyMemChunk readOnlyMemChunk : memChunks) {
        if (!memChunks.isEmpty()) {
          chunkMetaDataList.add(readOnlyMemChunk.getChunkMetaData());
        }
      }
    }

    /*
     * remove empty or invalid chunk metadata
     */
    chunkMetaDataList.removeIf(chunkMetaData -> (
        chunkMetaData.getStartTime() > chunkMetaData.getEndTime()));
    return chunkMetaDataList;
  }
}
