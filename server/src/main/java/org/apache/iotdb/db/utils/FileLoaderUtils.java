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

import org.apache.iotdb.db.engine.cache.ChunkMetadataCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.chunk.DiskChunkLoader;
import org.apache.iotdb.db.query.reader.chunk.MemChunkLoader;
import org.apache.iotdb.db.query.reader.chunk.MemChunkReader;
import org.apache.iotdb.db.query.reader.chunk.metadata.DiskChunkMetadataLoader;
import org.apache.iotdb.db.query.reader.chunk.metadata.MemChunkMetadataLoader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FileLoaderUtils {

  private FileLoaderUtils() {

  }

  public static void checkTsFileResource(TsFileResource tsFileResource) throws IOException {
    if (!tsFileResource.fileExists()) {
      // .resource file does not exist, read file metadata and recover tsfile resource
      try (TsFileSequenceReader reader = new TsFileSequenceReader(
          tsFileResource.getFile().getAbsolutePath())) {
        TsFileMetadata metaData = reader.readFileMetadata();
        updateTsFileResource(metaData, reader, tsFileResource);
      }
      // write .resource file
      tsFileResource.serialize();
    } else {
      tsFileResource.deserialize();
    }
    tsFileResource.setClosed(true);
  }

  public static void updateTsFileResource(TsFileMetadata metaData, TsFileSequenceReader reader,
      TsFileResource tsFileResource) throws IOException {
    for (String device : metaData.getDeviceMetadataIndex().keySet()) {
      Map<String, TimeseriesMetadata> chunkMetadataListInOneDevice = reader
          .readDeviceMetadata(device);
      for (TimeseriesMetadata timeseriesMetaData : chunkMetadataListInOneDevice.values()) {
        tsFileResource.updateStartTime(device, timeseriesMetaData.getStatistics().getStartTime());
        tsFileResource.updateEndTime(device, timeseriesMetaData.getStatistics().getEndTime());
      }
    }
  }


  /**
   *
   * @param resource TsFile
   * @param seriesPath Timeseries path
   * @param allSensors measurements queried at the same time of this device
   * @param filter any filter, only used to check time range
   */
  public static TimeseriesMetadata loadTimeSeriesMetadata(TsFileResource resource, Path seriesPath,
      QueryContext context, Filter filter, Set<String> allSensors) throws IOException {
    TimeseriesMetadata timeSeriesMetadata;
    if (resource.isClosed()) {
      timeSeriesMetadata = TimeSeriesMetadataCache.getInstance()
          .get(new TimeSeriesMetadataCache.TimeSeriesMetadataCacheKey(resource.getPath(),
              seriesPath.getDevice(), seriesPath.getMeasurement()), allSensors);
      if (timeSeriesMetadata != null) {
        timeSeriesMetadata.setChunkMetadataLoader(
            new DiskChunkMetadataLoader(resource, seriesPath, context, filter));
      }
    } else {
      timeSeriesMetadata = resource.getTimeSeriesMetadata();
      if (timeSeriesMetadata != null) {
        timeSeriesMetadata.setChunkMetadataLoader(
            new MemChunkMetadataLoader(resource, seriesPath, context, filter));
      }
    }

    if (timeSeriesMetadata != null) {
      List<Modification> pathModifications =
          context.getPathModifications(resource.getModFile(), seriesPath.getFullPath());
      timeSeriesMetadata.getStatistics().setCanUseStatistics(pathModifications.isEmpty());
      if (timeSeriesMetadata.getStatistics().getStartTime() > timeSeriesMetadata.getStatistics()
          .getEndTime()) {
        return null;
      }
      if (filter != null && !filter
          .satisfyStartEndTime(timeSeriesMetadata.getStatistics().getStartTime(),
              timeSeriesMetadata.getStatistics().getEndTime())) {
        return null;
      }
    }
    return timeSeriesMetadata;
  }

  /**
   * load all chunk metadata of one time series in one file.
   * @param timeSeriesMetadata the corresponding TimeSeriesMetadata in that file.
   */
  public static List<ChunkMetadata> loadChunkMetadataList(TimeseriesMetadata timeSeriesMetadata)
      throws IOException {
    return timeSeriesMetadata.loadChunkMetadataList();
  }


  /**
   * load all page readers in one chunk that satisfying the timeFilter
   * @param chunkMetaData the corresponding chunk metadata
   * @param timeFilter it should be a TimeFilter instead of a ValueFilter
   */
  public static List<IPageReader> loadPageReaderList(ChunkMetadata chunkMetaData, Filter timeFilter)
      throws IOException {
    if (chunkMetaData == null) {
      throw new IOException("Can't init null chunkMeta");
    }
    IChunkReader chunkReader;
    IChunkLoader chunkLoader = chunkMetaData.getChunkLoader();
    if (chunkLoader instanceof MemChunkLoader) {
      MemChunkLoader memChunkLoader = (MemChunkLoader) chunkLoader;
      chunkReader = new MemChunkReader(memChunkLoader.getChunk(), timeFilter);
    } else {
      Chunk chunk = chunkLoader.loadChunk(chunkMetaData);
      chunkReader = new ChunkReader(chunk, timeFilter);
      chunkReader.hasNextSatisfiedPage();
    }
    return chunkReader.loadPageReaderList();
  }


  /**
   * load all ChunkMetadatas belong to the seriesPath
   */
  public static List<ChunkMetadata> loadChunkMetadataFromTsFileResource(
      TsFileResource resource, Path seriesPath, QueryContext context) throws IOException {
    List<ChunkMetadata> chunkMetadataList;
    if (resource == null) {
      return new ArrayList<>();
    }
    if (resource.isClosed()) {
      chunkMetadataList = ChunkMetadataCache.getInstance().get(resource.getPath(), seriesPath);
    } else {
      chunkMetadataList = resource.getChunkMetadataList();
    }
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
    List<ReadOnlyMemChunk> memChunks = resource.getReadOnlyMemChunk();
    if (memChunks != null) {
      for (ReadOnlyMemChunk readOnlyMemChunk : memChunks) {
        if (!memChunks.isEmpty()) {
          chunkMetadataList.add(readOnlyMemChunk.getChunkMetaData());
        }
      }
    }

    /*
     * remove empty or invalid chunk metadata
     */
    chunkMetadataList.removeIf(chunkMetaData -> (
        chunkMetaData.getStartTime() > chunkMetaData.getEndTime()));
    return chunkMetadataList;
  }

  public static List<ChunkMetadata> getChunkMetadataList(Path path, String filePath) throws IOException {
    TsFileSequenceReader tsFileReader = FileReaderManager.getInstance().get(filePath, true);
    return tsFileReader.getChunkMetadataList(path);
  }

  public static TsFileMetadata getTsFileMetadata(String filePath) throws IOException {
    TsFileSequenceReader reader = FileReaderManager.getInstance().get(filePath, true);
    return reader.readFileMetadata();
  }
}
