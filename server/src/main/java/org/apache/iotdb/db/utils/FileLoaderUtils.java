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

import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.chunk.MemChunkLoader;
import org.apache.iotdb.db.query.reader.chunk.MemChunkReader;
import org.apache.iotdb.db.query.reader.chunk.metadata.DiskChunkMetadataLoader;
import org.apache.iotdb.db.query.reader.chunk.metadata.MemChunkMetadataLoader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.AlignedTimeSeriesMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.AlignedChunkReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

public class FileLoaderUtils {

  private FileLoaderUtils() {}

  public static void checkTsFileResource(TsFileResource tsFileResource) throws IOException {
    if (!tsFileResource.resourceFileExists()) {
      // .resource file does not exist, read file metadata and recover tsfile resource
      try (TsFileSequenceReader reader =
          new TsFileSequenceReader(tsFileResource.getTsFile().getAbsolutePath())) {
        updateTsFileResource(reader, tsFileResource);
      }
      // write .resource file
      tsFileResource.serialize();
    } else {
      tsFileResource.deserialize();
    }
    tsFileResource.setClosed(true);
  }

  public static void updateTsFileResource(
      TsFileSequenceReader reader, TsFileResource tsFileResource) throws IOException {
    for (Entry<String, List<TimeseriesMetadata>> entry :
        reader.getAllTimeseriesMetadata().entrySet()) {
      for (TimeseriesMetadata timeseriesMetaData : entry.getValue()) {
        tsFileResource.updateStartTime(
            entry.getKey(), timeseriesMetaData.getStatistics().getStartTime());
        tsFileResource.updateEndTime(
            entry.getKey(), timeseriesMetaData.getStatistics().getEndTime());
      }
    }
    tsFileResource.updatePlanIndexes(reader.getMinPlanIndex());
    tsFileResource.updatePlanIndexes(reader.getMaxPlanIndex());
  }

  /**
   * @param resource TsFile
   * @param seriesPath Timeseries path
   * @param allSensors measurements queried at the same time of this device
   * @param filter any filter, only used to check time range
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static ITimeSeriesMetadata loadTimeSeriesMetadata(
      TsFileResource resource,
      PartialPath seriesPath,
      QueryContext context,
      Filter filter,
      Set<String> allSensors)
      throws IOException {

    // common path
    ITimeSeriesMetadata timeSeriesMetadata;
    // If the tsfile is closed, we need to load from tsfile
    if (resource.isClosed()) {
      if (!resource.getTsFile().exists()) {
        return null;
      }
      timeSeriesMetadata =
          TimeSeriesMetadataCache.getInstance()
              .get(
                  new TimeSeriesMetadataCache.TimeSeriesMetadataCacheKey(
                      resource.getTsFilePath(),
                      seriesPath.getDevice(),
                      seriesPath.getMeasurement()),
                  allSensors,
                  context.isDebug());
      if (timeSeriesMetadata != null) {
        timeSeriesMetadata.setChunkMetadataLoader(
            new DiskChunkMetadataLoader(resource, seriesPath, context, filter));
      }
    } else { // if the tsfile is unclosed, we just get it directly from TsFileResource
      timeSeriesMetadata = resource.getTimeSeriesMetadata();
      if (timeSeriesMetadata != null) {
        timeSeriesMetadata.setChunkMetadataLoader(
            new MemChunkMetadataLoader(resource, seriesPath, context, filter));
      }
    }

    if (timeSeriesMetadata != null) {
      List<Modification> pathModifications =
          context.getPathModifications(resource.getModFile(), seriesPath);
      timeSeriesMetadata.setModified(!pathModifications.isEmpty());
      if (timeSeriesMetadata.getStatistics().getStartTime()
          > timeSeriesMetadata.getStatistics().getEndTime()) {
        return null;
      }
      if (filter != null
          && !filter.satisfyStartEndTime(
              timeSeriesMetadata.getStatistics().getStartTime(),
              timeSeriesMetadata.getStatistics().getEndTime())) {
        return null;
      }
    }
    return timeSeriesMetadata;
  }

  /**
   * Load VectorTimeSeriesMetadata for Vector
   *
   * @param resource corresponding TsFileResource
   * @param vectorPath instance of VectorPartialPath, vector's full path, e.g. (root.sg1.d1.vector,
   *     [root.sg1.d1.vector.s1, root.sg1.d1.vector.s2])
   * @param allSensors all sensors belonging to this device that appear in query
   */
  public static AlignedTimeSeriesMetadata loadTimeSeriesMetadata(
      TsFileResource resource,
      AlignedPath vectorPath,
      QueryContext context,
      Filter filter,
      Set<String> allSensors)
      throws IOException {
    AlignedTimeSeriesMetadata alignedTimeSeriesMetadata = null;
    // If the tsfile is closed, we need to load from tsfile
    if (resource.isClosed()) {
      if (!resource.getTsFile().exists()) {
        return null;
      }
      // load all the TimeseriesMetadata of vector, the first one is for time column and the
      // remaining is for sub sensors
      // the order of timeSeriesMetadata list is same as subSensorList's order
      List<TimeseriesMetadata> timeSeriesMetadata =
          TimeSeriesMetadataCache.getInstance()
              .get(
                  new TimeSeriesMetadataCache.TimeSeriesMetadataCacheKey(
                      resource.getTsFilePath(),
                      vectorPath.getDevice(),
                      vectorPath.getMeasurement()),
                  new ArrayList<>(vectorPath.getMeasurementList()),
                  allSensors,
                  context.isDebug());

      // assemble VectorTimeSeriesMetadata
      if (timeSeriesMetadata != null && !timeSeriesMetadata.isEmpty()) {
        timeSeriesMetadata
            .get(0)
            .setChunkMetadataLoader(
                new DiskChunkMetadataLoader(resource, vectorPath, context, filter));
        for (int i = 1; i < timeSeriesMetadata.size(); i++) {
          PartialPath subPath = vectorPath.getPathWithMeasurement(i - 1);
          timeSeriesMetadata
              .get(i)
              .setChunkMetadataLoader(
                  new DiskChunkMetadataLoader(resource, subPath, context, filter));
        }
        alignedTimeSeriesMetadata =
            new AlignedTimeSeriesMetadata(
                timeSeriesMetadata.get(0),
                timeSeriesMetadata.subList(1, timeSeriesMetadata.size()));
      }
    } else { // if the tsfile is unclosed, we just get it directly from TsFileResource
      alignedTimeSeriesMetadata = (AlignedTimeSeriesMetadata) resource.getTimeSeriesMetadata();
      if (alignedTimeSeriesMetadata != null) {
        alignedTimeSeriesMetadata.setChunkMetadataLoader(
            new MemChunkMetadataLoader(resource, vectorPath, context, filter));
      }
    }

    if (alignedTimeSeriesMetadata != null) {
      List<Modification> pathModifications =
          context.getPathModifications(resource.getModFile(), vectorPath);
      alignedTimeSeriesMetadata.getTimeseriesMetadata().setModified(!pathModifications.isEmpty());
      if (alignedTimeSeriesMetadata.getTimeseriesMetadata().getStatistics().getStartTime()
          > alignedTimeSeriesMetadata.getTimeseriesMetadata().getStatistics().getEndTime()) {
        return null;
      }
      if (filter != null
          && !filter.satisfyStartEndTime(
              alignedTimeSeriesMetadata.getTimeseriesMetadata().getStatistics().getStartTime(),
              alignedTimeSeriesMetadata.getTimeseriesMetadata().getStatistics().getEndTime())) {
        return null;
      }
      List<TimeseriesMetadata> valueTimeSeriesMetadataList =
          alignedTimeSeriesMetadata.getValueTimeseriesMetadataList();
      for (int i = 0; i < valueTimeSeriesMetadataList.size(); i++) {
        pathModifications =
            context.getPathModifications(
                resource.getModFile(), vectorPath.getPathWithMeasurement(i));
        valueTimeSeriesMetadataList.get(i).setModified(!pathModifications.isEmpty());
      }
    }
    return alignedTimeSeriesMetadata;
  }

  /**
   * load all chunk metadata of one time series in one file.
   *
   * @param timeSeriesMetadata the corresponding TimeSeriesMetadata in that file.
   */
  public static List<IChunkMetadata> loadChunkMetadataList(ITimeSeriesMetadata timeSeriesMetadata)
      throws IOException {
    return timeSeriesMetadata.loadChunkMetadataList();
  }

  /**
   * load all page readers in one chunk that satisfying the timeFilter
   *
   * @param chunkMetaData the corresponding chunk metadata
   * @param timeFilter it should be a TimeFilter instead of a ValueFilter
   */
  public static List<IPageReader> loadPageReaderList(
      IChunkMetadata chunkMetaData, Filter timeFilter) throws IOException {
    if (chunkMetaData == null) {
      throw new IOException("Can't init null chunkMeta");
    }
    IChunkReader chunkReader;
    IChunkLoader chunkLoader = chunkMetaData.getChunkLoader();
    if (chunkLoader instanceof MemChunkLoader) {
      MemChunkLoader memChunkLoader = (MemChunkLoader) chunkLoader;
      chunkReader = new MemChunkReader(memChunkLoader.getChunk(), timeFilter);
    } else {
      if (chunkMetaData instanceof ChunkMetadata) {
        Chunk chunk = chunkLoader.loadChunk((ChunkMetadata) chunkMetaData);
        chunk.setFromOldFile(chunkMetaData.isFromOldTsFile());
        chunkReader = new ChunkReader(chunk, timeFilter);
      } else {
        AlignedChunkMetadata alignedChunkMetadata = (AlignedChunkMetadata) chunkMetaData;
        Chunk timeChunk = alignedChunkMetadata.getTimeChunk();
        List<Chunk> valueChunkList = alignedChunkMetadata.getValueChunkList();
        chunkReader = new AlignedChunkReader(timeChunk, valueChunkList, timeFilter);
      }
    }
    return chunkReader.loadPageReaderList();
  }

  public static List<IChunkMetadata> getChunkMetadataList(Path path, String filePath)
      throws IOException {
    TsFileSequenceReader tsFileReader = FileReaderManager.getInstance().get(filePath, true);
    return new ArrayList<>(tsFileReader.getChunkMetadataList(path));
  }
}
