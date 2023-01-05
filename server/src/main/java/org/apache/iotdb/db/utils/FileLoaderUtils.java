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

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache.TimeSeriesMetadataCacheKey;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.db.mpp.metric.QueryMetricsManager;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.chunk.metadata.DiskAlignedChunkMetadataLoader;
import org.apache.iotdb.db.query.reader.chunk.metadata.DiskChunkMetadataLoader;
import org.apache.iotdb.db.query.reader.chunk.metadata.MemAlignedChunkMetadataLoader;
import org.apache.iotdb.db.query.reader.chunk.metadata.MemChunkMetadataLoader;
import org.apache.iotdb.tsfile.file.metadata.AlignedTimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.apache.iotdb.db.mpp.metric.SeriesScanCostMetricSet.LOAD_TIMESERIES_METADATA_ALIGNED_DISK;
import static org.apache.iotdb.db.mpp.metric.SeriesScanCostMetricSet.LOAD_TIMESERIES_METADATA_ALIGNED_MEM;
import static org.apache.iotdb.db.mpp.metric.SeriesScanCostMetricSet.LOAD_TIMESERIES_METADATA_NONALIGNED_DISK;
import static org.apache.iotdb.db.mpp.metric.SeriesScanCostMetricSet.LOAD_TIMESERIES_METADATA_NONALIGNED_MEM;
import static org.apache.iotdb.db.mpp.metric.SeriesScanCostMetricSet.TIMESERIES_METADATA_MODIFICATION_ALIGNED;
import static org.apache.iotdb.db.mpp.metric.SeriesScanCostMetricSet.TIMESERIES_METADATA_MODIFICATION_NONALIGNED;

public class FileLoaderUtils {

  private static final QueryMetricsManager QUERY_METRICS = QueryMetricsManager.getInstance();

  private FileLoaderUtils() {}

  public static void updateTsFileResource(
      TsFileSequenceReader reader, TsFileResource tsFileResource) throws IOException {
    updateTsFileResource(reader.getAllTimeseriesMetadata(false), tsFileResource);
    tsFileResource.updatePlanIndexes(reader.getMinPlanIndex());
    tsFileResource.updatePlanIndexes(reader.getMaxPlanIndex());
  }

  public static void updateTsFileResource(
      Map<String, List<TimeseriesMetadata>> device2Metadata, TsFileResource tsFileResource) {
    for (Entry<String, List<TimeseriesMetadata>> entry : device2Metadata.entrySet()) {
      for (TimeseriesMetadata timeseriesMetaData : entry.getValue()) {
        tsFileResource.updateStartTime(
            entry.getKey(), timeseriesMetaData.getStatistics().getStartTime());
        tsFileResource.updateEndTime(
            entry.getKey(), timeseriesMetaData.getStatistics().getEndTime());
      }
    }
  }

  /**
   * Generate {@link TsFileResource} from a closed {@link TsFileIOWriter}. Notice that the writer
   * should have executed {@link TsFileIOWriter#endFile()}. And this method will not record plan
   * Index of this writer.
   *
   * @param writer a {@link TsFileIOWriter}
   * @return a updated {@link TsFileResource}
   */
  public static TsFileResource generateTsFileResource(TsFileIOWriter writer) {
    TsFileResource resource = new TsFileResource(writer.getFile());
    for (ChunkGroupMetadata chunkGroupMetadata : writer.getChunkGroupMetadataList()) {
      String device = chunkGroupMetadata.getDevice();
      for (ChunkMetadata chunkMetadata : chunkGroupMetadata.getChunkMetadataList()) {
        resource.updateStartTime(device, chunkMetadata.getStartTime());
        resource.updateEndTime(device, chunkMetadata.getEndTime());
      }
    }
    resource.setStatus(TsFileResourceStatus.CLOSED);
    return resource;
  }

  /**
   * @param resource TsFile
   * @param seriesPath Timeseries path
   * @param allSensors measurements queried at the same time of this device
   * @param filter any filter, only used to check time range
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static TimeseriesMetadata loadTimeSeriesMetadata(
      TsFileResource resource,
      PartialPath seriesPath,
      QueryContext context,
      Filter filter,
      Set<String> allSensors)
      throws IOException {
    long t1 = System.nanoTime();
    boolean loadFromMem = false;
    try {
      // common path
      TimeseriesMetadata timeSeriesMetadata;
      // If the tsfile is closed, we need to load from tsfile
      if (resource.isClosed()) {
        // when resource.getTimeIndexType() == 1, TsFileResource.timeIndexType is deviceTimeIndex
        // we should not ignore the non-exist of device in TsFileMetadata
        timeSeriesMetadata =
            TimeSeriesMetadataCache.getInstance()
                .get(
                    new TimeSeriesMetadataCache.TimeSeriesMetadataCacheKey(
                        resource.getTsFilePath(),
                        seriesPath.getDevice(),
                        seriesPath.getMeasurement()),
                    allSensors,
                    resource.getTimeIndexType() != 1,
                    context.isDebug());
        if (timeSeriesMetadata != null) {
          timeSeriesMetadata.setChunkMetadataLoader(
              new DiskChunkMetadataLoader(resource, seriesPath, context, filter));
        }
      } else { // if the tsfile is unclosed, we just get it directly from TsFileResource
        loadFromMem = true;

        timeSeriesMetadata = (TimeseriesMetadata) resource.getTimeSeriesMetadata(seriesPath);
        if (timeSeriesMetadata != null) {
          timeSeriesMetadata.setChunkMetadataLoader(
              new MemChunkMetadataLoader(resource, seriesPath, context, filter));
        }
      }

      if (timeSeriesMetadata != null) {
        long t2 = System.nanoTime();
        try {
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
        } finally {
          QUERY_METRICS.recordSeriesScanCost(
              TIMESERIES_METADATA_MODIFICATION_NONALIGNED, System.nanoTime() - t2);
        }
      }
      return timeSeriesMetadata;
    } finally {
      QUERY_METRICS.recordSeriesScanCost(
          loadFromMem
              ? LOAD_TIMESERIES_METADATA_NONALIGNED_MEM
              : LOAD_TIMESERIES_METADATA_NONALIGNED_DISK,
          System.nanoTime() - t1);
    }
  }

  /**
   * Load VectorTimeSeriesMetadata for Vector
   *
   * @param resource corresponding TsFileResource
   * @param vectorPath instance of VectorPartialPath, vector's full path, e.g. (root.sg1.d1.vector,
   *     [root.sg1.d1.vector.s1, root.sg1.d1.vector.s2])
   */
  public static AlignedTimeSeriesMetadata loadTimeSeriesMetadata(
      TsFileResource resource, AlignedPath vectorPath, QueryContext context, Filter filter)
      throws IOException {
    long t1 = System.nanoTime();
    boolean loadFromMem = false;
    try {
      AlignedTimeSeriesMetadata alignedTimeSeriesMetadata = null;
      // If the tsfile is closed, we need to load from tsfile
      if (resource.isClosed()) {
        // load all the TimeseriesMetadata of vector, the first one is for time column and the
        // remaining is for sub sensors
        // the order of timeSeriesMetadata list is same as subSensorList's order
        TimeSeriesMetadataCache cache = TimeSeriesMetadataCache.getInstance();
        List<String> valueMeasurementList = vectorPath.getMeasurementList();
        Set<String> allSensors = new HashSet<>(valueMeasurementList);
        allSensors.add("");
        boolean isDebug = context.isDebug();
        String filePath = resource.getTsFilePath();
        String deviceId = vectorPath.getDevice();

        // when resource.getTimeIndexType() == 1, TsFileResource.timeIndexType is deviceTimeIndex
        // we should not ignore the non-exist of device in TsFileMetadata
        TimeseriesMetadata timeColumn =
            cache.get(
                new TimeSeriesMetadataCacheKey(filePath, deviceId, ""),
                allSensors,
                resource.getTimeIndexType() != 1,
                isDebug);
        if (timeColumn != null) {
          List<TimeseriesMetadata> valueTimeSeriesMetadataList =
              new ArrayList<>(valueMeasurementList.size());
          // if all the queried aligned sensors does not exist, we will return null
          boolean exist = false;
          for (String valueMeasurement : valueMeasurementList) {
            TimeseriesMetadata valueColumn =
                cache.get(
                    new TimeSeriesMetadataCacheKey(filePath, deviceId, valueMeasurement),
                    allSensors,
                    resource.getTimeIndexType() != 1,
                    isDebug);
            exist = (exist || (valueColumn != null));
            valueTimeSeriesMetadataList.add(valueColumn);
          }
          if (exist) {
            alignedTimeSeriesMetadata =
                new AlignedTimeSeriesMetadata(timeColumn, valueTimeSeriesMetadataList);
            alignedTimeSeriesMetadata.setChunkMetadataLoader(
                new DiskAlignedChunkMetadataLoader(resource, vectorPath, context, filter));
          }
        }
      } else { // if the tsfile is unclosed, we just get it directly from TsFileResource
        loadFromMem = true;

        alignedTimeSeriesMetadata =
            (AlignedTimeSeriesMetadata) resource.getTimeSeriesMetadata(vectorPath);
        if (alignedTimeSeriesMetadata != null) {
          alignedTimeSeriesMetadata.setChunkMetadataLoader(
              new MemAlignedChunkMetadataLoader(resource, vectorPath, context, filter));
        }
      }

      if (alignedTimeSeriesMetadata != null) {
        long t2 = System.nanoTime();
        try {
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

          // set modifications to each aligned path
          List<TimeseriesMetadata> valueTimeSeriesMetadataList =
              alignedTimeSeriesMetadata.getValueTimeseriesMetadataList();
          boolean modified = false;
          for (int i = 0; i < valueTimeSeriesMetadataList.size(); i++) {
            if (valueTimeSeriesMetadataList.get(i) != null) {
              List<Modification> pathModifications =
                  context.getPathModifications(
                      resource.getModFile(), vectorPath.getPathWithMeasurement(i));
              valueTimeSeriesMetadataList.get(i).setModified(!pathModifications.isEmpty());
              modified = (modified || !pathModifications.isEmpty());
            }
          }
          alignedTimeSeriesMetadata.getTimeseriesMetadata().setModified(modified);
        } finally {
          QUERY_METRICS.recordSeriesScanCost(
              TIMESERIES_METADATA_MODIFICATION_ALIGNED, System.nanoTime() - t2);
        }
      }
      return alignedTimeSeriesMetadata;
    } finally {
      QUERY_METRICS.recordSeriesScanCost(
          loadFromMem
              ? LOAD_TIMESERIES_METADATA_ALIGNED_MEM
              : LOAD_TIMESERIES_METADATA_ALIGNED_DISK,
          System.nanoTime() - t1);
    }
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
    IChunkLoader chunkLoader = chunkMetaData.getChunkLoader();
    IChunkReader chunkReader = chunkLoader.getChunkReader(chunkMetaData, timeFilter);
    return chunkReader.loadPageReaderList();
  }
}
