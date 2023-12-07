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

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache.TimeSeriesMetadataCacheKey;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.metadata.DiskAlignedChunkMetadataLoader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.metadata.DiskChunkMetadataLoader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.metadata.MemAlignedChunkMetadataLoader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.metadata.MemChunkMetadataLoader;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.AlignedTimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.LOAD_TIMESERIES_METADATA_ALIGNED_DISK;
import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.LOAD_TIMESERIES_METADATA_ALIGNED_MEM;
import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.LOAD_TIMESERIES_METADATA_NONALIGNED_DISK;
import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.LOAD_TIMESERIES_METADATA_NONALIGNED_MEM;
import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.TIMESERIES_METADATA_MODIFICATION_ALIGNED;
import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.TIMESERIES_METADATA_MODIFICATION_NONALIGNED;

public class FileLoaderUtils {

  private static final SeriesScanCostMetricSet SERIES_SCAN_COST_METRIC_SET =
      SeriesScanCostMetricSet.getInstance();

  private FileLoaderUtils() {
    // empty constructor
  }

  /**
   * Load TimeSeriesMetadata for non-aligned time series
   *
   * @param resource TsFile
   * @param seriesPath Timeseries path
   * @param allSensors measurements queried at the same time of this device
   * @param globalTimeFilter global time filter, only used to check time range
   * @throws IOException IOException may be thrown while reading it from disk.
   */
  public static TimeseriesMetadata loadTimeSeriesMetadata(
      TsFileResource resource,
      PartialPath seriesPath,
      QueryContext context,
      Filter globalTimeFilter,
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
                    resource.getTsFilePath(),
                    new TimeSeriesMetadataCache.TimeSeriesMetadataCacheKey(
                        resource.getTsFileID(),
                        seriesPath.getDevice(),
                        seriesPath.getMeasurement()),
                    allSensors,
                    resource.getTimeIndexType() != 1,
                    context.isDebug());
        if (timeSeriesMetadata != null) {
          List<Modification> pathModifications = context.getPathModifications(resource, seriesPath);
          timeSeriesMetadata.setModified(!pathModifications.isEmpty());
          timeSeriesMetadata.setChunkMetadataLoader(
              new DiskChunkMetadataLoader(resource, context, globalTimeFilter, pathModifications));
        }
      } else { // if the tsfile is unclosed, we just get it directly from TsFileResource
        loadFromMem = true;

        timeSeriesMetadata = (TimeseriesMetadata) resource.getTimeSeriesMetadata(seriesPath);
        if (timeSeriesMetadata != null) {
          timeSeriesMetadata.setChunkMetadataLoader(
              new MemChunkMetadataLoader(resource, seriesPath, context, globalTimeFilter));
        }
      }

      if (timeSeriesMetadata != null) {
        long t2 = System.nanoTime();
        try {
          if (timeSeriesMetadata.getStatistics().getStartTime()
              > timeSeriesMetadata.getStatistics().getEndTime()) {
            return null;
          }
          if (globalTimeFilter != null && globalTimeFilter.canSkip(timeSeriesMetadata)) {
            return null;
          }
        } finally {
          SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(
              TIMESERIES_METADATA_MODIFICATION_NONALIGNED, System.nanoTime() - t2);
        }
      }
      return timeSeriesMetadata;
    } finally {
      SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(
          loadFromMem
              ? LOAD_TIMESERIES_METADATA_NONALIGNED_MEM
              : LOAD_TIMESERIES_METADATA_NONALIGNED_DISK,
          System.nanoTime() - t1);
    }
  }

  /**
   * Load AlignedTimeSeriesMetadata for aligned time series.
   *
   * @param resource corresponding TsFileResource
   * @param alignedPath instance of VectorPartialPath, vector's full path, e.g. (root.sg1.d1.vector,
   *     [root.sg1.d1.vector.s1, root.sg1.d1.vector.s2])
   * @throws IOException IOException may be thrown while reading it from disk.
   */
  public static AlignedTimeSeriesMetadata loadTimeSeriesMetadata(
      TsFileResource resource,
      AlignedPath alignedPath,
      QueryContext context,
      Filter globalTimeFilter)
      throws IOException {
    final long t1 = System.nanoTime();
    boolean loadFromMem = false;
    try {
      AlignedTimeSeriesMetadata alignedTimeSeriesMetadata;
      // If the tsfile is closed, we need to load from tsfile
      if (resource.isClosed()) {
        alignedTimeSeriesMetadata = loadFromDisk(resource, alignedPath, context, globalTimeFilter);
      } else { // if the tsfile is unclosed, we just get it directly from TsFileResource
        loadFromMem = true;
        alignedTimeSeriesMetadata =
            (AlignedTimeSeriesMetadata) resource.getTimeSeriesMetadata(alignedPath);
        if (alignedTimeSeriesMetadata != null) {
          alignedTimeSeriesMetadata.setChunkMetadataLoader(
              new MemAlignedChunkMetadataLoader(resource, alignedPath, context, globalTimeFilter));
          // mem's modification already done in generating chunkmetadata
        }
      }

      if (alignedTimeSeriesMetadata != null) {
        final long t2 = System.nanoTime();
        try {
          if (alignedTimeSeriesMetadata.getTimeseriesMetadata().getStatistics().getStartTime()
              > alignedTimeSeriesMetadata.getTimeseriesMetadata().getStatistics().getEndTime()) {
            return null;
          }
          if (globalTimeFilter != null && globalTimeFilter.canSkip(alignedTimeSeriesMetadata)) {
            return null;
          }
        } finally {
          SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(
              TIMESERIES_METADATA_MODIFICATION_ALIGNED, System.nanoTime() - t2);
        }
      }
      return alignedTimeSeriesMetadata;
    } finally {
      SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(
          loadFromMem
              ? LOAD_TIMESERIES_METADATA_ALIGNED_MEM
              : LOAD_TIMESERIES_METADATA_ALIGNED_DISK,
          System.nanoTime() - t1);
    }
  }

  private static AlignedTimeSeriesMetadata loadFromDisk(
      TsFileResource resource,
      AlignedPath alignedPath,
      QueryContext context,
      Filter globalTimeFilter)
      throws IOException {
    AlignedTimeSeriesMetadata alignedTimeSeriesMetadata = null;
    // load all the TimeseriesMetadata of vector, the first one is for time column and the
    // remaining is for sub sensors
    // the order of timeSeriesMetadata list is same as subSensorList's order
    TimeSeriesMetadataCache cache = TimeSeriesMetadataCache.getInstance();
    List<String> valueMeasurementList = alignedPath.getMeasurementList();
    Set<String> allSensors = new HashSet<>(valueMeasurementList);
    allSensors.add("");
    boolean isDebug = context.isDebug();
    String filePath = resource.getTsFilePath();
    String deviceId = alignedPath.getDevice();

    // when resource.getTimeIndexType() == 1, TsFileResource.timeIndexType is deviceTimeIndex
    // we should not ignore the non-exist of device in TsFileMetadata
    TimeseriesMetadata timeColumn =
        cache.get(
            filePath,
            new TimeSeriesMetadataCacheKey(resource.getTsFileID(), deviceId, ""),
            allSensors,
            resource.getTimeIndexType() != 1,
            isDebug);
    if (timeColumn != null) {
      // only need time column, like count_time aggregation
      if (valueMeasurementList.isEmpty()) {
        alignedTimeSeriesMetadata =
            new AlignedTimeSeriesMetadata(timeColumn, Collections.emptyList());
        alignedTimeSeriesMetadata.setChunkMetadataLoader(
            new DiskAlignedChunkMetadataLoader(
                resource, context, globalTimeFilter, Collections.emptyList()));
      } else {
        List<TimeseriesMetadata> valueTimeSeriesMetadataList =
            new ArrayList<>(valueMeasurementList.size());
        // if all the queried aligned sensors does not exist, we will return null
        boolean exist = false;
        for (String valueMeasurement : valueMeasurementList) {
          TimeseriesMetadata valueColumn =
              cache.get(
                  filePath,
                  new TimeSeriesMetadataCacheKey(
                      resource.getTsFileID(), deviceId, valueMeasurement),
                  allSensors,
                  resource.getTimeIndexType() != 1,
                  isDebug);
          exist = (exist || (valueColumn != null));
          valueTimeSeriesMetadataList.add(valueColumn);
        }
        if (exist) {
          // set modifications to each aligned path
          alignedTimeSeriesMetadata =
              new AlignedTimeSeriesMetadata(timeColumn, valueTimeSeriesMetadataList);
          List<List<Modification>> pathModifications =
              setModifications(resource, alignedTimeSeriesMetadata, alignedPath, context);

          alignedTimeSeriesMetadata.setChunkMetadataLoader(
              new DiskAlignedChunkMetadataLoader(
                  resource, context, globalTimeFilter, pathModifications));
        }
      }
    }
    return alignedTimeSeriesMetadata;
  }

  private static List<List<Modification>> setModifications(
      TsFileResource resource,
      AlignedTimeSeriesMetadata alignedTimeSeriesMetadata,
      AlignedPath alignedPath,
      QueryContext context) {
    List<TimeseriesMetadata> valueTimeSeriesMetadataList =
        alignedTimeSeriesMetadata.getValueTimeseriesMetadataList();
    List<List<Modification>> res = new ArrayList<>();
    boolean modified = false;
    for (int i = 0; i < valueTimeSeriesMetadataList.size(); i++) {
      if (valueTimeSeriesMetadataList.get(i) != null) {
        List<Modification> pathModifications =
            context.getPathModifications(resource, alignedPath.getPathWithMeasurement(i));
        valueTimeSeriesMetadataList.get(i).setModified(!pathModifications.isEmpty());
        res.add(pathModifications);
        modified = (modified || !pathModifications.isEmpty());
      } else {
        res.add(Collections.emptyList());
      }
    }
    alignedTimeSeriesMetadata.getTimeseriesMetadata().setModified(modified);
    return res;
  }

  /**
   * load all chunk metadata of one time series in one file.
   *
   * @param timeSeriesMetadata the corresponding TimeSeriesMetadata in that file.
   */
  public static List<IChunkMetadata> loadChunkMetadataList(ITimeSeriesMetadata timeSeriesMetadata) {
    return timeSeriesMetadata.loadChunkMetadataList();
  }

  /**
   * load all page readers in one chunk that satisfying the globalTimeFilter.
   *
   * @param chunkMetaData the corresponding chunk metadata
   * @param globalTimeFilter it should be a TimeFilter instead of a ValueFilter
   * @throws IOException if chunkMetaData is null or errors happened while loading page readers,
   *     IOException will be thrown
   */
  public static List<IPageReader> loadPageReaderList(
      IChunkMetadata chunkMetaData, Filter globalTimeFilter) throws IOException {
    if (chunkMetaData == null) {
      throw new IOException("Can't init null chunkMeta");
    }
    IChunkLoader chunkLoader = chunkMetaData.getChunkLoader();
    IChunkReader chunkReader = chunkLoader.getChunkReader(chunkMetaData, globalTimeFilter);
    return chunkReader.loadPageReaderList();
  }
}
