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

import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache.TimeSeriesMetadataCacheKey;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.DiskAlignedChunkLoader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.DiskChunkLoader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.metadata.DiskAlignedChunkMetadataLoader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.metadata.DiskChunkMetadataLoader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.metadata.MemAlignedChunkMetadataLoader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.metadata.MemChunkMetadataLoader;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.db.utils.ModificationUtils;

import org.apache.tsfile.file.metadata.AbstractAlignedTimeSeriesMetadata;
import org.apache.tsfile.file.metadata.AlignedTimeSeriesMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.tsfile.file.metadata.TableDeviceMetadata;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.controller.IChunkLoader;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.IChunkReader;
import org.apache.tsfile.read.reader.IPageReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

public class FileLoaderUtils {

  private static final SeriesScanCostMetricSet SERIES_SCAN_COST_METRIC_SET =
      SeriesScanCostMetricSet.getInstance();

  private FileLoaderUtils() {
    // util class
  }

  /**
   * Load TimeSeriesMetadata for non-aligned time series.
   *
   * @param resource TsFile
   * @param seriesPath Timeseries path
   * @param allSensors measurements queried at the same time of this device
   * @param globalTimeFilter global time filter, only used to check time range
   * @throws IOException may be thrown while reading it from disk.
   * @param isSeq if it is a sequence file
   */
  public static TimeseriesMetadata loadTimeSeriesMetadata(
      TsFileResource resource,
      NonAlignedFullPath seriesPath,
      QueryContext context,
      Filter globalTimeFilter,
      Set<String> allSensors,
      boolean isSeq)
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
                        seriesPath.getDeviceId(),
                        seriesPath.getMeasurement()),
                    allSensors,
                    resource.getTimeIndexType() == ITimeIndex.FILE_TIME_INDEX_TYPE,
                    context.isDebug());
        if (timeSeriesMetadata != null) {
          long t2 = System.nanoTime();
          List<Modification> pathModifications =
              context.getPathModifications(
                  resource, seriesPath.getDeviceId(), seriesPath.getMeasurement());
          timeSeriesMetadata.setModified(!pathModifications.isEmpty());
          timeSeriesMetadata.setChunkMetadataLoader(
              new DiskChunkMetadataLoader(resource, context, globalTimeFilter, pathModifications));
          int modificationCount = pathModifications.size();
          if (modificationCount != 0) {
            long costTime = System.nanoTime() - t2;
            context
                .getQueryStatistics()
                .getNonAlignedTimeSeriesMetadataModificationCount()
                .getAndAdd(modificationCount);
            context
                .getQueryStatistics()
                .getNonAlignedTimeSeriesMetadataModificationTime()
                .getAndAdd(costTime);
          }
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
        if (timeSeriesMetadata.getStatistics().getStartTime()
            > timeSeriesMetadata.getStatistics().getEndTime()) {
          return null;
        }
        if (globalTimeFilter != null && globalTimeFilter.canSkip(timeSeriesMetadata)) {
          return null;
        }
      }
      return timeSeriesMetadata;
    } finally {
      long costTime = System.nanoTime() - t1;
      if (loadFromMem) {
        if (isSeq) {
          context.getQueryStatistics().getLoadTimeSeriesMetadataMemSeqCount().getAndAdd(1);
          context.getQueryStatistics().getLoadTimeSeriesMetadataMemSeqTime().getAndAdd(costTime);
        } else {
          context.getQueryStatistics().getLoadTimeSeriesMetadataMemUnSeqCount().getAndAdd(1);
          context.getQueryStatistics().getLoadTimeSeriesMetadataMemUnSeqTime().getAndAdd(costTime);
        }
      } else {
        if (isSeq) {
          context.getQueryStatistics().getLoadTimeSeriesMetadataDiskSeqCount().getAndAdd(1);
          context.getQueryStatistics().getLoadTimeSeriesMetadataDiskSeqTime().getAndAdd(costTime);
        } else {
          context.getQueryStatistics().getLoadTimeSeriesMetadataDiskUnSeqCount().getAndAdd(1);
          context.getQueryStatistics().getLoadTimeSeriesMetadataDiskUnSeqTime().getAndAdd(costTime);
        }
      }
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
  public static AbstractAlignedTimeSeriesMetadata loadAlignedTimeSeriesMetadata(
      TsFileResource resource,
      AlignedFullPath alignedPath,
      QueryContext context,
      Filter globalTimeFilter,
      boolean isSeq,
      boolean ignoreAllNullRows)
      throws IOException {
    final long t1 = System.nanoTime();
    boolean loadFromMem = false;
    try {
      AbstractAlignedTimeSeriesMetadata alignedTimeSeriesMetadata;
      // If the tsfile is closed, we need to load from tsfile
      if (resource.isClosed()) {
        alignedTimeSeriesMetadata =
            loadAlignedTimeSeriesMetadataFromDisk(
                resource, alignedPath, context, globalTimeFilter, ignoreAllNullRows);
      } else { // if the tsfile is unclosed, we just get it directly from TsFileResource
        loadFromMem = true;
        alignedTimeSeriesMetadata =
            (AbstractAlignedTimeSeriesMetadata) resource.getTimeSeriesMetadata(alignedPath);
        if (alignedTimeSeriesMetadata != null) {
          alignedTimeSeriesMetadata.setChunkMetadataLoader(
              new MemAlignedChunkMetadataLoader(
                  resource, alignedPath, context, globalTimeFilter, ignoreAllNullRows));
          // mem's modification already done in generating chunkmetadata
        }
      }

      if (alignedTimeSeriesMetadata != null) {
        if (alignedTimeSeriesMetadata.getTimeseriesMetadata().getStatistics().getStartTime()
            > alignedTimeSeriesMetadata.getTimeseriesMetadata().getStatistics().getEndTime()) {
          return null;
        }
        if (globalTimeFilter != null && globalTimeFilter.canSkip(alignedTimeSeriesMetadata)) {
          return null;
        }
      }
      return alignedTimeSeriesMetadata;
    } finally {
      long costTime = System.nanoTime() - t1;
      if (loadFromMem) {
        if (isSeq) {
          context.getQueryStatistics().getLoadTimeSeriesMetadataAlignedMemSeqCount().getAndAdd(1);
          context
              .getQueryStatistics()
              .getLoadTimeSeriesMetadataAlignedMemSeqTime()
              .getAndAdd(costTime);
        } else {
          context.getQueryStatistics().getLoadTimeSeriesMetadataAlignedMemUnSeqCount().getAndAdd(1);
          context
              .getQueryStatistics()
              .getLoadTimeSeriesMetadataAlignedMemUnSeqTime()
              .getAndAdd(costTime);
        }
      } else {
        if (isSeq) {
          context.getQueryStatistics().getLoadTimeSeriesMetadataAlignedDiskSeqCount().getAndAdd(1);
          context
              .getQueryStatistics()
              .getLoadTimeSeriesMetadataAlignedDiskSeqTime()
              .getAndAdd(costTime);
        } else {
          context
              .getQueryStatistics()
              .getLoadTimeSeriesMetadataAlignedDiskUnSeqCount()
              .getAndAdd(1);
          context
              .getQueryStatistics()
              .getLoadTimeSeriesMetadataAlignedDiskUnSeqTime()
              .getAndAdd(costTime);
        }
      }
    }
  }

  private static AbstractAlignedTimeSeriesMetadata loadAlignedTimeSeriesMetadataFromDisk(
      TsFileResource resource,
      AlignedFullPath alignedPath,
      QueryContext context,
      Filter globalTimeFilter,
      boolean ignoreAllNullRows)
      throws IOException {
    AbstractAlignedTimeSeriesMetadata alignedTimeSeriesMetadata = null;
    // load all the TimeseriesMetadata of vector, the first one is for time column and the
    // remaining is for sub sensors
    // the order of timeSeriesMetadata list is same as subSensorList's order
    TimeSeriesMetadataCache cache = TimeSeriesMetadataCache.getInstance();
    List<String> valueMeasurementList = alignedPath.getMeasurementList();
    Set<String> allSensors = new HashSet<>(valueMeasurementList);
    allSensors.add("");
    boolean isDebug = context.isDebug();
    String filePath = resource.getTsFilePath();
    IDeviceID deviceId = alignedPath.getDeviceId();

    // when resource.getTimeIndexType() == 1, TsFileResource.timeIndexType is deviceTimeIndex
    // we should not ignore the non-exist of device in TsFileMetadata
    TimeseriesMetadata timeColumn =
        cache.get(
            filePath,
            new TimeSeriesMetadataCacheKey(resource.getTsFileID(), deviceId, ""),
            allSensors,
            resource.getTimeIndexType() == ITimeIndex.FILE_TIME_INDEX_TYPE,
            isDebug);
    if (timeColumn != null) {
      // only need time column, like count_time aggregation
      if (valueMeasurementList.isEmpty()) {
        // whatever for table or tree model, if we only need time column, that means that we
        // shouldn't ignore all null rows
        alignedTimeSeriesMetadata =
            setModifications(
                resource,
                timeColumn,
                Collections.emptyList(),
                alignedPath,
                context,
                globalTimeFilter,
                false);
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
                  resource.getTimeIndexType() == ITimeIndex.FILE_TIME_INDEX_TYPE,
                  isDebug);
          exist = (exist || (valueColumn != null));
          valueTimeSeriesMetadataList.add(valueColumn);
        }
        if (!ignoreAllNullRows || exist) {
          alignedTimeSeriesMetadata =
              setModifications(
                  resource,
                  timeColumn,
                  valueTimeSeriesMetadataList,
                  alignedPath,
                  context,
                  globalTimeFilter,
                  ignoreAllNullRows);
        }
      }
    }
    return alignedTimeSeriesMetadata;
  }

  private static AbstractAlignedTimeSeriesMetadata setModifications(
      TsFileResource resource,
      TimeseriesMetadata timeColumnMetadata,
      List<TimeseriesMetadata> valueColumnMetadataList,
      AlignedFullPath alignedPath,
      QueryContext context,
      Filter globalTimeFilter,
      boolean ignoreAllNullRows) {
    long startTime = System.nanoTime();

    // deal with time column
    List<Modification> timeModifications =
        context.getPathModifications(
            resource, alignedPath.getDeviceId(), timeColumnMetadata.getMeasurementId());
    // all rows are deleted, just return null to skip device data in this file
    if (ModificationUtils.isAllDeletedByMods(
        timeModifications,
        timeColumnMetadata.getStatistics().getStartTime(),
        timeColumnMetadata.getStatistics().getEndTime())) {
      return null;
    }
    boolean modified = !timeModifications.isEmpty();
    timeColumnMetadata.setModified(modified);
    context
        .getQueryStatistics()
        .getAlignedTimeSeriesMetadataModificationCount()
        .getAndAdd(timeModifications.size());

    // deal with value columns
    boolean hasNonNullValueColumns = false;
    List<List<Modification>> valueColumnsModifications = new ArrayList<>();
    for (int i = 0, size = valueColumnMetadataList.size(); i < size; i++) {
      TimeseriesMetadata valueColumnMetadata = valueColumnMetadataList.get(i);
      if (valueColumnMetadata != null) {
        List<Modification> modifications =
            context.getPathModifications(
                resource, alignedPath.getDeviceId(), valueColumnMetadata.getMeasurementId());
        valueColumnMetadata.setModified(!modifications.isEmpty());
        valueColumnsModifications.add(modifications);
        modified = (modified || !modifications.isEmpty());
        context
            .getQueryStatistics()
            .getAlignedTimeSeriesMetadataModificationCount()
            .getAndAdd(modifications.size());

        // this value column is all deleted
        if (ModificationUtils.isAllDeletedByMods(
            modifications,
            valueColumnMetadata.getStatistics().getStartTime(),
            valueColumnMetadata.getStatistics().getEndTime())) {
          valueColumnMetadataList.set(i, null);
        } else {
          hasNonNullValueColumns = true;
        }
      } else {
        valueColumnsModifications.add(Collections.emptyList());
      }
    }

    if (ignoreAllNullRows && !hasNonNullValueColumns) {
      return null;
    }

    timeColumnMetadata.setModified(modified);

    AbstractAlignedTimeSeriesMetadata alignedTimeSeriesMetadata =
        ignoreAllNullRows
            ? new AlignedTimeSeriesMetadata(timeColumnMetadata, valueColumnMetadataList)
            : new TableDeviceMetadata(timeColumnMetadata, valueColumnMetadataList);

    alignedTimeSeriesMetadata.setChunkMetadataLoader(
        new DiskAlignedChunkMetadataLoader(
            resource,
            context,
            globalTimeFilter,
            timeModifications,
            valueColumnsModifications,
            ignoreAllNullRows));

    long costTime = System.nanoTime() - startTime;
    context.getQueryStatistics().getAlignedTimeSeriesMetadataModificationTime().getAndAdd(costTime);
    return alignedTimeSeriesMetadata;
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
    checkArgument(chunkMetaData != null, "Can't init null chunkMeta");

    IChunkLoader chunkLoader = chunkMetaData.getChunkLoader();
    IChunkReader chunkReader = chunkLoader.getChunkReader(chunkMetaData, globalTimeFilter);
    return chunkReader.loadPageReaderList();
  }

  /**
   * get the timestamp in file name of the chunk metadata.
   *
   * @param chunkMetaData the corresponding ChunkMetadata in that file.
   */
  public static long getTimestampInFileName(IChunkMetadata chunkMetaData) {
    IChunkLoader chunkLoader = chunkMetaData.getChunkLoader();
    if (chunkLoader instanceof DiskChunkLoader) {
      return ((DiskChunkLoader) chunkLoader).getTsFileID().getTimestamp();
    } else if (chunkLoader instanceof DiskAlignedChunkLoader) {
      return ((DiskAlignedChunkLoader) chunkLoader).getTsFileID().getTimestamp();
    } else {
      return Long.MAX_VALUE;
    }
  }
}
