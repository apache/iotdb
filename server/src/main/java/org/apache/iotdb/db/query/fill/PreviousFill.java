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
package org.apache.iotdb.db.query.fill;

import java.sql.Time;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;

import java.io.IOException;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

public class PreviousFill extends IFill {

  private Path seriesPath;
  private long beforeRange;
  private Set<String> allSensors;
  private Filter timeFilter;

  private QueryDataSource dataSource;

  private TimeseriesMetadata lastSeqTimeseriesMetadata;
  private List<TimeseriesMetadata> unseqTimeseriesMetadataList;

  public PreviousFill(TSDataType dataType, long queryTime, long beforeRange) {
    super(dataType, queryTime);
    this.beforeRange = beforeRange;
    this.unseqTimeseriesMetadataList = new ArrayList<>();
  }

  public PreviousFill(long beforeRange) {
    this.beforeRange = beforeRange;
  }

  @Override
  public IFill copy() {
    return new PreviousFill(dataType,  queryTime, beforeRange);
  }

  @Override
  Filter constructFilter() {
    Filter lowerBound = beforeRange == -1 ? TimeFilter.gtEq(Long.MIN_VALUE)
        : TimeFilter.gtEq(queryTime - beforeRange);
    // time in [queryTime - beforeRange, queryTime]
    return FilterFactory.and(lowerBound, TimeFilter.ltEq(queryTime));
  }

  public long getBeforeRange() {
    return beforeRange;
  }

  @Override
  public void configureFill(Path path, TSDataType dataType, long queryTime,
      Set<String> sensors, QueryContext context)
      throws StorageEngineException, QueryProcessException {
    this.seriesPath = path;
    this.dataType = dataType;
    this.queryTime = queryTime;
    this.allSensors = sensors;
    this.timeFilter = constructFilter();
    this.dataSource = QueryResourceManager.getInstance().getQueryDataSource(path, context, timeFilter);
    // update filter by TTL
    timeFilter = dataSource.updateFilterUsingTTL(timeFilter);
  }

  @Override
  public TimeValuePair getFillResult(QueryContext context) throws IOException {
    UnpackOverlappedFilesToTimeseriesMetadata(context);
    return getTimeseriesLastPoint();
  }

  private TimeValuePair getTimeseriesLastPoint() throws IOException {
    TimeValuePair lastPoint = new TimeValuePair(Long.MIN_VALUE, null);
    long lastVersion = 0;
    if (lastSeqTimeseriesMetadata != null) {
      if (lastSeqTimeseriesMetadata.getStatistics().getEndTime() <= queryTime) {
        lastPoint = constructLastPair(
            lastSeqTimeseriesMetadata.getStatistics().getEndTime(),
            lastSeqTimeseriesMetadata.getStatistics().getLastValue(), dataType);
      } else {
        List<ChunkMetadata> seqChunkMetadataList =
            lastSeqTimeseriesMetadata.loadChunkMetadataList();

        for (int i = seqChunkMetadataList.size() - 1; i >= 0; i--) {
          lastPoint = getChunkLastPoint(seqChunkMetadataList.get(i));
          // last point of this sequence chunk is valid, quit the loop
          if (lastPoint.getValue() != null) {
            lastVersion = seqChunkMetadataList.get(i).getVersion();
            break;
          }
        }
      }
    }

    while (!unseqTimeseriesMetadataList.isEmpty()) {
      TimeseriesMetadata timeseriesMetadata = unseqTimeseriesMetadataList.remove(0);
      List<ChunkMetadata> chunkMetadataList = timeseriesMetadata.loadChunkMetadataList();
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        TimeValuePair lastChunkPoint = getChunkLastPoint(chunkMetadata);
        if (shouldUpdate(lastPoint.getTimestamp(), lastVersion,
            lastChunkPoint.getTimestamp(), chunkMetadata.getVersion())) {
          lastPoint = lastChunkPoint;
          lastVersion = chunkMetadata.getVersion();
        }
      }
    }
    return lastPoint;
  }

  private TimeValuePair getChunkLastPoint(ChunkMetadata chunkMetaData) throws IOException {
    TimeValuePair lastPoint = new TimeValuePair(0, null);
    Statistics chunkStatistics = chunkMetaData.getStatistics();
    if (!timeFilter.satisfy(chunkStatistics)) {
      return lastPoint;
    }
    if (containedByTimeFilter(chunkStatistics)) {
      return constructLastPair(
          chunkStatistics.getEndTime(), chunkStatistics.getLastValue(), dataType);
    }
    List<IPageReader> pageReaders = FileLoaderUtils.loadPageReaderList(chunkMetaData, timeFilter);
    for (int i = pageReaders.size() - 1; i >= 0; i--) {
      IPageReader pageReader = pageReaders.get(i);
      Statistics pageStatistics = pageReader.getStatistics();
      if (!timeFilter.satisfy(pageStatistics)) {
        continue;
      }
      if (containedByTimeFilter(pageStatistics)) {
        lastPoint = constructLastPair(
            pageStatistics.getEndTime(), pageStatistics.getLastValue(), dataType);
      } else {
        BatchData batchData = pageReader.getAllSatisfiedPageData();
        lastPoint = batchData.getLastPairBeforeOrEqualTimestamp(queryTime);
      }
      break;
    }
    return lastPoint;
  }

  private boolean shouldUpdate(long time, long version, long newTime, long newVersion) {
    return time < newTime || (time == newTime && version < newVersion);
  }

  private PriorityQueue<TsFileResource> sortUnSeqFileResourcesInDecendingOrder(
      List<TsFileResource> tsFileResources) {
    PriorityQueue<TsFileResource> unseqTsFilesSet =
        new PriorityQueue<>(
            (o1, o2) -> {
              Map<String, Long> startTimeMap = o1.getEndTimeMap();
              Long minTimeOfO1 = startTimeMap.get(seriesPath.getDevice());
              Map<String, Long> startTimeMap2 = o2.getEndTimeMap();
              Long minTimeOfO2 = startTimeMap2.get(seriesPath.getDevice());

              return Long.compare(minTimeOfO2, minTimeOfO1);
            });
    unseqTsFilesSet.addAll(tsFileResources);
    return unseqTsFilesSet;
  }

  /**
   * find the last TimeseriesMetadata and unpack all overlapped seq/unseq files
   */
  private void UnpackOverlappedFilesToTimeseriesMetadata(QueryContext context) throws IOException {
    List<TsFileResource> seqFileResource = dataSource.getSeqResources();
    PriorityQueue<TsFileResource> unseqFileResource =
        sortUnSeqFileResourcesInDecendingOrder(dataSource.getUnseqResources());

    TimeseriesMetadata lastTimeseriesMetadata = null;
    for (int index = seqFileResource.size() - 1; index >= 0; index--) {
      TsFileResource resource = seqFileResource.get(index);
      TimeseriesMetadata timeseriesMetadata = FileLoaderUtils.loadTimeSeriesMetadata(
          resource, seriesPath, context, timeFilter, allSensors);
      if (timeseriesMetadata != null) {
        lastSeqTimeseriesMetadata = timeseriesMetadata;
        lastTimeseriesMetadata = timeseriesMetadata;
        // last sequence TimeseriesMetadata endTime satisfies timeFilter and is larger than
        // any unseq file endTimes, then skip all the unseq files
        long seqLastPoint = lastTimeseriesMetadata.getStatistics().getEndTime();
        if (unseqFileResource.isEmpty() || (seqLastPoint <= queryTime &&
            seqLastPoint > unseqFileResource.peek().getEndTimeMap().get(seriesPath.getDevice()))) {
          return;
        }
        break;
      }
      seqFileResource.remove(index);
    }

    while (!unseqFileResource.isEmpty()) {
      TsFileResource resource = unseqFileResource.peek();
      TimeseriesMetadata timeseriesMetadata = FileLoaderUtils.loadTimeSeriesMetadata(
          resource, seriesPath, context, timeFilter, allSensors);
      if (timeseriesMetadata != null) {
        if (lastTimeseriesMetadata == null
            || (lastTimeseriesMetadata.getStatistics().getEndTime()
                < timeseriesMetadata.getStatistics().getEndTime())) {
          lastTimeseriesMetadata = timeseriesMetadata;
        }
        break;
      }
      unseqFileResource.poll();
    }

    // unpack all overlapped unseq files and fill chunkMetadata list
    while (!unseqFileResource.isEmpty()
        && (lastTimeseriesMetadata == null
        || (lastTimeseriesMetadata.getStatistics().getStartTime()
        <= unseqFileResource.peek().getEndTimeMap().get(seriesPath.getDevice())))) {
      unseqTimeseriesMetadataList.add(
          FileLoaderUtils.loadTimeSeriesMetadata(
              unseqFileResource.poll(), seriesPath, context, timeFilter, allSensors));
    }
  }

  private boolean containedByTimeFilter(Statistics statistics) {
    return timeFilter.containStartEndTime(statistics.getStartTime(), statistics.getEndTime());
  }

  private TimeValuePair constructLastPair(long timestamp, Object value, TSDataType dataType) {
    return new TimeValuePair(timestamp, TsPrimitiveType.getByType(dataType, value));
  }
}
