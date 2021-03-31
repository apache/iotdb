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
package org.apache.iotdb.db.query.reader.series;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.VectorPartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.VectorTimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

public class LastPointReader {

  long queryTime;
  TSDataType dataType;
  protected PartialPath seriesPath;
  protected QueryContext context;

  // measurements of the same device as "seriesPath"
  protected Set<String> deviceMeasurements;

  protected Filter timeFilter;

  protected QueryDataSource dataSource;

  protected TimeValuePair cachedLastPair;

  protected List<ITimeSeriesMetadata> unseqTimeseriesMetadataList = new ArrayList<>();

  public LastPointReader() {}

  public LastPointReader(
      PartialPath seriesPath,
      TSDataType dataType,
      Set<String> deviceMeasurements,
      QueryContext context,
      QueryDataSource dataSource,
      long queryTime,
      Filter timeFilter)
      throws IOException {
    this.seriesPath = seriesPath;
    this.dataType = dataType;
    this.dataSource = dataSource;
    this.context = context;
    this.queryTime = queryTime;
    this.deviceMeasurements = deviceMeasurements;
    this.timeFilter = timeFilter;
  }

  /*public TimeValuePair getCachedLastPair() {
    return cachedLastPair;
  }*/

  public TimeValuePair getCachedLastPair() throws IOException {
    cacheLastFromSeqFiles();
    UnpackOverlappedUnseqFiles(cachedLastPair.getTimestamp());
    PriorityQueue<IChunkMetadata> sortedChunkMetatdataList = sortUnseqChunkMetadatasByEndtime();
    IChunkMetadata lastChunk = null;
    while (!sortedChunkMetatdataList.isEmpty()
        && cachedLastPair.getTimestamp() <= sortedChunkMetatdataList.peek().getEndTime()) {
      IChunkMetadata chunkMetadata = sortedChunkMetatdataList.poll();
      TimeValuePair chunkLastPoint = readChunkLastPoint(chunkMetadata);
      if (chunkLastPoint.getTimestamp() > cachedLastPair.getTimestamp()
          || (chunkLastPoint.getTimestamp() == cachedLastPair.getTimestamp()
              && (lastChunk == null || shouldUpdate(lastChunk, chunkMetadata)))) {
        lastChunk = chunkMetadata;
        cachedLastPair = chunkLastPoint;
      }
    }
    return cachedLastPair;
  }

  /** Pick up and cache the last sequence TimeseriesMetadata that satisfies timeFilter */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void cacheLastFromSeqFiles() throws IOException {
    List<TsFileResource> seqFileResource = dataSource.getSeqResources();
    TimeseriesMetadata lastMetadata;
    for (int index = seqFileResource.size() - 1; index >= 0; index--) {
      TsFileResource resource = seqFileResource.get(index);
      lastMetadata =
          FileLoaderUtils.loadTimeSeriesMetadata(
              resource, seriesPath, context, timeFilter, deviceMeasurements);
      if (lastMetadata == null) {
        continue;
      }
      if (!lastMetadata.isModified()
          && endtimeContainedByTimeFilter(lastMetadata.getStatistics())) {
        cachedLastPair =
            new TimeValuePair(
                lastMetadata.getStatistics().getEndTime(),
                lastMetadata.getStatisticalLastValue());
      } else {
        cachedLastPair = getLastByUnpackingTimeseries(lastMetadata);
        if (cachedLastPair != null) {
          return;
        }
      }
    }
  }

  protected TimeValuePair getLastByUnpackingTimeseries(ITimeSeriesMetadata metadata)
      throws IOException {
    List<IChunkMetadata> seqChunkMetadataList = metadata.loadChunkMetadataList();
    for (int i = seqChunkMetadataList.size() - 1; i >= 0; i--) {
      if (seqChunkMetadataList.get(i) != null) {
        TimeValuePair valuePair = readChunkLastPoint(seqChunkMetadataList.get(i));
        // last point of this sequence chunk is valid, quit the loop
        if (valuePair.getValue() != null) {
          return valuePair;
        }
      }
    }
    return null;
  }

  /** find the last TimeseriesMetadata in unseq files and unpack all overlapped unseq files */
  protected void UnpackOverlappedUnseqFiles(long lBoundTime) throws IOException {
    PriorityQueue<TsFileResource> unseqFileResource =
        sortUnSeqFileResourcesInDecendingOrder(dataSource.getUnseqResources());

    while (!unseqFileResource.isEmpty()
        && (lBoundTime <= unseqFileResource.peek().getEndTime(seriesPath.getDevice()))) {
      TimeseriesMetadata timeseriesMetadata =
          FileLoaderUtils.loadTimeSeriesMetadata(
              unseqFileResource.poll(), seriesPath, context, timeFilter, deviceMeasurements);

      if (timeseriesMetadata == null
          || (!timeseriesMetadata.isModified()
              && timeseriesMetadata.getStatistics().getEndTime() < lBoundTime)) {
        continue;
      }
      if (!timeseriesMetadata.isModified()) {
        if (endtimeContainedByTimeFilter(timeseriesMetadata.getStatistics())) {
          lBoundTime = Math.max(lBoundTime, timeseriesMetadata.getStatistics().getEndTime());
        } else {
          lBoundTime = Math.max(lBoundTime, timeseriesMetadata.getStatistics().getStartTime());
        }
      }
      unseqTimeseriesMetadataList.add(timeseriesMetadata);
    }
  }

  protected TimeValuePair readChunkLastPoint(IChunkMetadata chunkMetaData) throws IOException {
    TimeValuePair lastPoint = new TimeValuePair(Long.MIN_VALUE, null);
    if (chunkMetaData == null) {
      return lastPoint;
    }
    Statistics chunkStatistics = chunkMetaData.getStatistics();

    if (!chunkMetaData.isModified() && endtimeContainedByTimeFilter(chunkStatistics)) {
      return new TimeValuePair(
          chunkStatistics.getEndTime(), chunkMetaData.getStatisticalLastValue());
    }
    List<IPageReader> pageReaders = FileLoaderUtils.loadPageReaderList(chunkMetaData, timeFilter);
    for (int i = pageReaders.size() - 1; i >= 0; i--) {
      IPageReader pageReader = pageReaders.get(i);
      Statistics pageStatistics = pageReader.getStatistics();
      if (!pageReader.isModified() && endtimeContainedByTimeFilter(pageStatistics)) {
        lastPoint =
            new TimeValuePair(pageStatistics.getEndTime(), pageReader.getStatisticalLastValue());
      } else {
        BatchData batchData = pageReader.getAllSatisfiedPageData(true);
        lastPoint = batchData.getLastPairBeforeOrEqualTimestamp(queryTime);
      }
      if (lastPoint.getValue() != null) {
        return lastPoint;
      }
    }
    return lastPoint;
  }

  protected boolean shouldUpdate(IChunkMetadata cachedChunk, IChunkMetadata newChunk) {
    return (newChunk.getVersion() > cachedChunk.getVersion())
        || (newChunk.getVersion() == cachedChunk.getVersion()
            && newChunk.getOffsetOfChunkHeader() > cachedChunk.getOffsetOfChunkHeader());
  }

  protected PriorityQueue<TsFileResource> sortUnSeqFileResourcesInDecendingOrder(
      List<TsFileResource> tsFileResources) {
    PriorityQueue<TsFileResource> unseqTsFilesSet =
        new PriorityQueue<>(
            (o1, o2) -> {
              Long maxTimeOfO1 = o1.getEndTime(seriesPath.getDevice());
              Long maxTimeOfO2 = o2.getEndTime(seriesPath.getDevice());
              return Long.compare(maxTimeOfO2, maxTimeOfO1);
            });
    unseqTsFilesSet.addAll(tsFileResources);
    return unseqTsFilesSet;
  }

  protected PriorityQueue<IChunkMetadata> sortUnseqChunkMetadatasByEndtime() throws IOException {
    PriorityQueue<IChunkMetadata> chunkMetadataList =
        new PriorityQueue<>(
            (o1, o2) -> {
              long endTime1 = o1.getEndTime();
              long endTime2 = o2.getEndTime();
              if (endTime1 < endTime2) {
                return 1;
              } else if (endTime1 > endTime2) {
                return -1;
              }
              if (o2.getVersion() > o1.getVersion()) {
                return 1;
              }
              return (o2.getVersion() < o1.getVersion()
                  ? -1
                  : Long.compare(o2.getOffsetOfChunkHeader(), o1.getOffsetOfChunkHeader()));
            });
    for (ITimeSeriesMetadata timeseriesMetadata : unseqTimeseriesMetadataList) {
      if (timeseriesMetadata != null) {
        chunkMetadataList.addAll(timeseriesMetadata.loadChunkMetadataList());
      }
    }
    return chunkMetadataList;
  }

  protected boolean endtimeContainedByTimeFilter(Statistics statistics) {
    if (timeFilter == null) {
      return true;
    }
    return timeFilter.containStartEndTime(statistics.getEndTime(), statistics.getEndTime());
  }
}
