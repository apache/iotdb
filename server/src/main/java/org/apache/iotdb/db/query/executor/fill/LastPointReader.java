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
package org.apache.iotdb.db.query.executor.fill;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.idtable.IDTable;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

public class LastPointReader {

  // if it is a common path, it will be MeasurementPath
  // if it is a measurement of an aligned device, it will be AlignedPath with only one measurement
  private final PartialPath seriesPath;
  // only used for linear fill and previous fill, otherwise, it will be Long.MAX_VALUE
  long queryTime;
  TSDataType dataType;
  private final QueryContext context;

  // measurements of the same device in seriesPath
  private final Set<String> measurements;

  private final Filter timeFilter;

  private final QueryDataSource dataSource;

  private IChunkMetadata cachedLastChunk;

  private final List<ITimeSeriesMetadata> unseqTimeseriesMetadataList = new ArrayList<>();

  public LastPointReader(
      PartialPath seriesPath,
      TSDataType dataType,
      Set<String> measurements,
      QueryContext context,
      QueryDataSource dataSource,
      long queryTime,
      Filter timeFilter) {
    this.seriesPath = IDTable.translateQueryPath(seriesPath);
    this.dataType = dataType;
    this.dataSource = dataSource;
    this.context = context;
    this.queryTime = queryTime;
    this.measurements = measurements;
    this.timeFilter = timeFilter;
  }

  public TimeValuePair readLastPoint() throws IOException {
    TimeValuePair resultPoint = retrieveValidLastPointFromSeqFiles();
    UnpackOverlappedUnseqFiles(resultPoint.getTimestamp());

    PriorityQueue<IChunkMetadata> sortedChunkMetatdataList = sortUnseqChunkMetadatasByEndtime();
    while (!sortedChunkMetatdataList.isEmpty()
        && resultPoint.getTimestamp() <= sortedChunkMetatdataList.peek().getEndTime()) {
      IChunkMetadata chunkMetadata = sortedChunkMetatdataList.poll();
      TimeValuePair chunkLastPoint = getChunkLastPoint(chunkMetadata);
      if (chunkLastPoint.getTimestamp() > resultPoint.getTimestamp()
          || (chunkLastPoint.getTimestamp() == resultPoint.getTimestamp()
              && (cachedLastChunk == null || shouldUpdate(cachedLastChunk, chunkMetadata)))) {
        cachedLastChunk = chunkMetadata;
        resultPoint = chunkLastPoint;
      }
    }
    return resultPoint;
  }

  /** Pick up and cache the last sequence TimeseriesMetadata that satisfies timeFilter */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private TimeValuePair retrieveValidLastPointFromSeqFiles() throws IOException {
    List<TsFileResource> seqFileResource = dataSource.getSeqResources();
    TimeValuePair lastPoint = new TimeValuePair(Long.MIN_VALUE, null);
    for (int index = seqFileResource.size() - 1; index >= 0; index--) {
      TsFileResource resource = seqFileResource.get(index);
      ITimeSeriesMetadata timeseriesMetadata;
      timeseriesMetadata =
          loadTimeSeriesMetadata(resource, seriesPath, context, timeFilter, measurements);
      if (timeseriesMetadata != null) {
        if (!timeseriesMetadata.isModified()
            && endtimeContainedByTimeFilter(timeseriesMetadata.getStatistics())) {
          return constructLastPair(
              timeseriesMetadata.getStatistics().getEndTime(),
              timeseriesMetadata.getStatistics().getLastValue(),
              dataType);
        } else {
          List<IChunkMetadata> seqChunkMetadataList = timeseriesMetadata.loadChunkMetadataList();
          for (int i = seqChunkMetadataList.size() - 1; i >= 0; i--) {
            lastPoint = getChunkLastPoint(seqChunkMetadataList.get(i));
            // last point of this sequence chunk is valid, quit the loop
            if (lastPoint.getValue() != null) {
              return lastPoint;
            }
          }
        }
      }
    }

    return lastPoint;
  }

  protected ITimeSeriesMetadata loadTimeSeriesMetadata(
      TsFileResource resource,
      PartialPath seriesPath,
      QueryContext context,
      Filter filter,
      Set<String> allSensors)
      throws IOException {
    return FileLoaderUtils.loadTimeSeriesMetadata(
        resource, seriesPath, context, filter, allSensors);
  }

  /** find the last TimeeriesMetadata in unseq files and unpack all overlapped unseq files */
  private void UnpackOverlappedUnseqFiles(long lBoundTime) throws IOException {
    PriorityQueue<TsFileResource> unseqFileResource =
        sortUnSeqFileResourcesInDecendingOrder(dataSource.getUnseqResources());

    while (!unseqFileResource.isEmpty()
        && (lBoundTime <= unseqFileResource.peek().getEndTime(seriesPath.getDevice()))) {
      ITimeSeriesMetadata timeseriesMetadata =
          loadTimeSeriesMetadata(
              unseqFileResource.poll(), seriesPath, context, timeFilter, measurements);

      if (timeseriesMetadata == null
          || (!timeseriesMetadata.isModified()
              && timeseriesMetadata.getStatistics().getEndTime() < lBoundTime)) {
        continue;
      }
      unseqTimeseriesMetadataList.add(timeseriesMetadata);
      if (!timeseriesMetadata.isModified()) {
        if (endtimeContainedByTimeFilter(timeseriesMetadata.getStatistics())) {
          lBoundTime = Math.max(lBoundTime, timeseriesMetadata.getStatistics().getEndTime());
        } else {
          lBoundTime = Math.max(lBoundTime, timeseriesMetadata.getStatistics().getStartTime());
        }
      }
    }
  }

  private TimeValuePair getChunkLastPoint(IChunkMetadata chunkMetaData) throws IOException {
    TimeValuePair lastPoint = new TimeValuePair(Long.MIN_VALUE, null);
    if (chunkMetaData == null || chunkMetaData.getStatistics() == null) {
      return lastPoint;
    }
    Statistics chunkStatistics = chunkMetaData.getStatistics();

    if (!chunkMetaData.isModified() && endtimeContainedByTimeFilter(chunkStatistics)) {
      return constructLastPair(
          chunkStatistics.getEndTime(), chunkStatistics.getLastValue(), dataType);
    }
    List<IPageReader> pageReaders = FileLoaderUtils.loadPageReaderList(chunkMetaData, timeFilter);
    Iterator it;
    if (pageReaders instanceof LinkedList) {
      it = ((LinkedList<IPageReader>) pageReaders).descendingIterator();
    } else {
      // for singleton list
      it = pageReaders.iterator();
    }
    while (it.hasNext()) {
      IPageReader pageReader = (IPageReader) it.next();
      Statistics pageStatistics = pageReader.getStatistics();
      if (pageStatistics != null
          && !pageReader.isModified()
          && endtimeContainedByTimeFilter(pageStatistics)) {
        lastPoint =
            constructLastPair(pageStatistics.getEndTime(), pageStatistics.getLastValue(), dataType);
      } else {
        BatchData batchData = pageReader.getAllSatisfiedPageData();
        lastPoint = batchData.getLastPairBeforeOrEqualTimestamp(queryTime);
      }
      if (lastPoint.getValue() != null) {
        return lastPoint;
      }
    }
    return lastPoint;
  }

  private boolean shouldUpdate(IChunkMetadata cachedChunk, IChunkMetadata newChunk) {
    return (newChunk.getVersion() > cachedChunk.getVersion())
        || (newChunk.getVersion() == cachedChunk.getVersion()
            && newChunk.getOffsetOfChunkHeader() > cachedChunk.getOffsetOfChunkHeader());
  }

  private PriorityQueue<TsFileResource> sortUnSeqFileResourcesInDecendingOrder(
      List<TsFileResource> tsFileResources) {
    PriorityQueue<TsFileResource> unseqTsFilesSet =
        new PriorityQueue<>(
            (o1, o2) -> {
              long maxTimeOfO1 = o1.getEndTime(seriesPath.getDevice());
              long maxTimeOfO2 = o2.getEndTime(seriesPath.getDevice());
              return Long.compare(maxTimeOfO2, maxTimeOfO1);
            });
    unseqTsFilesSet.addAll(tsFileResources);
    return unseqTsFilesSet;
  }

  private PriorityQueue<IChunkMetadata> sortUnseqChunkMetadatasByEndtime() throws IOException {
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

  private boolean endtimeContainedByTimeFilter(Statistics statistics) {
    if (timeFilter == null) {
      return true;
    }
    return timeFilter.containStartEndTime(statistics.getEndTime(), statistics.getEndTime());
  }

  private TimeValuePair constructLastPair(long timestamp, Object value, TSDataType dataType) {
    return new TimeValuePair(timestamp, TsPrimitiveType.getByType(dataType, value));
  }
}
