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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import javax.management.Query;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.chunk.MemChunkLoader;
import org.apache.iotdb.db.query.reader.chunk.MemChunkReader;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;

import java.io.IOException;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

public class PreviousFill extends IFill {

  private Path seriesPath;
  private long beforeRange;
  private Set<String> allSensors;
  private Filter timeFilter;

  /*
   * file cache
   */
  private QueryDataSource dataSource;

  /*
   * chunk cache
   */
  private ChunkMetadata lastChunkMetadata;
  private List<ChunkMetadata> chunkMetadatas;

  public PreviousFill(Path seriesPath, TSDataType dataType, long queryTime, long beforeRange) {
    super(dataType, queryTime);
    this.seriesPath = seriesPath;
    this.beforeRange = beforeRange;
    this.timeFilter = constructFilter();
    this.chunkMetadatas = new ArrayList<>();
  }

  public PreviousFill(long beforeRange) {
    this.beforeRange = beforeRange;
  }

  @Override
  public IFill copy() {
    return new PreviousFill(seriesPath, dataType,  queryTime, beforeRange);
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
  public TimeValuePair getFillResult(QueryContext context) throws IOException {
    UnpackAllOverlappedFilesToChunkMetadatas(context);

    TimeValuePair lastPoint = new TimeValuePair(Long.MIN_VALUE, null);
    long lastVersion = 0;
    while (!chunkMetadatas.isEmpty()) {
      ChunkMetadata chunkMetaData = chunkMetadatas.remove(0);
      TimeValuePair lastChunkPoint = getChunkLastPoint(chunkMetaData);
      if (shouldUpdate(
          lastPoint.getTimestamp(), chunkMetaData.getVersion(),
          lastChunkPoint.getTimestamp(), lastVersion)) {
        lastPoint = lastChunkPoint;
      }
    }
    return lastPoint;
  }

  @Override
  public void configureFill(Path path, Set<String> sensors, QueryContext context)
      throws StorageEngineException, QueryProcessException {
    allSensors = sensors;
    dataSource = QueryResourceManager.getInstance().getQueryDataSource(path, context, timeFilter);
    // update filter by TTL
    timeFilter = dataSource.updateFilterUsingTTL(timeFilter);
  }

  private TimeValuePair getChunkLastPoint(ChunkMetadata chunkMetaData) throws IOException {
    Statistics chunkStatistics = chunkMetaData.getStatistics();
    if (!timeFilter.satisfy(chunkStatistics)) {
      return null;
    }
    if (containedByTimeFilter(chunkStatistics)) {
      return constructLastPair(
          chunkStatistics.getEndTime(), chunkStatistics.getLastValue(), dataType);
    } else {
      List<IPageReader> pageReaders = unpackChunkReaderToPageReaderList(chunkMetaData);
      TimeValuePair lastPoint = new TimeValuePair(0, null);
      for (IPageReader pageReader : pageReaders) {
        TimeValuePair lastPagePoint = getPageLastPoint(pageReader);
        if (lastPoint.getTimestamp() < lastPagePoint.getTimestamp()) {
          lastPoint = lastPagePoint;
        }
      }
      return lastPoint;
    }
  }

  private TimeValuePair getPageLastPoint(IPageReader pageReader) throws IOException {
    Statistics pageStatistics = pageReader.getStatistics();
    if (!timeFilter.satisfy(pageStatistics)) {
      return null;
    }
    if (containedByTimeFilter(pageStatistics)) {
      return constructLastPair(
          pageStatistics.getEndTime(), pageStatistics.getLastValue(), dataType);
    } else {
      BatchData batchData = pageReader.getAllSatisfiedPageData();
      return batchData.getLastPairBeforeOrEqualTimestamp(queryTime);
    }
  }

  private boolean shouldUpdate(long time, long version, long newTime, long newVersion) {
    return time < newTime || (time == newTime && version < newVersion);
  }

  private List<IPageReader> unpackChunkReaderToPageReaderList(ChunkMetadata metaData) throws IOException {
    if (metaData == null) {
      throw new IOException("Can't init null chunkMeta");
    }
    IChunkReader chunkReader;
    IChunkLoader chunkLoader = metaData.getChunkLoader();
    if (chunkLoader instanceof MemChunkLoader) {
      MemChunkLoader memChunkLoader = (MemChunkLoader) chunkLoader;
      chunkReader = new MemChunkReader(memChunkLoader.getChunk(), timeFilter);
    } else {
      Chunk chunk = chunkLoader.getChunk(metaData);
      chunkReader = new ChunkReader(chunk, timeFilter);
      chunkReader.hasNextSatisfiedPage();
    }
    return chunkReader.getPageReaderList();
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
   * find the last chunk metadata and unpack all overlapped seq/unseq files
   */
  private void UnpackAllOverlappedFilesToChunkMetadatas(QueryContext context) throws IOException {
    List<TsFileResource> seqFileResource = dataSource.getSeqResources();
    PriorityQueue<TsFileResource> unseqFileResource = sortUnSeqFileResourcesInDecendingOrder(dataSource.getUnseqResources());
    for (int index = seqFileResource.size() - 1; index >= 0; index--) {
      TsFileResource resource = seqFileResource.get(index);
      TimeseriesMetadata timeseriesMetadata = FileLoaderUtils.loadTimeSeriesMetadata(
          resource, seriesPath, context, timeFilter, allSensors);
      if (timeseriesMetadata != null) {
        // The last seq file satisfies timeFilter, pick up the last chunk
        List<ChunkMetadata> chunkMetadatas = timeseriesMetadata.getChunkMetadataList();
        lastChunkMetadata = chunkMetadatas.get(chunkMetadatas.size() - 1);
        break;
      }
      seqFileResource.remove(index);
    }

    while (!unseqFileResource.isEmpty()) {
      TsFileResource resource = unseqFileResource.peek();
      TimeseriesMetadata timeseriesMetadata = FileLoaderUtils.loadTimeSeriesMetadata(
          resource, seriesPath, context, timeFilter, allSensors);
      if (timeseriesMetadata != null) {
        List<ChunkMetadata> chunkMetadatas = timeseriesMetadata.getChunkMetadataList();
        ChunkMetadata lastUnseqChunkMetadata = chunkMetadatas.get(chunkMetadatas.size() - 1);
        if (lastChunkMetadata == null) {
          lastChunkMetadata = lastUnseqChunkMetadata;
        } else if (lastChunkMetadata.getEndTime() < lastUnseqChunkMetadata.getEndTime()) {
          lastChunkMetadata = lastUnseqChunkMetadata;
        }
        break;
      }
      unseqFileResource.poll();
    }

    // unpack overlapped seq files and fill chunkMetadata list
    for (int index = seqFileResource.size() - 1; index >= 0; index--) {
      if (lastChunkMetadata != null
          && (lastChunkMetadata.getStartTime()
          <= seqFileResource.get(index).getEndTimeMap().get(seriesPath.getDevice()))) {
        chunkMetadatas.addAll(FileLoaderUtils.loadChunkMetadataFromTsFileResource(
            seqFileResource.remove(index), seriesPath, context));
      }
    }

    // unpack all overlapped unseq files and fill chunkMetadata list
    while (!unseqFileResource.isEmpty()
        && (lastChunkMetadata == null || (lastChunkMetadata.getStartTime()
        <= unseqFileResource.peek().getEndTimeMap().get(seriesPath.getDevice())))) {
      chunkMetadatas.addAll(
          FileLoaderUtils.loadChunkMetadataFromTsFileResource(
              unseqFileResource.poll(), seriesPath, context));
    }
  }

  private boolean containedByTimeFilter(Statistics statistics) {
    return timeFilter == null
        || timeFilter.containStartEndTime(statistics.getStartTime(), statistics.getEndTime());
  }

  private TimeValuePair constructLastPair(long timestamp, Object value, TSDataType dataType) {
    return new TimeValuePair(timestamp, TsPrimitiveType.getByType(dataType, value));
  }
}
