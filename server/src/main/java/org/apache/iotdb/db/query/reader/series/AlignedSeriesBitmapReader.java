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
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryTimeManager;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.universal.AlignedDescPriorityMergeReader;
import org.apache.iotdb.db.query.reader.universal.AlignedPriorityMergeReader;
import org.apache.iotdb.db.query.reader.universal.DescPriorityMergeReader;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.AlignedTimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class AlignedSeriesBitmapReader extends SeriesBitmapReader {

  public AlignedSeriesBitmapReader(
      PartialPath seriesPath,
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      QueryDataSource dataSource,
      Filter timeFilter,
      Filter valueFilter,
      TsFileFilter fileFilter,
      boolean ascending) {
    super(
        seriesPath,
        allSensors,
        dataType,
        context,
        dataSource,
        timeFilter,
        valueFilter,
        fileFilter,
        ascending);
  }

  @TestOnly
  public AlignedSeriesBitmapReader(
      PartialPath seriesPath,
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      List<TsFileResource> seqFileResource,
      List<TsFileResource> unseqFileResource,
      Filter timeFilter,
      Filter valueFilter,
      boolean ascending) {
    super(
        seriesPath,
        allSensors,
        dataType,
        context,
        seqFileResource,
        unseqFileResource,
        timeFilter,
        valueFilter,
        ascending);
  }

  @Override
  protected PriorityMergeReader getPriorityMergeReader() {
    return new AlignedPriorityMergeReader();
  }

  @Override
  protected DescPriorityMergeReader getDescPriorityMergeReader() {
    return new AlignedDescPriorityMergeReader();
  }

  @Override
  protected AlignedTimeSeriesMetadata loadTimeSeriesMetadata(
      TsFileResource resource,
      PartialPath seriesPath,
      QueryContext context,
      Filter filter,
      Set<String> allSensors)
      throws IOException {
    return FileLoaderUtils.loadTimeSeriesMetadata(
        resource, (AlignedPath) seriesPath, context, filter);
  }

  @Override
  /**
   * This method should be called after hasNextChunk() until no next page, make sure that all
   * overlapped pages are consumed
   */
  @SuppressWarnings("squid:S3776")
  // Suppress high Cognitive Complexity warning
  boolean hasNextPage() throws IOException {
    if (!QueryTimeManager.checkQueryAlive(context.getQueryId())) {
      return false;
    }

    /*
     * has overlapped data before
     */
    if (hasCachedNextOverlappedPage) {
      return true;
    } else if (mergeReader.hasNextTimeValuePair() || firstPageOverlapped()) {
      if (hasNextOverlappedPage()) {
        cachedBatchData = nextOverlappedPage();
        if (cachedBatchData != null && cachedBatchData.hasCurrent()) {
          hasCachedNextOverlappedPage = true;
          return true;
        }
      }
    }

    if (firstPageReader != null) {
      return true;
    }

    /*
     * construct first page reader
     */
    if (firstChunkMetadata != null) {
      /*
       * try to unpack all overlapped ChunkMetadata to cachedPageReaders
       */
      unpackAllOverlappedChunkMetadataToPageReaders(
          orderUtils.getOverlapCheckTime(firstChunkMetadata.getStatistics()), true);
    } else {
      /*
       * first chunk metadata is already unpacked, consume cached pages
       */
      initFirstPageReader();
    }

    if (isExistOverlappedPage()) {
      return true;
    }

    // make sure firstPageReader won't be null while the unSeqPageReaders has more cached page
    // readers
    while (firstPageReader == null && (!seqPageReaders.isEmpty() || !unSeqPageReaders.isEmpty())) {

      initFirstPageReader();

      if (isExistOverlappedPage()) {
        return true;
      }
    }
    return firstPageReader != null;
  }
}
