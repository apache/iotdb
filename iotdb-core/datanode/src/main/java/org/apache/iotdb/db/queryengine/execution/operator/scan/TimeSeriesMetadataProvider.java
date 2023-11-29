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

package org.apache.iotdb.db.queryengine.execution.operator.scan;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.series.PaginationController;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

public class TimeSeriesMetadataProvider {

  private ITimeSeriesMetadata currentTimeSeriesMetadata;

  private final List<ITimeSeriesMetadata> seqTimeSeriesMetadata;
  private final PriorityQueue<ITimeSeriesMetadata> unSeqTimeSeriesMetadata;

  private final SeriesScanUtil.TimeOrderUtils orderUtils;

  public TimeSeriesMetadataProvider(SeriesScanUtil.TimeOrderUtils orderUtils) {
    this.orderUtils = orderUtils;
    this.seqTimeSeriesMetadata = new LinkedList<>();
    this.unSeqTimeSeriesMetadata =
        new PriorityQueue<>(
            orderUtils.comparingLong(
                timeSeriesMetadata -> orderUtils.getOrderTime(timeSeriesMetadata.getStatistics())));
  }

  public boolean hasCurrentFile() {
    return currentTimeSeriesMetadata != null;
  }

  public ITimeSeriesMetadata getCurrentFile() {
    return currentTimeSeriesMetadata;
  }

  public boolean hasNext() {
    return orderUtils.hasNextSeqResource()
        || orderUtils.hasNextUnseqResource()
        || !seqTimeSeriesMetadata.isEmpty()
        || !unSeqTimeSeriesMetadata.isEmpty();
  }

  @SuppressWarnings("unchecked")
  public boolean currentFileOverlapped() {
    checkState(hasCurrentFile(), "current file is null");

    Statistics<? extends Serializable> fileStatistics = currentTimeSeriesMetadata.getStatistics();
    return !seqTimeSeriesMetadata.isEmpty()
            && orderUtils.isOverlapped(fileStatistics, seqTimeSeriesMetadata.get(0).getStatistics())
        || !unSeqTimeSeriesMetadata.isEmpty()
            && orderUtils.isOverlapped(
                fileStatistics, unSeqTimeSeriesMetadata.peek().getStatistics());
  }

  public void skipCurrentFile() {
    currentTimeSeriesMetadata = null;
  }

  public boolean currentFileModified() {
    checkState(hasCurrentFile(), "current file is null");

    return currentTimeSeriesMetadata.isModified();
  }

  public void unpackAllOverlappedTsFilesToTimeSeriesMetadata(long endpointTime) throws IOException {
    while (orderUtils.hasNextUnseqResource()
        && orderUtils.isOverlapped(endpointTime, orderUtils.getNextUnseqFileResource(false))) {
      unpackUnseqTsFileResource();
    }
    while (orderUtils.hasNextSeqResource()
        && orderUtils.isOverlapped(endpointTime, orderUtils.getNextSeqFileResource(false))) {
      unpackSeqTsFileResource();
    }
  }

  public void updateCurrentTimeSeriesMetadata() {
    if (!seqTimeSeriesMetadata.isEmpty() && unSeqTimeSeriesMetadata.isEmpty()) {
      // only has seq
      currentTimeSeriesMetadata = seqTimeSeriesMetadata.remove(0);
    } else if (seqTimeSeriesMetadata.isEmpty() && !unSeqTimeSeriesMetadata.isEmpty()) {
      // only has unseq
      currentTimeSeriesMetadata = unSeqTimeSeriesMetadata.poll();
    } else if (!seqTimeSeriesMetadata.isEmpty()) {
      // has seq and unseq
      if (orderUtils.isTakeSeqAsFirst(
          seqTimeSeriesMetadata.get(0).getStatistics(),
          unSeqTimeSeriesMetadata.peek().getStatistics())) {
        currentTimeSeriesMetadata = seqTimeSeriesMetadata.remove(0);
      } else {
        currentTimeSeriesMetadata = unSeqTimeSeriesMetadata.poll();
      }
    }
  }

  public void filterFirstTimeSeriesMetadata(
      Filter queryFilter, PaginationController paginationController) {
    if (hasCurrentFile() && !currentFileOverlapped() && !currentFileModified()) {
      Statistics statistics = getCurrentFile().getStatistics();
      if (queryFilter == null || queryFilter.allSatisfy(statistics)) {
        long rowCount = statistics.getCount();
        if (paginationController.hasCurOffset(rowCount)) {
          skipCurrentFile();
          paginationController.consumeOffset(rowCount);
        }
      } else if (!queryFilter.satisfy(statistics)) {
        skipCurrentFile();
      }
    }
  }

  /**
   * unpack all overlapped seq/unseq files and find the first TimeSeriesMetadata
   *
   * <p>Because there may be too many files in the scenario used by the user, we cannot open all the
   * chunks at once, which may cause OOM, so we can only unpack one file at a time when needed. This
   * approach is likely to be ubiquitous, but it keeps the system running smoothly
   */
  public void tryToUnpackAllOverlappedFilesToTimeSeriesMetadata() throws IOException {
    /*
     * Fill sequence TimeSeriesMetadata List until it is not empty
     */
    while (seqTimeSeriesMetadata.isEmpty() && orderUtils.hasNextSeqResource()) {
      unpackSeqTsFileResource();
    }

    /*
     * Fill unSequence TimeSeriesMetadata Priority Queue until it is not empty
     */
    while (unSeqTimeSeriesMetadata.isEmpty() && orderUtils.hasNextUnseqResource()) {
      unpackUnseqTsFileResource();
    }

    // find end time of the first TimeSeriesMetadata
    long endTime = -1L;
    if (!seqTimeSeriesMetadata.isEmpty() && unSeqTimeSeriesMetadata.isEmpty()) {
      // only has seq
      endTime = orderUtils.getOverlapCheckTime(seqTimeSeriesMetadata.get(0).getStatistics());
    } else if (seqTimeSeriesMetadata.isEmpty() && !unSeqTimeSeriesMetadata.isEmpty()) {
      // only has unseq
      endTime = orderUtils.getOverlapCheckTime(unSeqTimeSeriesMetadata.peek().getStatistics());
    } else if (!seqTimeSeriesMetadata.isEmpty()) {
      // has seq and unseq
      endTime =
          orderUtils.getCurrentEndPoint(
              seqTimeSeriesMetadata.get(0).getStatistics(),
              unSeqTimeSeriesMetadata.peek().getStatistics());
    }

    // unpack all directly overlapped seq/unseq files with first TimeSeriesMetadata
    if (endTime != -1) {
      unpackAllOverlappedTsFilesToTimeSeriesMetadata(endTime);
    }

    updateCurrentTimeSeriesMetadata();
  }

  private void unpackSeqTsFileResource() throws IOException {
    ITimeSeriesMetadata timeseriesMetadata =
        loadTimeSeriesMetadata(
            orderUtils.getNextSeqFileResource(true),
            seriesPath,
            context,
            getGlobalTimeFilter(),
            scanOptions.getAllSensors());
    if (timeseriesMetadata != null) {
      timeseriesMetadata.setSeq(true);
      seqTimeSeriesMetadata.add(timeseriesMetadata);
    }
  }

  private void unpackUnseqTsFileResource() throws IOException {
    ITimeSeriesMetadata timeseriesMetadata =
        loadTimeSeriesMetadata(
            orderUtils.getNextUnseqFileResource(true),
            seriesPath,
            context,
            getGlobalTimeFilter(),
            scanOptions.getAllSensors());
    if (timeseriesMetadata != null) {
      timeseriesMetadata.setModified(true);
      timeseriesMetadata.setSeq(false);
      unSeqTimeSeriesMetadata.add(timeseriesMetadata);
    }
  }

  private ITimeSeriesMetadata loadTimeSeriesMetadata(
      TsFileResource resource,
      PartialPath seriesPath,
      QueryContext context,
      Filter filter,
      Set<String> allSensors)
      throws IOException {
    return FileLoaderUtils.loadTimeSeriesMetadata(
        resource, seriesPath, context, filter, allSensors);
  }
}
