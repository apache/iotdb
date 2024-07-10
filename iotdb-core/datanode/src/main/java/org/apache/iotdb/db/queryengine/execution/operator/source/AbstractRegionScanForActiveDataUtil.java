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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.storageengine.dataregion.read.IQueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSourceForRegionScan;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.IChunkHandle;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.IFileScanHandle;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.AbstractChunkOffset;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.AbstractDeviceChunkMetaData;
import org.apache.iotdb.db.utils.TimeFilterForDeviceTTL;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.basic.TimeFilter;
import org.apache.tsfile.utils.Accountable;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class AbstractRegionScanForActiveDataUtil implements Accountable {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(List.class)
          + RamUsageEstimator.shallowSizeOfInstance(List.class)
          + RamUsageEstimator.shallowSizeOfInstance(TimeFilter.class);

  protected QueryDataSourceForRegionScan queryDataSource;
  protected final TimeFilterForDeviceTTL timeFilter;

  protected final List<AbstractChunkOffset> chunkToBeScanned = new ArrayList<>();
  protected final List<Statistics<? extends Serializable>> chunkStatistics = new ArrayList<>();

  protected IFileScanHandle curFileScanHandle = null;
  protected Iterator<AbstractDeviceChunkMetaData> deviceChunkMetaDataIterator = null;
  protected Iterator<IChunkHandle> chunkHandleIterator = null;
  protected IChunkHandle currentChunkHandle = null;

  protected AbstractRegionScanForActiveDataUtil(Filter timeFilter, Map<IDeviceID, Long> ttlCache) {
    this.timeFilter = new TimeFilterForDeviceTTL(timeFilter, ttlCache);
  }

  public void initQueryDataSource(IQueryDataSource dataSource) {
    this.queryDataSource = (QueryDataSourceForRegionScan) dataSource;
  }

  public abstract boolean isCurrentTsFileFinished();

  public abstract void processDeviceChunkMetadata(AbstractDeviceChunkMetaData deviceChunkMetaData)
      throws IllegalPathException;

  public abstract boolean isCurrentChunkHandleValid();

  public abstract void processActiveChunk(IDeviceID deviceID, String measurementId);

  public void finishCurrentFile() {
    curFileScanHandle = null;
    deviceChunkMetaDataIterator = null;
    chunkHandleIterator = null;
    currentChunkHandle = null;
  }

  public boolean hasMoreData() {
    return queryDataSource != null && queryDataSource.hasNext();
  }

  // Filter the chunkMetaData in current TsFile and build chunkOffset if we need more information
  // from real data.
  // return true if there is still some chunkMetaData to be checked.
  // return false if all the chunkMetaData in current TsFile is checked or current tsFile is
  // finished.
  public boolean filterChunkMetaData() throws IOException, IllegalPathException {

    if (isCurrentTsFileFinished()) {
      return false;
    }

    if (deviceChunkMetaDataIterator == null) {
      deviceChunkMetaDataIterator = curFileScanHandle.getAllDeviceChunkMetaData();
    }

    if (deviceChunkMetaDataIterator.hasNext()) {
      AbstractDeviceChunkMetaData deviceChunkMetaData = deviceChunkMetaDataIterator.next();
      processDeviceChunkMetadata(deviceChunkMetaData);
      // If the phase of chunkMetaData check is finished.
      return deviceChunkMetaDataIterator.hasNext();
    } else {
      return false;
    }
  }

  public boolean filterChunkData() throws IOException, IllegalPathException {

    // If there is no device to be checked in current TsFile, just finish.
    if (isCurrentTsFileFinished()) {
      return false;
    }

    if (chunkHandleIterator == null) {
      chunkHandleIterator =
          curFileScanHandle.getChunkHandles(chunkToBeScanned, chunkStatistics, getOrderedIndex());
      chunkToBeScanned.clear();
      chunkStatistics.clear();
    }

    // 1. init a chunkHandle with data
    // if there is no more chunkHandle, all the data in current TsFile is scanned, just return
    // false.
    while (currentChunkHandle == null || !currentChunkHandle.hasNextPage()) {
      if (!chunkHandleIterator.hasNext()) {
        chunkHandleIterator = null;
        return false;
      }
      currentChunkHandle = chunkHandleIterator.next();
      // Skip currentChunkHandle if corresponding device is already active.
      if (!isCurrentChunkHandleValid()) {
        currentChunkHandle = null;
      }
    }

    // 2. check page statistics
    currentChunkHandle.nextPage();
    IDeviceID curDevice = currentChunkHandle.getDeviceID();
    String curMeasurement = currentChunkHandle.getMeasurement();
    long[] pageStatistics = currentChunkHandle.getPageStatisticsTime();
    if (!timeFilter.satisfyStartEndTime(pageStatistics[0], pageStatistics[1], curDevice)) {
      // All the data in current page is not valid, just skip.
      currentChunkHandle.skipCurrentPage();
      return true;
    }

    if ((timeFilter.satisfy(pageStatistics[0], curDevice)
            && !curFileScanHandle.isTimeSeriesTimeDeleted(
                curDevice, curMeasurement, pageStatistics[0]))
        || (timeFilter.satisfy(pageStatistics[1], curDevice)
            && !curFileScanHandle.isTimeSeriesTimeDeleted(
                curDevice, curMeasurement, pageStatistics[1]))) {
      // If the page in curChunk has valid start time, curChunk is active in this time range.
      processActiveChunk(curDevice, curMeasurement);
      return true;
    }

    // 3. check page data
    long[] timeDataForPage = currentChunkHandle.getDataTime();
    for (long time : timeDataForPage) {
      if (!timeFilter.satisfy(time, curDevice)) {
        continue;
      }

      if (!curFileScanHandle.isTimeSeriesTimeDeleted(curDevice, curMeasurement, time)) {
        // If the chunkData in curDevice has valid time, curChunk is active.
        processActiveChunk(curDevice, curMeasurement);
        return true;
      }
    }
    return currentChunkHandle.hasNextPage() || chunkHandleIterator.hasNext();
  }

  private List<Integer> getOrderedIndex() {
    List<Integer> indexList = new ArrayList<>();
    for (int i = 0; i < chunkToBeScanned.size(); i++) {
      indexList.add(i);
    }
    indexList.sort(Comparator.comparingLong(i -> chunkToBeScanned.get(i).getOffSet()));
    return indexList;
  }

  public boolean isFinished() {
    return queryDataSource == null || !queryDataSource.hasNext();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE;
  }
}
