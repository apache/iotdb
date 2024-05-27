package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.storageengine.dataregion.read.IQueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSourceForRegionScan;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.IChunkHandle;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.IFileScanHandle;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.AbstractChunkOffset;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.AbstractDeviceChunkMetaData;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.DeviceStartEndTime;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileDeviceStartEndTimeIterator;

import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class RegionScanForActiveDeviceUtil {

  private QueryDataSourceForRegionScan queryDataSource;

  private final Filter timeFilter;

  private final Set<IDeviceID> deviceSetForCurrentTsFile = new HashSet<>();
  private final List<AbstractChunkOffset> chunkToBeScanned = new ArrayList<>();
  private final List<Statistics<? extends Serializable>> chunkStatistics = new ArrayList<>();

  private IFileScanHandle curFileScanHandle = null;
  private Iterator<AbstractDeviceChunkMetaData> deviceChunkMetaDataIterator = null;
  private Iterator<IChunkHandle> chunkHandleIterator = null;
  private IChunkHandle currentChunkHandle = null;

  private final List<IDeviceID> activeDevices = new ArrayList<>();

  public RegionScanForActiveDeviceUtil(Filter timeFilter) {
    this.timeFilter = timeFilter;
  }

  public void initQueryDataSource(IQueryDataSource dataSource) {
    this.queryDataSource = (QueryDataSourceForRegionScan) dataSource;
  }

  public boolean isCurrentTsFileFinished() {
    return deviceSetForCurrentTsFile.isEmpty();
  }

  public boolean nextTsFileHandle(Set<IDeviceID> targetDevices)
      throws IOException, IllegalPathException {

    if (!queryDataSource.hasNext()) {
      // There is no more TsFileHandles to be scanned.
      return false;
    }

    curFileScanHandle = queryDataSource.next();
    deviceChunkMetaDataIterator = null;

    // Init deviceSet for current tsFileHandle
    TsFileDeviceStartEndTimeIterator iterator = curFileScanHandle.getDeviceStartEndTimeIterator();
    while (iterator.hasNext()) {
      DeviceStartEndTime deviceStartEndTime = iterator.next();
      IDeviceID deviceID = deviceStartEndTime.getDevicePath();

      // If this device has already been removed by another TsFileHandle, we should skip it.
      if (!targetDevices.contains(deviceID)) {
        continue;
      }

      long startTime = deviceStartEndTime.getStartTime();
      long endTime = deviceStartEndTime.getEndTime();
      if (!timeFilter.satisfyStartEndTime(startTime, endTime)) {
        // If the time range is filtered, the devicePath is not active in this time range.
        continue;
      }
      boolean[] isDeleted =
          curFileScanHandle.isDeviceTimeDeleted(
              deviceStartEndTime.getDevicePath(), new long[] {startTime, endTime});
      if (!isDeleted[0] || !isDeleted[1]) {
        // Only if one time is not deleted, the devicePath is active in this time range.
        activeDevices.add(deviceID);
      } else {
        // Else, we need more infos to check if the device is active in the following procedure
        deviceSetForCurrentTsFile.add(deviceID);
      }
    }
    return true;
  }

  public boolean filterChunkMetaData() throws IOException, IllegalPathException {

    if (deviceSetForCurrentTsFile.isEmpty()) {
      return true;
    }

    if (deviceChunkMetaDataIterator == null) {
      deviceChunkMetaDataIterator = curFileScanHandle.getAllDeviceChunkMetaData();
    }

    if (deviceChunkMetaDataIterator.hasNext()) {
      AbstractDeviceChunkMetaData deviceChunkMetaData = deviceChunkMetaDataIterator.next();
      IDeviceID curDevice = deviceChunkMetaData.getDevicePath();
      if (deviceSetForCurrentTsFile.contains(curDevice)) {
        // If the chunkMeta in curDevice has valid start or end time, curDevice is active in this
        // time range.
        if (checkChunkMetaDataOfDevice(curDevice, deviceChunkMetaData)) {
          deviceSetForCurrentTsFile.remove(curDevice);
          activeDevices.add(curDevice);
        }
      }
      // If the phase of chunkMetaData check is finished.
      return !deviceChunkMetaDataIterator.hasNext();
    } else {
      return true;
    }
  }

  public boolean filterChunkData() throws IOException, IllegalPathException {

    // If there is no device to be checked in current TsFile, just finish.
    if (deviceSetForCurrentTsFile.isEmpty()) {
      return true;
    }

    if (chunkHandleIterator == null) {
      chunkHandleIterator = curFileScanHandle.getChunkHandles(chunkToBeScanned, chunkStatistics);
    }

    // 1. init a chunkHandle with data
    // if there is no more chunkHandle, all the data in current TsFile is scanned, just return true.
    while (currentChunkHandle == null || !currentChunkHandle.hasNextPage()) {
      if (!chunkHandleIterator.hasNext()) {
        return true;
      }
      currentChunkHandle = chunkHandleIterator.next();
      // Skip currentChunkHandle if corresponding device is already active.
      if (!deviceSetForCurrentTsFile.contains(currentChunkHandle.getDeviceID())) {
        currentChunkHandle = null;
      }
    }

    // 2. check page statistics
    IDeviceID curDevice = currentChunkHandle.getDeviceID();
    long[] pageStatistics = currentChunkHandle.getPageStatisticsTime();
    if (!timeFilter.satisfyStartEndTime(pageStatistics[0], pageStatistics[1])) {
      // All the data in current page is not valid, just skip.
      currentChunkHandle.skipCurrentPage();
      return false;
    }
    boolean[] isDeleted =
        curFileScanHandle.isTimeSeriesTimeDeleted(
            curDevice,
            currentChunkHandle.getMeasurement(),
            new long[] {pageStatistics[0], pageStatistics[1]});
    if (!isDeleted[0] || !isDeleted[1]) {
      // If the chunkMeta in curDevice has valid start or end time, curDevice is active in this time
      // range.
      deviceSetForCurrentTsFile.remove(curDevice);
      activeDevices.add(curDevice);
      currentChunkHandle = null;
      return false;
    }

    // 3. check page data
    long[] timeDataForPage = currentChunkHandle.getDataTime();
    for (long time : timeDataForPage) {
      if (!timeFilter.satisfy(time, null)) {
        continue;
      }
      isDeleted =
          curFileScanHandle.isTimeSeriesTimeDeleted(
              curDevice, currentChunkHandle.getMeasurement(), new long[] {time});
      if (!isDeleted[0]) {
        // If the chunkData in curDevice has valid time, curDevice is active.
        deviceSetForCurrentTsFile.remove(curDevice);
        activeDevices.add(curDevice);
        currentChunkHandle = null;
        return false;
      }
    }
    return !(currentChunkHandle.hasNextPage() || chunkHandleIterator.hasNext());
  }

  private boolean checkChunkMetaDataOfDevice(
      IDeviceID deviceID, AbstractDeviceChunkMetaData deviceChunkMetaData)
      throws IllegalPathException {
    List<AbstractChunkOffset> chunkOffsetsForCurrentDevice = new ArrayList<>();
    List<Statistics<? extends Serializable>> chunkStatisticsForCurrentDevice = new ArrayList<>();
    while (deviceChunkMetaData.hasNextValueChunkMetadata()) {
      IChunkMetadata valueChunkMetaData = deviceChunkMetaData.nextValueChunkMetadata();
      long startTime = valueChunkMetaData.getStartTime();
      long endTime = valueChunkMetaData.getEndTime();
      if (!timeFilter.satisfyStartEndTime(startTime, endTime)) {
        continue;
      }
      boolean[] isDeleted =
          curFileScanHandle.isTimeSeriesTimeDeleted(
              deviceID, valueChunkMetaData.getMeasurementUid(), new long[] {startTime, endTime});
      if (!isDeleted[0] || !isDeleted[1]) {
        // If the chunkMeta in curDevice has valid start or end time, curDevice is active in this
        // time range.
        return true;
      }
      chunkOffsetsForCurrentDevice.add(deviceChunkMetaData.getChunkOffset());
      chunkStatisticsForCurrentDevice.add(valueChunkMetaData.getStatistics());
    }
    chunkToBeScanned.addAll(chunkOffsetsForCurrentDevice);
    chunkStatistics.addAll(chunkStatisticsForCurrentDevice);
    return false;
  }

  public List<IDeviceID> getActiveDevices() {
    return activeDevices;
  }

  public void finishCurrentFile() {
    queryDataSource.releaseFileScanHandle();
    deviceSetForCurrentTsFile.clear();
    activeDevices.clear();
  }

  public boolean isFinished() {
    return queryDataSource == null || !queryDataSource.hasNext();
  }
}
