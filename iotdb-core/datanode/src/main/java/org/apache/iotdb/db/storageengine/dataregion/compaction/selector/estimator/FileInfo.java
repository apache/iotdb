package org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator;

public class FileInfo {
  // total chunk num in this tsfile
  int totalChunkNum = 0;
  // max chunk num of one timeseries in this tsfile
  int maxSeriesChunkNum = 0;
  // max aligned series num in one device. If there is no aligned series in this file, then it
  // turns to be -1.
  int maxAlignedSeriesNumInDevice = -1;
  // max chunk num of one device in this tsfile
  @SuppressWarnings("squid:S1068")
  int maxDeviceChunkNum = 0;

  long averageChunkMetadataSize = 0;

  public FileInfo(
      int totalChunkNum,
      int maxSeriesChunkNum,
      int maxAlignedSeriesNumInDevice,
      int maxDeviceChunkNum,
      long averageChunkMetadataSize) {
    this.totalChunkNum = totalChunkNum;
    this.maxSeriesChunkNum = maxSeriesChunkNum;
    this.maxAlignedSeriesNumInDevice = maxAlignedSeriesNumInDevice;
    this.maxDeviceChunkNum = maxDeviceChunkNum;
    this.averageChunkMetadataSize = averageChunkMetadataSize;
  }
}
