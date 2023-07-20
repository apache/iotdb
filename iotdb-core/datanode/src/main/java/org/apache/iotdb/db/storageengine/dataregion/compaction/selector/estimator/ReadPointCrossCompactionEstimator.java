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

package org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.read.TsFileDeviceIterator;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ReadPointCrossCompactionEstimator extends AbstractCrossSpaceEstimator {
  private static final Logger logger =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

  // the max cost of reading source seq file among all source seq files of this cross compaction
  // task
  private long maxCostOfReadingSeqFile;

  // the max cost of writing target file
  private long maxCostOfWritingTargetFile;

  private int maxConcurrentSeriesNum = 1;

  // the number of timeseries being compacted at the same time
  private final int subCompactionTaskNum =
      IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum();

  public ReadPointCrossCompactionEstimator() {
    this.maxCostOfReadingSeqFile = 0;
    this.maxCostOfWritingTargetFile = 0;
  }

  @Override
  public long estimateCrossCompactionMemory(
      List<TsFileResource> seqResources, TsFileResource unseqResource) throws IOException {
    if (!addReadLock(seqResources, unseqResource)) {
      // there is file been deleted during selection, return -1
      return -1L;
    }
    try {
      long cost = 0;
      cost += calculateReadingUnseqFile(unseqResource);
      cost += calculateReadingSeqFiles(seqResources);
      cost += calculatingWritingTargetFiles(seqResources, unseqResource);
      return cost;
    } finally {
      releaseReadLock(seqResources, unseqResource);
    }
  }

  /** Add read lock. Return false if any of the file were deleted. */
  private boolean addReadLock(List<TsFileResource> seqResources, TsFileResource unseqResource) {
    List<TsFileResource> allResources = new ArrayList<>(seqResources);
    allResources.add(unseqResource);
    for (int i = 0; i < allResources.size(); i++) {
      TsFileResource resource = allResources.get(i);
      resource.readLock();
      if (resource.isDeleted()) {
        // release read lock
        for (int j = 0; j <= i; j++) {
          allResources.get(j).readUnlock();
        }
        return false;
      }
    }
    return true;
  }

  private void releaseReadLock(List<TsFileResource> seqResources, TsFileResource unseqResource) {
    seqResources.forEach(TsFileResource::readUnlock);
    unseqResource.readUnlock();
  }

  /**
   * Calculate memory cost of reading source unseq files in the cross space compaction. Double the
   * total size of the timeseries to be compacted at the same time in all unseq files.
   *
   * @throws IOException if io errors occurred
   */
  private long calculateReadingUnseqFile(TsFileResource unseqResource) throws IOException {
    TsFileSequenceReader reader = getFileReader(unseqResource);
    FileInfo fileInfo = getSeriesAndDeviceChunkNum(reader);
    // it is max aligned series num of one device when tsfile contains aligned series,
    // else is sub compaction task num.
    int concurrentSeriesNum =
        fileInfo.maxAlignedSeriesNumInDevice == -1
            ? subCompactionTaskNum
            : fileInfo.maxAlignedSeriesNumInDevice;
    maxConcurrentSeriesNum = Math.max(maxConcurrentSeriesNum, concurrentSeriesNum);
    if (fileInfo.totalChunkNum == 0) { // If totalChunkNum ==0, i.e. this unSeq tsFile has no chunk.
      logger.warn(
          "calculateReadingUnseqFile(), find 1 empty unSeq tsFile: {}.",
          unseqResource.getTsFilePath());
      return 0;
    }
    // it means the max size of a timeseries in this file when reading all of its chunk into memory.
    return compressionRatio
        * concurrentSeriesNum
        * (unseqResource.getTsFileSize() * fileInfo.maxSeriesChunkNum / fileInfo.totalChunkNum);
  }

  /**
   * Calculate memory cost of reading source seq files in the cross space compaction. Select the
   * maximun size of the timeseries to be compacted at the same time in one seq file, because only
   * one seq file will be queried at the same time.
   *
   * @throws IOException if io errors occurred
   */
  private long calculateReadingSeqFiles(List<TsFileResource> seqResources) throws IOException {
    long cost = 0;
    for (TsFileResource seqResource : seqResources) {
      TsFileSequenceReader reader = getFileReader(seqResource);
      FileInfo fileInfo = getSeriesAndDeviceChunkNum(reader);
      // it is max aligned series num of one device when tsfile contains aligned series,
      // else is sub compaction task num.
      int concurrentSeriesNum =
          fileInfo.maxAlignedSeriesNumInDevice == -1
              ? subCompactionTaskNum
              : fileInfo.maxAlignedSeriesNumInDevice;
      maxConcurrentSeriesNum = Math.max(maxConcurrentSeriesNum, concurrentSeriesNum);
      long seqFileCost;
      if (fileInfo.totalChunkNum == 0) { // If totalChunkNum ==0, i.e. this seq tsFile has no chunk.
        logger.warn(
            "calculateReadingSeqFiles(), find 1 empty seq tsFile: {}.",
            seqResource.getTsFilePath());
        seqFileCost = 0;
      } else {
        // We need to multiply the compression ratio here.
        seqFileCost =
            compressionRatio
                * seqResource.getTsFileSize()
                * concurrentSeriesNum
                / fileInfo.totalChunkNum;
      }

      if (seqFileCost > maxCostOfReadingSeqFile) {
        // Only one seq file will be read at the same time.
        // not only reading chunk into chunk cache, but also need to deserialize data point into
        // merge reader. We have to add the cost in merge reader here and the cost of chunk cache is
        // unnecessary.
        cost -= maxCostOfReadingSeqFile;
        cost += seqFileCost;
        maxCostOfReadingSeqFile = seqFileCost;
      }
    }
    return cost;
  }

  /**
   * Calculate memory cost of writing target files in the cross space compaction. Including metadata
   * size of all source files and size of concurrent target chunks.
   *
   * @throws IOException if io errors occurred
   */
  private long calculatingWritingTargetFiles(
      List<TsFileResource> seqResources, TsFileResource unseqResource) throws IOException {
    long cost = 0;
    for (TsFileResource seqResource : seqResources) {
      TsFileSequenceReader reader = getFileReader(seqResource);
      // add seq file metadata size
      cost += reader.getFileMetadataSize();
    }
    // add unseq file metadata size
    cost += getFileReader(unseqResource).getFileMetadataSize();

    // concurrent series chunk size
    long writingTargetCost = maxConcurrentSeriesNum * config.getTargetChunkSize();
    if (writingTargetCost > maxCostOfWritingTargetFile) {
      cost -= maxCostOfWritingTargetFile;
      cost += writingTargetCost;
      maxCostOfWritingTargetFile = writingTargetCost;
    }

    return cost;
  }

  /**
   * Get the details of the tsfile, the returned array contains the following elements in sequence:
   *
   * <p>total chunk num in this tsfile
   *
   * <p>max chunk num of one timeseries in this tsfile
   *
   * <p>max aligned series num in one device. If there is no aligned series in this file, then it
   * turns to be -1.
   *
   * <p>max chunk num of one device in this tsfile
   *
   * @throws IOException if io errors occurred
   */
  private FileInfo getSeriesAndDeviceChunkNum(TsFileSequenceReader reader) throws IOException {
    int totalChunkNum = 0;
    int maxChunkNum = 0;
    int maxAlignedSeriesNumInDevice = -1;
    int maxDeviceChunkNum = 0;
    TsFileDeviceIterator deviceIterator = reader.getAllDevicesIteratorWithIsAligned();
    while (deviceIterator.hasNext()) {
      int deviceChunkNum = 0;
      int alignedSeriesNumInDevice = 0;

      Pair<String, Boolean> deviceWithIsAlignedPair = deviceIterator.next();
      String device = deviceWithIsAlignedPair.left;
      boolean isAlignedDevice = deviceWithIsAlignedPair.right;

      Iterator<Map<String, List<ChunkMetadata>>> measurementChunkMetadataListMapIterator
          = reader.getMeasurementChunkMetadataListMapIterator(device);
      while (measurementChunkMetadataListMapIterator.hasNext()) {
        Map<String, List<ChunkMetadata>> measurementChunkMetadataListMap = measurementChunkMetadataListMapIterator.next();
        if (isAlignedDevice) {
          alignedSeriesNumInDevice += measurementChunkMetadataListMap.size();
        }

        for (Map.Entry<String, List<ChunkMetadata>> measurementChunkMetadataList : measurementChunkMetadataListMap.entrySet()) {
          int currentChunkMetadataListSize = measurementChunkMetadataList.getValue().size();
          deviceChunkNum += currentChunkMetadataListSize;
          totalChunkNum += currentChunkMetadataListSize;
          maxChunkNum = Math.max(maxChunkNum, currentChunkMetadataListSize);
        }
      }
      if (isAlignedDevice) {
        maxAlignedSeriesNumInDevice = Math.max(maxAlignedSeriesNumInDevice, alignedSeriesNumInDevice);
      }
      maxDeviceChunkNum = Math.max(maxDeviceChunkNum, deviceChunkNum);
    }
    return new FileInfo(totalChunkNum, maxChunkNum, maxAlignedSeriesNumInDevice, maxDeviceChunkNum);
  }

  private class FileInfo {
    // total chunk num in this tsfile
    private int totalChunkNum = 0;
    // max chunk num of one timeseries in this tsfile
    private int maxSeriesChunkNum = 0;
    // max aligned series num in one device. If there is no aligned series in this file, then it
    // turns to be -1.
    private int maxAlignedSeriesNumInDevice = -1;
    // max chunk num of one device in this tsfile
    @SuppressWarnings("squid:S1068")
    private int maxDeviceChunkNum = 0;

    public FileInfo(
        int totalChunkNum,
        int maxSeriesChunkNum,
        int maxAlignedSeriesNumInDevice,
        int maxDeviceChunkNum) {
      this.totalChunkNum = totalChunkNum;
      this.maxSeriesChunkNum = maxSeriesChunkNum;
      this.maxAlignedSeriesNumInDevice = maxAlignedSeriesNumInDevice;
      this.maxDeviceChunkNum = maxDeviceChunkNum;
    }
  }
}
