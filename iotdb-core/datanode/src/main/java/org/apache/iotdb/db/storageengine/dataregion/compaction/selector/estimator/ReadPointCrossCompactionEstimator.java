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
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
    return CompactionEstimateUtils.addReadLock(allResources);
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
    FileInfo fileInfo = CompactionEstimateUtils.getSeriesAndDeviceChunkNum(reader);
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
      FileInfo fileInfo = CompactionEstimateUtils.getSeriesAndDeviceChunkNum(reader);
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
}
