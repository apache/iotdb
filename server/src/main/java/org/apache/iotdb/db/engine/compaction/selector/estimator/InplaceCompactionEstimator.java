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
package org.apache.iotdb.db.engine.compaction.selector.estimator;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InplaceCompactionEstimator extends AbstractCrossSpaceEstimator {
  private static final Logger logger = LoggerFactory.getLogger(InplaceCompactionEstimator.class);
  private static final String LOG_FILE_COST = "Memory cost of file {} is {}";

  private boolean tightEstimate;
  private long maxSeqFileCost;

  // the number of timeseries being compacted at the same time
  private final int concurrentSeriesNum =
      IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum();

  /** Total metadata size of each file. */
  private final Map<TsFileResource, Long> fileMetaSizeMap = new HashMap<>();

  /** Maximum memory cost of querying a timeseries in each file. */
  private final Map<TsFileResource, Long> maxSeriesQueryCostMap = new HashMap<>();

  public InplaceCompactionEstimator() {
    this.tightEstimate = false;
    this.maxSeqFileCost = 0;
  }

  @Override
  public long estimateCrossCompactionMemory(
      List<TsFileResource> seqResources, TsFileResource unseqResource) throws IOException {
    if (tightEstimate) {
      return calculateTightMemoryCost(unseqResource, seqResources);
    } else {
      return calculateLooseMemoryCost(unseqResource, seqResources);
    }
  }

  private long calculateMemoryCost(
      TsFileResource unseqResource,
      List<TsFileResource> seqResources,
      IFileQueryMemMeasurement unseqMeasurement,
      IFileQueryMemMeasurement seqMeasurement)
      throws IOException {
    long cost = 0;
    Long fileCost = unseqMeasurement.measure(unseqResource);
    cost += fileCost;

    for (TsFileResource seqFile : seqResources) {
      fileCost = seqMeasurement.measure(seqFile);
      if (fileCost > maxSeqFileCost) {
        // only one file will be read at the same time, so only the largest one is recorded here
        cost -= maxSeqFileCost;
        cost += fileCost;
        maxSeqFileCost = fileCost;
      }
      // but writing data into a new file may generate the same amount of metadata in memory
      cost += calculateMetadataSize(seqFile);
    }
    return cost;
  }

  private long calculateLooseMemoryCost(
      TsFileResource unseqResource, List<TsFileResource> seqResources) throws IOException {
    return calculateMemoryCost(
        unseqResource, seqResources, TsFileResource::getTsFileSize, this::calculateMetadataSize);
  }

  private long calculateTightMemoryCost(
      TsFileResource unseqResource, List<TsFileResource> seqResources) throws IOException {
    return calculateMemoryCost(
        unseqResource,
        seqResources,
        this::calculateTightUnseqMemoryCost,
        this::calculateTightSeqMemoryCost);
  }

  private long calculateMetadataSize(TsFileResource seqFile) throws IOException {
    Long cost = fileMetaSizeMap.get(seqFile);
    if (cost == null) {
      cost = getFileReader(seqFile).getFileMetadataSize();
      fileMetaSizeMap.put(seqFile, cost);
      logger.debug(LOG_FILE_COST, seqFile, cost);
    }
    return cost;
  }

  private long calculateTightFileMemoryCost(
      TsFileResource seqFile, IFileQueryMemMeasurement measurement) throws IOException {
    Long cost = maxSeriesQueryCostMap.get(seqFile);
    if (cost == null) {
      long[] chunkNums = findTotalAndLargestSeriesChunkNum(seqFile, getFileReader(seqFile));
      long totalChunkNum = chunkNums[0];
      long maxChunkNum = chunkNums[1];
      cost = measurement.measure(seqFile) * maxChunkNum / totalChunkNum;
      maxSeriesQueryCostMap.put(seqFile, cost);
      logger.debug(LOG_FILE_COST, seqFile, cost);
    }
    return cost;
  }

  // this method traverses all ChunkMetadata to find out which series has the most chunks and uses
  // its proportion to all series to get a maximum estimation
  private long calculateTightSeqMemoryCost(TsFileResource seqFile) throws IOException {
    long singleSeriesCost = calculateTightFileMemoryCost(seqFile, this::calculateMetadataSize);
    long multiSeriesCost = concurrentSeriesNum * singleSeriesCost;
    long maxCost = calculateMetadataSize(seqFile);
    return Math.min(multiSeriesCost, maxCost);
  }

  // this method traverses all ChunkMetadata to find out which series has the most chunks and uses
  // its proportion among all series to get a maximum estimation
  private long calculateTightUnseqMemoryCost(TsFileResource unseqFile) throws IOException {
    long singleSeriesCost = calculateTightFileMemoryCost(unseqFile, TsFileResource::getTsFileSize);
    long multiSeriesCost = concurrentSeriesNum * singleSeriesCost;
    long maxCost = unseqFile.getTsFileSize();
    return Math.min(multiSeriesCost, maxCost);
  }

  // returns totalChunkNum of a file and the max number of chunks of a series
  private long[] findTotalAndLargestSeriesChunkNum(
      TsFileResource tsFileResource, TsFileSequenceReader sequenceReader) throws IOException {
    long totalChunkNum = 0;
    long maxChunkNum = Long.MIN_VALUE;
    List<Path> paths = sequenceReader.getAllPaths();

    for (Path path : paths) {
      List<ChunkMetadata> chunkMetadataList = sequenceReader.getChunkMetadataList(path, true);
      totalChunkNum += chunkMetadataList.size();
      maxChunkNum = chunkMetadataList.size() > maxChunkNum ? chunkMetadataList.size() : maxChunkNum;
    }
    logger.debug(
        "In file {}, total chunk num {}, series max chunk num {}",
        tsFileResource,
        totalChunkNum,
        maxChunkNum);
    return new long[] {totalChunkNum, maxChunkNum};
  }

  public void setTightEstimate(boolean tightEstimate) {
    this.tightEstimate = tightEstimate;
  }
}
