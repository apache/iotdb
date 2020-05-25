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
package org.apache.iotdb.db.engine.merge.utils;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeMemCalculator {

  private static final Logger logger = LoggerFactory
      .getLogger(MergeMemCalculator.class);
  private static final String LOG_FILE_COST = "Memory cost of file {} is {}";
  /**
   * Total metadata size of each file.
   */
  private Map<TsFileResource, Long> fileMetaSizeMap;
  /**
   * Maximum memory cost of querying a timeseries in each file.
   */
  private Map<TsFileResource, Long> maxSeriesQueryCostMap;
  private MergeResource resource;

  public MergeMemCalculator(MergeResource resource) {
    this.fileMetaSizeMap = new HashMap<>();
    maxSeriesQueryCostMap = new HashMap<>();
    this.resource = resource;
  }

  public Pair<Long, Long> calculateSeqFileCost(TsFileResource seqFile, boolean useTightBound,
      long tempMaxSeqFileCost)
      throws IOException {
    long fileCost = 0;
    long fileReadCost = useTightBound ? calculateTightSeqMemoryCost(seqFile)
        : calculateMetadataSize(seqFile);
    logger.debug("File read cost of {} is {}", seqFile, fileReadCost);
    if (fileReadCost > tempMaxSeqFileCost) {
      // memory used when read data from a seq file:
      // only one file will be read at the same time, so only the largest one is recorded here
      fileCost -= tempMaxSeqFileCost;
      fileCost += fileReadCost;
      tempMaxSeqFileCost = fileReadCost;
    }
    // memory used to cache the metadata before the new file is closed
    // but writing data into a new file may generate the same amount of metadata in memory
    fileCost += calculateMetadataSize(seqFile);
    logger.debug("File cost of {} is {}", seqFile, fileCost);
    return new Pair<>(fileCost, tempMaxSeqFileCost);
  }

  public long calculateLooseMemoryCost(TsFileResource tmpSelectedUnseqFile,
      Collection<Integer> tmpSelectedSeqFileIdxs, List<TsFileResource> seqFiles, long startTime,
      long timeLimit)
      throws IOException {
    long cost = 0;
    Long fileCost = tmpSelectedUnseqFile.getFileSize();
    cost += fileCost;

    long tempMaxSeqFileCost = 0;
    for (Integer seqFileIdx : tmpSelectedSeqFileIdxs) {
      TsFileResource seqFile = seqFiles.get(seqFileIdx);
      fileCost = calculateMetadataSize(seqFile);
      if (fileCost > tempMaxSeqFileCost) {
        // only one file will be read at the same time, so only the largest one is recorded here
        tempMaxSeqFileCost = fileCost;
      }
      // but writing data into a new file may generate the same amount of metadata in memory
      cost += calculateMetadataSize(seqFile);
      long timeConsumption = System.currentTimeMillis() - startTime;
      if (timeConsumption > timeLimit) {
        return Long.MAX_VALUE;
      }
    }
    cost += tempMaxSeqFileCost;
    return cost;
  }

  public long calculateLooseMemoryCost(TsFileResource tmpSelectedUnseqFile,
      Collection<TsFileResource> tmpSelectedSeqFiles, long startTime,
      long timeLimit)
      throws IOException {
    long cost = 0;
    Long fileCost = tmpSelectedUnseqFile.getFileSize();
    cost += fileCost;

    long tempMaxSeqFileCost = 0;
    for (TsFileResource seqFile : tmpSelectedSeqFiles) {
      fileCost = calculateMetadataSize(seqFile);
      if (fileCost > tempMaxSeqFileCost) {
        // only one file will be read at the same time, so only the largest one is recorded here
        tempMaxSeqFileCost = fileCost;
      }
      // but writing data into a new file may generate the same amount of metadata in memory
      cost += calculateMetadataSize(seqFile);
      long timeConsumption = System.currentTimeMillis() - startTime;
      if (timeConsumption > timeLimit) {
        return Long.MAX_VALUE;
      }
    }
    cost += tempMaxSeqFileCost;
    return cost;
  }

  public long calculateTightMemoryCost(TsFileResource tmpSelectedUnseqFile,
      Collection<Integer> tmpSelectedSeqFileIdxs, List<TsFileResource> seqFiles, long startTime,
      long timeLimit)
      throws IOException {
    long cost = 0;
    Long fileCost = calculateTightUnseqMemoryCost(tmpSelectedUnseqFile);
    cost += fileCost;

    long tempMaxSeqFileCost = 0;
    for (Integer seqFileIdx : tmpSelectedSeqFileIdxs) {
      TsFileResource seqFile = seqFiles.get(seqFileIdx);
      fileCost = calculateTightSeqMemoryCost(seqFile);
      if (fileCost > tempMaxSeqFileCost) {
        // only one file will be read at the same time, so only the largest one is recorded here\
        tempMaxSeqFileCost = fileCost;
      }
      // but writing data into a new file may generate the same amount of metadata in memory
      cost += calculateMetadataSize(seqFile);
      long timeConsumption = System.currentTimeMillis() - startTime;
      if (timeConsumption > timeLimit) {
        return Long.MAX_VALUE;
      }
    }
    cost += tempMaxSeqFileCost;
    return cost;
  }

  public long calculateTightMemoryCost(TsFileResource tmpSelectedUnseqFile,
      Collection<TsFileResource> tmpSelectedSeqFiles, long startTime,
      long timeLimit)
      throws IOException {
    long cost = 0;
    Long fileCost = calculateTightUnseqMemoryCost(tmpSelectedUnseqFile);
    cost += fileCost;

    long tempMaxSeqFileCost = 0;
    for (TsFileResource seqFile : tmpSelectedSeqFiles) {
      fileCost = calculateTightSeqMemoryCost(seqFile);
      if (fileCost > tempMaxSeqFileCost) {
        // only one file will be read at the same time, so only the largest one is recorded here\
        tempMaxSeqFileCost = fileCost;
      }
      // but writing data into a new file may generate the same amount of metadata in memory
      cost += calculateMetadataSize(seqFile);
      long timeConsumption = System.currentTimeMillis() - startTime;
      if (timeConsumption > timeLimit) {
        return Long.MAX_VALUE;
      }
    }
    cost += tempMaxSeqFileCost;
    return cost;
  }

  public long calculateMetadataSize(TsFileResource seqFile)
      throws IOException {
    Long cost = fileMetaSizeMap.get(seqFile);
    if (cost == null) {
      cost = MergeUtils.getFileMetaSize(seqFile, resource.getFileReader(seqFile));
      fileMetaSizeMap.put(seqFile, cost);
      logger.debug(LOG_FILE_COST, seqFile, cost);
    }
    return cost;
  }

  private long calculateTightFileMemoryCost(TsFileResource seqFile,
      IFileQueryMemMeasurement measurement)
      throws IOException {
    Long cost = maxSeriesQueryCostMap.get(seqFile);
    if (cost == null) {
      long[] chunkNums = MergeUtils
          .findTotalAndLargestSeriesChunkNum(seqFile, resource.getFileReader(seqFile));
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
  public long calculateTightSeqMemoryCost(TsFileResource seqFile)
      throws IOException {
    long multiSeriesCost = calculateTightFileMemoryCost(seqFile, this::calculateMetadataSize);
    long maxCost = calculateMetadataSize(seqFile);
    return multiSeriesCost > maxCost ? maxCost : multiSeriesCost;
  }

  // this method traverses all ChunkMetadata to find out which series has the most chunks and uses
  // its proportion among all series to get a maximum estimation
  private long calculateTightUnseqMemoryCost(TsFileResource unseqFile)
      throws IOException {
    long multiSeriesCost = calculateTightFileMemoryCost(unseqFile, TsFileResource::getFileSize);
    long maxCost = unseqFile.getFileSize();
    return multiSeriesCost > maxCost ? maxCost : multiSeriesCost;
  }

  /**
   * Estimate how much memory a file may occupy when being queried during merge.
   *
   * @return
   * @throws IOException
   */
  @FunctionalInterface
  interface IFileQueryMemMeasurement {

    long measure(TsFileResource resource) throws IOException;
  }
}
