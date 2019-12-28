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

package org.apache.iotdb.db.engine.merge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.db.utils.UpgradeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseFileSelector implements IMergeFileSelector{

  private static final Logger logger = LoggerFactory.getLogger(BaseFileSelector.class);
  private static final String LOG_FILE_COST = "Memory cost of file {} is {}";

  protected MergeResource resource;
  protected long totalCost;
  protected long memoryBudget;

  // the number of timeseries being queried at the same time
  protected int concurrentMergeNum = 1;
  protected long tempMaxSeqFileCost;
  protected long maxSeqFileCost;

  /**
   * Total metadata size of each file.
   */
  private Map<TsFileResource, Long> fileMetaSizeMap = new HashMap<>();
  /**
   * Maximum memory cost of querying a timeseries in each file.
   */
  private Map<TsFileResource, Long> maxSeriesQueryCostMap = new HashMap<>();

  protected List<TsFileResource> selectedUnseqFiles;
  protected List<TsFileResource> selectedSeqFiles;

  protected int seqSelectedNum;

  protected TmpSelectedSeqIterable tmpSelectedSeqIterable;
  protected long startTime = 0;
  protected long timeConsumption = 0;
  protected long timeLimit = 0;

  @Override
  public void select() throws MergeException {
    startTime = System.currentTimeMillis();
    timeConsumption = 0;
    timeLimit = IoTDBDescriptor.getInstance().getConfig().getMergeFileSelectionTimeBudget();
    if (timeLimit < 0) {
      timeLimit = Long.MAX_VALUE;
    }
    try {
      logger.info("Selecting merge candidates from {} seqFile, {} unseqFiles",
          resource.getSeqFiles().size(), resource.getUnseqFiles().size());
      select(false);
      if (selectedUnseqFiles.isEmpty()) {
        select(true);
      }
      resource.setSeqFiles(selectedSeqFiles);
      resource.setUnseqFiles(selectedUnseqFiles);
      resource.removeOutdatedSeqReaders();
      if (selectedUnseqFiles.isEmpty() && selectedSeqFiles.isEmpty()) {
        logger.info("No merge candidates are found");
        return;
      }
    } catch (IOException e) {
      throw new MergeException(e);
    }
    if (logger.isInfoEnabled()) {
      logger.info("Selected merge candidates, {} seqFiles, {} unseqFiles, total memory cost {}, "
              + "time consumption {}ms",
          selectedSeqFiles.size(), selectedUnseqFiles.size(), totalCost,
          System.currentTimeMillis() - startTime);
    }
  }

  protected void selectByUnseq(boolean useTightBound) throws IOException {
    seqSelectedNum = 0;
    selectedSeqFiles = new ArrayList<>();
    selectedUnseqFiles = new ArrayList<>();
    long maxSeqFileCost = 0;
    tempMaxSeqFileCost = 0;

    totalCost = 0;

    int unseqIndex = 0;
    long startTime = System.currentTimeMillis();
    long timeConsumption = 0;
    long timeLimit = IoTDBDescriptor.getInstance().getConfig().getMergeFileSelectionTimeBudget();
    if (timeLimit < 0) {
      timeLimit = Long.MAX_VALUE;
    }

    while (unseqIndex < resource.getUnseqFiles().size() && timeConsumption < timeLimit) {
      // select next unseq files
      TsFileResource unseqFile = resource.getUnseqFiles().get(unseqIndex);
      if (UpgradeUtils.isNeedUpgrade(unseqFile)) {
        continue;
      }

      selectOverlappedSeqFiles(unseqFile);

      tempMaxSeqFileCost = maxSeqFileCost;
      long newCost = useTightBound ? calculateTightMemoryCost(unseqFile, startTime,
          timeLimit) :
          calculateLooseMemoryCost(unseqFile, startTime, timeLimit);
      updateCost(newCost, unseqFile);

      unseqIndex++;
      timeConsumption = System.currentTimeMillis() - startTime;
    }
  }

  protected abstract void selectOverlappedSeqFiles(TsFileResource unseqFile);

  protected abstract void updateCost(long newCost, TsFileResource unseqFile);

  protected long calculateMetadataSize(TsFileResource seqFile) throws IOException {
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
      long[] chunkNums = MergeUtils.findTotalAndLargestSeriesChunkNum(seqFile, resource.getFileReader(seqFile));
      long totalChunkNum = chunkNums[0];
      long maxChunkNum = chunkNums[1];
      logger.debug("File {} has {} chunks, max chunk num {}", seqFile, totalChunkNum, maxChunkNum);
      cost = measurement.measure(seqFile) * maxChunkNum / totalChunkNum;
      maxSeriesQueryCostMap.put(seqFile, cost);
      logger.debug(LOG_FILE_COST, seqFile, cost);
    }
    return cost;
  }

  // this method traverses all ChunkMetadata to find out which series has the most chunks and uses
  // its proportion to all series to get a maximum estimation
  protected long calculateTightSeqMemoryCost(TsFileResource seqFile) throws IOException {
    long singleSeriesCost = calculateTightFileMemoryCost(seqFile, this::calculateMetadataSize);
    long multiSeriesCost = concurrentMergeNum * singleSeriesCost;
    long maxCost = calculateMetadataSize(seqFile);
    return Math.min(multiSeriesCost, maxCost);
  }

  // this method traverses all ChunkMetadata to find out which series has the most chunks and uses
  // its proportion among all series to get a maximum estimation
  private long calculateTightUnseqMemoryCost(TsFileResource unseqFile) throws IOException {
    long singleSeriesCost = calculateTightFileMemoryCost(unseqFile, TsFileResource::getFileSize);
    long multiSeriesCost = concurrentMergeNum * singleSeriesCost;
    long maxCost = unseqFile.getFileSize();
    return Math.min(multiSeriesCost, maxCost);
  }

  private long calculateMemoryCost(TsFileResource tmpSelectedUnseqFile,
      IFileQueryMemMeasurement unseqMeasurement,
      IFileQueryMemMeasurement seqMeasurement, long startTime, long timeLimit) throws IOException {
    long cost = 0;
    Long fileReadCost = unseqMeasurement.measure(tmpSelectedUnseqFile);
    cost += fileReadCost;

    for (Integer seqFileIdx : tmpSelectedSeqIterable) {
      TsFileResource seqFile = resource.getSeqFiles().get(seqFileIdx);
      if (UpgradeUtils.isNeedUpgrade(seqFile)) {
        return Long.MAX_VALUE;
      }
      fileReadCost = seqMeasurement.measure(seqFile);
      if (fileReadCost > tempMaxSeqFileCost) {
        // memory used when read data from a seq file:
        // only one file will be read at the same time, so only the largest one is recorded here
        cost -= tempMaxSeqFileCost;
        cost += fileReadCost;
        tempMaxSeqFileCost = fileReadCost;
      }
      // memory used to cache the metadata before the new file is closed
      // but writing data into a new file may generate the same amount of metadata in memory
      cost += calculateMetadataSize(seqFile);
      long timeConsumption = System.currentTimeMillis() - startTime;
      if (timeConsumption > timeLimit) {
        return Long.MAX_VALUE;
      }
    }
    return cost;
  }

  private long calculateLooseMemoryCost(TsFileResource tmpSelectedUnseqFile,
      long startTime, long timeLimit) throws IOException {
    return calculateMemoryCost(tmpSelectedUnseqFile,
        TsFileResource::getFileSize, this::calculateMetadataSize, startTime, timeLimit);
  }

  private long calculateTightMemoryCost(TsFileResource tmpSelectedUnseqFile,
      long startTime, long timeLimit) throws IOException {
    return calculateMemoryCost(tmpSelectedUnseqFile,
        this::calculateTightUnseqMemoryCost, this::calculateTightSeqMemoryCost, startTime, timeLimit);
  }

  @Override
  public void setConcurrentMergeNum(int concurrentMergeNum) {
    this.concurrentMergeNum = concurrentMergeNum;
  }

  @Override
  public MergeResource getResource() {
    return resource;
  }

  @Override
  public List<TsFileResource> getSelectedSeqFiles() {
    return selectedSeqFiles;
  }

  @Override
  public List<TsFileResource> getSelectedUnseqFiles() {
    return selectedUnseqFiles;
  }

  @Override
  public long getTotalCost() {
    return totalCost;
  }

  @Override
  public int getConcurrentMergeNum() {
    return concurrentMergeNum;
  }

  protected abstract static class TmpSelectedSeqIterable implements Iterable<Integer> {
  }
}