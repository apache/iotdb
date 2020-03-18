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

package org.apache.iotdb.db.engine.merge.squeeze.selector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.merge.BaseFileSelector;
import org.apache.iotdb.db.engine.merge.IFileQueryMemMeasurement;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.db.utils.UpgradeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MaxFileMergeFileSelector selects the most files from given seqFiles and unseqFiles which can be
 * merged without exceeding given memory budget. It always assume the number of timeseries being
 * queried at the same time is 1 to maximize the number of file merged.
 */
public class SqueezeMaxFileSelector extends BaseFileSelector {

  private static final Logger logger = LoggerFactory.getLogger(
      SqueezeMaxFileSelector.class);

  // the file selection of squeeze strategy is different from that of inplace strategy, consider:
  // seqFile1 has: (device1, [1,100]) (device2, [1,100])
  // seqFile2 has: (device1, [101,200]) (device2, [101,200])
  // seqFile3 has: (device1, [201,300]) (device2, [201,300])
  // unseqFile1 has: (device1, [1,100]) (device2, [201,300])
  // When using inplace strategy, unseqFile1 will merge with seqFile1 and seqFile3 and generates
  // 2 files which still don't overlap seqFile2.
  // However, when using squeeze strategy, unseqFile1 must also merge with seqFile2, otherwise,
  // the generated file will overlap seqFiles2.
  // As a result, we must find the file that firstly overlaps the unseqFile and the file that
  // lastly overlaps the unseqFile and merge all files in between.
  private int firstOverlapIdx = Integer.MAX_VALUE;
  private int lastOverlapIdx = Integer.MIN_VALUE;

  private int tmpFirstOverlapIdx = Integer.MAX_VALUE;
  private int tmpLastOverlapIdx = Integer.MIN_VALUE;

  private static final String LOG_FILE_COST = "Memory cost of file {} is {}";

  protected MergeResource resource;
  private long totalCost;
  private long memoryBudget;

  // the number of timeseries being queried at the same time
  private int concurrentMergeNum = 1;
  private long tempMaxSeqFileCost;
  private long maxSeqFileCost;

  /**
   * Total metadata size of each file.
   */
  private Map<TsFileResource, Long> fileMetaSizeMap = new HashMap<>();
  /**
   * Maximum memory cost of querying a timeseries in each file.
   */
  private Map<TsFileResource, Long> maxSeriesQueryCostMap = new HashMap<>();

  private List<TsFileResource> selectedUnseqFiles;
  private List<TsFileResource> selectedSeqFiles;

  private int seqSelectedNum;

  private TmpSelectedSeqIterable tmpSelectedSeqIterable;
  protected long startTime = 0;
  private long timeConsumption = 0;
  private long timeLimit = 0;

  public SqueezeMaxFileSelector(MergeResource resource, long memoryBudget) {
    this.resource = resource;
    this.memoryBudget = memoryBudget;
    this.tmpSelectedSeqIterable = new BoarderTmpSeqIter();
  }

  public void select(boolean useTightBound) throws IOException {
    firstOverlapIdx = Integer.MAX_VALUE;
    lastOverlapIdx = Integer.MIN_VALUE;

    tmpFirstOverlapIdx = Integer.MAX_VALUE;
    tmpLastOverlapIdx = Integer.MIN_VALUE;

    logger.info("Select using tight bound:{}", useTightBound);
    selectByUnseq(useTightBound);
    logger.info("After selecting by unseq, first seq index:{}, last seq index:{}", firstOverlapIdx,
        lastOverlapIdx);
    if (firstOverlapIdx <= lastOverlapIdx) {
      // selectByUnseq has found candidates, check if we can extend the selection
      logger.info("Try extending the seq files");
      extendCurrentSelection(useTightBound);
      logger.info("After seq extension, first seq index:{}, last seq index:{}", firstOverlapIdx,
          lastOverlapIdx);
    } else {
      // try selecting only seq files as candidates
      logger.info("Try selecting only seq files");
      selectBySeq(useTightBound);
      logger.info("After seq selection, first seq index:{}, last seq index:{}", firstOverlapIdx,
          lastOverlapIdx);
    }
    for (int i = firstOverlapIdx; i <= lastOverlapIdx; i++) {
      selectedSeqFiles.add(resource.getSeqFiles().get(i));
    }
  }

  private void selectBySeq(boolean useTightBound) throws IOException {
    for (int i = 0; i < resource.getSeqFiles().size() - 1 && timeConsumption < timeLimit; i++) {
      // try to find candidates starting from i
      TsFileResource seqFile = resource.getSeqFiles().get(i);
      logger
          .debug("Try selecting seq file {}/{}, {}", i, resource.getSeqFiles().size() - 1, seqFile);
      long fileCost = calculateSeqFileCost(seqFile, useTightBound);
      if (fileCost < memoryBudget) {
        firstOverlapIdx = i;
        lastOverlapIdx = i;
        totalCost = fileCost;
        logger.debug("Seq file {} can fit memory, search from it", seqFile);
        extendCurrentSelection(useTightBound);
        if (lastOverlapIdx > firstOverlapIdx) {
          // if candidates starting from i are found, return
          return;
        } else {
          totalCost = 0;
          firstOverlapIdx = Integer.MAX_VALUE;
          lastOverlapIdx = Integer.MIN_VALUE;
          logger.debug("The next file of {} cannot fit memory together, search the next file",
              seqFile);
        }
      } else {
        logger.info("File {} cannot fie memory {}/{}", seqFile, fileCost, memoryBudget);
      }
      timeConsumption = System.currentTimeMillis() - startTime;
    }
  }

  // if we have selected seqFiles[3] to seqFiles[6], check if we can add seqFiles[7] into the
  // selection without exceeding the budget
  private void extendCurrentSelection(boolean useTightBound) throws IOException {
    for (int i = lastOverlapIdx + 1;
        i < resource.getSeqFiles().size() && timeConsumption < timeLimit; i++) {
      TsFileResource seqFile = resource.getSeqFiles().get(i);
      logger.debug("Try extending seq file {}", seqFile);
      long fileCost = calculateSeqFileCost(seqFile, useTightBound);

      if (fileCost + totalCost < memoryBudget) {
        maxSeqFileCost = tempMaxSeqFileCost;
        totalCost += fileCost;
        lastOverlapIdx++;
        logger.debug("Extended seq file {}", seqFile);
      } else {
        tempMaxSeqFileCost = maxSeqFileCost;
        logger.debug("Cannot extend seq file {}", seqFile);
        break;
      }
      timeConsumption = System.currentTimeMillis() - startTime;
    }
  }

  private long calculateSeqFileCost(TsFileResource seqFile, boolean useTightBound)
      throws IOException {
    long fileCost = 0;
    long fileReadCost = useTightBound ? calculateTightSeqMemoryCost(seqFile) :
        calculateMetadataSize(seqFile);
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
    return fileCost;
  }

  public void updateCost(long newCost, TsFileResource unseqFile) {
    if (totalCost + newCost < memoryBudget) {
      selectedUnseqFiles.add(unseqFile);
      maxSeqFileCost = tempMaxSeqFileCost;

      firstOverlapIdx = tmpFirstOverlapIdx;
      lastOverlapIdx = tmpLastOverlapIdx;

      int newSeqNum = lastOverlapIdx - firstOverlapIdx + 1;
      int deltaSeqNum = newSeqNum - seqSelectedNum;
      seqSelectedNum = newSeqNum;

      totalCost += newCost;
      logger.debug("Adding a new unseqFile {} and {} seqFiles as candidates, new cost {}, total"
          + " cost {}", unseqFile, deltaSeqNum, newCost, totalCost);
    }
  }

  public void selectOverlappedSeqFiles(TsFileResource unseqFile) {
    if (seqSelectedNum == resource.getSeqFiles().size()) {
      return;
    }

    for (Entry<String, Long> deviceStartTimeEntry : unseqFile.getStartTimeMap().entrySet()) {
      String deviceId = deviceStartTimeEntry.getKey();
      Long unseqStartTime = deviceStartTimeEntry.getValue();
      Long unseqEndTime = unseqFile.getEndTimeMap().get(deviceId);

      boolean noMoreOverlap = false;
      for (int i = 0; i < resource.getSeqFiles().size() && !noMoreOverlap; i++) {
        TsFileResource seqFile = resource.getSeqFiles().get(i);
        if (!seqFile.getEndTimeMap().containsKey(deviceId)) {
          continue;
        }
        Long seqEndTime = seqFile.getEndTimeMap().get(deviceId);
        if (unseqEndTime <= seqEndTime) {
          // the unseqFile overlaps current seqFile
          tmpFirstOverlapIdx = Math.min(firstOverlapIdx, i);
          tmpLastOverlapIdx = Math.max(lastOverlapIdx, i);
          // the device of the unseqFile can not merge with later seqFiles
          noMoreOverlap = true;
        } else if (unseqStartTime <= seqEndTime) {
          // the device of the unseqFile may merge with later seqFiles
          // and the unseqFile overlaps current seqFile
          tmpFirstOverlapIdx = Math.min(firstOverlapIdx, i);
          tmpLastOverlapIdx = Math.max(lastOverlapIdx, i);
        }
      }
    }
  }

  @Override
  public void select() throws MergeException {
    startTime = System.currentTimeMillis();
    timeConsumption = 0;
    timeLimit = IoTDBDescriptor.getInstance().getConfig().getMergeFileSelectionTimeBudget();
    if (timeLimit < 0) {
      timeLimit = Long.MAX_VALUE;
    }
    try {
      logger.debug("Selecting merge candidates from {} seqFile, {} unseqFiles",
          resource.getSeqFiles().size(), resource.getUnseqFiles().size());
      select(false);
      if (selectedUnseqFiles.isEmpty()) {
        select(true);
      }
      resource.setSeqFiles(selectedSeqFiles);
      resource.setUnseqFiles(selectedUnseqFiles);
      resource.removeOutdatedSeqReaders();
      if (selectedUnseqFiles.isEmpty() && selectedSeqFiles.isEmpty()) {
        logger.debug("No merge candidates are found");
        return;
      }
    } catch (IOException e) {
      throw new MergeException(e);
    }
    if (logger.isInfoEnabled()) {
      logger.debug("Selected merge candidates, {} seqFiles, {} unseqFiles, total memory cost {}, "
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
      long[] chunkNums = MergeUtils
          .findTotalAndLargestSeriesChunkNum(seqFile, resource.getFileReader(seqFile));
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
        this::calculateTightUnseqMemoryCost, this::calculateTightSeqMemoryCost, startTime,
        timeLimit);
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

  protected class BoarderTmpSeqIter extends TmpSelectedSeqIterable {

    @Override
    public Iterator<Integer> iterator() {
      return new Iterator<Integer>() {
        int _next = firstOverlapIdx;

        @Override
        public boolean hasNext() {
          return _next <= lastOverlapIdx;
        }

        @Override
        public Integer next() {
          return _next++;
        }
      };
    }
  }
}
