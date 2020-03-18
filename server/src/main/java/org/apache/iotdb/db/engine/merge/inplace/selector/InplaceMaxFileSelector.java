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

package org.apache.iotdb.db.engine.merge.inplace.selector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.merge.BaseFileSelector;
import org.apache.iotdb.db.engine.merge.IFileQueryMemMeasurement;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.selection.FileSelectionUtils;
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
public class InplaceMaxFileSelector extends BaseFileSelector {

  private static final Logger logger = LoggerFactory.getLogger(InplaceMaxFileSelector.class);

  private Collection<Integer> tmpSelectedSeqFiles;

  private boolean[] seqSelected;

  private static final String LOG_FILE_COST = "Memory cost of file {} is {}";

  protected MergeResource resource;
  private long totalCost;
  private long memoryBudget;

  // the number of timeseries being queried at the same time
  private int concurrentMergeNum = 1;

  private long tempMaxSeqFileCost;

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

  public InplaceMaxFileSelector(MergeResource resource, long memoryBudget) {
    this.resource = resource;
    this.memoryBudget = memoryBudget;
    this.tmpSelectedSeqIterable = new ListTmpSeqIter();
  }

  @Override
  public void select(boolean useTightBound) throws IOException {
    tmpSelectedSeqFiles = new HashSet<>();
    seqSelected = new boolean[resource.getSeqFiles().size()];
    selectByUnseq(useTightBound);
  }

  @Override
  public void updateCost(long newCost, TsFileResource unseqFile) {
    if (totalCost + newCost < memoryBudget) {
      selectedUnseqFiles.add(unseqFile);

      for (Integer seqIdx : tmpSelectedSeqFiles) {
        seqSelected[seqIdx] = true;
        seqSelectedNum++;
        selectedSeqFiles.add(resource.getSeqFiles().get(seqIdx));
      }
      totalCost += newCost;
      logger.debug("Adding a new unseqFile {} and seqFiles {} as candidates, new cost {}, total"
              + " cost {}",
          unseqFile, tmpSelectedSeqFiles, newCost, totalCost);
    }
    tmpSelectedSeqFiles.clear();
  }

  public void selectOverlappedSeqFiles(TsFileResource unseqFile) {
    if (seqSelectedNum == resource.getSeqFiles().size()) {
      return;
    }
    int tmpSelectedNum = 0;
    for (Entry<String, Long> deviceStartTimeEntry : unseqFile.getStartTimeMap().entrySet()) {
      String deviceId = deviceStartTimeEntry.getKey();
      Long unseqStartTime = deviceStartTimeEntry.getValue();
      Long unseqEndTime = unseqFile.getEndTimeMap().get(deviceId);

      boolean noMoreOverlap = false;
      for (int i = 0; i < resource.getSeqFiles().size() && !noMoreOverlap; i++) {
        TsFileResource seqFile = resource.getSeqFiles().get(i);
        if (seqSelected[i] || !seqFile.getEndTimeMap().containsKey(deviceId)) {
          continue;
        }
        Long seqEndTime = seqFile.getEndTimeMap().get(deviceId);
        if (unseqEndTime <= seqEndTime) {
          // the unseqFile overlaps current seqFile
          if (!tmpSelectedSeqFiles.contains(i)) {
            tmpSelectedSeqFiles.add(i);
            tmpSelectedNum++;
          }
          // the device of the unseqFile can not merge with later seqFiles
          noMoreOverlap = true;
        } else if (unseqStartTime <= seqEndTime) {
          // the device of the unseqFile may merge with later seqFiles
          // and the unseqFile overlaps current seqFile
          if (!tmpSelectedSeqFiles.contains(i)) {
            tmpSelectedSeqFiles.add(i);
            tmpSelectedNum++;
          }
        }
      }
      if (tmpSelectedNum + seqSelectedNum == resource.getSeqFiles().size()) {
        break;
      }
    }
  }

  @Override
  public void select() throws MergeException {
    FileSelectionUtils.select(resource, selectedUnseqFiles, selectedSeqFiles, totalCost,
        useTightBound -> {
          try {
            select(useTightBound);
          } catch (IOException ignored) {
          }
        });
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

  protected class ListTmpSeqIter extends TmpSelectedSeqIterable {

    @Override
    public Iterator<Integer> iterator() {
      return tmpSelectedSeqFiles.iterator();
    }
  }
}
