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

package org.apache.iotdb.db.engine.compaction.cross.rewrite.selector;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.manage.CrossSpaceMergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.db.utils.MergeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * MaxFileMergeFileSelector selects the most files from given seqFiles and unseqFiles which can be
 * merged without exceeding given memory budget. It always assume the number of timeseries being
 * queried at the same time is 1 to maximize the number of file merged.
 */
public class RewriteCompactionFileSelector implements ICrossSpaceMergeFileSelector {

  private static final Logger logger = LoggerFactory.getLogger(RewriteCompactionFileSelector.class);
  private static final String LOG_FILE_COST = "Memory cost of file {} is {}";

  CrossSpaceMergeResource resource;

  long totalCost;
  private long memoryBudget;
  private long maxSeqFileCost;

  // the number of timeseries being queried at the same time
  int concurrentMergeNum = 1;

  /** Total metadata size of each file. */
  private Map<TsFileResource, Long> fileMetaSizeMap = new HashMap<>();
  /** Maximum memory cost of querying a timeseries in each file. */
  private Map<TsFileResource, Long> maxSeriesQueryCostMap = new HashMap<>();

  List<TsFileResource> selectedUnseqFiles;
  List<TsFileResource> selectedSeqFiles;

  private Collection<Integer> tmpSelectedSeqFiles;
  private long tempMaxSeqFileCost;

  private boolean[] seqSelected;
  private int seqSelectedNum;

  public RewriteCompactionFileSelector(CrossSpaceMergeResource resource, long memoryBudget) {
    this.resource = resource;
    this.memoryBudget = memoryBudget;
  }

  /**
   * Select merge candidates from seqFiles and unseqFiles under the given memoryBudget. This process
   * iteratively adds the next unseqFile from unseqFiles and its overlapping seqFiles as newly-added
   * candidates and computes their estimated memory cost. If the current cost pluses the new cost is
   * still under the budget, accept the unseqFile and the seqFiles as candidates, otherwise go to
   * the next iteration. The memory cost of a file is calculated in two ways: The rough estimation:
   * for a seqFile, the size of its metadata is used for estimation. Since in the worst case, the
   * file only contains one timeseries and all its metadata will be loaded into memory with at most
   * one actual data chunk (which is negligible) and writing the timeseries into a new file generate
   * metadata of the similar size, so the size of all seqFiles' metadata (generated when writing new
   * chunks) pluses the largest one (loaded when reading a timeseries from the seqFiles) is the
   * total estimation of all seqFiles; for an unseqFile, since the merge reader may read all chunks
   * of a series to perform a merge read, the whole file may be loaded into memory, so we use the
   * file's length as the maximum estimation. The tight estimation: based on the rough estimation,
   * we scan the file's metadata to count the number of chunks for each series, find the series
   * which have the most chunks in the file and use its chunk proportion to refine the rough
   * estimation. The rough estimation is performed first, if no candidates can be found using rough
   * estimation, we run the selection again with tight estimation.
   *
   * @return two lists of TsFileResource, the former is selected seqFiles and the latter is selected
   *     unseqFiles or an empty array if there are no proper candidates by the budget.
   */
  @Override
  public List[] select() throws MergeException {
    long startTime = System.currentTimeMillis();
    try {
      logger.info(
          "Selecting merge candidates from {} seqFile, {} unseqFiles",
          resource.getSeqFiles().size(),
          resource.getUnseqFiles().size());
      select(false);
      if (selectedUnseqFiles.isEmpty()) {
        select(true);
      }
      resource.setSeqFiles(selectedSeqFiles);
      resource.setUnseqFiles(selectedUnseqFiles);
      resource.removeOutdatedSeqReaders();
      if (selectedUnseqFiles.isEmpty()) {
        logger.info("No merge candidates are found");
        return new List[0];
      }
    } catch (IOException e) {
      throw new MergeException(e);
    }
    if (logger.isInfoEnabled()) {
      logger.info(
          "Selected merge candidates, {} seqFiles, {} unseqFiles, total memory cost {}, "
              + "time consumption {}ms",
          selectedSeqFiles.size(),
          selectedUnseqFiles.size(),
          totalCost,
          System.currentTimeMillis() - startTime);
    }
    return new List[] {selectedSeqFiles, selectedUnseqFiles};
  }

  /**
   * In a preset time (30 seconds), for each unseqFile, find the list of seqFiles that overlap with
   * it and have not been selected by the file selector of this compaction task. After finding each
   * unseqFile and its corresponding overlap seqFile list, estimate the additional memory overhead
   * that may be added by compacting them (preferably using the loop estimate), and if it does not
   * exceed the memory overhead preset by the system for the compaction thread, put them into the
   * selectedSeqFiles and selectedUnseqFiles.
   *
   * @param useTightBound whether is tight estimate or loop estimate
   * @throws IOException
   */
  void select(boolean useTightBound) throws IOException {
    tmpSelectedSeqFiles = new HashSet<>();
    seqSelected = new boolean[resource.getSeqFiles().size()];
    seqSelectedNum = 0;
    selectedSeqFiles = new ArrayList<>();
    selectedUnseqFiles = new ArrayList<>();
    maxSeqFileCost = 0;
    tempMaxSeqFileCost = 0;

    totalCost = 0;

    int unseqIndex = 0;
    long startTime = System.currentTimeMillis();
    long timeConsumption = 0;
    long timeLimit =
        IoTDBDescriptor.getInstance().getConfig().getCrossCompactionFileSelectionTimeBudget();
    if (timeLimit < 0) {
      timeLimit = Long.MAX_VALUE;
    }
    while (unseqIndex < resource.getUnseqFiles().size() && timeConsumption < timeLimit) {
      // select next unseq files
      TsFileResource unseqFile = resource.getUnseqFiles().get(unseqIndex);

      if (seqSelectedNum != resource.getSeqFiles().size()) {
        selectOverlappedSeqFiles(unseqFile);
      }
      boolean isSeqFilesValid = checkIsSeqFilesValid();
      if (!isSeqFilesValid) {
        tmpSelectedSeqFiles.clear();
        break;
      }

      tempMaxSeqFileCost = maxSeqFileCost;
      long newCost =
          useTightBound
              ? calculateTightMemoryCost(unseqFile, tmpSelectedSeqFiles, startTime, timeLimit)
              : calculateLooseMemoryCost(unseqFile, tmpSelectedSeqFiles, startTime, timeLimit);
      if (!updateSelectedFiles(newCost, unseqFile)) {
        // older unseq files must be merged before newer ones
        break;
      }

      tmpSelectedSeqFiles.clear();
      unseqIndex++;
      timeConsumption = System.currentTimeMillis() - startTime;
    }
    for (int i = 0; i < seqSelected.length; i++) {
      if (seqSelected[i]) {
        selectedSeqFiles.add(resource.getSeqFiles().get(i));
      }
    }
  }

  private boolean updateSelectedFiles(long newCost, TsFileResource unseqFile) {
    if (totalCost + newCost < memoryBudget) {
      selectedUnseqFiles.add(unseqFile);
      maxSeqFileCost = tempMaxSeqFileCost;

      for (Integer seqIdx : tmpSelectedSeqFiles) {
        if (!seqSelected[seqIdx]) {
          seqSelectedNum++;
          seqSelected[seqIdx] = true;
        }
      }
      totalCost += newCost;
      logger.debug(
          "Adding a new unseqFile {} and seqFiles {} as candidates, new cost {}, total"
              + " cost {}",
          unseqFile,
          tmpSelectedSeqFiles,
          newCost,
          totalCost);
      return true;
    }
    return false;
  }

  /**
   * To avoid redundant data in seq files, cross space compaction should select all the seq files
   * which have overlap with unseq files whether they are compacting or not. Therefore, before
   * adding task into the queue, cross space compaction task should be check whether source seq
   * files are being compacted or not to speed up compaction.
   */
  private boolean checkIsSeqFilesValid() {
    for (Integer seqIdx : tmpSelectedSeqFiles) {
      if (resource.getSeqFiles().get(seqIdx).isCompactionCandidate()
          || resource.getSeqFiles().get(seqIdx).isCompacting()
          || !resource.getSeqFiles().get(seqIdx).isClosed()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Put the index of the seqFile that has an overlap with the specific unseqFile and has not been
   * selected by the file selector of the compaction task into the tmpSelectedSeqFiles list. To
   * determine whether overlap exists is to traverse each device ChunkGroup in unseqFiles, and
   * determine whether it overlaps with the same device ChunkGroup of each seqFile that are not
   * selected by the compaction task, if so, select this seqFile.
   *
   * @param unseqFile the tsFileResource of unseqFile to be compacted
   */
  private void selectOverlappedSeqFiles(TsFileResource unseqFile) {
    int tmpSelectedNum = 0;
    for (String deviceId : unseqFile.getDevices()) {
      long unseqStartTime = unseqFile.getStartTime(deviceId);
      long unseqEndTime = unseqFile.getEndTime(deviceId);

      boolean noMoreOverlap = false;
      for (int i = 0; i < resource.getSeqFiles().size() && !noMoreOverlap; i++) {
        TsFileResource seqFile = resource.getSeqFiles().get(i);
        if (!seqFile.mayContainsDevice(deviceId)) {
          continue;
        }

        long seqEndTime = seqFile.getEndTime(deviceId);
        long seqStartTime = seqFile.getStartTime(deviceId);
        if (unseqEndTime < seqStartTime) {
          // Suppose the time range in unseq file is 10-20, seq file is 30-40. If this unseq file
          // has no overlapped seq files, then select this seq file. Otherwise, skip this seq file.
          // There is no more overlap later.
          if (tmpSelectedSeqFiles.size() == 0) {
            tmpSelectedSeqFiles.add(i);
          }
          noMoreOverlap = true;
        } else if (!seqFile.isClosed()) {
          // we cannot make sure whether unclosed file has overlap or not, so we just add it.
          tmpSelectedSeqFiles.add(i);
          tmpSelectedNum++;
        } else if (unseqEndTime <= seqEndTime) {
          // if time range in unseq file is 10-20, seq file is 15-25, then select this seq file and
          // there is no more overlap later.
          tmpSelectedSeqFiles.add(i);
          tmpSelectedNum++;
          noMoreOverlap = true;
        } else if (unseqStartTime <= seqEndTime) {
          // if time range in unseq file is 10-20, seq file is 0-15, then select this seq file and
          // there may be overlap later.
          tmpSelectedSeqFiles.add(i);
          tmpSelectedNum++;
        }
      }
      if (tmpSelectedNum + seqSelectedNum == resource.getSeqFiles().size()) {
        break;
      }
    }
  }

  private long calculateMemoryCost(
      TsFileResource tmpSelectedUnseqFile,
      Collection<Integer> tmpSelectedSeqFiles,
      IFileQueryMemMeasurement unseqMeasurement,
      IFileQueryMemMeasurement seqMeasurement,
      long startTime,
      long timeLimit)
      throws IOException {
    long cost = 0;
    Long fileCost = unseqMeasurement.measure(tmpSelectedUnseqFile);
    cost += fileCost;

    for (Integer seqFileIdx : tmpSelectedSeqFiles) {
      TsFileResource seqFile = resource.getSeqFiles().get(seqFileIdx);
      fileCost = seqMeasurement.measure(seqFile);
      if (fileCost > tempMaxSeqFileCost) {
        // only one file will be read at the same time, so only the largest one is recorded here
        cost -= tempMaxSeqFileCost;
        cost += fileCost;
        tempMaxSeqFileCost = fileCost;
      }
      // but writing data into a new file may generate the same amount of metadata in memory
      cost += calculateMetadataSize(seqFile);
      long timeConsumption = System.currentTimeMillis() - startTime;
      if (timeConsumption > timeLimit) {
        return Long.MAX_VALUE;
      }
    }
    return cost;
  }

  private long calculateLooseMemoryCost(
      TsFileResource tmpSelectedUnseqFile,
      Collection<Integer> tmpSelectedSeqFiles,
      long startTime,
      long timeLimit)
      throws IOException {
    return calculateMemoryCost(
        tmpSelectedUnseqFile,
        tmpSelectedSeqFiles,
        TsFileResource::getTsFileSize,
        this::calculateMetadataSize,
        startTime,
        timeLimit);
  }

  private long calculateTightMemoryCost(
      TsFileResource tmpSelectedUnseqFile,
      Collection<Integer> tmpSelectedSeqFiles,
      long startTime,
      long timeLimit)
      throws IOException {
    return calculateMemoryCost(
        tmpSelectedUnseqFile,
        tmpSelectedSeqFiles,
        this::calculateTightUnseqMemoryCost,
        this::calculateTightSeqMemoryCost,
        startTime,
        timeLimit);
  }

  private long calculateMetadataSize(TsFileResource seqFile) throws IOException {
    Long cost = fileMetaSizeMap.get(seqFile);
    if (cost == null) {
      cost = MergeUtils.getFileMetaSize(seqFile, resource.getFileReader(seqFile));
      fileMetaSizeMap.put(seqFile, cost);
      logger.debug(LOG_FILE_COST, seqFile, cost);
    }
    return cost;
  }

  private long calculateTightFileMemoryCost(
      TsFileResource seqFile, IFileQueryMemMeasurement measurement) throws IOException {
    Long cost = maxSeriesQueryCostMap.get(seqFile);
    if (cost == null) {
      long[] chunkNums =
          MergeUtils.findTotalAndLargestSeriesChunkNum(seqFile, resource.getFileReader(seqFile));
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
    long multiSeriesCost = concurrentMergeNum * singleSeriesCost;
    long maxCost = calculateMetadataSize(seqFile);
    return Math.min(multiSeriesCost, maxCost);
  }

  // this method traverses all ChunkMetadata to find out which series has the most chunks and uses
  // its proportion among all series to get a maximum estimation
  private long calculateTightUnseqMemoryCost(TsFileResource unseqFile) throws IOException {
    long singleSeriesCost = calculateTightFileMemoryCost(unseqFile, TsFileResource::getTsFileSize);
    long multiSeriesCost = concurrentMergeNum * singleSeriesCost;
    long maxCost = unseqFile.getTsFileSize();
    return Math.min(multiSeriesCost, maxCost);
  }

  @Override
  public int getConcurrentMergeNum() {
    return concurrentMergeNum;
  }
}
