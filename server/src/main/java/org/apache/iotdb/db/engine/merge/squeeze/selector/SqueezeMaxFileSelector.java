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
import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.iotdb.db.engine.merge.BaseFileSelector;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
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
    super.selectByUnseq(useTightBound);
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

  protected void updateCost(long newCost, TsFileResource unseqFile) {
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

  protected void selectOverlappedSeqFiles(TsFileResource unseqFile) {
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
