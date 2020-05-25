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
package org.apache.iotdb.db.engine.merge.seqMerge.inplace.selector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.merge.BaseFileSelector;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.utils.SelectorContext;
import org.apache.iotdb.db.engine.merge.utils.MergeFileSelectorUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.db.utils.UpgradeUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InplaceMaxFileSelector extends BaseFileSelector {

  private static final Logger logger = LoggerFactory
      .getLogger(InplaceMaxFileSelector.class);

  public InplaceMaxFileSelector(Collection<TsFileResource> seqFiles,
      Collection<TsFileResource> unseqFiles, long budget) {
    this(seqFiles, unseqFiles, budget, Long.MIN_VALUE);
  }

  public InplaceMaxFileSelector(Collection<TsFileResource> seqFiles,
      Collection<TsFileResource> unseqFiles, long budget, long timeLowerBound) {
    super(seqFiles, unseqFiles, budget, timeLowerBound);
    this.timeLimit = IoTDBDescriptor.getInstance().getConfig().getMergeFileSelectionTimeBudget();
    if (this.timeLimit < 0) {
      this.timeLimit = Long.MAX_VALUE;
    }
  }

  @Override
  public Pair<MergeResource, SelectorContext> selectMergedFiles() throws MergeException {
    return select();
  }

  protected Pair<List<TsFileResource>, List<TsFileResource>> select(boolean useTightBound)
      throws IOException {
    Set<Integer> selectedSeqFileIdxs = new HashSet<>();
    List<TsFileResource> selectedUnseqFiles = new ArrayList<>();
    long maxSeqFileCost = 0;
    long tempMaxSeqFileCost;
    int unseqIndex = 0;
    this.selectorContext.setStartTime(System.currentTimeMillis());
    this.selectorContext.clearTimeConsumption();
    this.selectorContext.clearTotalCost();
    while (unseqIndex < unseqFiles.size()
        && this.selectorContext.getTimeConsumption() < timeLimit) {
      Set<Integer> tmpSelectedSeqFiles = new HashSet<>();
      // select next unseq files
      TsFileResource unseqFile = unseqFiles.get(unseqIndex);

      if (tmpSelectedSeqFiles.size() != seqFiles.size() && !UpgradeUtils
          .isNeedUpgrade(unseqFile)) {
        int tmpSelectedNum = 0;
        for (Entry<String, Long> deviceStartTimeEntry : unseqFile.getStartTimeMap().entrySet()) {
          String deviceId = deviceStartTimeEntry.getKey();
          Long unseqStartTime = deviceStartTimeEntry.getValue();
          Long unseqEndTime = unseqFile.getEndTimeMap().get(deviceId);

          boolean noMoreOverlap = false;
          for (int i = 0; i < seqFiles.size() && !noMoreOverlap; i++) {
            TsFileResource seqFile = seqFiles.get(i);
            if (selectedSeqFileIdxs.contains(i) || !seqFile.getEndTimeMap().containsKey(deviceId)) {
              continue;
            }
            long seqEndTime = seqFile.getEndTimeMap().get(deviceId);
            if (unseqEndTime <= seqEndTime) {
              // the unseqFile overlaps current seqFile
              tmpSelectedSeqFiles.add(i);
              tmpSelectedNum++;
              // the device of the unseqFile can not merge with later seqFiles
              noMoreOverlap = true;
            } else if (unseqStartTime <= seqEndTime) {
              // the device of the unseqFile may merge with later seqFiles
              // and the unseqFile overlaps current seqFile
              tmpSelectedSeqFiles.add(i);
              tmpSelectedNum++;
            }
          }
          if (tmpSelectedNum + tmpSelectedSeqFiles.size() == seqFiles.size()) {
            break;
          }
        }
      }

      // skip if the unseqFile and tmpSelectedSeqFiles has TsFileResources that need to be upgraded
      if (MergeFileSelectorUtils.checkForUpgrade(unseqFile, tmpSelectedSeqFiles, this.seqFiles)) {
        tmpSelectedSeqFiles.clear();
        unseqIndex++;
        this.selectorContext.updateTimeConsumption();
        continue;
      }

      tempMaxSeqFileCost = maxSeqFileCost;
      long newCost = useTightBound ? this.memCalculator
          .calculateTightMemoryCost(unseqFile, tmpSelectedSeqFiles, this.seqFiles,
              this.selectorContext.getStartTime(), timeLimit) : this.memCalculator
          .calculateLooseMemoryCost(unseqFile, tmpSelectedSeqFiles, this.seqFiles,
              this.selectorContext.getStartTime(), timeLimit);
      if (selectorContext.getTotalCost() + newCost < memoryBudget) {
        selectedUnseqFiles.add(unseqFile);
        maxSeqFileCost = tempMaxSeqFileCost;
        selectorContext.incTotalCost(newCost);
        logger.debug("Adding a new unseqFile {} and seqFiles {} as candidates, new cost {}, total"
                + " cost {}",
            unseqFile, tmpSelectedSeqFiles, newCost, selectorContext.getTotalCost());
        selectedSeqFileIdxs.addAll(tmpSelectedSeqFiles);
      }

      tmpSelectedSeqFiles.clear();
      unseqIndex++;
      this.selectorContext.updateTimeConsumption();
    }
    List<TsFileResource> selectedSeqFiles = new ArrayList<>();
    for (Integer seqIdx : selectedSeqFileIdxs) {
      selectedSeqFiles.add(seqFiles.get(seqIdx));
    }
    return new Pair<>(selectedSeqFiles, selectedUnseqFiles);
  }

  List<TsFileResource> getSeqFiles() {
    return seqFiles;
  }

  List<TsFileResource> getUnseqFiles() {
    return unseqFiles;
  }

  long getTotalCost() {
    return selectorContext.getTotalCost();
  }
}
