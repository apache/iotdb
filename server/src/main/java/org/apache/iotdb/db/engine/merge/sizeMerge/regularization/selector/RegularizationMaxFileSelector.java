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

package org.apache.iotdb.db.engine.merge.sizeMerge.regularization.selector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.merge.IMergeFileSelector;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.utils.MergeFileSelectorUtils;
import org.apache.iotdb.db.engine.merge.utils.MergeMemCalculator;
import org.apache.iotdb.db.engine.merge.utils.SelectorContext;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.db.utils.UpgradeUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RegularizationMaxFileSelector selects the most files from given seqFiles which can be merged as
 * files with all devices' max and min time larger than given time block
 */
public class RegularizationMaxFileSelector implements IMergeFileSelector {

  private static final Logger logger = LoggerFactory.getLogger(
      RegularizationMaxFileSelector.class);

  private long timeBlock;
  private long memoryBudget;
  private long timeLimit;

  private SelectorContext selectorContext;
  private MergeMemCalculator memCalculator;
  private MergeResource resource;

  private List<TsFileResource> seqFiles;

  public RegularizationMaxFileSelector(Collection<TsFileResource> seqFiles, long budget) {
    this(seqFiles, budget, Long.MIN_VALUE);
  }

  public RegularizationMaxFileSelector(Collection<TsFileResource> seqFiles, long budget,
      long timeLowerBound) {
    this.selectorContext = new SelectorContext();
    this.resource = new MergeResource();
    this.memCalculator = new MergeMemCalculator(this.resource);
    this.memoryBudget = budget;
    this.seqFiles = seqFiles.stream().filter(
        tsFileResource -> MergeFileSelectorUtils.filterResource(tsFileResource, timeLowerBound))
        .collect(Collectors.toList());
    timeLimit = IoTDBDescriptor.getInstance().getConfig().getMergeFileSelectionTimeBudget();
    if (timeLimit < 0) {
      timeLimit = Long.MAX_VALUE;
    }
    timeBlock = IoTDBDescriptor.getInstance().getConfig().getMergeFileTimeBlock();
    if (timeBlock < 0) {
      timeBlock = Long.MAX_VALUE;
    }
  }

  @Override
  public Pair<MergeResource, SelectorContext> selectMergedFiles() throws MergeException {
    this.selectorContext.setStartTime(System.currentTimeMillis());
    this.selectorContext.clearTimeConsumption();
    try {
      logger.info("Selecting merge candidates from {} seqFile", seqFiles.size());
      List<TsFileResource> selectedSeqFiles = select(false);
      if (selectedSeqFiles.isEmpty()) {
        selectedSeqFiles = select(true);
      }
      resource.setSeqFiles(selectedSeqFiles);
      resource.removeOutdatedSeqReaders();
      if (resource.getSeqFiles().isEmpty()) {
        logger.info("No merge candidates are found");
        return new Pair<>(resource, selectorContext);
      }
    } catch (IOException e) {
      throw new MergeException(e);
    }
    if (logger.isInfoEnabled()) {
      logger.info("Selected merge candidates, {} seqFiles, total memory cost {}, "
              + "time consumption {}ms", resource.getSeqFiles().size(), selectorContext.getTotalCost(),
          System.currentTimeMillis() - selectorContext.getStartTime());
    }
    return new Pair<>(resource, selectorContext);
  }

  public List<TsFileResource> select(boolean useTightBound) throws IOException {
    this.selectorContext.setStartTime(System.currentTimeMillis());
    this.selectorContext.clearTimeConsumption();
    this.selectorContext.clearTotalCost();
    int tmpStartIdx = -1;
    int tmpEndIdx = -1;
    int startIdx = -1;
    int endIdx = -1;
    int seqIndex = 0;
    while (seqIndex < seqFiles.size() && this.selectorContext.getTimeConsumption() < timeLimit) {
      TsFileResource seqFile = seqFiles.get(seqIndex);
      if (!UpgradeUtils.isNeedUpgrade(seqFile)) {
        Map<String, Long> endTimeMap = seqFile.getEndTimeMap();
        for (Entry<String, Long> deviceStartTimeEntry : seqFile.getStartTimeMap().entrySet()) {
          String deviceId = deviceStartTimeEntry.getKey();
          Long seqStartTime = deviceStartTimeEntry.getValue();
          Long seqEndTime = endTimeMap.get(deviceId);
          long intervalTime = seqEndTime - seqStartTime;
          if (intervalTime < timeBlock) {
            if (tmpStartIdx != -1) {
              tmpEndIdx = seqIndex;
            } else {
              tmpStartIdx = seqIndex;
            }
            break;
          }
        }
      }
      long newCost = useTightBound ? this.memCalculator
          .calculateTightSeqMemoryCost(seqFile, this.selectorContext.getConcurrentMergeNum())
          : seqFile.getFileSize();
      if (this.selectorContext.getTotalCost() + newCost < memoryBudget) {
        startIdx = tmpStartIdx;
        endIdx = tmpEndIdx;
        this.selectorContext.incTotalCost(newCost);
        logger.debug("Adding a new {} seqFile as candidates, new cost {}, total"
            + " cost {}", seqFile, newCost, this.selectorContext.getTotalCost());
      }
      seqIndex++;
      this.selectorContext.updateTimeConsumption();
    }
    if (startIdx == -1 || endIdx == -1) {
      return new ArrayList<>();
    }
    return seqFiles.subList(startIdx, endIdx + 1);
  }

  List<TsFileResource> getSeqFiles() {
    return seqFiles;
  }
}
