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
import java.util.Collections;
import java.util.List;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MaxSeriesMergeFileSelector is an extension of IMergeFileSelector which tries to maximize the
 * number of timeseries that can be merged at the same time.
 */
public class MaxSeriesMergeFileSelector<T extends IMergeFileSelector> implements IMergeFileSelector {

  private T baseSelector;
  private MergeResource resource;

  public static final int MAX_SERIES_NUM = 256;
  private static final Logger logger = LoggerFactory.getLogger(
      MaxSeriesMergeFileSelector.class);

  private List<TsFileResource> lastSelectedSeqFiles = Collections.emptyList();
  private List<TsFileResource> lastSelectedUnseqFiles = Collections.emptyList();
  private long lastTotalMemoryCost;

  private int concurrentMergeNum;
  private long totalCost;

  private long startTime;
  private long timeConsumption;
  private long timeLimit;


  public MaxSeriesMergeFileSelector(T baseSelector) {
    this.baseSelector = baseSelector;
    this.resource = baseSelector.getResource();
  }

  @Override
  public void select() throws MergeException {
    startTime = System.currentTimeMillis();
    timeLimit = IoTDBDescriptor.getInstance().getConfig().getMergeFileSelectionTimeBudget();
    timeConsumption = 0;
    try {
      logger.debug("Selecting merge candidates from {} seqFile, {} unseqFiles",
          resource.getSeqFiles().size(),
          resource.getUnseqFiles().size());

      searchMaxSeriesNum();
      List<TsFileResource> selectedSeqFiles = baseSelector.getSelectedSeqFiles();
      List<TsFileResource> selectedUnseqFiles = baseSelector.getSelectedUnseqFiles();
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
              + "concurrent merge num {}" + "time consumption {}ms",
          resource.getSeqFiles().size(), resource.getUnseqFiles().size(), baseSelector.getTotalCost(),
          baseSelector.getConcurrentMergeNum(),
          System.currentTimeMillis() - startTime);
    }
  }

  @Override
  public void select(boolean useTightBound) throws MergeException {
    select();
  }

  @Override
  public int getConcurrentMergeNum() {
    return concurrentMergeNum;
  }

  @Override
  public void setConcurrentMergeNum(int concurrentMergeNum) {

  }

  @Override
  public MergeResource getResource() {
    return resource;
  }

  @Override
  public List<TsFileResource> getSelectedSeqFiles() {
    return lastSelectedSeqFiles;
  }

  @Override
  public List<TsFileResource> getSelectedUnseqFiles() {
    return lastSelectedUnseqFiles;
  }

  @Override
  public long getTotalCost() {
    return totalCost;
  }

  private void searchMaxSeriesNum() throws IOException, MergeException {
    binSearch();
  }

  private void binSearch() throws IOException, MergeException {
    int lb = 0;
    int ub = MAX_SERIES_NUM + 1;
    while (timeConsumption < timeLimit) {
      int mid = (lb + ub) / 2;
      if (mid == lb) {
        break;
      }
      baseSelector.setConcurrentMergeNum(mid);
      baseSelector.select(false);
      if (baseSelector.getSelectedUnseqFiles().size() + baseSelector.getSelectedSeqFiles().size() <= 1) {
        baseSelector.select(true);
      }
      if (baseSelector.getSelectedUnseqFiles().size() + baseSelector.getSelectedSeqFiles().size() <= 1) {
        // did not find candidates, lower concurrent merge number and retry
        ub = mid;
      } else if (baseSelector.getSelectedUnseqFiles().size() == 0 && lastSelectedUnseqFiles != null
          && lastSelectedUnseqFiles.size() > 0) {
        // found candidates with no unseq files while the previous result has unseq files
        // take the previous results because we try to merge at least one unseq file
        ub = mid;
      } else {
        // found candidates, record them and try to find a higher concurrent number
        lastSelectedSeqFiles = baseSelector.getSelectedSeqFiles();
        lastSelectedUnseqFiles = baseSelector.getSelectedUnseqFiles();
        lastTotalMemoryCost = baseSelector.getTotalCost();
        lb = mid;
      }
      timeConsumption = System.currentTimeMillis() - startTime;
    }
    concurrentMergeNum = lb;
    totalCost = lastTotalMemoryCost;
  }
}
