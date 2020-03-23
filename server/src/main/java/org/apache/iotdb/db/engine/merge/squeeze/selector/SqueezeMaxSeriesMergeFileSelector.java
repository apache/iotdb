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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.engine.merge.IMergeFileSelector;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.utils.MergeFileSelectorUtils;
import org.apache.iotdb.db.engine.merge.utils.SelectorContext;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.tsfile.utils.Pair;

/**
 * MaxSeriesMergeFileSelector is an extension of IMergeFileSelector which tries to maximize the
 * number of timeseries that can be merged at the same time.
 */
public class SqueezeMaxSeriesMergeFileSelector implements IMergeFileSelector {

  public static final int MAX_SERIES_NUM = 256;

  private SqueezeMaxFileSelector maxFileMergeFileSelector;
  private SelectorContext selectorContext;

  @Override
  public Pair<MergeResource, SelectorContext> selectMergedFiles() throws MergeException {
    return MergeFileSelectorUtils
        .selectSeries(maxFileMergeFileSelector.getSeqFiles(),
            maxFileMergeFileSelector.getUnseqFiles(), this::searchMaxSeriesNum,
            this.selectorContext);
  }

  public SqueezeMaxSeriesMergeFileSelector(Collection<TsFileResource> seqFiles,
      Collection<TsFileResource> unseqFiles, long budget) {
    this(seqFiles, unseqFiles, budget, Long.MIN_VALUE);
  }

  public SqueezeMaxSeriesMergeFileSelector(Collection<TsFileResource> seqFiles,
      Collection<TsFileResource> unseqFiles, long budget, long timeLowerBound) {
    this.selectorContext = new SelectorContext();
    this.maxFileMergeFileSelector = new SqueezeMaxFileSelector(seqFiles, unseqFiles,
        budget, timeLowerBound);
  }

  private Pair<List<TsFileResource>, List<TsFileResource>> searchMaxSeriesNum() throws IOException {
    return binSearch();
  }

  private Pair<List<TsFileResource>, List<TsFileResource>> binSearch() throws IOException {
    List<TsFileResource> lastSelectedSeqFiles = Collections.emptyList();
    List<TsFileResource> lastSelectedUnseqFiles = Collections.emptyList();
    long lastTotalMemoryCost = 0;
    int lb = 0;
    int ub = MAX_SERIES_NUM + 1;
    while (true) {
      int mid = (lb + ub) / 2;
      if (mid == lb) {
        break;
      }
      selectorContext.setConcurrentMergeNum(mid);
      Pair<List<TsFileResource>, List<TsFileResource>> selectedFiles = maxFileMergeFileSelector
          .select(false);
      if (selectedFiles.right.isEmpty()) {
        selectedFiles = maxFileMergeFileSelector.select(true);
      }
      if (selectedFiles.right.isEmpty()) {
        ub = mid;
      } else {
        lastSelectedSeqFiles = selectedFiles.left;
        lastSelectedUnseqFiles = selectedFiles.right;
        lastTotalMemoryCost = maxFileMergeFileSelector.getTotalCost();
        lb = mid;
      }
    }
    selectorContext.setConcurrentMergeNum(lb);
    selectorContext.setTotalCost(lastTotalMemoryCost);
    return new Pair<>(lastSelectedSeqFiles, lastSelectedUnseqFiles);
  }
}
