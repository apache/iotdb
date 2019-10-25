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
import java.util.Collection;
import java.util.HashSet;
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
public class InplaceMaxFileSelector extends BaseFileSelector {

  private static final Logger logger = LoggerFactory.getLogger(InplaceMaxFileSelector.class);

  private Collection<Integer> tmpSelectedSeqFiles;

  private boolean[] seqSelected;

  public InplaceMaxFileSelector(MergeResource resource, long memoryBudget) {
    this.resource = resource;
    this.memoryBudget = memoryBudget;
    this.tmpSelectedSeqIterable = new ListTmpSeqIter();
  }

  @Override
  public void select(boolean useTightBound) throws IOException {
    tmpSelectedSeqFiles = new HashSet<>();
    seqSelected = new boolean[resource.getSeqFiles().size()];
    super.select(useTightBound);
  }

  @Override
  protected void updateCost(long newCost, TsFileResource unseqFile) {
    if (totalCost + newCost < memoryBudget) {
      selectedUnseqFiles.add(unseqFile);
      maxSeqFileCost = tempMaxSeqFileCost;

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

  protected void selectOverlappedSeqFiles(TsFileResource unseqFile) {
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
            tmpSelectedNum ++;
          }
          // the device of the unseqFile can not merge with later seqFiles
          noMoreOverlap = true;
        } else if (unseqStartTime <= seqEndTime) {
          // the device of the unseqFile may merge with later seqFiles
          // and the unseqFile overlaps current seqFile
          if (!tmpSelectedSeqFiles.contains(i)) {
            tmpSelectedSeqFiles.add(i);
            tmpSelectedNum ++;
          }
        }
      }
      if (tmpSelectedNum + seqSelectedNum == resource.getSeqFiles().size()) {
        break;
      }
    }
  }



  protected class ListTmpSeqIter extends TmpSelectedSeqIterable {

    @Override
    public Iterator<Integer> iterator() {
      return tmpSelectedSeqFiles.iterator();
    }
  }
}
