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

package org.apache.iotdb.db.engine.compaction.cross;

import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractCrossSpaceCompactionTask extends AbstractCompactionTask {

  List<TsFileResource> selectedSequenceFiles;
  List<TsFileResource> selectedUnsequenceFiles;

  public AbstractCrossSpaceCompactionTask(
      String fullStorageGroupName,
      long timePartition,
      AtomicInteger currentTaskNum,
      List<TsFileResource> selectedSequenceFiles,
      List<TsFileResource> selectedUnsequenceFiles) {
    super(fullStorageGroupName, timePartition, currentTaskNum);
    this.selectedSequenceFiles = selectedSequenceFiles;
    this.selectedUnsequenceFiles = selectedUnsequenceFiles;
  }

  public AbstractCrossSpaceCompactionTask(
      String fullStorageGroupName, long timePartition, AtomicInteger currentTaskNum) {
    super(fullStorageGroupName, timePartition, currentTaskNum);
    this.selectedSequenceFiles = null;
    this.selectedUnsequenceFiles = null;
  }

  public List<TsFileResource> getSelectedSequenceFiles() {
    return selectedSequenceFiles;
  }

  public List<TsFileResource> getSelectedUnsequenceFiles() {
    return selectedUnsequenceFiles;
  }

  @Override
  public boolean checkValidAndSetMerging() {
    for (TsFileResource resource : selectedSequenceFiles) {
      if (resource.isMerging() || !resource.isClosed() || !resource.getTsFile().exists()) {
        return false;
      }
    }

    for (TsFileResource resource : selectedUnsequenceFiles) {
      if (resource.isMerging() || !resource.isClosed() || !resource.getTsFile().exists()) {
        return false;
      }
    }

    for (TsFileResource resource : selectedSequenceFiles) {
      resource.setMerging(true);
    }

    for (TsFileResource resource : selectedUnsequenceFiles) {
      resource.setMerging(true);
    }

    return true;
  }

  @Override
  public String toString() {
    return new StringBuilder()
        .append(fullStorageGroupName)
        .append("-")
        .append(timePartition)
        .append(" task seq file num is ")
        .append(selectedSequenceFiles.size())
        .append(" , unseq file num is ")
        .append(selectedUnsequenceFiles.size())
        .toString();
  }
}
