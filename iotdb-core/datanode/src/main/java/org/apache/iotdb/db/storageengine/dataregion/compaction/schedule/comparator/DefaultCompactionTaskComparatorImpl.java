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

package org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.comparator;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InsertionCrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.SettleCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.constant.CompactionPriority;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.util.List;

public class DefaultCompactionTaskComparatorImpl implements ICompactionTaskComparator {
  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  @SuppressWarnings({"squid:S3776", "javabugs:S6320"})
  @Override
  public int compare(AbstractCompactionTask o1, AbstractCompactionTask o2) {
    if (o1 instanceof InsertionCrossSpaceCompactionTask
        && o2 instanceof InsertionCrossSpaceCompactionTask) {
      return o1.getSerialId() < o2.getSerialId() ? -1 : 1;
    } else if (o1 instanceof InsertionCrossSpaceCompactionTask) {
      return -1;
    } else if (o2 instanceof InsertionCrossSpaceCompactionTask) {
      return 1;
    }
    if (o1 instanceof SettleCompactionTask && o2 instanceof SettleCompactionTask) {
      return compareSettleCompactionTask((SettleCompactionTask) o1, (SettleCompactionTask) o2);
    } else if (o1 instanceof SettleCompactionTask) {
      return -1;
    } else if (o2 instanceof SettleCompactionTask) {
      return 1;
    }

    if ((((o1 instanceof InnerSpaceCompactionTask) && (o2 instanceof CrossSpaceCompactionTask))
        || ((o2 instanceof InnerSpaceCompactionTask)
            && (o1 instanceof CrossSpaceCompactionTask)))) {
      if (config.getCompactionPriority() == CompactionPriority.CROSS_INNER) {
        // priority is CROSS_INNER
        return o1 instanceof CrossSpaceCompactionTask ? -1 : 1;
      } else if (config.getCompactionPriority() == CompactionPriority.INNER_CROSS) {
        // priority is INNER_CROSS
        return o1 instanceof InnerSpaceCompactionTask ? -1 : 1;
      } else {
        // priority is BALANCE
        if (o1.getSerialId() != o2.getSerialId()) {
          return o1.getSerialId() < o2.getSerialId() ? -1 : 1;
        } else {
          return o1 instanceof CrossSpaceCompactionTask ? -1 : 1;
        }
      }
    } else if (o1 instanceof InnerSpaceCompactionTask) {
      return compareInnerSpaceCompactionTask(
          (InnerSpaceCompactionTask) o1, (InnerSpaceCompactionTask) o2);
    } else {
      return compareCrossSpaceCompactionTask(
          (CrossSpaceCompactionTask) o1, (CrossSpaceCompactionTask) o2);
    }
  }

  public int compareInnerSpaceCompactionTask(
      InnerSpaceCompactionTask o1, InnerSpaceCompactionTask o2) {
    // if compactionTaskType of o1 and o2 are different
    // we prefer to execute task type with Repair type
    if (o1.getCompactionTaskType() != o2.getCompactionTaskType()) {
      return o1.getCompactionTaskType() == CompactionTaskType.REPAIR ? -1 : 1;
    }

    // if the sum of compaction count of the selected files are different
    // we prefer to execute task with smaller compaction count
    // this can reduce write amplification
    if (((double) o1.getSumOfCompactionCount()) / o1.getSelectedTsFileResourceList().size()
        != ((double) o2.getSumOfCompactionCount()) / o2.getSelectedTsFileResourceList().size()) {
      return o1.getSumOfCompactionCount() / o1.getSelectedTsFileResourceList().size()
          - o2.getSumOfCompactionCount() / o2.getSelectedTsFileResourceList().size();
    }

    // if the time partition of o1 and o2 are different
    // we prefer to execute task with greater time partition
    // because we want to compact files with new data
    if (o1.getTimePartition() != o2.getTimePartition()) {
      return o2.getTimePartition() > o1.getTimePartition() ? 1 : -1;
    }

    // if the max file version of o1 and o2 are different
    // we prefer to execute task with greater file version
    // because we want to compact newly written files
    if (o1.getDataRegionId().equals(o2.getDataRegionId())
        && o1.getTimePartition() == o2.getTimePartition()
        && o1.getMaxFileVersion() != o2.getMaxFileVersion()) {
      return o2.getMaxFileVersion() > o1.getMaxFileVersion() ? 1 : -1;
    }

    List<TsFileResource> selectedFilesOfO1 = o1.getSelectedTsFileResourceList();
    List<TsFileResource> selectedFilesOfO2 = o2.getSelectedTsFileResourceList();

    // if the number of selected files are different
    // we prefer to execute task with more files
    int fileNumDiff = Math.abs(selectedFilesOfO1.size() - selectedFilesOfO2.size());
    if (2 * fileNumDiff >= Math.min(selectedFilesOfO1.size(), selectedFilesOfO2.size())) {
      return selectedFilesOfO2.size() - selectedFilesOfO1.size();
    }

    // if the number of selected files is roughly the same,
    // we prefer to execute the one with the smaller total
    // file size
    return o2.getSelectedFileSize() > o1.getSelectedFileSize() ? -1 : 1;
  }

  public int compareCrossSpaceCompactionTask(
      CrossSpaceCompactionTask o1, CrossSpaceCompactionTask o2) {
    // if the time partition of o1 and o2 are different
    // we prefer to execute task with greater time partition
    // because we want to compact files with new data
    if (o1.getTimePartition() != o2.getTimePartition()) {
      return o2.getTimePartition() > o1.getTimePartition() ? 1 : -1;
    }

    if (o1.getSelectedSequenceFiles().size() != o2.getSelectedSequenceFiles().size()) {
      // we prefer the task with fewer sequence files
      // because this type of tasks consume fewer memory during execution
      return o1.getSelectedSequenceFiles().size() - o2.getSelectedSequenceFiles().size();
    }

    // if the serial id of the tasks are different
    // we prefer task with small serial id
    if (o1.getSerialId() != o2.getSerialId()) {
      return o1.getSerialId() > o2.getSerialId() ? 1 : -1;
    }

    // we prefer the task with more unsequence files
    // because this type of tasks reduce more unsequence files
    return o2.getSelectedUnsequenceFiles().size() - o1.getSelectedUnsequenceFiles().size();
  }

  public int compareSettleCompactionTask(SettleCompactionTask o1, SettleCompactionTask o2) {
    // we prefer the task with more all_deleted files
    if (o1.getFullyDirtyFiles().size() != o2.getFullyDirtyFiles().size()) {
      return o1.getFullyDirtyFiles().size() > o2.getFullyDirtyFiles().size() ? -1 : 1;
    }
    // we prefer the task with larger all_deleted files
    if (o1.getFullyDirtyFileSize() != o2.getFullyDirtyFileSize()) {
      return o1.getFullyDirtyFileSize() > o2.getFullyDirtyFileSize() ? -1 : 1;
    }
    // we prefer the task with larger mods file
    if (o1.getTotalModsSize() != o2.getTotalModsSize()) {
      return o1.getTotalModsSize() > o2.getTotalModsSize() ? -1 : 1;
    }
    // we prefer the task with more partial_deleted files
    if (o1.getPartiallyDirtyFiles().size() != o2.getPartiallyDirtyFiles().size()) {
      return o1.getPartiallyDirtyFiles().size() > o2.getPartiallyDirtyFiles().size() ? -1 : 1;
    }
    // we prefer the task with larger partial_deleted files
    if (o1.getPartiallyDirtyFileSize() != o2.getPartiallyDirtyFileSize()) {
      return o1.getPartiallyDirtyFileSize() > o2.getPartiallyDirtyFileSize() ? -1 : 1;
    }
    // we prefer task with smaller serial id
    if (o1.getSerialId() != o2.getSerialId()) {
      return o1.getSerialId() < o2.getSerialId() ? -1 : 1;
    }
    return 0;
  }
}
