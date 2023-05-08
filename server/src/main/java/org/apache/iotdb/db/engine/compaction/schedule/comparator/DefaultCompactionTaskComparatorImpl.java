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

package org.apache.iotdb.db.engine.compaction.schedule.comparator;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.schedule.constant.CompactionPriority;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import java.util.List;

public class DefaultCompactionTaskComparatorImpl implements ICompactionTaskComparator {
  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  @Override
  public int compare(AbstractCompactionTask o1, AbstractCompactionTask o2) {
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
    // if the sum of compaction count of the selected files are different
    // we prefer to execute task with smaller compaction count
    // this can reduce write amplification
    if (((double) o1.getSumOfCompactionCount()) / o1.getSelectedTsFileResourceList().size()
        != ((double) o2.getSumOfCompactionCount()) / o2.getSelectedTsFileResourceList().size()) {
      return o1.getSumOfCompactionCount() / o1.getSelectedTsFileResourceList().size()
          - o2.getSumOfCompactionCount() / o2.getSelectedTsFileResourceList().size();
    }

    // if the max file version of o1 and o2 are different
    // we prefer to execute task with greater file version
    // because we want to compact newly written files
    if (o1.getMaxFileVersion() != o2.getMaxFileVersion()) {
      return o2.getMaxFileVersion() > o1.getMaxFileVersion() ? 1 : -1;
    }

    List<TsFileResource> selectedFilesOfO1 = o1.getSelectedTsFileResourceList();
    List<TsFileResource> selectedFilesOfO2 = o2.getSelectedTsFileResourceList();

    // if the number of selected files are different
    // we prefer to execute task with more files
    if (selectedFilesOfO1.size() != selectedFilesOfO2.size()) {
      return selectedFilesOfO2.size() - selectedFilesOfO1.size();
    }

    // if the serial id of the tasks are different
    // we prefer task with small serial id
    if (o1.getSerialId() != o2.getSerialId()) {
      return o1.getSerialId() > o2.getSerialId() ? 1 : -1;
    }

    // if the size of selected files are different
    // we prefer to execute task with smaller file size
    // because small files can be compacted quickly
    if (o1.getSelectedFileSize() != o2.getSelectedFileSize()) {
      return (int) (o1.getSelectedFileSize() - o2.getSelectedFileSize());
    }

    return 0;
  }

  public int compareCrossSpaceCompactionTask(
      CrossSpaceCompactionTask o1, CrossSpaceCompactionTask o2) {
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
}
