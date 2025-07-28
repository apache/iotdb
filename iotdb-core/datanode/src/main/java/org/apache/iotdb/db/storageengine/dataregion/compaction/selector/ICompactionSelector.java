/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.compaction.selector;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.constant.CrossCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.SettleCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.AbstractCompactionEstimator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.FastCrossSpaceCompactionEstimator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.CrossCompactionTaskResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.util.List;

/**
 * AbstractCompactionSelector is the base class of all CompactionSelector. It runs the file
 * selection process, if there still threads availabe for compaction task, it will submit a
 * compaction task to {@link CompactionTaskManager} and increase the global compaction task count.
 */
public interface ICompactionSelector {
  /*
   * This method should be implemented by all SequenceSpaceInnerSelector and UnsequenceSpaceInnerSelector.
   * It takes the list of tsfile in a time partition as input, and returns a list of list. Each list in
   * the returned list is the source files of one compaction tasks.
   */
  default List<InnerSpaceCompactionTask> selectInnerSpaceTask(List<TsFileResource> resources) {
    throw new RuntimeException("This kind of selector cannot be used to select inner space task");
  }

  /*
   * This method should be implemented by all CrossSpaceSelector. It takes the list of sequence files and
   * list of unsequence files as input, and returns a list of pair of list and list. Each pair in the returned
   * list contains two list: the left one is the selected sequence files, the right one is the selected
   * unsequence files. Each pair is corresponding to a cross space compaction task.
   */
  default List<CrossCompactionTaskResource> selectCrossSpaceTask(
      List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    {
      throw new RuntimeException("This kind of selector cannot be used to select cross space task");
    }
  }

  default List<SettleCompactionTask> selectSettleTask(List<TsFileResource> tsFileResources) {
    throw new RuntimeException("This kind of selector cannot be used to select settle task");
  }

  static AbstractCompactionEstimator getCompactionEstimator(
      CrossCompactionPerformer compactionPerformer, boolean isInnerSpace) {
    switch (compactionPerformer) {
      case READ_POINT:
      case FAST:
        if (!isInnerSpace) {
          return new FastCrossSpaceCompactionEstimator();
        }
      default:
        throw new RuntimeException(
            "Corresponding memory estimator for "
                + compactionPerformer
                + " performer of "
                + (isInnerSpace ? "inner" : "cross")
                + " space compaction is not existed.");
    }
  }
}
