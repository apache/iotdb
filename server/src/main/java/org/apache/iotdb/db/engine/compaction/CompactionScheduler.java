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

package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.AbstractCrossSpaceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.cross.CrossSpaceCompactionTaskFactory;
import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.inner.InnerSpaceCompactionTaskFactory;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionSelector;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;

/**
 * CompactionScheduler schedules and submits the compaction task periodically, and it counts the
 * total number of running compaction task. There are three compaction strategy: BALANCE,
 * INNER_CROSS, CROSS_INNER. Difference strategies will lead to different compaction preferences.
 * For different types of compaction task(e.g. InnerSpaceCompaction), CompactionScheduler will call
 * the corresponding {@link org.apache.iotdb.db.engine.compaction.task.AbstractCompactionSelector
 * selector} according to the compaction machanism of the task(e.g. LevelCompaction,
 * SizeTiredCompaction), and the selection and submission process is carried out in the {@link
 * AbstractCompactionSelector#selectAndSubmit() selectAndSubmit()} in selector.
 */
public class CompactionScheduler {

  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  public static void scheduleCompaction(TsFileManager tsFileManager, long timePartition) {
    if (!tsFileManager.isAllowCompaction()) {
      return;
    }
    tryToSubmitInnerSpaceCompactionTask(
        tsFileManager.getStorageGroupName(),
        tsFileManager.getVirtualStorageGroup(),
        timePartition,
        tsFileManager,
        true,
        new InnerSpaceCompactionTaskFactory());
    tryToSubmitInnerSpaceCompactionTask(
        tsFileManager.getStorageGroupName(),
        tsFileManager.getVirtualStorageGroup(),
        timePartition,
        tsFileManager,
        false,
        new InnerSpaceCompactionTaskFactory());
    tryToSubmitCrossSpaceCompactionTask(
        tsFileManager.getStorageGroupName(),
        tsFileManager.getVirtualStorageGroup(),
        tsFileManager.getStorageGroupDir(),
        timePartition,
        tsFileManager,
        new CrossSpaceCompactionTaskFactory());
  }

  public static void tryToSubmitInnerSpaceCompactionTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartition,
      TsFileManager tsFileManager,
      boolean sequence,
      InnerSpaceCompactionTaskFactory taskFactory) {
    if ((!config.isEnableSeqSpaceCompaction() && sequence)
        || (!config.isEnableUnseqSpaceCompaction() && !sequence)) {
      return;
    }

    AbstractInnerSpaceCompactionSelector innerSpaceCompactionSelector =
        config
            .getInnerCompactionStrategy()
            .getCompactionSelector(
                logicalStorageGroupName,
                virtualStorageGroupName,
                timePartition,
                tsFileManager,
                sequence,
                taskFactory);
    innerSpaceCompactionSelector.selectAndSubmit();
  }

  private static void tryToSubmitCrossSpaceCompactionTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      String storageGroupDir,
      long timePartition,
      TsFileManager tsFileManager,
      CrossSpaceCompactionTaskFactory taskFactory) {
    if (!config.isEnableCrossSpaceCompaction()) {
      return;
    }
    AbstractCrossSpaceCompactionSelector crossSpaceCompactionSelector =
        config
            .getCrossCompactionStrategy()
            .getCompactionSelector(
                logicalStorageGroupName,
                virtualStorageGroupName,
                storageGroupDir,
                timePartition,
                tsFileManager,
                taskFactory);
    crossSpaceCompactionSelector.selectAndSubmit();
  }
}
