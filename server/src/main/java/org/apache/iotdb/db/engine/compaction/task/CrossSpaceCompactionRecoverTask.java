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

package org.apache.iotdb.db.engine.compaction.task;

import static org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.MERGING_MODIFICATION_FILE_NAME;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionContext;
import org.apache.iotdb.db.engine.compaction.CompactionScheduler;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.merge.task.RecoverCrossMergeTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CrossSpaceCompactionRecoverTask extends CrossSpaceCompactionTask {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(CrossSpaceCompactionRecoverTask.class);

  public CrossSpaceCompactionRecoverTask(
      CompactionContext context) {
    super(context);
  }

  @Override
  public void doCompaction() throws IOException, MetadataException {
    String taskName = storageGroupName + "-" + System.currentTimeMillis();
    File mergingMods =
        SystemFileFactory.INSTANCE.getFile(storageGroupDir, MERGING_MODIFICATION_FILE_NAME);
    if (mergingMods.exists()) {
      CompactionScheduler
          .newModification(storageGroupName, timePartition, mergingMods.getAbsolutePath());
    }
    Iterator<TsFileResource> seqIterator = seqTsFileResourceList.iterator();
    Iterator<TsFileResource> unSeqIterator = unSeqTsFileResourceList.iterator();
    List<TsFileResource> seqFileList = new ArrayList<>();
    List<TsFileResource> unSeqFileList = new ArrayList<>();
    while (seqIterator.hasNext()) {
      seqFileList.add(seqIterator.next());
    }
    while (unSeqIterator.hasNext()) {
      unSeqFileList.add(unSeqIterator.next());
    }
    RecoverCrossMergeTask recoverCrossMergeTask =
        new RecoverCrossMergeTask(
            seqFileList,
            unSeqFileList,
            storageGroupDir,
            this::mergeEndAction,
            taskName,
            IoTDBDescriptor.getInstance().getConfig().isForceFullMerge(),
            storageGroupName);
    LOGGER.info(
        "{} a RecoverMergeTask {} starts...",
        storageGroupName,
        taskName);
    recoverCrossMergeTask.recoverMerge(
        IoTDBDescriptor.getInstance().getConfig().isContinueMergeAfterReboot());
    if (!IoTDBDescriptor.getInstance().getConfig().isContinueMergeAfterReboot()) {
      mergingMods.delete();
    }
  }
}
