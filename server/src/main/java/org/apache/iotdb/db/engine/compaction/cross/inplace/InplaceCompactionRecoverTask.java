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
package org.apache.iotdb.db.engine.compaction.cross.inplace;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.AbstractCrossSpaceCompactionRecoverTask;
import org.apache.iotdb.db.engine.compaction.cross.inplace.task.CleanLastCrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.exception.metadata.MetadataException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class InplaceCompactionRecoverTask extends InplaceCompactionTask {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractCrossSpaceCompactionRecoverTask.class);
  private File logFile;

  public InplaceCompactionRecoverTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartitionId,
      String storageGroupDir,
      TsFileResourceList seqTsFileResourceList,
      TsFileResourceList unSeqTsFileResourceList,
      int concurrentMergeCount,
      File logFile,
      AtomicInteger currentTaskNum) {
    super(
        logicalStorageGroupName,
        virtualStorageGroupName,
        timePartitionId,
        null,
        storageGroupDir,
        seqTsFileResourceList,
        unSeqTsFileResourceList,
        seqTsFileResourceList,
        unSeqTsFileResourceList,
        concurrentMergeCount,
        currentTaskNum);
    this.logFile = logFile;
  }

  @Override
  public void doCompaction() throws IOException, MetadataException {
    String taskName = fullStorageGroupName + "-" + System.currentTimeMillis();
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
    CleanLastCrossSpaceCompactionTask cleanLastCrossSpaceCompactionTask =
        new CleanLastCrossSpaceCompactionTask(
            seqFileList,
            unSeqFileList,
            storageGroupDir,
            this::mergeEndAction,
            taskName,
            IoTDBDescriptor.getInstance().getConfig().isForceFullMerge(),
            logicalStorageGroupName);
    LOGGER.info(
        "{} a CleanLastCrossSpaceCompactionTask {} starts...", fullStorageGroupName, taskName);
    cleanLastCrossSpaceCompactionTask.cleanLastCrossSpaceCompactionInfo(
        IoTDBDescriptor.getInstance().getConfig().isContinueMergeAfterReboot(), logFile);
    if (!IoTDBDescriptor.getInstance().getConfig().isContinueMergeAfterReboot()) {
      for (TsFileResource seqFile : seqFileList) {
        ModificationFile.getCompactionMods(seqFile).remove();
      }
    }
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask other) {
    if (other instanceof InplaceCompactionRecoverTask) {
      return logFile.equals(((InplaceCompactionRecoverTask) other).logFile);
    }
    return false;
  }
}
