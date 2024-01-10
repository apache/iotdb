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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.RepairUnsortedFileCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.RepairUnsortedFileCompactionEstimator;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileRepairStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

/**
 * Repair the internal unsorted file by compaction and move it to unSequence space after compaction
 * whether the source TsFileResource is sequence or not.
 */
public class RepairUnsortedFileCompactionTask extends InnerSpaceCompactionTask {

  private final TsFileResource sourceFile;

  public RepairUnsortedFileCompactionTask(
      long timePartition,
      TsFileManager tsFileManager,
      TsFileResource sourceFile,
      boolean sequence,
      long serialId) {
    super(
        timePartition,
        tsFileManager,
        Collections.singletonList(sourceFile),
        sequence,
        new RepairUnsortedFileCompactionPerformer(),
        serialId,
        CompactionTaskPriorityType.NORMAL);
    this.sourceFile = sourceFile;
    this.innerSpaceEstimator = new RepairUnsortedFileCompactionEstimator();
  }

  @Override
  protected void prepare() throws IOException {
    targetTsFileResource =
        new TsFileResource(generateTargetFile(), TsFileResourceStatus.COMPACTING);
    String dataDirectory = sourceFile.getTsFile().getParent();
    logFile =
        new File(
            dataDirectory
                + File.separator
                + targetTsFileResource.getTsFile().getName()
                + CompactionLogger.INNER_COMPACTION_LOG_NAME_SUFFIX);
  }

  private File generateTargetFile() throws IOException {
    String path = sourceFile.getTsFile().getParentFile().getPath();
    // if source file is sequence, the sequence data path should be replaced to unsequence
    if (sourceFile.isSeq()) {
      int pos = path.lastIndexOf("sequence");
      path = path.substring(0, pos) + "unsequence" + path.substring(pos + "sequence".length());
    }

    TsFileNameGenerator.TsFileName tsFileName =
        TsFileNameGenerator.getTsFileName(sourceFile.getTsFile().getName());
    tsFileName.setInnerCompactionCnt(tsFileName.getInnerCompactionCnt() + 1);
    String fileNameStr =
        String.format(
            "%d-%d-%d-%d.tsfile",
            tsFileName.getTime(), tsFileName.getVersion(), tsFileName.getInnerCompactionCnt(), 0);
    File targetTsFile = new File(path + File.separator + fileNameStr);
    if (!targetTsFile.getParentFile().exists()) {
      targetTsFile.getParentFile().mkdirs();
    }
    return targetTsFile;
  }

  @Override
  public long getEstimatedMemoryCost() {
    if (innerSpaceEstimator != null && memoryCost == 0L) {
      try {
        memoryCost = innerSpaceEstimator.estimateInnerCompactionMemory(selectedTsFileResourceList);
      } catch (IOException e) {
        innerSpaceEstimator.cleanup();
      }
    }
    if (memoryCost > SystemInfo.getInstance().getMemorySizeForCompaction()) {
      sourceFile.setTsFileRepairStatus(TsFileRepairStatus.CAN_NOT_REPAIR);
      LOGGER.warn(
          "[RepairUnsortedFileCompactionTask]"
              + " Can not repair unsorted file {} "
              + "because the required memory to repair"
              + " is greater than the total compaction memory budget",
          sourceFile.getTsFile().getAbsolutePath());
    }
    return memoryCost;
  }
}
