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

package org.apache.iotdb.db.engine.compaction.cross.inplace.task;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * RecoverCrossMergeTask is an extension of MergeTask, which resumes the last merge progress by
 * scanning merge.log using LogAnalyzer and continue the unfinished merge.
 */
public class CleanLastCrossSpaceCompactionTask extends CrossSpaceMergeTask {

  private static final Logger logger =
      LoggerFactory.getLogger(CleanLastCrossSpaceCompactionTask.class);

  public CleanLastCrossSpaceCompactionTask(
      List<TsFileResource> seqFiles,
      List<TsFileResource> unseqFiles,
      String storageGroupSysDir,
      MergeCallback callback,
      String taskName,
      boolean fullMerge,
      String storageGroupName) {
    super(
        seqFiles, unseqFiles, storageGroupSysDir, callback, taskName, fullMerge, storageGroupName);
  }

  public void cleanLastCrossSpaceCompactionInfo(boolean continueMerge, File logFile)
      throws IOException {
    if (!logFile.exists()) {
      logger.info("{} no merge.log, cross space compaction clean ends.", taskName);
      return;
    }
    long startTime = System.currentTimeMillis();
    cleanUp(continueMerge);
    if (logger.isInfoEnabled()) {
      logger.info(
          "{} cross space compaction clean ends after {}ms.",
          taskName,
          (System.currentTimeMillis() - startTime));
    }
  }
}
