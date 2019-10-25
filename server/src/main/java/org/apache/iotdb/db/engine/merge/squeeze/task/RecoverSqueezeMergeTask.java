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

package org.apache.iotdb.db.engine.merge.squeeze.task;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.merge.IRecoverMergeTask;
import org.apache.iotdb.db.engine.merge.MergeCallback;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.squeeze.recover.LogAnalyzer;
import org.apache.iotdb.db.engine.merge.squeeze.recover.LogAnalyzer.Status;
import org.apache.iotdb.db.engine.merge.squeeze.recover.SqueezeMergeLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RecoverMergeTask is an extension of MergeTask, which resumes the last merge progress by
 * scanning merge.log using LogAnalyzer and continue the unfinished merge.
 */
public class RecoverSqueezeMergeTask extends SqueezeMergeTask implements IRecoverMergeTask {

  private static final Logger logger = LoggerFactory.getLogger(RecoverSqueezeMergeTask.class);

  public RecoverSqueezeMergeTask(List<TsFileResource> seqFiles,
      List<TsFileResource> unseqFiles, String storageGroupSysDir,
      MergeCallback callback, String taskName, String storageGroupName) {
    super(new MergeResource(seqFiles, unseqFiles), storageGroupSysDir, callback, taskName,
        1, storageGroupName);
  }

  // continueMerge does not work for squeeze strategy
  public void recoverMerge(boolean continueMerge) throws IOException {
    File logFile = SystemFileFactory.INSTANCE.getFile(storageGroupSysDir, SqueezeMergeLogger.MERGE_LOG_NAME);
    if (!logFile.exists()) {
      logger.info("{} no merge.log, merge recovery ends", taskName);
      return;
    }
    long startTime = System.currentTimeMillis();

    LogAnalyzer analyzer = new LogAnalyzer(resource, taskName, logFile);
    Status status = analyzer.analyze();
    if (logger.isInfoEnabled()) {
      logger.info("{} merge recovery status determined: {} after {}ms", taskName, status,
          (System.currentTimeMillis() - startTime));
    }
    switch (status) {
      case NONE:
        logFile.delete();
        break;
      case MERGE_START:
        removeMergedFile();
        // set the files to empty to let the StorageGroupProcessor do a clean up
        resource.setSeqFiles(Collections.emptyList());
        resource.setUnseqFiles(Collections.emptyList());
        cleanUp(true);
        break;
      case ALL_TS_MERGED:
        newResource = analyzer.getNewResource();
        cleanUp(true);
        break;
      default:
        throw new UnsupportedOperationException(taskName + " found unrecognized status " + status);
    }
    if (logger.isInfoEnabled()) {
      logger.info("{} merge recovery ends after {}ms", taskName,
          (System.currentTimeMillis() - startTime));
    }
  }

  private void removeMergedFile() {
    File sgDir =
        FSFactoryProducer.getFSFactory().getFile(resource.getSeqFiles().get(0).getFile().getParent());
    File[] mergeFiles = sgDir.listFiles(file -> file.getName().endsWith(MERGE_SUFFIX));
    if (mergeFiles != null) {
      for  (File file : mergeFiles) {
        file.delete();
      }
    }
  }
}
