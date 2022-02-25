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

package org.apache.iotdb.db.engine.merge.task;

import org.apache.iotdb.db.engine.compaction.TsFileManagement;
import org.apache.iotdb.db.engine.merge.recover.MergeLogAnalyzer;
import org.apache.iotdb.db.engine.merge.recover.MergeLogAnalyzer.Status;
import org.apache.iotdb.db.engine.merge.recover.MergeLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.engine.storagegroup.TsFileResource.modifyTsFileNameUnseqMergCnt;

/**
 * RecoverMergeTask is an extension of MergeTask, which resumes the last merge progress by scanning
 * merge.log using LogAnalyzer and continue the unfinished merge.
 */
public class RecoverMergeTask extends MergeTask {

  private static final Logger logger = LoggerFactory.getLogger(RecoverMergeTask.class);

  private MergeLogAnalyzer analyzer;
  private TsFileManagement tsFileManagement;

  public RecoverMergeTask(
      TsFileManagement tsfileManagement,
      String storageGroupSysDir,
      MergeCallback callback,
      String taskName,
      boolean fullMerge,
      String storageGroupName) {
    super(
        tsfileManagement.getTsFileList(true),
        tsfileManagement.getTsFileList(false),
        storageGroupSysDir,
        callback,
        taskName,
        fullMerge,
        storageGroupName);
    this.tsFileManagement = tsfileManagement;
  }

  public void recoverMerge() throws IOException, MetadataException {
    File logFile = new File(storageGroupSysDir, MergeLogger.MERGE_LOG_NAME);
    if (!logFile.exists()) {
      logger.info("{} no merge.log, merge recovery ends", taskName);
      return;
    }
    long startTime = System.currentTimeMillis();

    analyzer = new MergeLogAnalyzer(resource, taskName, logFile, storageGroupName);
    Status status = analyzer.analyze();
    switch (status) {
      case NONE:
        logFile.delete();
        break;
      case All_SOURCE_FILES_EXIST:
        handleWhenAllSourceFilesExist();
        break;
      case SOME_SOURCE_FILES_LOST:
        handleWhenSomeSourceFilesLost();
        break;
      default:
        throw new UnsupportedOperationException(taskName + " found unrecognized status " + status);
    }
    if (logFile.exists()) {
      logFile.delete();
    }
    if (logger.isInfoEnabled()) {
      logger.info(
          "{} merge recovery ends after {}ms", taskName, (System.currentTimeMillis() - startTime));
    }
  }

  /** Delete .merge file and merging mods file. */
  private void handleWhenAllSourceFilesExist() throws IOException {
    cleanUp(false);
    tsFileManagement.removeMergingModification();
  }

  /**
   * 1. If target file does not exist, then move .merge file to target file and serialize target
   * resource file. <br>
   * 2. Append merging modification to target mods file and delete merging mods file. <br>
   * 3. Delete source files and .merge file. <br>
   * 4. Update resource memory of tsfileManagement. <br>
   */
  private void handleWhenSomeSourceFilesLost() throws IOException {
    List<TsFileResource> targetResouces = new ArrayList<>();
    for (TsFileResource sourceSeqResource : resource.getSeqFiles()) {
      File targetFile = modifyTsFileNameUnseqMergCnt(sourceSeqResource.getTsFile());
      TsFileResource targetTsFileResouce = new TsFileResource(targetFile);
      // move to target file and serialize resource file
      if (!targetFile.exists()) {
        // move target file
        File tmpTargetFile = new File(sourceSeqResource.getTsFilePath() + MERGE_SUFFIX);
        FSFactoryProducer.getFSFactory().moveFile(tmpTargetFile, targetFile);

        // serialize target resource file
        try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getAbsolutePath())) {
          FileLoaderUtils.updateTsFileResource(reader, targetTsFileResouce);
        }
        targetTsFileResouce.serialize();
        targetResouces.add(targetTsFileResouce);
      }

      // write merging modifications to new mods file
      tsFileManagement.updateMergeModification(targetTsFileResouce);

      // delete source seq file
      sourceSeqResource.remove();

      // delete merge file
      File mergeFile = new File(sourceSeqResource.getTsFilePath() + MERGE_SUFFIX);
      if (mergeFile.exists()) {
        mergeFile.delete();
      }
    }

    // update memory
    tsFileManagement.replace(
        resource.getSeqFiles(), resource.getUnseqFiles(), targetResouces, true);

    // delete unseq source files
    for (TsFileResource unseqResource : resource.getUnseqFiles()) {
      unseqResource.remove();
    }

    // delete merging mods file
    tsFileManagement.removeMergingModification();
  }
}
