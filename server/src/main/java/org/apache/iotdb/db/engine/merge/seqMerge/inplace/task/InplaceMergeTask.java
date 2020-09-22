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

package org.apache.iotdb.db.engine.merge.seqMerge.inplace.task;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.iotdb.db.engine.merge.MergeCallback;
import org.apache.iotdb.db.engine.merge.MergeTask;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.seqMerge.inplace.recover.InplaceMergeLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MergeTask merges given seqFiles and unseqFiles into new ones, which basically consists of three
 * steps: 1. rewrite overflowed, modified or small-sized chunks into temp merge files 2. move the
 * merged chunks in the temp files back to the seqFiles or move the unmerged chunks in the seqFiles
 * into temp files and replace the seqFiles with the temp files. 3. remove unseqFiles
 */
public class InplaceMergeTask extends MergeTask {

  public static final String MERGE_SUFFIX = ".merge.inplace";
  private static final Logger logger = LoggerFactory.getLogger(InplaceMergeTask.class);

  MergeMultiChunkTask chunkTask;
  MergeFileTask fileTask;

  public InplaceMergeTask(MergeResource mergeResource, String storageGroupSysDir,
      MergeCallback callback, String taskName, boolean fullMerge, String storageGroupName) {
    super(mergeResource, storageGroupSysDir, callback, taskName, fullMerge, storageGroupName);
  }

  @Override
  public Void call() throws Exception {
    try {
      doMerge();
    } catch (Exception e) {
      logger.error("Runtime exception in merge {}", taskName, e);
      abort();
    }
    return null;
  }

  private void abort() throws IOException {
    states = States.ABORTED;
    cleanUp(false);
    // call the callback to make sure the StorageGroup exit merging status, but passing 2
    // empty file lists to avoid files being deleted.
    callback.call(Collections.emptyList(), Collections.emptyList(),
        new File(storageGroupSysDir, InplaceMergeLogger.MERGE_LOG_NAME), null);
  }

  private void doMerge() throws IOException, MetadataException {
    if (logger.isInfoEnabled()) {
      logger.info("{} starts to merge {} seqFiles", taskName, resource.getSeqFiles().size());
    }
    long startTime = System.currentTimeMillis();
    long totalFileSize = MergeUtils.collectFileSizes(resource.getSeqFiles(),
        resource.getUnseqFiles());
    mergeLogger = new InplaceMergeLogger(storageGroupSysDir);

    mergeLogger.logFiles(resource);

    resource.setChunkWriterCache(MergeUtils.constructChunkWriterCache(storageGroupName));

    Set<PartialPath> devices = IoTDB.metaManager.getDevices(new PartialPath(storageGroupName));
    Map<PartialPath, MeasurementSchema> measurementSchemaMap = new HashMap<>();
    List<PartialPath> unmergedSeries = new ArrayList<>();
    for (PartialPath device : devices) {
      MNode deviceNode = IoTDB.metaManager.getNodeByPath(device);
      for (Entry<String, MNode> entry : deviceNode.getChildren().entrySet()) {
        PartialPath path = device.concatNode(entry.getKey());
        measurementSchemaMap.put(path, ((MeasurementMNode) entry.getValue()).getSchema());
        unmergedSeries.add(path);
      }
    }

    mergeLogger.logMergeStart();

    chunkTask = new MergeMultiChunkTask(mergeContext, taskName, (InplaceMergeLogger) mergeLogger,
        resource,
        fullMerge, unmergedSeries, storageGroupName);
    states = States.MERGE_CHUNKS;
    chunkTask.mergeSeries();
    if (Thread.interrupted()) {
      logger.info("Merge task {} aborted", taskName);
      abort();
      return;
    }

    fileTask = new MergeFileTask(taskName, mergeContext, (InplaceMergeLogger) mergeLogger, resource,
        resource.getSeqFiles());
    states = States.MERGE_FILES;
    chunkTask = null;
    fileTask.mergeFiles();
    if (Thread.interrupted()) {
      logger.info("Merge task {} aborted", taskName);
      abort();
      return;
    }

    states = States.CLEAN_UP;
    fileTask = null;
    cleanUp(true);
    if (logger.isInfoEnabled()) {
      double elapsedTime = (double) (System.currentTimeMillis() - startTime) / 1000.0;
      double byteRate = totalFileSize / elapsedTime / 1024 / 1024;
      double seriesRate = unmergedSeries.size() / elapsedTime;
      double chunkRate = mergeContext.getTotalChunkWritten() / elapsedTime;
      double fileRate =
          (resource.getSeqFiles().size() + resource.getUnseqFiles().size()) / elapsedTime;
      double ptRate = mergeContext.getTotalPointWritten() / elapsedTime;
      logger.info("{} ends after {}s, byteRate: {}MB/s, seriesRate {}/s, chunkRate: {}/s, "
              + "fileRate: {}/s, ptRate: {}/s",
          taskName, elapsedTime, byteRate, seriesRate, chunkRate, fileRate, ptRate);
    }
  }

  void cleanUp(boolean executeCallback) throws IOException {
    logger.info("{} is cleaning up", taskName);

    resource.clear();
    mergeContext.clear();

    if (mergeLogger != null) {
      mergeLogger.close();
    }

    for (TsFileResource seqFile : resource.getSeqFiles()) {
      File mergeFile = new File(seqFile.getTsFilePath() + MERGE_SUFFIX);
      mergeFile.delete();
      seqFile.setMerging(false);
    }
    for (TsFileResource unseqFile : resource.getUnseqFiles()) {
      unseqFile.setMerging(false);
    }

    File logFile = new File(storageGroupSysDir, InplaceMergeLogger.MERGE_LOG_NAME);
    if (executeCallback) {
      // make sure merge.log is not deleted until unseqFiles are cleared so that when system
      // reboots, the undeleted files can be deleted again
      callback.call(resource.getSeqFiles(), resource.getUnseqFiles(), logFile, null);
    } else {
      logFile.delete();
    }
  }
}
