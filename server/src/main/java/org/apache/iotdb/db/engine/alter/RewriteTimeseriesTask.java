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

package org.apache.iotdb.db.engine.alter;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.alter.log.AlteringLogger;
import org.apache.iotdb.db.engine.cache.AlteringRecordsCache;
import org.apache.iotdb.db.engine.compaction.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.log.TsFileIdentifier;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.exception.write.TsFileNotCompleteException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.commons.conf.IoTDBConstant.ALTER_OLD_TMP_FILE_SUFFIX;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

public class RewriteTimeseriesTask extends AbstractCompactionTask {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

  private static final long waitTimeout =
      IoTDBDescriptor.getInstance().getConfig().getRewriteCandidateStatusWaitTimeoutInMs();

  private final List<TsFileResource> candidateResourceList = new ArrayList<>(32);

  private final List<TsFileResource> readyResourceList = new ArrayList<>(32);

  private final AlteringRecordsCache alteringRecordsCache = AlteringRecordsCache.getInstance();

  private final File logFile;

  private final String logKey;

  public RewriteTimeseriesTask(
      String storageGroupName,
      String dataRegionId,
      List<Long> timePartitions,
      TsFileManager tsFileManager,
      AtomicInteger currentTaskNum,
      long serialId,
      boolean isClearBegin,
      Set<TsFileIdentifier> doneFiles,
      File logFile) {

    super(storageGroupName, dataRegionId, 0L, tsFileManager, currentTaskNum, serialId);
    this.logKey = storageGroupName + dataRegionId;
    this.logFile = logFile;
    this.performer = new TsFileRewritePerformer();
    if (timePartitions == null || timePartitions.size() <= 0) {
      LOGGER.warn("[rewriteTimeseries] {} timePartitions is null or empty!!!!!!", logKey);
      return;
    }
    timePartitions.forEach(
        curTimePartition ->
            candidateResourceSelectorByTimePartition(
                storageGroupName, tsFileManager, isClearBegin, doneFiles, curTimePartition));
  }

  private void candidateResourceSelectorByTimePartition(
      String storageGroupName,
      TsFileManager tsFileManager,
      boolean isClearBegin,
      Set<TsFileIdentifier> doneFiles,
      Long curTimePartition) {
    // get seq tsFile list
    TsFileResourceList seqTsFileResourcList =
        tsFileManager.getSequenceListByTimePartition(curTimePartition);
    // Gets the device list from the cache
    Set<String> devicesCache = alteringRecordsCache.getDevicesCache(storageGroupName);
    if (seqTsFileResourcList != null && seqTsFileResourcList.size() > 0) {
      seqTsFileResourcList.forEach(
          tsFileResource -> {
            Set<String> devices = tsFileResource.getDevices();
            if (devices == null) {
              return;
            }
            // AlteringLog filter
            if (isClearBegin && doneFiles != null && doneFiles.size() > 0) {
              for (TsFileIdentifier tsFileIdentifier : doneFiles) {
                try {
                  TsFileNameGenerator.TsFileName logTsFileName =
                      TsFileNameGenerator.getTsFileName(tsFileIdentifier.getFilename());
                  TsFileNameGenerator.TsFileName tsFileName =
                      TsFileNameGenerator.getTsFileName(tsFileResource.getTsFile().getName());
                  // As long as time and version are the same, they are considered to be the same
                  // file
                  if (logTsFileName.getTime() == tsFileName.getTime()
                      && logTsFileName.getVersion() == tsFileName.getVersion()) {
                    LOGGER.info(
                        "[rewriteTimeseries] {} the file {} has been rewritten",
                        logKey,
                        tsFileResource.getTsFilePath());
                    return;
                  }
                } catch (IOException e) {
                  LOGGER.warn("tsfile-{} name parseFailed", tsFileIdentifier);
                  return;
                }
              }
            }
            // device filter
            // In most scenarios, the number of devices in the tsfile file is greater than the
            // number of devices to be modified
            for (String device : devicesCache) {
              // Looking for intersection
              // TODO Use a better algorithm instead
              if (devices.contains(device)) {
                candidateResourceList.add(tsFileResource);
                break;
              }
            }
          });
    }
  }

  /** Continuously trying to update the status to CANDIDATE for a certain amount of time */
  @Override
  public void setSourceFilesToCompactionCandidate() {

    long begin = System.currentTimeMillis();
    while (candidateResourceList.size() > 0) {
      List<TsFileResource> removeList = new ArrayList<>(candidateResourceList.size());
      candidateResourceList.forEach(
          x -> {
            if (x.isDeleted() || !x.isClosed()) {
              removeList.add(x);
              return;
            }
            if (x.getStatus() == TsFileResourceStatus.CLOSED) {
              try {
                x.setStatus(TsFileResourceStatus.COMPACTION_CANDIDATE);
                readyResourceList.add(x);
                removeList.add(x);
              } catch (Exception e) {
                // do nothing
              }
            }
          });
      candidateResourceList.removeAll(removeList);
      if ((System.currentTimeMillis() - begin) > waitTimeout) {
        LOGGER.error("[rewriteTimeseries] {} setSourceFilesToCompactionCandidate timeout", logKey);
        break;
      }
    }
  }

  @Override
  protected void doCompaction() {

    // rewrite target tsfiles
    try (AlteringLogger alteringLogger = new AlteringLogger(logFile)) {
      if (readyResourceList.isEmpty()) {
        return;
      }
      int size = readyResourceList.size();
      LOGGER.info("[rewriteTimeseries] {} rewrite begin, ready resource size:{}", logKey, size);
      for (int i = 0; i < size; i++) {
        TsFileResource tsFileResource = readyResourceList.get(i);
        if (tsFileResource == null || !tsFileResource.isClosed()) {
          return;
        }
        rewriteDataInTsFile(tsFileResource);
        // log file done
        alteringLogger.doneFile(tsFileResource);
        LOGGER.info(
            "[rewriteTimeseries] {} rewriteDataInTsFile {} end, fileNum:{}/{}, fileSize:{}",
            logKey,
            tsFileResource.getTsFilePath(),
            i + 1,
            size,
            tsFileResource.getTsFileSize());
      }
    } catch (Exception e) {
      LOGGER.error("[rewriteTimeseries] " + logKey + " error", e);
    } finally {
      // The process is complete and the logFile is deleted
      if (logFile.exists()) {
        try {
          FileUtils.delete(logFile);
        } catch (IOException e) {
          LOGGER.error("[rewriteTimeseries] " + logKey + " logFile delete failed", e);
        }
      }
    }
  }

  private void rewriteDataInTsFile(TsFileResource tsFileResource)
      throws IOException, StorageEngineException, InterruptedException, MetadataException,
          ExecutionException {

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "[rewriteTimeseries] {} rewriteDataInTsFile:{}, fileSize:{} start",
          logKey,
          tsFileResource.getTsFilePath(),
          tsFileResource.getTsFileSize());
    }
    // Generate the target tsFileResource
    TsFileResource targetTsFileResource =
        TsFileNameGenerator.generateNewAlterTsFileResource(tsFileResource);
    // Data is read from the.tsfile file, re-encoded, compressed, and written to the .alter file
    this.performer.setSourceFiles(Collections.singletonList(tsFileResource));
    this.performer.setTargetFiles(Collections.singletonList(targetTsFileResource));
    this.performer.setSummary(this.summary);
    this.performer.perform();
    boolean hasRewrite = ((TsFileRewritePerformer) this.performer).hasRewrite();
    if (!hasRewrite) {
      CompactionUtils.deleteTsFileWithoutMods(targetTsFileResource);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "[rewriteTimeseries] {} tsFile:{} does not need to be rewrite",
            logKey,
            tsFileResource.getTsFileSize());
      }

      return;
    }
    // .tsfile->.alter.old .alter->.tsfile
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("[rewriteTimeseries] {} move tsfile", logKey);
    }
    tsFileResource.moveTsFile(TSFILE_SUFFIX, ALTER_OLD_TMP_FILE_SUFFIX);
    targetTsFileResource.moveTsFile(IoTDBConstant.ALTER_TMP_FILE_SUFFIX, TSFILE_SUFFIX);
    // replace
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("[rewriteTimeseries] {} replace tsfile", logKey);
    }
    tsFileManager.replace(
        Collections.singletonList(tsFileResource),
        Collections.emptyList(),
        Collections.singletonList(targetTsFileResource),
        timePartition,
        true);
    // check & delete tsfile from disk
    checkAndDeleteOldTsFile(tsFileResource, targetTsFileResource, logKey);
  }

  private void checkAndDeleteOldTsFile(
      TsFileResource tsFileResource, TsFileResource targetTsFileResource, String logKey)
      throws IOException {
    // check
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("[rewriteTimeseries] {} check tsfile", logKey);
    }
    if (targetTsFileResource.getTsFile().exists()
        && targetTsFileResource.getTsFile().length()
            < TSFileConfig.MAGIC_STRING.getBytes().length * 2L + Byte.BYTES) {
      // the file size is smaller than magic string and version number
      throw new TsFileNotCompleteException(
          String.format(
              "target file %s is smaller than magic string and version number size",
              targetTsFileResource));
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("[rewriteTimeseries] {} delete tsfile", logKey);
    }
    LOGGER.info(
        "[rewriteTimeseries] {} alter {} finish, start to delete old files",
        logKey,
        tsFileResource.getTsFilePath());
    // delete the old files
    CompactionUtils.deleteTsFileWithoutMods(tsFileResource);
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask otherTask) {
    if (!(otherTask instanceof RewriteTimeseriesTask)) {
      return false;
    }
    RewriteTimeseriesTask task = (RewriteTimeseriesTask) otherTask;
    return task.getStorageGroupName().equals(this.getStorageGroupName())
        && task.getDataRegionId().equals(this.getDataRegionId());
  }

  /** copy from InnerSpaceCompactionTask */
  @Override
  public boolean checkValidAndSetMerging() {
    if (!tsFileManager.isAllowCompaction()) {
      return false;
    }
    try {
      for (TsFileResource resource : readyResourceList) {
        if (resource.isCompacting()
            || !resource.isClosed()
            || !resource.getTsFile().exists()
            || resource.isDeleted()) {
          // this source file cannot be compacted
          // release the lock of locked files, and return
          resetMergingStatus();
          return false;
        }
      }

      for (TsFileResource resource : readyResourceList) {
        resource.setStatus(TsFileResourceStatus.COMPACTING);
      }
    } catch (Throwable e) {
      resetMergingStatus();
      throw e;
    }
    return true;
  }

  /** set the merging status of selected files to false copy from InnerSpaceCompactionTask */
  protected void resetMergingStatus() {
    for (TsFileResource resource : readyResourceList) {
      try {
        if (!resource.isDeleted()) {
          resource.setStatus(TsFileResourceStatus.CLOSED);
        }
      } catch (Throwable e) {
        LOGGER.error("Exception occurs when resetting resource status", e);
      }
    }
  }

  @Override
  public void resetCompactionCandidateStatusForAllSourceFiles() {
    readyResourceList.forEach(x -> x.setStatus(TsFileResourceStatus.CLOSED));
  }
}
