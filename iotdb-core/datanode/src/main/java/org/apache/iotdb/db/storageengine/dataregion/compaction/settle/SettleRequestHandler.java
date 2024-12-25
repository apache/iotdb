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

package org.apache.iotdb.db.storageengine.dataregion.compaction.settle;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSettleReq;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduler;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceList;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.TsFileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@SuppressWarnings("squid:S6548")
public class SettleRequestHandler {
  private static final Logger logger = LoggerFactory.getLogger(SettleRequestHandler.class);

  private SettleRequestHandler() {}

  private boolean testMode = false;

  public boolean isTestMode() {
    return testMode;
  }

  public void setTestMode(boolean testMode) {
    this.testMode = testMode;
  }

  public static SettleRequestHandler getInstance() {
    return SettleRequestHandlerHolder.INSTANCE;
  }

  public TSStatus handleSettleRequest(TSettleReq req) {
    List<String> paths = req.getPaths();

    CompactionScheduler.exclusiveLockCompactionSelection();
    try {
      SettleRequestContext context = new SettleRequestContext(paths);
      TSStatus validationResult = context.validate();
      if (validationResult.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return validationResult;
      }
      if (testMode) {
        return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      }
      List<TsFileResource> selectedTsFileResources = context.getTsFileResourcesByFileNames();
      return context.submitCompactionTask(selectedTsFileResources);
    } finally {
      CompactionScheduler.exclusiveUnlockCompactionSelection();
    }
  }

  private static class SettleRequestContext {
    private ConsistentSettleInfo targetConsistentSettleInfo;

    private boolean hasSeqFiles;
    private boolean hasUnseqFiles;
    private boolean hasModsFiles;
    private List<String> paths;
    private Set<String> tsFileNames;
    private TsFileResourceList allTsFileResourceList;

    private TsFileManager tsFileManager;
    private IoTDBConfig config;

    private SettleRequestContext(List<String> paths) {
      this.paths = paths;
      this.tsFileNames = new HashSet<>();
      this.config = IoTDBDescriptor.getInstance().getConfig();
    }

    @SuppressWarnings("squid:S3776")
    private TSStatus validate() {
      if (paths == null || paths.isEmpty()) {
        return RpcUtils.getStatus(
            TSStatusCode.ILLEGAL_PARAMETER, "The files to settle is not offered.");
      }

      int maxInnerCompactionCandidateFileNum = config.getInnerCompactionCandidateFileNum();
      if (paths.size() > maxInnerCompactionCandidateFileNum) {
        return RpcUtils.getStatus(
            TSStatusCode.UNSUPPORTED_OPERATION,
            "Too many files offered, the limited count of system config is "
                + maxInnerCompactionCandidateFileNum
                + ", the input file count is "
                + tsFileNames.size());
      }

      TSStatus validationResult;

      for (String path : paths) {
        File currentTsFile = new File(path);
        if (!currentTsFile.exists()) {
          return RpcUtils.getStatus(
              TSStatusCode.PATH_NOT_EXIST, "The specified file does not exist in " + path);
        }
        File modFile = ModificationFile.getExclusiveMods(currentTsFile);
        hasModsFiles |= modFile.exists();

        ConsistentSettleInfo currentInfo = calculateConsistentInfo(currentTsFile);
        if (!currentInfo.isValid) {
          return RpcUtils.getStatus(
              TSStatusCode.ILLEGAL_PATH, "The File Name of the TsFile is not valid: " + path);
        }
        if (this.targetConsistentSettleInfo == null) {
          this.targetConsistentSettleInfo = currentInfo;
        }

        validationResult = targetConsistentSettleInfo.checkConsistency(currentInfo);
        if (!isSuccess(validationResult)) {
          return validationResult;
        }

        if (TsFileUtils.isSequence(currentTsFile)) {
          hasSeqFiles = true;
        } else {
          hasUnseqFiles = true;
        }
        tsFileNames.add(currentTsFile.getName());
        if (hasSeqFiles && hasUnseqFiles) {
          return RpcUtils.getStatus(
              TSStatusCode.UNSUPPORTED_OPERATION, "Settle by cross compaction is not allowed.");
        }
      }

      if (!hasModsFiles) {
        return RpcUtils.getStatus(
            TSStatusCode.ILLEGAL_PARAMETER,
            "Every selected TsFile does not contains the mods file.");
      }
      DataRegion dataRegion =
          StorageEngine.getInstance()
              .getDataRegion(new DataRegionId(targetConsistentSettleInfo.dataRegionId));
      if (dataRegion == null) {
        return RpcUtils.getStatus(TSStatusCode.ILLEGAL_PATH, "DataRegion not exist");
      }
      tsFileManager = dataRegion.getTsFileManager();

      validationResult = checkCompactionConfigs();
      if (!isSuccess(validationResult)) {
        return validationResult;
      }

      if (hasSeqFiles) {
        allTsFileResourceList =
            tsFileManager.getOrCreateSequenceListByTimePartition(
                targetConsistentSettleInfo.timePartitionId);
      } else {
        allTsFileResourceList =
            tsFileManager.getOrCreateUnsequenceListByTimePartition(
                targetConsistentSettleInfo.timePartitionId);
      }

      return validateTsFileResources();
    }

    private ConsistentSettleInfo calculateConsistentInfo(File tsFile) {
      ConsistentSettleInfo values = new ConsistentSettleInfo();
      values.dataRegionId = TsFileUtils.getDataRegionId(tsFile);
      values.storageGroupName = TsFileUtils.getStorageGroup(tsFile);
      values.timePartitionId = TsFileUtils.getTimePartition(tsFile);
      values.isValid = true;

      String fileNameStr = tsFile.getName();
      TsFileNameGenerator.TsFileName tsFileName;
      try {
        tsFileName = TsFileNameGenerator.getTsFileName(fileNameStr);
      } catch (IOException e) {
        values.isValid = false;
        return values;
      }
      values.level = tsFileName.getInnerCompactionCnt();
      return values;
    }

    private boolean isSuccess(TSStatus status) {
      return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
    }

    private TSStatus checkCompactionConfigs() {
      if (!tsFileManager.isAllowCompaction()) {
        return RpcUtils.getStatus(
            TSStatusCode.UNSUPPORTED_OPERATION, "Compaction in this DataRegion is not allowed.");
      }
      if (hasSeqFiles && !config.isEnableSeqSpaceCompaction()) {
        return RpcUtils.getStatus(
            TSStatusCode.UNSUPPORTED_OPERATION, "Compaction in Seq Space is not enabled");
      }
      if (hasUnseqFiles && !config.isEnableUnseqSpaceCompaction()) {
        return RpcUtils.getStatus(
            TSStatusCode.UNSUPPORTED_OPERATION, "Compaction in Unseq Space is not enabled");
      }
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    }

    private TSStatus validateTsFileResources() {
      int continuousCount = 0;
      for (TsFileResource tsFileResource : allTsFileResourceList) {
        File tsFile = tsFileResource.getTsFile();
        if (tsFileNames.contains(tsFile.getName())) {
          if (tsFileResource.getStatus() != TsFileResourceStatus.NORMAL) {
            return RpcUtils.getStatus(
                TSStatusCode.ILLEGAL_PARAMETER,
                "The TsFile is not valid: " + tsFile.getAbsolutePath());
          }
          continuousCount++;
        } else if (continuousCount != 0) {
          break;
        }
      }
      if (continuousCount != tsFileNames.size()) {
        return RpcUtils.getStatus(
            TSStatusCode.ILLEGAL_PARAMETER, "Selected TsFiles are not continuous.");
      }
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    }

    private List<TsFileResource> getTsFileResourcesByFileNames() {
      List<TsFileResource> selectedTsFileResources = new ArrayList<>(tsFileNames.size());
      for (TsFileResource tsFileResource : allTsFileResourceList) {
        if (tsFileNames.contains(tsFileResource.getTsFile().getName())) {
          selectedTsFileResources.add(tsFileResource);
        }
        if (selectedTsFileResources.size() == tsFileNames.size()) {
          break;
        }
      }
      return selectedTsFileResources;
    }

    private TSStatus submitCompactionTask(List<TsFileResource> tsFileResources) {
      ICompactionPerformer performer =
          hasSeqFiles
              ? config.getInnerSeqCompactionPerformer().createInstance()
              : config.getInnerUnseqCompactionPerformer().createInstance();
      AbstractCompactionTask task =
          new InnerSpaceCompactionTask(
              targetConsistentSettleInfo.timePartitionId,
              tsFileManager,
              tsFileResources,
              hasSeqFiles,
              performer,
              tsFileManager.getNextCompactionTaskId());
      try {
        CompactionTaskManager.getInstance().addTaskToWaitingQueue(task);
      } catch (InterruptedException e) {
        logger.error(
            "meet error when adding task-{} to compaction waiting queue: {}",
            task.getSerialId(),
            e.getMessage());
        Thread.currentThread().interrupt();
        return RpcUtils.getStatus(
            TSStatusCode.COMPACTION_ERROR, "meet error when submit settle task.");
      }
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    }
  }

  private static class ConsistentSettleInfo {
    private int dataRegionId;
    private int level;
    private String storageGroupName;
    private long timePartitionId;
    private boolean isValid;

    private TSStatus checkConsistency(ConsistentSettleInfo other) {
      if (this.dataRegionId != other.dataRegionId) {
        return RpcUtils.getStatus(
            TSStatusCode.ILLEGAL_PATH, "DataRegion of files is not consistent.");
      }
      if (!this.storageGroupName.equals(other.storageGroupName)) {
        return RpcUtils.getStatus(
            TSStatusCode.ILLEGAL_PATH, "StorageGroup of files is not consistent.");
      }
      if (this.timePartitionId != other.timePartitionId) {
        return RpcUtils.getStatus(
            TSStatusCode.ILLEGAL_PATH, "TimePartition of files is not consistent.");
      }
      if (this.level != other.level) {
        return RpcUtils.getStatus(
            TSStatusCode.ILLEGAL_PARAMETER, "Level of files is not consistent.");
      }
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    }
  }

  private static class SettleRequestHandlerHolder {
    private static final SettleRequestHandler INSTANCE = new SettleRequestHandler();
  }
}
