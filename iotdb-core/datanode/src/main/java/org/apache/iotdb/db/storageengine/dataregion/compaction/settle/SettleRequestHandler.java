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
import org.apache.iotdb.db.i18n.StorageEngineMessages;
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
import org.apache.iotdb.db.utils.EncryptDBUtils;
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
            TSStatusCode.ILLEGAL_PARAMETER,
            StorageEngineMessages.MESSAGE_THE_FILES_TO_SETTLE_IS_NOT_OFFERED_95C7CC1E);
      }

      int maxInnerCompactionCandidateFileNum = config.getInnerCompactionCandidateFileNum();
      if (paths.size() > maxInnerCompactionCandidateFileNum) {
        return RpcUtils.getStatus(
            TSStatusCode.UNSUPPORTED_OPERATION,
            String.format(
                StorageEngineMessages
                    .MESSAGE_TOO_MANY_FILES_OFFERED_THE_LIMITED_COUNT_OF_SYSTEM_CONFIG_IS_ARG_THE_INPUT_FILE_COUNT_IS_ARG_85D07B55,
                String.valueOf(maxInnerCompactionCandidateFileNum),
                String.valueOf(tsFileNames.size())));
      }

      TSStatus validationResult;

      for (String path : paths) {
        File currentTsFile = new File(path);
        if (!currentTsFile.exists()) {
          return RpcUtils.getStatus(
              TSStatusCode.PATH_NOT_EXIST,
              String.format(
                  StorageEngineMessages.MESSAGE_THE_SPECIFIED_FILE_DOES_NOT_EXIST_IN_ARG_EEFC638B,
                  String.valueOf(path)));
        }
        File modFile = ModificationFile.getExclusiveMods(currentTsFile);
        hasModsFiles |= modFile.exists();

        ConsistentSettleInfo currentInfo = calculateConsistentInfo(currentTsFile);
        if (!currentInfo.isValid) {
          return RpcUtils.getStatus(
              TSStatusCode.ILLEGAL_PATH,
              String.format(
                  StorageEngineMessages
                      .MESSAGE_THE_FILE_NAME_OF_THE_TSFILE_IS_NOT_VALID_ARG_67195C7D,
                  String.valueOf(path)));
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
              TSStatusCode.UNSUPPORTED_OPERATION,
              StorageEngineMessages.MESSAGE_SETTLE_BY_CROSS_COMPACTION_IS_NOT_ALLOWED_FD1312E6);
        }
      }

      if (!hasModsFiles) {
        return RpcUtils.getStatus(
            TSStatusCode.ILLEGAL_PARAMETER,
            StorageEngineMessages
                .MESSAGE_EVERY_SELECTED_TSFILE_DOES_NOT_CONTAINS_THE_MODS_FILE_EB7EDC38);
      }
      DataRegion dataRegion =
          StorageEngine.getInstance()
              .getDataRegion(new DataRegionId(targetConsistentSettleInfo.dataRegionId));
      if (dataRegion == null) {
        return RpcUtils.getStatus(
            TSStatusCode.ILLEGAL_PATH, StorageEngineMessages.MESSAGE_DATAREGION_NOT_EXIST_34C4A229);
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
            TSStatusCode.UNSUPPORTED_OPERATION,
            StorageEngineMessages.MESSAGE_COMPACTION_IN_THIS_DATAREGION_IS_NOT_ALLOWED_0549D4A4);
      }
      if (hasSeqFiles && !config.isEnableSeqSpaceCompaction()) {
        return RpcUtils.getStatus(
            TSStatusCode.UNSUPPORTED_OPERATION,
            StorageEngineMessages.MESSAGE_COMPACTION_IN_SEQ_SPACE_IS_NOT_ENABLED_045A20C5);
      }
      if (hasUnseqFiles && !config.isEnableUnseqSpaceCompaction()) {
        return RpcUtils.getStatus(
            TSStatusCode.UNSUPPORTED_OPERATION,
            StorageEngineMessages.MESSAGE_COMPACTION_IN_UNSEQ_SPACE_IS_NOT_ENABLED_952F1A1E);
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
                String.format(
                    StorageEngineMessages.MESSAGE_THE_TSFILE_IS_NOT_VALID_ARG_5B5DC96E,
                    String.valueOf(tsFile.getAbsolutePath())));
          }
          continuousCount++;
        } else if (continuousCount != 0) {
          break;
        }
      }
      if (continuousCount != tsFileNames.size()) {
        return RpcUtils.getStatus(
            TSStatusCode.ILLEGAL_PARAMETER,
            StorageEngineMessages.MESSAGE_SELECTED_TSFILES_ARE_NOT_CONTINUOUS_853F6027);
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
              ? config
                  .getInnerSeqCompactionPerformer()
                  .createInstance(
                      EncryptDBUtils.getFirstEncryptParamFromDatabase(
                          tsFileManager.getStorageGroupName()))
              : config
                  .getInnerUnseqCompactionPerformer()
                  .createInstance(
                      EncryptDBUtils.getFirstEncryptParamFromDatabase(
                          tsFileManager.getStorageGroupName()));
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
            StorageEngineMessages
                .STORAGE_LOG_MEET_ERROR_WHEN_ADDING_TASK_TO_COMPACTION_WAITING_QUEUE_84AA345D,
            task.getSerialId(),
            e.getMessage());
        Thread.currentThread().interrupt();
        return RpcUtils.getStatus(
            TSStatusCode.COMPACTION_ERROR,
            StorageEngineMessages.MESSAGE_MEET_ERROR_WHEN_SUBMIT_SETTLE_TASK_2F356FA4);
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
            TSStatusCode.ILLEGAL_PATH,
            StorageEngineMessages.MESSAGE_DATAREGION_OF_FILES_IS_NOT_CONSISTENT_CFBDEDC4);
      }
      if (!this.storageGroupName.equals(other.storageGroupName)) {
        return RpcUtils.getStatus(
            TSStatusCode.ILLEGAL_PATH,
            StorageEngineMessages.MESSAGE_STORAGEGROUP_OF_FILES_IS_NOT_CONSISTENT_2EA6791C);
      }
      if (this.timePartitionId != other.timePartitionId) {
        return RpcUtils.getStatus(
            TSStatusCode.ILLEGAL_PATH,
            StorageEngineMessages.MESSAGE_TIMEPARTITION_OF_FILES_IS_NOT_CONSISTENT_AB12799B);
      }
      if (this.level != other.level) {
        return RpcUtils.getStatus(
            TSStatusCode.ILLEGAL_PARAMETER,
            StorageEngineMessages.MESSAGE_LEVEL_OF_FILES_IS_NOT_CONSISTENT_84F11E20);
      }
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    }
  }

  private static class SettleRequestHandlerHolder {
    private static final SettleRequestHandler INSTANCE = new SettleRequestHandler();
  }
}
