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

package org.apache.iotdb.db.engine.settle;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSettleReq;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.inner.InnerSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.TsFileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SettleRequestHandler {

  public static SettleRequestHandler getInstance() {
    return SettleRequestHandlerHolder.INSTANCE;
  }

  public TSStatus handleSettleRequest(TSettleReq req) {
    List<String> paths = req.getPaths();

    boolean hasSeqFile = false, hasUnSeqFile = false, hasModsFile = false;
    Integer dataRegionId = null;
    DataRegion dataRegion = null;
    Integer level = null;
    String storageGroupName = null;
    Long timePartitionId = null;

    Set<String> tsFileNames = new HashSet<>();

    for (String path : paths) {
      File tsFile = new File(path);
      if (!tsFile.exists()) {
        return RpcUtils.getStatus(TSStatusCode.PATH_NOT_EXIST);
      }
      File modsFile = new File(path + ".mods");
      hasModsFile |= modsFile.exists();

      int fileDataRegionId = TsFileUtils.getDataRegionId(tsFile);
      if (dataRegion == null) {
        dataRegionId = fileDataRegionId;
        dataRegion = StorageEngine.getInstance().getDataRegion(new DataRegionId(dataRegionId));
      } else if (dataRegionId != fileDataRegionId) {
        return RpcUtils.getStatus(TSStatusCode.ILLEGAL_PATH);
      }

      String sgOfFile = TsFileUtils.getStorageGroup(tsFile);
      if (storageGroupName == null) {
        storageGroupName = sgOfFile;
      } else if (!storageGroupName.equals(sgOfFile)) {
        return RpcUtils.getStatus(TSStatusCode.ILLEGAL_PATH);
      }

      long timePartitionOfFile = TsFileUtils.getTimePartition(tsFile);
      if (timePartitionId == null) {
        timePartitionId = timePartitionOfFile;
      } else if (timePartitionId != timePartitionOfFile) {
        return RpcUtils.getStatus(TSStatusCode.ILLEGAL_PATH);
      }

      String fileName = tsFile.getName();
      TsFileNameGenerator.TsFileName tsFileName;
      try {
        tsFileName = TsFileNameGenerator.getTsFileName(fileName);
      } catch (IOException e) {
        return RpcUtils.getStatus(TSStatusCode.ILLEGAL_PATH);
      }

      int levelOfFile = tsFileName.getInnerCompactionCnt();
      if (level == null) {
        level = levelOfFile;
      } else if (level != levelOfFile) {
        return RpcUtils.getStatus(TSStatusCode.ILLEGAL_PARAMETER);
      }

      if (TsFileUtils.isSequence(tsFile)) {
        hasSeqFile = true;
      } else {
        hasUnSeqFile = true;
      }
      tsFileNames.add(fileName);
      if (hasSeqFile && hasUnSeqFile) {
        return RpcUtils.getStatus(TSStatusCode.UNSUPPORTED_OPERATION);
      }
    }

    if (!hasModsFile) {
      return RpcUtils.getStatus(TSStatusCode.ILLEGAL_PARAMETER);
    }
    if (dataRegion == null) {
      return RpcUtils.getStatus(TSStatusCode.ILLEGAL_PATH);
    }
    TsFileManager tsFileManager = dataRegion.getTsFileManager();
    if (!tsFileManager.isAllowCompaction()) {
      return RpcUtils.getStatus(TSStatusCode.COMPACTION_ERROR);
    }

    List<TsFileResource> tsFileResources
        = getTsFileResourcesByFileNames(tsFileManager, timePartitionId, hasSeqFile, tsFileNames);
    if (tsFileResources.size() != tsFileNames.size()) {
      return RpcUtils.getStatus(TSStatusCode.ILLEGAL_PARAMETER);
    }

    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    if (hasSeqFile && !config.isEnableSeqSpaceCompaction()) {
      return RpcUtils.getStatus(TSStatusCode.UNSUPPORTED_OPERATION);
    }
    if (hasUnSeqFile && !config.isEnableUnseqSpaceCompaction()) {
      return RpcUtils.getStatus(TSStatusCode.UNSUPPORTED_OPERATION);
    }

    AbstractCompactionTask task = new InnerSpaceCompactionTask(timePartitionId,
        tsFileManager,
        tsFileResources,
        hasSeqFile,
        config.getInnerSeqCompactionPerformer().createInstance(),
        CompactionTaskManager.currentTaskNum,
        tsFileManager.getNextCompactionTaskId());
    try {
      CompactionTaskManager.getInstance().addTaskToWaitingQueue(task);
    } catch (InterruptedException e) {
      return RpcUtils.getStatus(TSStatusCode.COMPACTION_ERROR);
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  private List<TsFileResource> getTsFileResourcesByFileNames(TsFileManager tsFileManager,
                                                             long timePartition,
                                                             boolean isSeq,
                                                             Set<String> fileNames) {
    TsFileResourceList allTsFileResourceList;
    if (isSeq) {
      allTsFileResourceList = tsFileManager.getSequenceListByTimePartition(timePartition);
    } else {
      allTsFileResourceList = tsFileManager.getUnsequenceListByTimePartition(timePartition);
    }

    boolean selected = false;
    List<TsFileResource> selectedTsFileResources = new ArrayList<>();
    for (TsFileResource tsFileResource : allTsFileResourceList) {
      if (fileNames.contains(tsFileResource.getTsFile().getName())) {
        boolean tsFileResourceNotValid = !tsFileResource.isClosed() || tsFileResource.isDeleted()
            || tsFileResource.isCompacting() || tsFileResource.isCompactionCandidate();
        if (tsFileResourceNotValid) {
          break;
        }
        selectedTsFileResources.add(tsFileResource);
        selected = true;
      } else if (selected) {
        break;
      }
    }
    return selectedTsFileResources;
  }

  private static class SettleRequestHandlerHolder {
    private static final SettleRequestHandler INSTANCE = new SettleRequestHandler();
  }
}
