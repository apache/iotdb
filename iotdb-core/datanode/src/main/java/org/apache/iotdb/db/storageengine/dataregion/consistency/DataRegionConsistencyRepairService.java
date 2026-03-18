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

package org.apache.iotdb.db.storageengine.dataregion.consistency;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.queryengine.plan.scheduler.load.LoadTsFileScheduler;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileSplitter;
import org.apache.iotdb.mpp.rpc.thrift.TConsistencyMerkleFile;
import org.apache.iotdb.mpp.rpc.thrift.TDataRegionConsistencySnapshotReq;
import org.apache.iotdb.mpp.rpc.thrift.TDataRegionConsistencySnapshotResp;
import org.apache.iotdb.mpp.rpc.thrift.TLoadCommandReq;
import org.apache.iotdb.mpp.rpc.thrift.TLoadResp;
import org.apache.iotdb.mpp.rpc.thrift.TRepairTransferTsFileReq;
import org.apache.iotdb.mpp.rpc.thrift.TTimePartitionConsistencyView;
import org.apache.iotdb.mpp.rpc.thrift.TTsFilePieceReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.PublicBAOS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** DataNode-side snapshot and direct TsFile repair primitives for replica consistency repair. */
public class DataRegionConsistencyRepairService {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DataRegionConsistencyRepairService.class);

  private static final long MAX_PIECE_NODE_SIZE =
      IoTDBDescriptor.getInstance().getConfig().getThriftMaxFrameSize() >> 2;

  private final StorageEngine storageEngine = StorageEngine.getInstance();

  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager =
      new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
          .createClientManager(
              new ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());

  public TDataRegionConsistencySnapshotResp getSnapshot(TDataRegionConsistencySnapshotReq req) {
    DataRegion dataRegion = getDataRegion(req.getConsensusGroupId());
    if (dataRegion == null) {
      return new TDataRegionConsistencySnapshotResp(
          RpcUtils.getStatus(
              TSStatusCode.DATAREGION_PROCESS_ERROR,
              "DataRegion " + req.getConsensusGroupId() + " is not found on this DataNode"));
    }

    List<TTimePartitionConsistencyView> partitionViews = new ArrayList<>();
    List<Long> timePartitions = new ArrayList<>(dataRegion.getTimePartitions());
    timePartitions.sort(Long::compareTo);

    try {
      for (Long timePartition : timePartitions) {
        List<TConsistencyMerkleFile> merkleFiles = collectMerkleFiles(dataRegion, timePartition);
        if (!merkleFiles.isEmpty()) {
          partitionViews.add(new TTimePartitionConsistencyView(timePartition, merkleFiles));
        }
      }
      return new TDataRegionConsistencySnapshotResp(RpcUtils.SUCCESS_STATUS)
          .setTimePartitionViews(partitionViews);
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to build consistency snapshot for region {}", req.getConsensusGroupId(), e);
      return new TDataRegionConsistencySnapshotResp(
          RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage()));
    }
  }

  public TSStatus repairTransferTsFile(TRepairTransferTsFileReq req) {
    DataRegion dataRegion = getDataRegion(req.getConsensusGroupId());
    if (dataRegion == null) {
      return RpcUtils.getStatus(
          TSStatusCode.DATAREGION_PROCESS_ERROR,
          "DataRegion " + req.getConsensusGroupId() + " is not found on this DataNode");
    }
    if (req.getTargetDataNodes() == null || req.getTargetDataNodes().isEmpty()) {
      return RpcUtils.getStatus(
          TSStatusCode.ILLEGAL_PARAMETER, "Repair transfer requires at least one target DataNode");
    }

    TsFileResource tsFileResource = findTsFileResource(dataRegion, req.getSourceTsFilePath());
    if (tsFileResource == null) {
      return RpcUtils.getStatus(
          TSStatusCode.LOAD_FILE_ERROR,
          "Cannot find sealed TsFile " + req.getSourceTsFilePath() + " on leader");
    }
    if (!tsFileResource.isClosed()
        || tsFileResource.isDeleted()
        || !tsFileResource.getTsFile().exists()) {
      return RpcUtils.getStatus(
          TSStatusCode.LOAD_FILE_ERROR,
          "TsFile " + req.getSourceTsFilePath() + " is not available for repair transfer");
    }
    if (tsFileResource.isSpanMultiTimePartitions()) {
      return RpcUtils.getStatus(
          TSStatusCode.UNSUPPORTED_OPERATION,
          "Replica consistency repair does not support multi-time-partition TsFiles yet");
    }

    for (TDataNodeLocation targetDataNode : req.getTargetDataNodes()) {
      TSStatus status =
          transferOneTarget(req.getConsensusGroupId(), tsFileResource, targetDataNode);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }
    }
    return RpcUtils.SUCCESS_STATUS;
  }

  private List<TConsistencyMerkleFile> collectMerkleFiles(DataRegion dataRegion, long timePartition)
      throws IOException {
    List<TConsistencyMerkleFile> merkleFiles = new ArrayList<>();
    for (boolean sequence : new boolean[] {true, false}) {
      for (TsFileResource tsFileResource :
          dataRegion.getTsFileManager().getTsFileListSnapshot(timePartition, sequence)) {
        if (!tsFileResource.isClosed()
            || tsFileResource.isDeleted()
            || !tsFileResource.getTsFile().exists()) {
          continue;
        }

        String tsFilePath = tsFileResource.getTsFilePath();
        List<org.apache.iotdb.commons.consensus.iotv2.consistency.merkle.MerkleEntry> entries =
            MerkleHashComputer.computeEntries(tsFilePath);
        merkleFiles.add(
            new TConsistencyMerkleFile(
                tsFilePath,
                tsFileResource.getTsFileSize(),
                org.apache.iotdb.commons.consensus.iotv2.consistency.merkle.MerkleFileWriter
                    .computeFileXorHash(entries),
                org.apache.iotdb.commons.consensus.iotv2.consistency.merkle.MerkleFileWriter
                    .computeFileAddHash(entries)));
      }
    }

    merkleFiles.sort(Comparator.comparing(TConsistencyMerkleFile::getSourceTsFilePath));
    return merkleFiles;
  }

  private TSStatus transferOneTarget(
      TConsensusGroupId consensusGroupId,
      TsFileResource tsFileResource,
      TDataNodeLocation targetDataNode) {
    String uuid = UUID.randomUUID().toString();
    TransferTracker tracker = new TransferTracker();

    try {
      sendAllPieces(
          tsFileResource.getTsFile(),
          uuid,
          consensusGroupId,
          targetDataNode.getInternalEndPoint(),
          tracker);
      TSStatus secondPhaseStatus =
          sendLoadCommand(
              targetDataNode.getInternalEndPoint(),
              buildLoadCommandReq(uuid, tsFileResource, LoadTsFileScheduler.LoadCommand.EXECUTE));
      if (secondPhaseStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return secondPhaseStatus;
      }
      return RpcUtils.SUCCESS_STATUS;
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to transfer TsFile {} to follower {}",
          tsFileResource.getTsFilePath(),
          targetDataNode.getDataNodeId(),
          e);
      if (tracker.hasSentPieces) {
        try {
          TSStatus rollbackStatus =
              sendLoadCommand(
                  targetDataNode.getInternalEndPoint(),
                  buildLoadCommandReq(
                      uuid, tsFileResource, LoadTsFileScheduler.LoadCommand.ROLLBACK));
          if (rollbackStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            rollbackStatus.setMessage(
                rollbackStatus.getMessage() + ", original transfer failure: " + e.getMessage());
            return rollbackStatus;
          }
        } catch (IOException rollbackBuildException) {
          return RpcUtils.getStatus(
              TSStatusCode.LOAD_FILE_ERROR,
              "Failed to build rollback command for TsFile "
                  + tsFileResource.getTsFilePath()
                  + ": "
                  + rollbackBuildException.getMessage()
                  + ", original transfer failure: "
                  + e.getMessage());
        }
      }
      return RpcUtils.getStatus(
          TSStatusCode.LOAD_FILE_ERROR,
          "Failed to transfer TsFile "
              + tsFileResource.getTsFilePath()
              + " to follower "
              + targetDataNode.getDataNodeId()
              + ": "
              + e.getMessage());
    }
  }

  private void sendAllPieces(
      File tsFile,
      String uuid,
      TConsensusGroupId consensusGroupId,
      TEndPoint targetEndPoint,
      TransferTracker tracker)
      throws IOException, LoadFileException {
    final LoadTsFilePieceNode[] pieceHolder = {
      new LoadTsFilePieceNode(new PlanNodeId("repair-tsfile-piece"), tsFile)
    };

    new TsFileSplitter(
            tsFile,
            tsFileData -> {
              pieceHolder[0].addTsFileData(tsFileData);
              if (pieceHolder[0].getDataSize() >= MAX_PIECE_NODE_SIZE) {
                dispatchPieceNode(targetEndPoint, uuid, consensusGroupId, pieceHolder[0]);
                tracker.hasSentPieces = true;
                pieceHolder[0] =
                    new LoadTsFilePieceNode(new PlanNodeId("repair-tsfile-piece"), tsFile);
              }
              return true;
            })
        .splitTsFileByDataPartition();

    if (pieceHolder[0].getDataSize() > 0) {
      dispatchPieceNode(targetEndPoint, uuid, consensusGroupId, pieceHolder[0]);
      tracker.hasSentPieces = true;
    }
  }

  private void dispatchPieceNode(
      TEndPoint targetEndPoint,
      String uuid,
      TConsensusGroupId consensusGroupId,
      LoadTsFilePieceNode pieceNode)
      throws LoadFileException {
    TTsFilePieceReq request =
        new TTsFilePieceReq(pieceNode.serializeToByteBuffer(), uuid, consensusGroupId);
    try (SyncDataNodeInternalServiceClient client = clientManager.borrowClient(targetEndPoint)) {
      TLoadResp response = client.sendTsFilePieceNode(request);
      if (!response.isAccepted()) {
        throw new LoadFileException(
            response.isSetStatus() ? response.getStatus().getMessage() : response.getMessage());
      }
    } catch (LoadFileException e) {
      throw e;
    } catch (Exception e) {
      throw new LoadFileException(
          "Failed to dispatch TsFile piece to DataNode " + targetEndPoint, e);
    }
  }

  private TSStatus sendLoadCommand(TEndPoint targetEndPoint, TLoadCommandReq request) {
    try (SyncDataNodeInternalServiceClient client = clientManager.borrowClient(targetEndPoint)) {
      TLoadResp response = client.sendLoadCommand(request);
      if (response.isAccepted()) {
        return RpcUtils.SUCCESS_STATUS;
      }
      if (response.isSetStatus()) {
        return response.getStatus();
      }
      return RpcUtils.getStatus(TSStatusCode.LOAD_FILE_ERROR, response.getMessage());
    } catch (Exception e) {
      return RpcUtils.getStatus(TSStatusCode.LOAD_FILE_ERROR, e.getMessage());
    }
  }

  private TLoadCommandReq buildLoadCommandReq(
      String uuid, TsFileResource tsFileResource, LoadTsFileScheduler.LoadCommand loadCommand)
      throws IOException {
    TLoadCommandReq request = new TLoadCommandReq(loadCommand.ordinal(), uuid);
    Map<TTimePartitionSlot, ByteBuffer> timePartition2ProgressIndex = new HashMap<>();
    timePartition2ProgressIndex.put(
        new TTimePartitionSlot(tsFileResource.getTimePartition()),
        serializeProgressIndex(tsFileResource.getMaxProgressIndex()));
    request.setTimePartition2ProgressIndex(timePartition2ProgressIndex);
    request.setIsGeneratedByPipe(false);
    return request;
  }

  private ByteBuffer serializeProgressIndex(ProgressIndex progressIndex) throws IOException {
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
      progressIndex.serialize(dataOutputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  private TsFileResource findTsFileResource(DataRegion dataRegion, String sourceTsFilePath) {
    List<Long> timePartitions = new ArrayList<>(dataRegion.getTimePartitions());
    timePartitions.sort(Long::compareTo);

    for (Long timePartition : timePartitions) {
      for (boolean sequence : new boolean[] {true, false}) {
        for (TsFileResource tsFileResource :
            dataRegion.getTsFileManager().getTsFileListSnapshot(timePartition, sequence)) {
          if (sourceTsFilePath.equals(tsFileResource.getTsFilePath())) {
            return tsFileResource;
          }
        }
      }
    }
    return null;
  }

  private DataRegion getDataRegion(TConsensusGroupId consensusGroupId) {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(consensusGroupId);
    if (!(groupId instanceof DataRegionId)) {
      return null;
    }
    return storageEngine.getDataRegion((DataRegionId) groupId);
  }

  private static final class TransferTracker {
    private boolean hasSentPieces;
  }
}
