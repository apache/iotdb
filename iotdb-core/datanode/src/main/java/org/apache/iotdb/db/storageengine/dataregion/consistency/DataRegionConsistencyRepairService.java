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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.consensus.pipe.metric.IoTConsensusV2SyncLagManager;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResource;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResourceManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.AbstractDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.mpp.rpc.thrift.TApplyLogicalRepairBatchReq;
import org.apache.iotdb.mpp.rpc.thrift.TConsistencyDeletionSummary;
import org.apache.iotdb.mpp.rpc.thrift.TDecodeLeafDiffReq;
import org.apache.iotdb.mpp.rpc.thrift.TDecodeLeafDiffResp;
import org.apache.iotdb.mpp.rpc.thrift.TEstimateLeafDiffReq;
import org.apache.iotdb.mpp.rpc.thrift.TEstimateLeafDiffResp;
import org.apache.iotdb.mpp.rpc.thrift.TFinishLogicalRepairSessionReq;
import org.apache.iotdb.mpp.rpc.thrift.TGetConsistencyEligibilityReq;
import org.apache.iotdb.mpp.rpc.thrift.TGetConsistencyEligibilityResp;
import org.apache.iotdb.mpp.rpc.thrift.TGetSnapshotSubtreeReq;
import org.apache.iotdb.mpp.rpc.thrift.TGetSnapshotSubtreeResp;
import org.apache.iotdb.mpp.rpc.thrift.TLeafDiffEntry;
import org.apache.iotdb.mpp.rpc.thrift.TLeafDiffEstimate;
import org.apache.iotdb.mpp.rpc.thrift.TLogicalRepairBatch;
import org.apache.iotdb.mpp.rpc.thrift.TLogicalRepairLeafSelector;
import org.apache.iotdb.mpp.rpc.thrift.TPartitionConsistencyEligibility;
import org.apache.iotdb.mpp.rpc.thrift.TSnapshotSubtreeNode;
import org.apache.iotdb.mpp.rpc.thrift.TStreamLogicalRepairReq;
import org.apache.iotdb.mpp.rpc.thrift.TStreamLogicalRepairResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.PublicBAOS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/** DataNode-side logical snapshot and logical repair primitives for replica consistency repair. */
public class DataRegionConsistencyRepairService {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DataRegionConsistencyRepairService.class);

  private final StorageEngine storageEngine = StorageEngine.getInstance();
  private final DataRegionConsistencyManager consistencyManager =
      DataRegionConsistencyManager.getInstance();
  private final LogicalRepairSessionJournal repairSessionJournal =
      new LogicalRepairSessionJournal();

  public TGetConsistencyEligibilityResp getConsistencyEligibility(
      TGetConsistencyEligibilityReq req) {
    DataRegion dataRegion = getDataRegion(req.getConsensusGroupId());
    if (dataRegion == null) {
      return new TGetConsistencyEligibilityResp(
          RpcUtils.getStatus(
              TSStatusCode.DATAREGION_PROCESS_ERROR,
              "DataRegion " + req.getConsensusGroupId() + " is not found on this DataNode"),
          Long.MAX_VALUE,
          Long.MIN_VALUE);
    }

    try {
      long syncLag =
          IoTConsensusV2SyncLagManager.getInstance(
                  normalizeConsensusGroupIdString(req.getConsensusGroupId()))
              .calculateSyncLag();
      long safeWatermark =
          dataRegion.getDelayAnalyzer() == null
              ? Long.MAX_VALUE
              : dataRegion.getDelayAnalyzer().getSafeWatermark(System.currentTimeMillis());
      List<TConsistencyDeletionSummary> regionDeletionSummaries =
          collectDeletionSummaries(req.getConsensusGroupId());
      List<Long> timePartitions = new ArrayList<>(dataRegion.getTimePartitions());
      augmentTimePartitionsWithDeletionRanges(timePartitions, regionDeletionSummaries);
      timePartitions.sort(Long::compareTo);

      List<TPartitionConsistencyEligibility> partitions = new ArrayList<>();
      for (Long timePartition : timePartitions) {
        // Eligibility must expose follower partitions even when the follower-local DelayAnalyzer
        // has
        // not warmed up yet. ConfigNode applies cold-partition pruning from the leader view; if we
        // filter here on every replica, a follower can disappear from the compare set and keep the
        // partition stuck in DIRTY forever despite the leader already being safe.
        List<TConsistencyDeletionSummary> partitionDeletionSummaries =
            filterDeletionSummariesForPartition(regionDeletionSummaries, timePartition);
        DataRegionConsistencyManager.PartitionInspection inspection =
            consistencyManager.inspectPartition(
                req.getConsensusGroupId(), dataRegion, timePartition, partitionDeletionSummaries);
        partitions.add(
            new TPartitionConsistencyEligibility(
                inspection.getPartitionId(),
                inspection.getPartitionMutationEpoch(),
                inspection.getSnapshotEpoch(),
                inspection.getSnapshotState().name(),
                inspection.getLiveRootDigest().getXorHash(),
                inspection.getLiveRootDigest().getAdditiveHash(),
                inspection.getTombstoneRootDigest().getXorHash(),
                inspection.getTombstoneRootDigest().getAdditiveHash()));
      }

      return new TGetConsistencyEligibilityResp(RpcUtils.SUCCESS_STATUS, syncLag, safeWatermark)
          .setPartitions(partitions);
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to build consistency eligibility for region {}", req.getConsensusGroupId(), e);
      return new TGetConsistencyEligibilityResp(
          RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage()),
          Long.MAX_VALUE,
          Long.MIN_VALUE);
    }
  }

  public TGetSnapshotSubtreeResp getSnapshotSubtree(TGetSnapshotSubtreeReq req) {
    DataRegion dataRegion = getDataRegion(req.getConsensusGroupId());
    if (dataRegion == null) {
      return new TGetSnapshotSubtreeResp(
              RpcUtils.getStatus(
                  TSStatusCode.DATAREGION_PROCESS_ERROR,
                  "DataRegion " + req.getConsensusGroupId() + " is not found on this DataNode"),
              req.getTimePartitionId(),
              req.getSnapshotEpoch())
          .setStale(true);
    }
    try {
      List<TConsistencyDeletionSummary> partitionDeletionSummaries =
          filterDeletionSummariesForPartition(
              collectDeletionSummaries(req.getConsensusGroupId()), req.getTimePartitionId());
      DataRegionConsistencyManager.SnapshotSubtreeResult subtreeResult =
          consistencyManager.getSnapshotSubtree(
              req.getConsensusGroupId(),
              dataRegion,
              req.getTimePartitionId(),
              req.getSnapshotEpoch(),
              req.getTreeKind(),
              req.getNodeHandles(),
              partitionDeletionSummaries);
      List<TSnapshotSubtreeNode> nodes =
          subtreeResult.getNodes().stream()
              .map(
                  node ->
                      new TSnapshotSubtreeNode(
                              node.getParentNodeHandle(),
                              node.getNodeHandle(),
                              req.getTreeKind(),
                              node.getDepth(),
                              node.isLeaf(),
                              node.getDigest().getXorHash(),
                              node.getDigest().getAdditiveHash(),
                              node.getItemCount())
                          .setLeafId(node.getLeafId())
                          .setKeyRangeStart(node.getKeyRangeStart())
                          .setKeyRangeEnd(node.getKeyRangeEnd()))
              .collect(Collectors.toList());
      return new TGetSnapshotSubtreeResp(
              RpcUtils.SUCCESS_STATUS, req.getTimePartitionId(), subtreeResult.getSnapshotEpoch())
          .setStale(subtreeResult.isStale())
          .setNodes(nodes);
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to build snapshot subtree for region {} partition {}",
          req.getConsensusGroupId(),
          req.getTimePartitionId(),
          e);
      return new TGetSnapshotSubtreeResp(
              RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage()),
              req.getTimePartitionId(),
              req.getSnapshotEpoch())
          .setStale(true);
    }
  }

  public TEstimateLeafDiffResp estimateLeafDiff(TEstimateLeafDiffReq req) {
    DataRegion dataRegion = getDataRegion(req.getConsensusGroupId());
    if (dataRegion == null) {
      return new TEstimateLeafDiffResp(
              RpcUtils.getStatus(
                  TSStatusCode.DATAREGION_PROCESS_ERROR,
                  "DataRegion " + req.getConsensusGroupId() + " is not found on this DataNode"),
              req.getTimePartitionId(),
              req.getSnapshotEpoch())
          .setStale(true);
    }
    try {
      List<TConsistencyDeletionSummary> partitionDeletionSummaries =
          filterDeletionSummariesForPartition(
              collectDeletionSummaries(req.getConsensusGroupId()), req.getTimePartitionId());
      DataRegionConsistencyManager.LeafEstimate estimate =
          consistencyManager.estimateLeaf(
              req.getConsensusGroupId(),
              dataRegion,
              req.getTimePartitionId(),
              req.getSnapshotEpoch(),
              req.getTreeKind(),
              req.getLeafId(),
              partitionDeletionSummaries);
      if (estimate == null) {
        return new TEstimateLeafDiffResp(
                RpcUtils.SUCCESS_STATUS, req.getTimePartitionId(), req.getSnapshotEpoch())
            .setStale(true);
      }
      return new TEstimateLeafDiffResp(
              RpcUtils.SUCCESS_STATUS, req.getTimePartitionId(), estimate.getSnapshotEpoch())
          .setLeafDiff(
              new TLeafDiffEstimate(
                      estimate.getPartitionId(),
                      estimate.getSnapshotEpoch(),
                      estimate.getTreeKind(),
                      estimate.getLeafId(),
                      estimate.getRowCount(),
                      estimate.getTombstoneCount(),
                      estimate.getStrataEstimate())
                  .setKeyRangeStart(estimate.getKeyRangeStart())
                  .setKeyRangeEnd(estimate.getKeyRangeEnd()))
          .setStale(false);
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to estimate leaf diff for region {} partition {} leaf {}",
          req.getConsensusGroupId(),
          req.getTimePartitionId(),
          req.getLeafId(),
          e);
      return new TEstimateLeafDiffResp(
              RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage()),
              req.getTimePartitionId(),
              req.getSnapshotEpoch())
          .setStale(true);
    }
  }

  public TDecodeLeafDiffResp decodeLeafDiff(TDecodeLeafDiffReq req) {
    DataRegion dataRegion = getDataRegion(req.getConsensusGroupId());
    if (dataRegion == null) {
      return new TDecodeLeafDiffResp(
              RpcUtils.getStatus(
                  TSStatusCode.DATAREGION_PROCESS_ERROR,
                  "DataRegion " + req.getConsensusGroupId() + " is not found on this DataNode"),
              req.getTimePartitionId(),
              req.getSnapshotEpoch())
          .setStale(true);
    }
    try {
      List<TConsistencyDeletionSummary> partitionDeletionSummaries =
          filterDeletionSummariesForPartition(
              collectDeletionSummaries(req.getConsensusGroupId()), req.getTimePartitionId());
      List<DataRegionConsistencyManager.LeafDiffEntry> entries =
          consistencyManager.decodeLeaf(
              req.getConsensusGroupId(),
              dataRegion,
              req.getTimePartitionId(),
              req.getSnapshotEpoch(),
              req.getTreeKind(),
              req.getLeafId(),
              partitionDeletionSummaries);
      if (entries == null) {
        return new TDecodeLeafDiffResp(
                RpcUtils.SUCCESS_STATUS, req.getTimePartitionId(), req.getSnapshotEpoch())
            .setStale(true);
      }
      return new TDecodeLeafDiffResp(
              RpcUtils.SUCCESS_STATUS, req.getTimePartitionId(), req.getSnapshotEpoch())
          .setStale(false)
          .setDiffEntries(
              entries.stream()
                  .map(entry -> new TLeafDiffEntry(entry.getLogicalKey(), entry.getDiffType()))
                  .collect(Collectors.toList()));
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to decode leaf diff for region {} partition {} leaf {}",
          req.getConsensusGroupId(),
          req.getTimePartitionId(),
          req.getLeafId(),
          e);
      return new TDecodeLeafDiffResp(
              RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage()),
              req.getTimePartitionId(),
              req.getSnapshotEpoch())
          .setStale(true);
    }
  }

  public TStreamLogicalRepairResp streamLogicalRepair(TStreamLogicalRepairReq req) {
    DataRegion dataRegion = getDataRegion(req.getConsensusGroupId());
    if (dataRegion == null) {
      return new TStreamLogicalRepairResp(
              RpcUtils.getStatus(
                  TSStatusCode.DATAREGION_PROCESS_ERROR,
                  "DataRegion " + req.getConsensusGroupId() + " is not found on this DataNode"),
              req.getTimePartitionId())
          .setStale(true);
    }
    try {
      List<TConsistencyDeletionSummary> partitionDeletionSummaries =
          filterDeletionSummariesForPartition(
              collectDeletionSummaries(req.getConsensusGroupId()), req.getTimePartitionId());
      List<DataRegionConsistencyManager.LeafSelector> leafSelectors = new ArrayList<>();
      for (TLogicalRepairLeafSelector leafSelector : req.getLeafSelectors()) {
        leafSelectors.add(
            new DataRegionConsistencyManager.LeafSelector(
                leafSelector.getTreeKind(), leafSelector.getLeafId()));
      }
      List<DataRegionConsistencyManager.LogicalRepairBatch> batches =
          consistencyManager.streamLogicalRepair(
              req.getConsensusGroupId(),
              dataRegion,
              req.getTimePartitionId(),
              req.getRepairEpoch(),
              leafSelectors,
              partitionDeletionSummaries);
      List<TLogicalRepairBatch> thriftBatches =
          batches.stream()
              .map(
                  batch ->
                      new TLogicalRepairBatch(
                          batch.getSessionId(),
                          batch.getTreeKind(),
                          batch.getLeafId(),
                          batch.getSeqNo(),
                          batch.getBatchKind(),
                          batch.getPayload()))
              .collect(Collectors.toList());
      return new TStreamLogicalRepairResp(RpcUtils.SUCCESS_STATUS, req.getTimePartitionId())
          .setStale(false)
          .setBatches(thriftBatches);
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to stream logical repair for region {} partition {}",
          req.getConsensusGroupId(),
          req.getTimePartitionId(),
          e);
      return new TStreamLogicalRepairResp(
              RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage()),
              req.getTimePartitionId())
          .setStale(true);
    }
  }

  public TSStatus applyLogicalRepairBatch(TApplyLogicalRepairBatchReq req) {
    DataRegion dataRegion = getDataRegion(req.getConsensusGroupId());
    if (dataRegion == null) {
      return RpcUtils.getStatus(
          TSStatusCode.DATAREGION_PROCESS_ERROR,
          "DataRegion " + req.getConsensusGroupId() + " is not found on this DataNode");
    }
    try {
      repairSessionJournal.stageBatch(
          normalizeConsensusGroupIdString(req.getConsensusGroupId()),
          req.getTimePartitionId(),
          req.getRepairEpoch(),
          req.getSessionId(),
          req.getTreeKind(),
          req.getLeafId(),
          req.getSeqNo(),
          req.getBatchKind(),
          req.bufferForPayload());
      return RpcUtils.SUCCESS_STATUS;
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to stage logical repair batch for region {} partition {} session {} seq {}",
          req.getConsensusGroupId(),
          req.getTimePartitionId(),
          req.getSessionId(),
          req.getSeqNo(),
          e);
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }
  }

  public TSStatus finishLogicalRepairSession(TFinishLogicalRepairSessionReq req) {
    DataRegion dataRegion = getDataRegion(req.getConsensusGroupId());
    if (dataRegion == null) {
      return RpcUtils.getStatus(
          TSStatusCode.DATAREGION_PROCESS_ERROR,
          "DataRegion " + req.getConsensusGroupId() + " is not found on this DataNode");
    }
    try {
      List<LogicalRepairSessionJournal.StagedBatch> stagedBatches =
          repairSessionJournal.loadStagedBatches(
              normalizeConsensusGroupIdString(req.getConsensusGroupId()),
              req.getTimePartitionId(),
              req.getRepairEpoch(),
              req.getSessionId());
      if (stagedBatches.isEmpty()) {
        repairSessionJournal.completeSession(req.getSessionId());
        return RpcUtils.SUCCESS_STATUS;
      }
      consistencyManager.runWithLogicalRepairMutation(
          req.getConsensusGroupId(),
          req.getTimePartitionId(),
          req.getRepairEpoch(),
          () -> {
            for (LogicalRepairSessionJournal.StagedBatch stagedBatch : stagedBatches) {
              applyStagedBatch(dataRegion, req.getTimePartitionId(), stagedBatch);
            }
            return null;
          });
      repairSessionJournal.completeSession(req.getSessionId());
      return RpcUtils.SUCCESS_STATUS;
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to finish logical repair session for region {} partition {} session {}",
          req.getConsensusGroupId(),
          req.getTimePartitionId(),
          req.getSessionId(),
          e);
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }
  }

  private void applyStagedBatch(
      DataRegion dataRegion,
      long timePartitionId,
      LogicalRepairSessionJournal.StagedBatch stagedBatch)
      throws Exception {
    if ("RESET_SCOPE".equals(stagedBatch.getBatchKind())) {
      consistencyManager.resetLiveScope(
          dataRegion, timePartitionId, stagedBatch.duplicatePayload());
      return;
    }
    if ("RESET_LEAF".equals(stagedBatch.getBatchKind())) {
      consistencyManager.resetLiveLeaf(dataRegion, timePartitionId, stagedBatch.getLeafId());
      return;
    }
    PlanNode planNode = PlanNodeType.deserialize(stagedBatch.duplicatePayload());
    if (planNode instanceof InsertRowsNode) {
      dataRegion.insert((InsertRowsNode) planNode);
      return;
    }
    if (planNode instanceof DeleteDataNode) {
      DeleteDataNode deleteDataNode = (DeleteDataNode) planNode;
      for (MeasurementPath path : deleteDataNode.getPathList()) {
        dataRegion.deleteByDevice(path, deleteDataNode);
      }
      return;
    }
    throw new UnsupportedOperationException(
        "Unsupported logical repair batch payload: " + planNode.getClass().getSimpleName());
  }

  private List<TConsistencyDeletionSummary> collectDeletionSummaries(
      TConsensusGroupId consensusGroupId) throws IOException {
    DeletionResourceManager deletionResourceManager =
        DeletionResourceManager.getInstance(consensusGroupId.getId());
    if (deletionResourceManager == null) {
      return Collections.emptyList();
    }

    List<TConsistencyDeletionSummary> deletionSummaries = new ArrayList<>();
    for (DeletionResource deletionResource : deletionResourceManager.getAllDeletionResources()) {
      AbstractDeleteDataNode deleteDataNode = deletionResource.getDeleteDataNode();
      if (!(deleteDataNode instanceof DeleteDataNode)) {
        continue;
      }
      DeleteDataNode treeDeleteNode = (DeleteDataNode) deleteDataNode;
      ByteBuffer serializedProgressIndex =
          serializeProgressIndex(treeDeleteNode.getProgressIndex());
      for (MeasurementPath path : treeDeleteNode.getPathList()) {
        deletionSummaries.add(
            new TConsistencyDeletionSummary(
                path.getFullPath(),
                treeDeleteNode.getDeleteStartTime(),
                treeDeleteNode.getDeleteEndTime(),
                serializedProgressIndex.duplicate()));
      }
    }
    deletionSummaries.sort(
        Comparator.comparing(TConsistencyDeletionSummary::getPathPattern)
            .thenComparingLong(TConsistencyDeletionSummary::getTimeRangeStart)
            .thenComparingLong(TConsistencyDeletionSummary::getTimeRangeEnd));
    return deletionSummaries;
  }

  private List<TConsistencyDeletionSummary> filterDeletionSummariesForPartition(
      List<TConsistencyDeletionSummary> regionDeletionSummaries, long timePartition) {
    if (regionDeletionSummaries.isEmpty()) {
      return Collections.emptyList();
    }

    long timePartitionInterval =
        org.apache.iotdb.commons.conf.CommonDescriptor.getInstance()
            .getConfig()
            .getTimePartitionInterval();
    if (timePartitionInterval <= 0) {
      return new ArrayList<>(regionDeletionSummaries);
    }

    long partitionStart = timePartition * timePartitionInterval;
    long partitionEnd =
        Long.MAX_VALUE - timePartitionInterval < partitionStart
            ? Long.MAX_VALUE
            : partitionStart + timePartitionInterval - 1;

    List<TConsistencyDeletionSummary> filtered = new ArrayList<>();
    for (TConsistencyDeletionSummary summary : regionDeletionSummaries) {
      if (summary.getTimeRangeEnd() >= partitionStart
          && summary.getTimeRangeStart() <= partitionEnd) {
        filtered.add(summary);
      }
    }
    return filtered;
  }

  private void augmentTimePartitionsWithDeletionRanges(
      List<Long> timePartitions, List<TConsistencyDeletionSummary> regionDeletionSummaries) {
    long timePartitionInterval =
        org.apache.iotdb.commons.conf.CommonDescriptor.getInstance()
            .getConfig()
            .getTimePartitionInterval();
    if (timePartitionInterval <= 0 || regionDeletionSummaries.isEmpty()) {
      return;
    }

    for (TConsistencyDeletionSummary summary : regionDeletionSummaries) {
      long startPartition = Math.floorDiv(summary.getTimeRangeStart(), timePartitionInterval);
      long endPartition = Math.floorDiv(summary.getTimeRangeEnd(), timePartitionInterval);
      for (long partition = startPartition; partition <= endPartition; partition++) {
        if (!timePartitions.contains(partition)) {
          timePartitions.add(partition);
        }
      }
    }
  }

  private ByteBuffer serializeProgressIndex(ProgressIndex progressIndex) throws IOException {
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      if (progressIndex == null) {
        outputStream.writeByte(0);
      } else {
        progressIndex.serialize(outputStream);
      }
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  private DataRegion getDataRegion(TConsensusGroupId consensusGroupId) {
    if (consensusGroupId == null) {
      return null;
    }
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(consensusGroupId);
    if (!(groupId instanceof DataRegionId)) {
      return null;
    }
    return storageEngine.getDataRegion((DataRegionId) groupId);
  }

  private String normalizeConsensusGroupIdString(TConsensusGroupId consensusGroupId) {
    return ConsensusGroupId.Factory.createFromTConsensusGroupId(consensusGroupId).toString();
  }
}
