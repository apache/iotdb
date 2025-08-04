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

package org.apache.iotdb.db.pipe.consensus;

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.ProgressIndexType;
import org.apache.iotdb.commons.consensus.index.impl.HybridProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.RecoverProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeName;
import org.apache.iotdb.consensus.pipe.consensuspipe.ReplicateProgressManager;
import org.apache.iotdb.consensus.pipe.metric.PipeConsensusSyncLagManager;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class ReplicateProgressDataNodeManager implements ReplicateProgressManager {
  private static final int DATA_NODE_ID = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
  private static final Map<String, AtomicLong> consensusPipe2ReplicateIndex =
      new ConcurrentHashMap<>();
  private final Map<ConsensusGroupId, ProgressIndex> groupId2MaxProgressIndex;
  private final Map<ConsensusPipeName, Long> consensusPipe2pinnedCommitIndexForMigration;

  public ReplicateProgressDataNodeManager() {
    this.groupId2MaxProgressIndex = new ConcurrentHashMap<>();
    this.consensusPipe2pinnedCommitIndexForMigration = new ConcurrentHashMap<>();

    recoverMaxProgressIndexFromDataRegion();
  }

  public static long assignReplicateIndexForIoTV2(String consensusPipeName) {
    return consensusPipe2ReplicateIndex
        .compute(consensusPipeName, (k, v) -> v == null ? new AtomicLong(0) : v)
        .incrementAndGet();
  }

  public static void resetReplicateIndexForIoTV2(String consensusPipeName) {
    consensusPipe2ReplicateIndex.put(consensusPipeName, new AtomicLong(0));
  }

  public static long getReplicateIndexForIoTV2(String consensusPipeName) {
    return consensusPipe2ReplicateIndex.getOrDefault(consensusPipeName, new AtomicLong(0)).get();
  }

  public static ProgressIndex extractLocalSimpleProgressIndex(ProgressIndex progressIndex) {
    if (progressIndex instanceof RecoverProgressIndex) {
      final Map<Integer, SimpleProgressIndex> dataNodeId2LocalIndex =
          ((RecoverProgressIndex) progressIndex).getDataNodeId2LocalIndex();
      return dataNodeId2LocalIndex.containsKey(DATA_NODE_ID)
          ? dataNodeId2LocalIndex.get(DATA_NODE_ID)
          : MinimumProgressIndex.INSTANCE;
    } else if (progressIndex instanceof HybridProgressIndex) {
      final Map<Short, ProgressIndex> type2Index =
          ((HybridProgressIndex) progressIndex).getType2Index();
      if (!type2Index.containsKey(ProgressIndexType.RECOVER_PROGRESS_INDEX.getType())) {
        return MinimumProgressIndex.INSTANCE;
      }
      final Map<Integer, SimpleProgressIndex> dataNodeId2LocalIndex =
          ((RecoverProgressIndex)
                  type2Index.get(ProgressIndexType.RECOVER_PROGRESS_INDEX.getType()))
              .getDataNodeId2LocalIndex();
      return dataNodeId2LocalIndex.containsKey(DATA_NODE_ID)
          ? dataNodeId2LocalIndex.get(DATA_NODE_ID)
          : MinimumProgressIndex.INSTANCE;
    }
    return MinimumProgressIndex.INSTANCE;
  }

  private void recoverMaxProgressIndexFromDataRegion() {
    StorageEngine.getInstance()
        .getAllDataRegionIds()
        .forEach(
            dataRegionId -> {
              final TsFileManager tsFileManager =
                  StorageEngine.getInstance().getDataRegion(dataRegionId).getTsFileManager();

              final List<ProgressIndex> allProgressIndex = new ArrayList<>();
              allProgressIndex.addAll(
                  tsFileManager.getTsFileList(true).stream()
                      .map(TsFileResource::getMaxProgressIndex)
                      .collect(Collectors.toList()));
              allProgressIndex.addAll(
                  tsFileManager.getTsFileList(false).stream()
                      .map(TsFileResource::getMaxProgressIndex)
                      .collect(Collectors.toList()));

              ProgressIndex maxProgressIndex = MinimumProgressIndex.INSTANCE;
              for (ProgressIndex progressIndex : allProgressIndex) {
                maxProgressIndex =
                    maxProgressIndex.updateToMinimumEqualOrIsAfterProgressIndex(
                        extractLocalSimpleProgressIndex(progressIndex));
              }
              // Renew a variable to pass the examination of compiler
              final ProgressIndex finalMaxProgressIndex = maxProgressIndex;
              groupId2MaxProgressIndex.compute(
                  dataRegionId,
                  (key, value) ->
                      (value == null ? MinimumProgressIndex.INSTANCE : value)
                          .updateToMinimumEqualOrIsAfterProgressIndex(finalMaxProgressIndex));
            });
  }

  @Override
  public ProgressIndex getProgressIndex(ConsensusPipeName consensusPipeName) {
    return PipeDataNodeAgent.task()
        .getPipeTaskProgressIndex(
            consensusPipeName.toString(), consensusPipeName.getConsensusGroupId().getId());
  }

  @Override
  public ProgressIndex assignProgressIndex(ConsensusGroupId consensusGroupId) {
    return groupId2MaxProgressIndex.compute(
        consensusGroupId,
        (key, value) ->
            ((value == null ? MinimumProgressIndex.INSTANCE : value)
                .updateToMinimumEqualOrIsAfterProgressIndex(
                    PipeDataNodeAgent.runtime().assignProgressIndexForPipeConsensus())));
  }

  @Override
  public ProgressIndex getMaxAssignedProgressIndex(ConsensusGroupId consensusGroupId) {
    return groupId2MaxProgressIndex.getOrDefault(consensusGroupId, MinimumProgressIndex.INSTANCE);
  }

  @Override
  public long getSyncLagForSpecificConsensusPipe(
      ConsensusGroupId consensusGroupId, ConsensusPipeName consensusPipeName) {
    return PipeConsensusSyncLagManager.getInstance(consensusGroupId.toString())
        .getSyncLagForRegionMigration(
            consensusPipeName,
            this.consensusPipe2pinnedCommitIndexForMigration.getOrDefault(consensusPipeName, 0L));
  }

  @Override
  public void pinReplicateIndexForRegionMigration(
      ConsensusGroupId consensusGroupId, ConsensusPipeName consensusPipeName) {
    this.consensusPipe2pinnedCommitIndexForMigration.put(
        consensusPipeName,
        PipeConsensusSyncLagManager.getInstance(consensusGroupId.toString())
            .getCurrentLeaderReplicateIndex(consensusPipeName));
  }
}
