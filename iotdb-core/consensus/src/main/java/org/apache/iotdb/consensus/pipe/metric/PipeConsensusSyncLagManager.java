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

package org.apache.iotdb.consensus.pipe.metric;

import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeConnector;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeName;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is used to aggregate the write progress of all Connectors to calculate the minimum
 * synchronization progress of all follower copies, thereby calculating syncLag.
 *
 * <p>Note: every consensusGroup/dataRegion has and only has 1 instance of this class.
 */
public class PipeConsensusSyncLagManager {
  long syncLag = Long.MIN_VALUE;
  ReentrantLock lock = new ReentrantLock();
  Map<ConsensusPipeName, ConsensusPipeConnector> consensusPipe2ConnectorMap =
      new ConcurrentHashMap<>();
  Map<ConsensusPipeName, MaxIndexContainer> consensusPipe2MaxIndexContainerMap =
      new ConcurrentHashMap<>();

  /**
   * pinnedCommitIndex - currentReplicateProgress. If res <= 0, indicating that replication is
   * finished.
   */
  //  public long getSyncLagForRegionMigration(
  //      ConsensusPipeName consensusPipeName, long pinnedCommitIndex, int pinnedPipeRestartTimes) {
  //    return Optional.ofNullable(consensusPipe2ConnectorMap.get(consensusPipeName))
  //        .map(
  //            consensusPipeConnector -> {
  //              int pipeRestartTimes = consensusPipeConnector.getConsensusPipeRestartTimes();
  //              if (pipeRestartTimes > pinnedPipeRestartTimes) {
  //                long accumulatedReplicatedProgress = 0;
  //                for (int i = pinnedPipeRestartTimes; i <= pipeRestartTimes; i++) {
  //                  accumulatedReplicatedProgress +=
  //                      consensusPipe2MaxIndexContainerMap
  //                          .get(consensusPipeName)
  //                          .getMaxReplicateIndex(i);
  //                }
  //                return Math.max(pinnedCommitIndex - accumulatedReplicatedProgress, 0);
  //              } else {
  //                return Math.max(
  //                    pinnedCommitIndex
  //                        - consensusPipe2MaxIndexContainerMap
  //                            .get(consensusPipeName)
  //                            .getMaxReplicateIndex(pinnedPipeRestartTimes),
  //                    0L);
  //              }
  //            })
  //        .orElse(0L);
  //  }

  public long getCurrentCommitIndex(ConsensusPipeName consensusPipeName) {
    return Optional.ofNullable(consensusPipe2ConnectorMap.get(consensusPipeName))
        .map(ConsensusPipeConnector::getConsensusPipeCommitProgress)
        .orElse(0L);
  }

  public int getCurrentRestartTimes(ConsensusPipeName consensusPipeName) {
    return Optional.ofNullable(consensusPipe2ConnectorMap.get(consensusPipeName))
        .map(ConsensusPipeConnector::getConsensusPipeRestartTimes)
        .orElse(0);
  }

  public long getSyncLagForSpecificConsensusPipe(ConsensusPipeName consensusPipeName) {
    return Optional.ofNullable(consensusPipe2ConnectorMap.get(consensusPipeName))
        .map(
            consensusPipeConnector -> {
              int pipeRestartTimes = consensusPipeConnector.getConsensusPipeRestartTimes();
              long userWriteProgress =
                  consensusPipe2MaxIndexContainerMap
                      .get(consensusPipeName)
                      .computeUserWriteIndex(
                          pipeRestartTimes,
                          consensusPipeConnector.getConsensusPipeCommitProgress());
              long replicateProgress =
                  consensusPipe2MaxIndexContainerMap
                      .get(consensusPipeName)
                      .computeReplicateIndex(
                          pipeRestartTimes,
                          consensusPipeConnector.getConsensusPipeReplicateProgress());
              return Math.max(userWriteProgress - replicateProgress, 0L);
            })
        .orElse(0L);
  }

  public void addConsensusPipeConnector(
      ConsensusPipeName consensusPipeName, ConsensusPipeConnector consensusPipeConnector) {
    try {
      lock.lock();
      consensusPipe2ConnectorMap.put(consensusPipeName, consensusPipeConnector);
      consensusPipe2MaxIndexContainerMap.computeIfAbsent(
          consensusPipeName, k -> new MaxIndexContainer());
    } finally {
      lock.unlock();
    }
  }

  public void removeConsensusPipeConnector(ConsensusPipeName consensusPipeName) {
    try {
      lock.lock();
      consensusPipe2ConnectorMap.remove(consensusPipeName);
    } finally {
      lock.unlock();
    }
  }

  /**
   * SyncLag represents the biggest difference between the current replica users' write progress and
   * the synchronization progress of all other replicas. The semantics is how much data the leader
   * has left to synchronize.
   */
  public long calculateSyncLag() {
    try {
      lock.lock();
      // if there isn't a consensus pipe task, the syncLag is 0
      if (consensusPipe2ConnectorMap.isEmpty()) {
        return 0;
      }
      // else we find the biggest gap between leader and replicas in all consensus pipe task.
      syncLag = Long.MIN_VALUE;
      consensusPipe2ConnectorMap
          .keySet()
          .forEach(
              consensusPipeName ->
                  syncLag =
                      Math.max(syncLag, getSyncLagForSpecificConsensusPipe(consensusPipeName)));
      return syncLag;
    } finally {
      lock.unlock();
    }
  }

  public void clear() {
    this.consensusPipe2ConnectorMap.clear();
    this.consensusPipe2MaxIndexContainerMap.clear();
  }

  public static class MaxIndexContainer {
    // pipe restart times -> max(pipe event commit index)
    Map<Integer, Long> pipeRestartTimes2MaxUserWriteIndex = new ConcurrentHashMap<>();
    // pipe restart times -> max(replicated event commit index)
    Map<Integer, Long> pipeRestartTimes2MaxReplicateIndex = new ConcurrentHashMap<>();

    public long computeUserWriteIndex(int restartTimes, long commitIndex) {
      return pipeRestartTimes2MaxUserWriteIndex.compute(
          restartTimes,
          (k, v) -> {
            if (v == null) {
              return commitIndex;
            }
            return Math.max(v, commitIndex);
          });
    }

    public long computeReplicateIndex(int restartTimes, long commitIndex) {
      return pipeRestartTimes2MaxReplicateIndex.compute(
          restartTimes,
          (k, v) -> {
            if (v == null) {
              return commitIndex;
            }
            return Math.max(v, commitIndex);
          });
    }

    public Long getMaxUserWriteIndex(int restartTimes) {
      return pipeRestartTimes2MaxUserWriteIndex.getOrDefault(restartTimes, 0l);
    }

    public Long getMaxReplicateIndex(int restartTimes) {
      return pipeRestartTimes2MaxReplicateIndex.getOrDefault(restartTimes, 0l);
    }
  }

  private PipeConsensusSyncLagManager() {
    // do nothing
  }

  private static class PipeConsensusSyncLagManagerHolder {
    private static Map<String, PipeConsensusSyncLagManager> CONSENSU_GROUP_ID_2_INSTANCE_MAP;

    private PipeConsensusSyncLagManagerHolder() {
      // empty constructor
    }

    private static void build() {
      if (CONSENSU_GROUP_ID_2_INSTANCE_MAP == null) {
        CONSENSU_GROUP_ID_2_INSTANCE_MAP = new ConcurrentHashMap<>();
      }
    }
  }

  public static PipeConsensusSyncLagManager getInstance(String groupId) {
    return PipeConsensusSyncLagManagerHolder.CONSENSU_GROUP_ID_2_INSTANCE_MAP.computeIfAbsent(
        groupId, key -> new PipeConsensusSyncLagManager());
  }

  public static void release(String groupId) {
    PipeConsensusSyncLagManager.getInstance(groupId).clear();
    PipeConsensusSyncLagManagerHolder.CONSENSU_GROUP_ID_2_INSTANCE_MAP.remove(groupId);
  }

  // Only when consensus protocol is PipeConsensus, this method will be called once when construct
  // consensus class.
  public static void build() {
    PipeConsensusSyncLagManagerHolder.build();
  }
}
