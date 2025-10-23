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

import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeSink;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * This class is used to aggregate the write progress of all Connectors to calculate the minimum
 * synchronization progress of all follower copies, thereby calculating syncLag.
 *
 * <p>Note: every consensusGroup/dataRegion has and only has 1 instance of this class.
 */
public class PipeConsensusSyncLagManager {
  long userWriteProgress = 0;
  long minReplicateProgress = Long.MAX_VALUE;
  List<ConsensusPipeSink> consensusPipeSinkList = new CopyOnWriteArrayList<>();

  private void updateReplicateProgress() {
    minReplicateProgress = Long.MAX_VALUE;
    // if there isn't a consensus pipe task, replicate progress is Long.MAX_VALUE.
    if (consensusPipeSinkList.isEmpty()) {
      return;
    }
    // else we find the minimum progress in all consensus pipe task.
    consensusPipeSinkList.forEach(
        consensusPipeSink ->
            minReplicateProgress =
                Math.min(
                    minReplicateProgress, consensusPipeSink.getConsensusPipeReplicateProgress()));
  }

  private void updateUserWriteProgress() {
    // if there isn't a consensus pipe task, user write progress is 0.
    if (consensusPipeSinkList.isEmpty()) {
      userWriteProgress = 0;
      return;
    }
    // since the user write progress of different consensus pipes on the same DataRegion is the
    // same, we only need to take out one Connector to calculate
    try {
      ConsensusPipeSink connector = consensusPipeSinkList.get(0);
      userWriteProgress = connector.getConsensusPipeCommitProgress();
    } catch (Exception e) {
      // if removing the last connector happens after empty check, we may encounter
      // OutOfBoundsException, in this case, we set userWriteProgress to 0.
      userWriteProgress = 0;
    }
  }

  public void addConsensusPipeConnector(ConsensusPipeSink consensusPipeSink) {
    consensusPipeSinkList.add(consensusPipeSink);
  }

  public void removeConsensusPipeConnector(ConsensusPipeSink connector) {
    consensusPipeSinkList.remove(connector);
  }

  /**
   * SyncLag represents the difference between the current replica users' write progress and the
   * minimum synchronization progress of all other replicas. The semantics is how much data the
   * leader has left to synchronize.
   */
  public long calculateSyncLag() {
    updateUserWriteProgress();
    updateReplicateProgress();
    // if there isn't a consensus pipe task, the syncLag is userWriteProgress - 0
    if (minReplicateProgress == Long.MAX_VALUE) {
      return userWriteProgress;
    } else {
      // since we first update userWriteProgress then update replicateProgress, there may have some
      // cases that userWriteProgress is less than replicateProgress. In these cases, we return 0.
      return Math.max(userWriteProgress - minReplicateProgress, 0);
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
    PipeConsensusSyncLagManagerHolder.CONSENSU_GROUP_ID_2_INSTANCE_MAP.remove(groupId);
  }

  // Only when consensus protocol is PipeConsensus, this method will be called once when construct
  // consensus class.
  public static void build() {
    PipeConsensusSyncLagManagerHolder.build();
  }
}
