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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
  List<ConsensusPipeConnector> consensusPipeConnectorList = new ArrayList<>();

  private long getSyncLagForSpecificConsensusPipe(ConsensusPipeConnector consensusPipeConnector) {
    long userWriteProgress = consensusPipeConnector.getConsensusPipeCommitProgress();
    long replicateProgress = consensusPipeConnector.getConsensusPipeReplicateProgress();
    return Math.max(userWriteProgress - replicateProgress, 0);
  }

  public void addConsensusPipeConnector(ConsensusPipeConnector consensusPipeConnector) {
    try {
      lock.lock();
      consensusPipeConnectorList.add(consensusPipeConnector);
    } finally {
      lock.unlock();
    }
  }

  public void removeConsensusPipeConnector(ConsensusPipeConnector connector) {
    try {
      lock.lock();
      consensusPipeConnectorList.remove(connector);
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
      if (consensusPipeConnectorList.isEmpty()) {
        return 0;
      }
      // else we find the biggest gap between leader and replicas in all consensus pipe task.
      syncLag = Long.MIN_VALUE;
      consensusPipeConnectorList.forEach(
          consensusPipeConnector ->
              syncLag =
                  Math.max(syncLag, getSyncLagForSpecificConsensusPipe(consensusPipeConnector)));
      return syncLag;
    } finally {
      lock.unlock();
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
