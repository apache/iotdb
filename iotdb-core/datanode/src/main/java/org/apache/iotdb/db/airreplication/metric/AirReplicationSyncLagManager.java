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

package org.apache.iotdb.consensus.air.metric;

import org.apache.iotdb.consensus.air.airreplication.AirReplicationName;
import org.apache.iotdb.consensus.air.airreplication.AirReplicationSink;

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
public class AirReplicationSyncLagManager {
  long syncLag = Long.MIN_VALUE;
  ReentrantLock lock = new ReentrantLock();
  Map<AirReplicationName, AirReplicationSink> airReplication2ConnectorMap = new ConcurrentHashMap<>();

  /**
   * pinnedCommitIndex - currentReplicateProgress. If res <= 0, indicating that replication is
   * finished.
   */
  public long getSyncLagForRegionMigration(
      AirReplicationName airReplicationName, long pinnedCommitIndex) {
    return Optional.ofNullable(airReplication2ConnectorMap.get(airReplicationName))
        .map(
            airReplicationSink ->
                Math.max(pinnedCommitIndex - airReplicationSink.getFollowerApplyProgress(), 0L))
        .orElse(0L);
  }

  /**
   * userWriteProgress - currentReplicateProgress. If res <= 0, indicating that replication is
   * finished.
   */
  public long getSyncLagForSpecificAirReplication(AirReplicationName airReplicationName) {
    return Optional.ofNullable(airReplication2ConnectorMap.get(airReplicationName))
        .map(
            airReplicationSink -> {
              long userWriteProgress = airReplicationSink.getLeaderReplicateProgress();
              long replicateProgress = airReplicationSink.getFollowerApplyProgress();
              return Math.max(userWriteProgress - replicateProgress, 0L);
            })
        .orElse(0L);
  }

  public long getCurrentLeaderReplicateIndex(AirReplicationName airReplicationName) {
    return Optional.ofNullable(airReplication2ConnectorMap.get(airReplicationName))
        .map(AirReplicationSink::getLeaderReplicateProgress)
        .orElse(0L);
  }

  public void addAirReplicationConnector(
      AirReplicationName airReplicationName, AirReplicationSink airReplicationSink) {
    lock.lock();
    try {
      airReplication2ConnectorMap.put(airReplicationName, airReplicationSink);
    } finally {
      lock.unlock();
    }
  }

  public void removeAirReplicationConnector(AirReplicationName airReplicationName) {
    lock.lock();
    try {
      airReplication2ConnectorMap.remove(airReplicationName);
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
    lock.lock();
    try {
      // if there isn't a air replication task, the syncLag is 0
      if (airReplication2ConnectorMap.isEmpty()) {
        return 0;
      }
      // else we find the biggest gap between leader and replicas in all air replication task.
      syncLag = Long.MIN_VALUE;
      airReplication2ConnectorMap
          .keySet()
          .forEach(
              airReplicationName ->
                  syncLag =
                      Math.max(syncLag, getSyncLagForSpecificAirReplication(airReplicationName)));
      return syncLag;
    } finally {
      lock.unlock();
    }
  }

  public void clear() {
    this.airReplication2ConnectorMap.clear();
  }

  private AirReplicationSyncLagManager() {
    // do nothing
  }

  private static class AirReplicationSyncLagManagerHolder {
    private static Map<String, AirReplicationSyncLagManager> REPLICATION_GROUP_ID_2_INSTANCE_MAP;

    private AirReplicationSyncLagManagerHolder() {
      // empty constructor
    }

    private static void build() {
      if (REPLICATION_GROUP_ID_2_INSTANCE_MAP == null) {
        REPLICATION_GROUP_ID_2_INSTANCE_MAP = new ConcurrentHashMap<>();
      }
    }
  }

  public static AirReplicationSyncLagManager getInstance(String groupId) {
    return AirReplicationSyncLagManagerHolder.REPLICATION_GROUP_ID_2_INSTANCE_MAP.computeIfAbsent(
        groupId, key -> new AirReplicationSyncLagManager());
  }

  public static void release(String groupId) {
    AirReplicationSyncLagManager.getInstance(groupId).clear();
    AirReplicationSyncLagManagerHolder.REPLICATION_GROUP_ID_2_INSTANCE_MAP.remove(groupId);
  }

  // Only when replication protocol is AirReplication, this method will be called once when construct
  // replication class.
  public static void build() {
    AirReplicationSyncLagManagerHolder.build();
  }
}
