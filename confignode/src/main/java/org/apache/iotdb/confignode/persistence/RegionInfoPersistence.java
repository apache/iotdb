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

package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.response.StorageGroupSchemaDataSet;
import org.apache.iotdb.confignode.partition.StorageGroupSchema;
import org.apache.iotdb.confignode.physical.crud.CreateRegionsPlan;
import org.apache.iotdb.confignode.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** manage data partition and schema partition */
public class RegionInfoPersistence {

  // TODO: Serialize and Deserialize
  // Map<StorageGroupName, StorageGroupSchema>
  private final Map<String, StorageGroupSchema> storageGroupsMap;

  // Region allocate lock
  private final ReentrantReadWriteLock regionAllocateLock;
  // TODO: Serialize and Deserialize
  private int nextRegionGroupId = 0;

  // Region read write lock
  private final ReentrantReadWriteLock regionReadWriteLock;
  // Map<ConsensusGroupId, RegionReplicaSet>
  private final Map<ConsensusGroupId, RegionReplicaSet> regionMap;

  public RegionInfoPersistence() {
    this.regionAllocateLock = new ReentrantReadWriteLock();
    this.regionReadWriteLock = new ReentrantReadWriteLock();
    this.storageGroupsMap = new HashMap<>();
    this.regionMap = new HashMap<>();
  }

  /**
   * Persistence new StorageGroupSchema
   *
   * @param plan SetStorageGroupPlan
   * @return SUCCESS_STATUS
   */
  public TSStatus setStorageGroup(SetStorageGroupPlan plan) {
    TSStatus result;
    regionReadWriteLock.writeLock().lock();
    try {
      StorageGroupSchema schema = plan.getSchema();
      storageGroupsMap.put(schema.getName(), schema);
      result = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      regionReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  public StorageGroupSchemaDataSet getStorageGroupSchema() {
    StorageGroupSchemaDataSet result = new StorageGroupSchemaDataSet();
    regionReadWriteLock.readLock().lock();
    try {
      result.setSchemaList(new ArrayList<>(storageGroupsMap.values()));
    } finally {
      regionReadWriteLock.readLock().unlock();
      result.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    }
    return result;
  }

  /**
   * Persistence allocation result of new Regions
   *
   * @param plan CreateRegionsPlan
   * @return SUCCESS_STATUS
   */
  public TSStatus createRegions(CreateRegionsPlan plan) {
    TSStatus result;
    regionReadWriteLock.writeLock().lock();
    regionAllocateLock.writeLock().lock();
    try {
      StorageGroupSchema schema = storageGroupsMap.get(plan.getStorageGroup());

      for (RegionReplicaSet regionReplicaSet : plan.getRegionReplicaSets()) {
        nextRegionGroupId =
            Math.max(nextRegionGroupId, regionReplicaSet.getConsensusGroupId().getId());
        regionMap.put(regionReplicaSet.getConsensusGroupId(), regionReplicaSet);
        if (regionReplicaSet.getConsensusGroupId() instanceof DataRegionId) {
          schema.addDataRegionGroup(regionReplicaSet.getConsensusGroupId());
        } else if (regionReplicaSet.getConsensusGroupId() instanceof SchemaRegionId) {
          schema.addSchemaRegionGroup(regionReplicaSet.getConsensusGroupId());
        }
      }

      result = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      regionAllocateLock.writeLock().unlock();
      regionReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  /** @return The SchemaRegion ReplicaSets in the specific StorageGroup */
  public List<RegionReplicaSet> getSchemaRegionEndPoint(String storageGroup) {
    List<RegionReplicaSet> schemaRegionEndPoints = new ArrayList<>();
    regionReadWriteLock.readLock().lock();
    try {
      if (storageGroupsMap.containsKey(storageGroup)) {
        List<ConsensusGroupId> schemaRegionIds =
            storageGroupsMap.get(storageGroup).getSchemaRegionGroupIds();
        for (ConsensusGroupId consensusGroupId : schemaRegionIds) {
          schemaRegionEndPoints.add(regionMap.get(consensusGroupId));
        }
      }
    } finally {
      regionReadWriteLock.readLock().unlock();
    }

    return schemaRegionEndPoints;
  }

  /** @return The DataRegion ReplicaSets in the specific StorageGroup */
  public List<RegionReplicaSet> getDataRegionEndPoint(String storageGroup) {
    List<RegionReplicaSet> dataRegionEndPoints = new ArrayList<>();
    regionReadWriteLock.readLock().lock();
    try {
      if (storageGroupsMap.containsKey(storageGroup)) {
        List<ConsensusGroupId> dataRegionIds =
            storageGroupsMap.get(storageGroup).getDataRegionGroupIds();
        for (ConsensusGroupId consensusGroupId : dataRegionIds) {
          dataRegionEndPoints.add(regionMap.get(consensusGroupId));
        }
      }
    } finally {
      regionReadWriteLock.readLock().unlock();
    }

    return dataRegionEndPoints;
  }

  /**
   * Get all StorageGroups' name
   *
   * @return List<String>, all storageGroups' name
   */
  public List<String> getStorageGroupNames() {
    List<String> storageGroups;
    regionReadWriteLock.readLock().lock();
    try {
      storageGroups = new ArrayList<>(storageGroupsMap.keySet());
    } finally {
      regionReadWriteLock.readLock().unlock();
    }
    return storageGroups;
  }

  public int generateNextRegionGroupId() {
    int result;
    regionAllocateLock.writeLock().lock();
    try {
      result = nextRegionGroupId;
      nextRegionGroupId += 1;
    } finally {
      regionAllocateLock.writeLock().unlock();
    }
    return result;
  }

  public boolean containsStorageGroup(String storageName) {
    boolean result;
    regionReadWriteLock.readLock().lock();
    try {
      result = storageGroupsMap.containsKey(storageName);
    } finally {
      regionReadWriteLock.readLock().unlock();
    }
    return result;
  }

  @TestOnly
  public void clear() {
    nextRegionGroupId = 0;
    storageGroupsMap.clear();
    regionMap.clear();
  }

  private static class RegionInfoPersistenceHolder {

    private static final RegionInfoPersistence INSTANCE = new RegionInfoPersistence();

    private RegionInfoPersistenceHolder() {
      // empty constructor
    }
  }

  public static RegionInfoPersistence getInstance() {
    return RegionInfoPersistence.RegionInfoPersistenceHolder.INSTANCE;
  }
}
