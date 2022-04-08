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

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.response.StorageGroupSchemaDataSet;
import org.apache.iotdb.confignode.partition.StorageGroupSchema;
import org.apache.iotdb.confignode.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

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
   * StorageGroupSchema and Region allocation result persistence
   *
   * @param plan SetStorageGroupPlan
   * @return TSStatusCode.SUCCESS_STATUS if success
   */
  public TSStatus setStorageGroup(SetStorageGroupPlan plan) {
    TSStatus result;
    regionReadWriteLock.writeLock().lock();
    try {
      if (storageGroupsMap.containsKey(plan.getSchema().getName())) {
        result = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
        result.setMessage(
            String.format("StorageGroup %s is already set.", plan.getSchema().getName()));
      } else {
        StorageGroupSchema schema = plan.getSchema();
        storageGroupsMap.put(schema.getName(), schema);

        for (RegionReplicaSet regionReplicaSet : plan.getRegionReplicaSets()) {
          regionMap.put(regionReplicaSet.getId(), regionReplicaSet);
        }

        result = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      }
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
    return storageGroupsMap.containsKey(storageName);
  }

  @TestOnly
  public void clear() {
    storageGroupsMap.clear();
    schemaRegion.getSchemaRegionDataNodesMap().clear();
    dataRegion.getDataRegionDataNodesMap().clear();
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
