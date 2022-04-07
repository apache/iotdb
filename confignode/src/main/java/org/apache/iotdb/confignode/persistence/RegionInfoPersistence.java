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

import org.apache.iotdb.commons.cluster.DataNodeLocation;
import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.GroupType;
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
import java.util.stream.Collectors;

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

  /** @return key is schema region id, value is endpoint list */
  public List<RegionReplicaSet> getSchemaRegionEndPoint() {
    List<RegionReplicaSet> schemaRegionEndPoints = new ArrayList<>();

    schemaRegion
        .getSchemaRegionDataNodesMap()
        .entrySet()
        .forEach(
            entity -> {
              RegionReplicaSet schemaRegionReplicaSet = new RegionReplicaSet();
              List<Endpoint> endPoints = new ArrayList<>();
              entity
                  .getValue()
                  .forEach(
                      dataNodeId -> {
                        if (DataNodeInfoPersistence.getInstance()
                            .getOnlineDataNodes()
                            .containsKey(dataNodeId)) {
                          endPoints.add(
                              DataNodeInfoPersistence.getInstance()
                                  .getOnlineDataNodes()
                                  .get(dataNodeId)
                                  .getEndPoint());
                        }
                      });
              schemaRegionReplicaSet.setId(
                  new ConsensusGroupId(GroupType.SchemaRegion, entity.getKey()));
              // TODO: (xingtanzjr) We cannot get the dataNodeId here, use 0 as the placeholder
              schemaRegionReplicaSet.setDataNodeList(
                  endPoints.stream()
                      .map(endpoint -> new DataNodeLocation(0, endpoint))
                      .collect(Collectors.toList()));
              schemaRegionEndPoints.add(schemaRegionReplicaSet);
            });
    return schemaRegionEndPoints;
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
