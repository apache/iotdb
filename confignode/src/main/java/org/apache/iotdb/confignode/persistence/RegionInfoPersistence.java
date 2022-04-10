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

import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.partition.DataNodeLocation;
import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.response.StorageGroupSchemaDataSet;
import org.apache.iotdb.confignode.partition.DataRegionInfo;
import org.apache.iotdb.confignode.partition.SchemaRegionInfo;
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

  /** partition read write lock */
  private final ReentrantReadWriteLock partitionReadWriteLock;

  // TODO: Serialize and Deserialize
  // storageGroupName -> StorageGroupSchema
  private final Map<String, StorageGroupSchema> storageGroupsMap;

  // TODO: Serialize and Deserialize
  private int nextSchemaRegionGroup = 0;
  // TODO: Serialize and Deserialize
  private int nextDataRegionGroup = 0;

  // TODO: Serialize and Deserialize
  private final SchemaRegionInfo schemaRegion;

  // TODO: Serialize and Deserialize
  private final DataRegionInfo dataRegion;

  public RegionInfoPersistence() {
    this.partitionReadWriteLock = new ReentrantReadWriteLock();
    this.storageGroupsMap = new HashMap<>();
    this.schemaRegion = new SchemaRegionInfo();
    this.dataRegion = new DataRegionInfo();
  }

  /**
   * 1. region allocation 2. add to storage group map
   *
   * @param plan SetStorageGroupPlan
   * @return TSStatusCode.SUCCESS_STATUS if region allocate
   */
  public TSStatus setStorageGroup(SetStorageGroupPlan plan) {
    TSStatus result;
    partitionReadWriteLock.writeLock().lock();
    try {
      if (storageGroupsMap.containsKey(plan.getSchema().getName())) {
        result = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
        result.setMessage(
            String.format("StorageGroup %s is already set.", plan.getSchema().getName()));
      } else {
        StorageGroupSchema schema = new StorageGroupSchema(plan.getSchema().getName());
        storageGroupsMap.put(schema.getName(), schema);

        plan.getSchemaRegionInfo()
            .getSchemaRegionDataNodesMap()
            .entrySet()
            .forEach(
                entity -> {
                  schemaRegion.addSchemaRegion(entity.getKey(), entity.getValue());
                  entity
                      .getValue()
                      .forEach(
                          dataNodeId -> {
                            DataNodeInfoPersistence.getInstance()
                                .addSchemaRegionGroup(dataNodeId, entity.getKey());
                          });
                });

        plan.getDataRegionInfo()
            .getDataRegionDataNodesMap()
            .entrySet()
            .forEach(
                entity -> {
                  dataRegion.createDataRegion(entity.getKey(), entity.getValue());
                  entity
                      .getValue()
                      .forEach(
                          dataNodeId -> {
                            DataNodeInfoPersistence.getInstance()
                                .addDataRegionGroup(dataNodeId, entity.getKey());
                          });
                });

        result = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      }
    } finally {
      partitionReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  public StorageGroupSchemaDataSet getStorageGroupSchema() {
    StorageGroupSchemaDataSet result = new StorageGroupSchemaDataSet();
    partitionReadWriteLock.readLock().lock();
    try {
      result.setSchemaList(new ArrayList<>(storageGroupsMap.values()));
    } finally {
      partitionReadWriteLock.readLock().unlock();
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
              schemaRegionReplicaSet.setId(new SchemaRegionId(entity.getKey()));
              // TODO: (xingtanzjr) We cannot get the dataNodeId here, use 0 as the placeholder
              schemaRegionReplicaSet.setDataNodeList(
                  endPoints.stream()
                      .map(endpoint -> new DataNodeLocation(0, endpoint))
                      .collect(Collectors.toList()));
              schemaRegionEndPoints.add(schemaRegionReplicaSet);
            });
    return schemaRegionEndPoints;
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
