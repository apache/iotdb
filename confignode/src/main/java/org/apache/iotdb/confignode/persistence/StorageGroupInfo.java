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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.response.StorageGroupSchemaDataSet;
import org.apache.iotdb.confignode.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StorageGroupInfo {

  // StorageGroup read write lock
  private final ReentrantReadWriteLock storageGroupReadWriteLock;
  // TODO: Serialize and Deserialize
  // Map<StorageGroupName, StorageGroupSchema>
  private final Map<String, TStorageGroupSchema> storageGroupsMap;

  private StorageGroupInfo() {
    storageGroupReadWriteLock = new ReentrantReadWriteLock();
    storageGroupsMap = new HashMap<>();
  }

  /**
   * Persistence new StorageGroupSchema
   *
   * @param plan SetStorageGroupPlan
   * @return SUCCESS_STATUS
   */
  public TSStatus setStorageGroup(SetStorageGroupPlan plan) {
    TSStatus result;
    storageGroupReadWriteLock.writeLock().lock();
    try {
      TStorageGroupSchema storageGroupSchema = plan.getSchema();
      storageGroupsMap.put(storageGroupSchema.getName(), storageGroupSchema);
      result = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      storageGroupReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  /** @return List<StorageGroupName>, all storageGroups' name */
  public List<String> getStorageGroupNames() {
    List<String> storageGroups;
    storageGroupReadWriteLock.readLock().lock();
    try {
      storageGroups = new ArrayList<>(storageGroupsMap.keySet());
    } finally {
      storageGroupReadWriteLock.readLock().unlock();
    }
    return storageGroups;
  }

  /** @return All the StorageGroupSchema */
  public StorageGroupSchemaDataSet getStorageGroupSchema() {
    StorageGroupSchemaDataSet result = new StorageGroupSchemaDataSet();
    storageGroupReadWriteLock.readLock().lock();
    try {
      result.setSchemaList(new ArrayList<>(storageGroupsMap.values()));
    } finally {
      storageGroupReadWriteLock.readLock().unlock();
      result.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    }
    return result;
  }

  /** @return True if StorageGroupInfo contains the specific StorageGroup */
  public boolean containsStorageGroup(String storageName) {
    boolean result;
    storageGroupReadWriteLock.readLock().lock();
    try {
      result = storageGroupsMap.containsKey(storageName);
    } finally {
      storageGroupReadWriteLock.readLock().unlock();
    }
    return result;
  }

  /**
   * Get the SchemaRegionGroupIds or DataRegionGroupIds from the specific StorageGroup
   *
   * @param storageGroup StorageGroupName
   * @param type SchemaRegion or DataRegion
   * @return All SchemaRegionGroupIds when type is SchemaRegion, and all DataRegionGroupIds when
   *     type is DataRegion
   */
  public List<TConsensusGroupId> getRegionGroupIds(String storageGroup, TConsensusGroupType type) {
    List<TConsensusGroupId> result;
    storageGroupReadWriteLock.readLock().lock();
    try {
      switch (type) {
        case SchemaRegion:
          result = storageGroupsMap.get(storageGroup).getSchemaRegionGroupIds();
          break;
        case DataRegion:
          result = storageGroupsMap.get(storageGroup).getDataRegionGroupIds();
          break;
        default:
          result = new ArrayList<>();
      }
    } finally {
      storageGroupReadWriteLock.readLock().unlock();
    }
    return result;
  }

  @TestOnly
  public void clear() {
    storageGroupsMap.clear();
  }

  private static class StorageGroupInfoHolder {

    private static final StorageGroupInfo INSTANCE = new StorageGroupInfo();

    private StorageGroupInfoHolder() {
      // Empty constructor
    }
  }

  public static StorageGroupInfo getInstance() {
    return StorageGroupInfoHolder.INSTANCE;
  }
}
