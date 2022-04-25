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
import org.apache.iotdb.confignode.consensus.request.read.QueryStorageGroupSchemaReq;
import org.apache.iotdb.confignode.consensus.request.write.SetDataReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetSchemaReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTTLReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTimePartitionIntervalReq;
import org.apache.iotdb.confignode.consensus.response.StorageGroupSchemaResp;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.mtree.MTreeAboveSG;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.rpc.TSStatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StorageGroupInfo {

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageGroupInfo.class);

  // StorageGroup read write lock
  private final ReentrantReadWriteLock storageGroupReadWriteLock;

  private MTreeAboveSG mTree;

  private StorageGroupInfo() {
    storageGroupReadWriteLock = new ReentrantReadWriteLock();

    try {
      mTree = new MTreeAboveSG();
    } catch (MetadataException e) {
      LOGGER.error("Can't construct StorageGroupInfo", e);
    }
  }

  /**
   * Persistence new StorageGroupSchema
   *
   * @param req SetStorageGroupReq
   * @return SUCCESS_STATUS
   */
  public TSStatus setStorageGroup(SetStorageGroupReq req) {
    TSStatus result;
    storageGroupReadWriteLock.writeLock().lock();
    try {
      // Set StorageGroup
      TStorageGroupSchema storageGroupSchema = req.getSchema();
      PartialPath partialPathName = new PartialPath(storageGroupSchema.getName());
      mTree.setStorageGroup(partialPathName);

      // Set StorageGroupSchema
      StorageGroupMNode mNode = (StorageGroupMNode) mTree.getStorageGroupNodeByPath(partialPathName);
      mNode.setDataTTL(storageGroupSchema.getTTL());
      mNode.setSchemaReplicationFactor(storageGroupSchema.getSchemaReplicationFactor());
      mNode.setDataReplicationFactor(storageGroupSchema.getDataReplicationFactor());
      mNode.setTimePartitionInterval(storageGroupSchema.getTimePartitionInterval());

      result = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (MetadataException e) {
      result = new TSStatus();
    } finally {
      storageGroupReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  public TSStatus setTTL(SetTTLReq req) {
    TSStatus result;
    storageGroupReadWriteLock.writeLock().lock();
    try {
      PartialPath path = new PartialPath(req.getStorageGroup());
      if (mTree.isStorageGroupAlreadySet(path)) {
        mTree.getStorageGroupNodeByPath(path).getStorageGroupSchema().setTTL(req.getTTL());
        result = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      } else {
        result = new TSStatus(TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode());
      }
    } catch (MetadataException e) {
      LOGGER.error("Error StorageGroup name", e);
      return new TSStatus(TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode()).setMessage("Error StorageGroup name");
    } finally {
      storageGroupReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  public TSStatus setSchemaReplicationFactor(SetSchemaReplicationFactorReq req) {
    TSStatus result;
    storageGroupReadWriteLock.writeLock().lock();
    try {
      PartialPath path = new PartialPath(req.getStorageGroup());
      if (mTree.isStorageGroupAlreadySet(path)) {
        mTree.getStorageGroupNodeByPath(path).getStorageGroupSchema().setSchemaReplicationFactor(req.getSchemaReplicationFactor());
        result = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      } else {
        result = new TSStatus(TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode());
      }
    } catch (MetadataException e) {
      LOGGER.error("Error StorageGroup name", e);
      return new TSStatus(TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode()).setMessage("Error StorageGroup name");
    } finally {
      storageGroupReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  public TSStatus setDataReplicationFactor(SetDataReplicationFactorReq req) {
    TSStatus result;
    storageGroupReadWriteLock.writeLock().lock();
    try {
      PartialPath path = new PartialPath(req.getStorageGroup());
      if (mTree.isStorageGroupAlreadySet(path)) {
        mTree.getStorageGroupNodeByPath(path).getStorageGroupSchema().setDataReplicationFactor(req.getDataReplicationFactor());
        result = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      } else {
        result = new TSStatus(TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode());
      }
    } catch (MetadataException e) {
      LOGGER.error("Error StorageGroup name", e);
      return new TSStatus(TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode()).setMessage("Error StorageGroup name");
    } finally {
      storageGroupReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  public TSStatus setTimePartitionInterval(SetTimePartitionIntervalReq req) {
    TSStatus result;
    storageGroupReadWriteLock.writeLock().lock();
    try {
      PartialPath path = new PartialPath(req.getStorageGroup());
      if (mTree.isStorageGroupAlreadySet(path)) {
        mTree.getStorageGroupNodeByPath(path).getStorageGroupSchema().setTimePartitionInterval(req.getTimePartitionInterval());
        result = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      } else {
        result = new TSStatus(TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode());
      }
    } catch (MetadataException e) {
      LOGGER.error("Error StorageGroup name", e);
      return new TSStatus(TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode()).setMessage("Error StorageGroup name");
    } finally {
      storageGroupReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  /** @return List<StorageGroupName>, all storageGroups' name */
  public List<String> getStorageGroupNames() {
    List<String> storageGroups = new ArrayList<>();
    storageGroupReadWriteLock.readLock().lock();
    try {
      List<PartialPath> namePaths = mTree.getAllStorageGroupPaths();
      for (PartialPath path : namePaths) {
        storageGroups.add(path.getFullPath());
      }
    } finally {
      storageGroupReadWriteLock.readLock().unlock();
    }
    return storageGroups;
  }

  /** @return All the StorageGroupSchema */
  public StorageGroupSchemaResp getMatchedStorageGroupSchemas(QueryStorageGroupSchemaReq req) {
    StorageGroupSchemaResp result = new StorageGroupSchemaResp();
    storageGroupReadWriteLock.readLock().lock();
    try {
      Map<String, TStorageGroupSchema> schemaMap = new HashMap<>();
      for (String storageGroup : req.getStorageGroups()) {
        PartialPath sgPath = new PartialPath(storageGroup);
        schemaMap.put(storageGroup, mTree.getStorageGroupNodeByPath(sgPath).getStorageGroupSchema());
      }
      result.setSchemaMap(schemaMap);
    } catch (MetadataException e) {
      LOGGER.error("Error StorageGroup name", e);
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
      result = mTree.isStorageGroupAlreadySet(new PartialPath(storageName));
    } catch (IllegalPathException e) {
      LOGGER.error("Error StorageGroup name", e);
      return false;
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
      StorageGroupMNode mNode = (StorageGroupMNode) mTree.getStorageGroupNodeByPath(new PartialPath(storageGroup));
      switch (type) {
        case SchemaRegion:
          result = mNode.getStorageGroupSchema().getSchemaRegionGroupIds();
          break;
        case DataRegion:
          result = mNode.getStorageGroupSchema().getDataRegionGroupIds();
          break;
        default:
          result = new ArrayList<>();
      }
    } catch (MetadataException e) {
      LOGGER.error("Error StorageGroup name", e);
      return new ArrayList<>();
    } finally {
      storageGroupReadWriteLock.readLock().unlock();
    }
    return result;
  }

  @TestOnly
  public void clear() {
    mTree.clear();
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
