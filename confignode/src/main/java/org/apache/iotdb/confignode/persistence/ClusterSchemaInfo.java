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
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.request.read.CountStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.read.GetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionsReq;
import org.apache.iotdb.confignode.consensus.request.write.DeleteStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.SetDataReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetSchemaReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTTLReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTimePartitionIntervalReq;
import org.apache.iotdb.confignode.consensus.response.CountStorageGroupResp;
import org.apache.iotdb.confignode.consensus.response.StorageGroupSchemaResp;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.mtree.MTreeAboveSG;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The ClusterSchemaInfo stores cluster schema. The cluster schema including: 1. StorageGroupSchema
 * 2. Template (Not implement yet)
 */
public class ClusterSchemaInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterSchemaInfo.class);

  // StorageGroup read write lock
  private final ReentrantReadWriteLock storageGroupReadWriteLock;

  private MTreeAboveSG mTree;

  private final String snapshotFileName = "cluster_schema.bin";

  public ClusterSchemaInfo() {
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
   * @return SUCCESS_STATUS if the StorageGroup is set successfully. PERSISTENCE_FAILURE if fail to
   *     set StorageGroup in MTreeAboveSG.
   */
  public TSStatus setStorageGroup(SetStorageGroupReq req) {
    TSStatus result = new TSStatus();
    storageGroupReadWriteLock.writeLock().lock();
    try {
      // Set StorageGroup
      TStorageGroupSchema storageGroupSchema = req.getSchema();
      PartialPath partialPathName = new PartialPath(storageGroupSchema.getName());
      mTree.setStorageGroup(partialPathName);

      // Set StorageGroupSchema
      mTree
          .getStorageGroupNodeByStorageGroupPath(partialPathName)
          .setStorageGroupSchema(storageGroupSchema);

      result.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());

      LOGGER.info("Successfully set StorageGroup: {}", storageGroupSchema);
    } catch (MetadataException e) {
      LOGGER.error("Error StorageGroup name", e);
      result
          .setCode(TSStatusCode.PERSISTENCE_FAILURE.getStatusCode())
          .setMessage("Error StorageGroup name");
    } finally {
      storageGroupReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  /**
   * Delete StorageGroup
   *
   * @param req DeleteStorageGroupReq
   * @return SUCCESS_STATUS
   */
  public TSStatus deleteStorageGroup(DeleteStorageGroupReq req) {
    TSStatus result = new TSStatus();
    storageGroupReadWriteLock.writeLock().lock();
    try {
      // Delete StorageGroup
      TStorageGroupSchema storageGroupSchema = req.getStorageGroup();
      PartialPath partialPathName = new PartialPath(storageGroupSchema.getName());
      mTree.deleteStorageGroup(partialPathName);
      result.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (MetadataException e) {
      LOGGER.warn("Storage group not exist", e);
      result
          .setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode())
          .setMessage("Storage group not exist");
    } finally {
      storageGroupReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  /**
   * Persistence new RegionGroupIds on specific StorageGroupSchema
   *
   * @param req CreateRegionsReq
   * @return SUCCESS_STATUS if the new RegionGroupIds is persistence successfully.
   *     PERSISTENCE_FAILURE if fail to find StorageGroup in MTreeAboveSG.
   */
  public TSStatus createRegions(CreateRegionsReq req) {
    TSStatus result = new TSStatus();
    storageGroupReadWriteLock.writeLock().lock();

    try {
      for (Map.Entry<String, List<TRegionReplicaSet>> reqEntry : req.getRegionMap().entrySet()) {
        PartialPath partialPathName = new PartialPath(reqEntry.getKey());
        TStorageGroupSchema storageGroupSchema =
            mTree.getStorageGroupNodeByStorageGroupPath(partialPathName).getStorageGroupSchema();
        reqEntry
            .getValue()
            .forEach(
                regionReplicaSet -> {
                  switch (regionReplicaSet.getRegionId().getType()) {
                    case SchemaRegion:
                      storageGroupSchema
                          .getSchemaRegionGroupIds()
                          .add(regionReplicaSet.getRegionId());
                      break;
                    case DataRegion:
                      storageGroupSchema
                          .getDataRegionGroupIds()
                          .add(regionReplicaSet.getRegionId());
                      break;
                  }
                });
      }
      result.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (MetadataException e) {
      LOGGER.error("Error StorageGroup name", e);
      result.setCode(TSStatusCode.PERSISTENCE_FAILURE.getStatusCode());
    } finally {
      storageGroupReadWriteLock.writeLock().unlock();
    }

    return result;
  }

  public TSStatus setTTL(SetTTLReq req) {
    TSStatus result = new TSStatus();
    storageGroupReadWriteLock.writeLock().lock();
    try {
      PartialPath path = new PartialPath(req.getStorageGroup());
      if (mTree.isStorageGroupAlreadySet(path)) {
        mTree
            .getStorageGroupNodeByStorageGroupPath(path)
            .getStorageGroupSchema()
            .setTTL(req.getTTL());
        result.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      } else {
        result.setCode(TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode());
      }
    } catch (MetadataException e) {
      LOGGER.error("Error StorageGroup name", e);
      result
          .setCode(TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode())
          .setMessage("Error StorageGroupName");
    } finally {
      storageGroupReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  public TSStatus setSchemaReplicationFactor(SetSchemaReplicationFactorReq req) {
    TSStatus result = new TSStatus();
    storageGroupReadWriteLock.writeLock().lock();
    try {
      PartialPath path = new PartialPath(req.getStorageGroup());
      if (mTree.isStorageGroupAlreadySet(path)) {
        mTree
            .getStorageGroupNodeByStorageGroupPath(path)
            .getStorageGroupSchema()
            .setSchemaReplicationFactor(req.getSchemaReplicationFactor());
        result.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      } else {
        result.setCode(TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode());
      }
    } catch (MetadataException e) {
      LOGGER.error("Error StorageGroup name", e);
      result
          .setCode(TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode())
          .setMessage("Error StorageGroupName");
    } finally {
      storageGroupReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  public TSStatus setDataReplicationFactor(SetDataReplicationFactorReq req) {
    TSStatus result = new TSStatus();
    storageGroupReadWriteLock.writeLock().lock();
    try {
      PartialPath path = new PartialPath(req.getStorageGroup());
      if (mTree.isStorageGroupAlreadySet(path)) {
        mTree
            .getStorageGroupNodeByStorageGroupPath(path)
            .getStorageGroupSchema()
            .setDataReplicationFactor(req.getDataReplicationFactor());
        result.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      } else {
        result.setCode(TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode());
      }
    } catch (MetadataException e) {
      LOGGER.error("Error StorageGroup name", e);
      result
          .setCode(TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode())
          .setMessage("Error StorageGroupName");
    } finally {
      storageGroupReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  public TSStatus setTimePartitionInterval(SetTimePartitionIntervalReq req) {
    TSStatus result = new TSStatus();
    storageGroupReadWriteLock.writeLock().lock();
    try {
      PartialPath path = new PartialPath(req.getStorageGroup());
      if (mTree.isStorageGroupAlreadySet(path)) {
        mTree
            .getStorageGroupNodeByStorageGroupPath(path)
            .getStorageGroupSchema()
            .setTimePartitionInterval(req.getTimePartitionInterval());
        result.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      } else {
        result.setCode(TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode());
      }
    } catch (MetadataException e) {
      LOGGER.error("Error StorageGroup name", e);
      result
          .setCode(TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode())
          .setMessage("Error StorageGroupName");
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

  /** @return The number of matched StorageGroups by the specific StorageGroup pattern */
  public CountStorageGroupResp countMatchedStorageGroups(CountStorageGroupReq req) {
    CountStorageGroupResp result = new CountStorageGroupResp();
    storageGroupReadWriteLock.readLock().lock();
    try {
      PartialPath patternPath = new PartialPath(req.getStorageGroupPattern());
      result.setCount(mTree.getBelongedStorageGroups(patternPath).size());
      result.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    } catch (MetadataException e) {
      LOGGER.error("Error StorageGroup name", e);
      result.setStatus(
          new TSStatus(TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode())
              .setMessage("Error StorageGroup name"));
    } finally {
      storageGroupReadWriteLock.readLock().unlock();
    }
    return result;
  }

  /** @return All StorageGroupSchemas that matches to the specific StorageGroup pattern */
  public StorageGroupSchemaResp getMatchedStorageGroupSchemas(GetStorageGroupReq req) {
    StorageGroupSchemaResp result = new StorageGroupSchemaResp();
    storageGroupReadWriteLock.readLock().lock();
    try {
      Map<String, TStorageGroupSchema> schemaMap = new HashMap<>();
      PartialPath patternPath = new PartialPath(req.getStorageGroupPattern());
      List<PartialPath> matchedPaths = mTree.getBelongedStorageGroups(patternPath);
      for (PartialPath path : matchedPaths) {
        schemaMap.put(
            path.getFullPath(),
            mTree.getStorageGroupNodeByStorageGroupPath(path).getStorageGroupSchema());
      }
      result.setSchemaMap(schemaMap);
      result.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    } catch (MetadataException e) {
      LOGGER.error("Error StorageGroup name", e);
      result.setStatus(
          new TSStatus(TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode())
              .setMessage("Error StorageGroup name"));
    } finally {
      storageGroupReadWriteLock.readLock().unlock();
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
   * Get the specific StorageGroupSchema
   *
   * @param storageGroup StorageGroupName
   * @return The specific StorageGroupSchema
   * @throws MetadataException from MTree
   */
  public TStorageGroupSchema getMatchedStorageGroupSchemaByName(String storageGroup)
      throws MetadataException {
    storageGroupReadWriteLock.readLock().lock();
    try {
      return mTree
          .getStorageGroupNodeByStorageGroupPath(new PartialPath(storageGroup))
          .getStorageGroupSchema();
    } finally {
      storageGroupReadWriteLock.readLock().unlock();
    }
  }

  /** @return All StorageGroupSchemas that matches to the specific StorageGroup patterns */
  public Map<String, TStorageGroupSchema> getMatchedStorageGroupSchemasByName(
      List<String> rawPathList) {
    Map<String, TStorageGroupSchema> schemaMap = new HashMap<>();
    storageGroupReadWriteLock.readLock().lock();
    try {
      for (String rawPath : rawPathList) {
        PartialPath patternPath = new PartialPath(rawPath);
        List<PartialPath> matchedPaths = mTree.getBelongedStorageGroups(patternPath);
        for (PartialPath path : matchedPaths) {
          schemaMap.put(
              path.getFullPath(), mTree.getStorageGroupNodeByPath(path).getStorageGroupSchema());
        }
      }
    } catch (MetadataException e) {
      LOGGER.warn("Error StorageGroup name", e);
    } finally {
      storageGroupReadWriteLock.readLock().unlock();
    }
    return schemaMap;
  }

  /**
   * Get the SchemaRegionGroupIds or DataRegionGroupIds from the specific StorageGroup.
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
      StorageGroupMNode mNode =
          (StorageGroupMNode)
              mTree.getStorageGroupNodeByStorageGroupPath(new PartialPath(storageGroup));
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

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException {

    File snapshotFile = new File(snapshotDir, snapshotFileName);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take snapshot, because snapshot file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }

    File tmpFile = new File(snapshotFile.getAbsolutePath() + "-" + UUID.randomUUID());

    storageGroupReadWriteLock.readLock().lock();
    try {
      try (FileOutputStream fileOutputStream = new FileOutputStream(tmpFile);
          BufferedOutputStream outputStream = new BufferedOutputStream(fileOutputStream)) {
        mTree.serialize(outputStream);
        outputStream.flush();
      }
      return tmpFile.renameTo(snapshotFile);
    } finally {
      for (int retry = 0; retry < 5; retry++) {
        if (!tmpFile.exists() || tmpFile.delete()) {
          break;
        } else {
          LOGGER.warn(
              "Can't delete temporary snapshot file: {}, retrying...", tmpFile.getAbsolutePath());
        }
      }
      storageGroupReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws IOException {

    File snapshotFile = new File(snapshotDir, snapshotFileName);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot,snapshot file [{}] is not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }
    storageGroupReadWriteLock.writeLock().lock();
    try (FileInputStream fileInputStream = new FileInputStream(snapshotFile);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream)) {
      mTree.clear();
      mTree.deserialize(bufferedInputStream);
    } finally {
      storageGroupReadWriteLock.writeLock().unlock();
    }
  }

  public Pair<List<PartialPath>, Set<PartialPath>> getNodesListInGivenLevel(
      PartialPath partialPath, int level) {
    Pair<List<PartialPath>, Set<PartialPath>> matchedPathsInNextLevel =
        new Pair(new HashSet<>(), new HashSet<>());
    storageGroupReadWriteLock.readLock().lock();
    try {
      matchedPathsInNextLevel = mTree.getNodesListInGivenLevel(partialPath, level, true, null);
    } catch (MetadataException e) {
      LOGGER.error("Error get matched paths in given level.", e);
    } finally {
      storageGroupReadWriteLock.readLock().unlock();
    }
    return matchedPathsInNextLevel;
  }

  public Pair<Set<String>, Set<PartialPath>> getChildNodePathInNextLevel(PartialPath partialPath) {
    Pair<Set<String>, Set<PartialPath>> matchedPathsInNextLevel =
        new Pair(new HashSet<>(), new HashSet<>());
    storageGroupReadWriteLock.readLock().lock();
    try {
      matchedPathsInNextLevel = mTree.getChildNodePathInNextLevel(partialPath);
    } catch (MetadataException e) {
      LOGGER.error("Error get matched paths in next level.", e);
    } finally {
      storageGroupReadWriteLock.readLock().unlock();
    }
    return matchedPathsInNextLevel;
  }

  public Pair<Set<String>, Set<PartialPath>> getChildNodeNameInNextLevel(PartialPath partialPath) {
    Pair<Set<String>, Set<PartialPath>> matchedNamesInNextLevel =
        new Pair(new HashSet<>(), new HashSet<>());
    storageGroupReadWriteLock.readLock().lock();
    try {
      matchedNamesInNextLevel = mTree.getChildNodeNameInNextLevel(partialPath);
    } catch (MetadataException e) {
      LOGGER.error("Error get matched names in next level.", e);
    } finally {
      storageGroupReadWriteLock.readLock().unlock();
    }
    return matchedNamesInNextLevel;
  }

  @TestOnly
  public void clear() {
    mTree.clear();
  }
}
