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
package org.apache.iotdb.confignode.persistence.schema;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.request.read.CountStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.read.template.CheckTemplateSettablePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetPathsSetTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.AdjustMaxRegionGroupCountPlan;
import org.apache.iotdb.confignode.consensus.request.write.DeleteStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetDataReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetSchemaReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetTimePartitionIntervalPlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.SetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.response.AllTemplateSetInfoResp;
import org.apache.iotdb.confignode.consensus.response.CountStorageGroupResp;
import org.apache.iotdb.confignode.consensus.response.PathInfoResp;
import org.apache.iotdb.confignode.consensus.response.StorageGroupSchemaResp;
import org.apache.iotdb.confignode.consensus.response.TemplateInfoResp;
import org.apache.iotdb.confignode.exception.StorageGroupNotExistsException;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.db.metadata.mtree.ConfigMTree;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
  private final ConfigMTree mTree;

  private final String snapshotFileName = "cluster_schema.bin";

  private final TemplateTable templateTable;

  public ClusterSchemaInfo() throws IOException {
    storageGroupReadWriteLock = new ReentrantReadWriteLock();

    try {
      mTree = new ConfigMTree();
      templateTable = new TemplateTable();
    } catch (MetadataException e) {
      LOGGER.error("Can't construct StorageGroupInfo", e);
      throw new IOException(e);
    }
  }

  // ======================================================
  // Consensus read/write interfaces
  // ======================================================

  /**
   * Cache StorageGroupSchema
   *
   * @param plan SetStorageGroupPlan
   * @return SUCCESS_STATUS if the StorageGroup is set successfully. CACHE_FAILURE if fail to set
   *     StorageGroup in MTreeAboveSG.
   */
  public TSStatus setStorageGroup(SetStorageGroupPlan plan) {
    TSStatus result = new TSStatus();
    storageGroupReadWriteLock.writeLock().lock();
    try {
      // Set StorageGroup
      TStorageGroupSchema storageGroupSchema = plan.getSchema();
      PartialPath partialPathName = new PartialPath(storageGroupSchema.getName());
      mTree.setStorageGroup(partialPathName);

      // Set StorageGroupSchema
      mTree
          .getStorageGroupNodeByStorageGroupPath(partialPathName)
          .setStorageGroupSchema(storageGroupSchema);

      result.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (MetadataException e) {
      LOGGER.error("Error StorageGroup name", e);
      result.setCode(e.getErrorCode()).setMessage(e.getMessage());
    } finally {
      storageGroupReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  /**
   * Delete StorageGroup
   *
   * @param plan DeleteStorageGroupPlan
   * @return SUCCESS_STATUS
   */
  public TSStatus deleteStorageGroup(DeleteStorageGroupPlan plan) {
    TSStatus result = new TSStatus();
    storageGroupReadWriteLock.writeLock().lock();
    try {
      // Delete StorageGroup
      String storageGroup = plan.getName();
      PartialPath partialPathName = new PartialPath(storageGroup);
      mTree.deleteStorageGroup(partialPathName);

      result.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (MetadataException e) {
      LOGGER.warn("Storage group not exist", e);
      result
          .setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode())
          .setMessage("Storage group not exist: " + e.getMessage());
    } finally {
      storageGroupReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  /** @return The number of matched StorageGroups by the specific StorageGroup pattern */
  public CountStorageGroupResp countMatchedStorageGroups(CountStorageGroupPlan plan) {
    CountStorageGroupResp result = new CountStorageGroupResp();
    storageGroupReadWriteLock.readLock().lock();
    try {
      PartialPath patternPath = new PartialPath(plan.getStorageGroupPattern());
      result.setCount(mTree.getStorageGroupNum(patternPath, false));
      result.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    } catch (MetadataException e) {
      LOGGER.error("Error StorageGroup name", e);
      result.setStatus(
          new TSStatus(TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode())
              .setMessage("Error StorageGroup name: " + e.getMessage()));
    } finally {
      storageGroupReadWriteLock.readLock().unlock();
    }
    return result;
  }

  /** @return All StorageGroupSchemas that matches to the specific StorageGroup pattern */
  public StorageGroupSchemaResp getMatchedStorageGroupSchemas(GetStorageGroupPlan plan) {
    StorageGroupSchemaResp result = new StorageGroupSchemaResp();
    storageGroupReadWriteLock.readLock().lock();
    try {
      Map<String, TStorageGroupSchema> schemaMap = new HashMap<>();
      PartialPath patternPath = new PartialPath(plan.getStorageGroupPattern());
      List<PartialPath> matchedPaths = mTree.getMatchedStorageGroups(patternPath, false);
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
              .setMessage("Error StorageGroup name: " + e.getMessage()));
    } finally {
      storageGroupReadWriteLock.readLock().unlock();
    }
    return result;
  }

  public TSStatus setTTL(SetTTLPlan plan) {
    TSStatus result = new TSStatus();
    storageGroupReadWriteLock.writeLock().lock();
    try {
      PartialPath patternPath = new PartialPath(plan.getStorageGroupPathPattern());
      List<PartialPath> matchedPaths = mTree.getBelongedStorageGroups(patternPath);
      if (matchedPaths.size() != 0) {
        for (PartialPath path : matchedPaths) {
          mTree
              .getStorageGroupNodeByStorageGroupPath(path)
              .getStorageGroupSchema()
              .setTTL(plan.getTTL());
        }
        result.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      } else {
        result.setCode(TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode());
        result.setMessage("StorageGroup does not exist");
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

  public TSStatus setSchemaReplicationFactor(SetSchemaReplicationFactorPlan plan) {
    TSStatus result = new TSStatus();
    storageGroupReadWriteLock.writeLock().lock();
    try {
      PartialPath path = new PartialPath(plan.getStorageGroup());
      if (mTree.isStorageGroupAlreadySet(path)) {
        mTree
            .getStorageGroupNodeByStorageGroupPath(path)
            .getStorageGroupSchema()
            .setSchemaReplicationFactor(plan.getSchemaReplicationFactor());
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

  public TSStatus setDataReplicationFactor(SetDataReplicationFactorPlan plan) {
    TSStatus result = new TSStatus();
    storageGroupReadWriteLock.writeLock().lock();
    try {
      PartialPath path = new PartialPath(plan.getStorageGroup());
      if (mTree.isStorageGroupAlreadySet(path)) {
        mTree
            .getStorageGroupNodeByStorageGroupPath(path)
            .getStorageGroupSchema()
            .setDataReplicationFactor(plan.getDataReplicationFactor());
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

  public TSStatus setTimePartitionInterval(SetTimePartitionIntervalPlan plan) {
    TSStatus result = new TSStatus();
    storageGroupReadWriteLock.writeLock().lock();
    try {
      PartialPath path = new PartialPath(plan.getStorageGroup());
      if (mTree.isStorageGroupAlreadySet(path)) {
        mTree
            .getStorageGroupNodeByStorageGroupPath(path)
            .getStorageGroupSchema()
            .setTimePartitionInterval(plan.getTimePartitionInterval());
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

  /**
   * Adjust the maximum RegionGroup count of each StorageGroup
   *
   * @param plan AdjustMaxRegionGroupCountPlan
   * @return SUCCESS_STATUS
   */
  public TSStatus adjustMaxRegionGroupCount(AdjustMaxRegionGroupCountPlan plan) {
    TSStatus result = new TSStatus();
    storageGroupReadWriteLock.writeLock().lock();
    try {
      for (Map.Entry<String, Pair<Integer, Integer>> entry :
          plan.getMaxRegionGroupCountMap().entrySet()) {
        PartialPath path = new PartialPath(entry.getKey());
        TStorageGroupSchema storageGroupSchema =
            mTree.getStorageGroupNodeByStorageGroupPath(path).getStorageGroupSchema();
        storageGroupSchema.setMaxSchemaRegionGroupCount(entry.getValue().getLeft());
        storageGroupSchema.setMaxDataRegionGroupCount(entry.getValue().getRight());
      }
      result.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (MetadataException e) {
      LOGGER.error("Error StorageGroup name", e);
      result.setCode(TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode());
    } finally {
      storageGroupReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  // ======================================================
  // Leader scheduling interfaces
  // ======================================================

  /**
   * Only leader use this interface.
   *
   * @return List<StorageGroupName>, all storageGroups' name
   */
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

  /**
   * Only leader use this interface. Check if the specific StorageGroup already exists.
   *
   * @param storageName The specific StorageGroup's name
   * @throws MetadataException If the specific StorageGroup already exists
   */
  public void checkContainsStorageGroup(String storageName) throws MetadataException {
    storageGroupReadWriteLock.readLock().lock();
    try {
      mTree.checkStorageGroupAlreadySet(new PartialPath(storageName));
    } finally {
      storageGroupReadWriteLock.readLock().unlock();
    }
  }

  /**
   * Only leader use this interface. Get the specific StorageGroupSchema
   *
   * @param storageGroup StorageGroupName
   * @return The specific StorageGroupSchema
   * @throws StorageGroupNotExistsException When the specific StorageGroup doesn't exist
   */
  public TStorageGroupSchema getMatchedStorageGroupSchemaByName(String storageGroup)
      throws StorageGroupNotExistsException {
    storageGroupReadWriteLock.readLock().lock();
    try {
      return mTree
          .getStorageGroupNodeByStorageGroupPath(new PartialPath(storageGroup))
          .getStorageGroupSchema();
    } catch (MetadataException e) {
      throw new StorageGroupNotExistsException(storageGroup);
    } finally {
      storageGroupReadWriteLock.readLock().unlock();
    }
  }

  /**
   * Only leader use this interface. Get the matched StorageGroupSchemas.
   *
   * @param rawPathList StorageGroups' path patterns or full paths
   * @return All StorageGroupSchemas that matches to the specific StorageGroup patterns
   */
  public Map<String, TStorageGroupSchema> getMatchedStorageGroupSchemasByName(
      List<String> rawPathList) {
    Map<String, TStorageGroupSchema> schemaMap = new HashMap<>();
    storageGroupReadWriteLock.readLock().lock();
    try {
      for (String rawPath : rawPathList) {
        PartialPath patternPath = new PartialPath(rawPath);
        List<PartialPath> matchedPaths = mTree.getMatchedStorageGroups(patternPath, false);
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
   * Only leader use this interface. Get the maxRegionGroupCount of specific StorageGroup.
   *
   * @param storageGroup StorageGroupName
   * @param consensusGroupType SchemaRegion or DataRegion
   * @return maxSchemaRegionGroupCount or maxDataRegionGroupCount
   */
  public int getMaxRegionGroupCount(String storageGroup, TConsensusGroupType consensusGroupType) {
    storageGroupReadWriteLock.readLock().lock();
    try {
      PartialPath path = new PartialPath(storageGroup);
      TStorageGroupSchema storageGroupSchema =
          mTree.getStorageGroupNodeByStorageGroupPath(path).getStorageGroupSchema();
      switch (consensusGroupType) {
        case SchemaRegion:
          return storageGroupSchema.getMaxSchemaRegionGroupCount();
        case DataRegion:
        default:
          return storageGroupSchema.getMaxDataRegionGroupCount();
      }
    } catch (MetadataException e) {
      LOGGER.warn("Error StorageGroup name", e);
      return -1;
    } finally {
      storageGroupReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    processMtreeTakeSnapshot(snapshotDir);
    return templateTable.processTakeSnapshot(snapshotDir);
  }

  public boolean processMtreeTakeSnapshot(File snapshotDir) throws IOException {
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
        // Take snapshot for MTree
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
    processMtreeLoadSnapshot(snapshotDir);
    templateTable.processLoadSnapshot(snapshotDir);
  }

  public void processMtreeLoadSnapshot(File snapshotDir) throws IOException {
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
      // Load snapshot of MTree
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

  public Pair<Set<TSchemaNode>, Set<PartialPath>> getChildNodePathInNextLevel(
      PartialPath partialPath) {
    Pair<Set<TSchemaNode>, Set<PartialPath>> matchedPathsInNextLevel =
        new Pair<>(new HashSet<>(), new HashSet<>());
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
        new Pair<>(new HashSet<>(), new HashSet<>());
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

  public TSStatus createSchemaTemplate(CreateSchemaTemplatePlan createSchemaTemplatePlan) {
    try {
      Template template = createSchemaTemplatePlan.getTemplate();
      templateTable.createTemplate(template);
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (MetadataException e) {
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  public TemplateInfoResp getAllTemplates() {
    TemplateInfoResp result = new TemplateInfoResp();
    List<Template> resp = templateTable.getAllTemplate();
    result.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    result.setTemplateList(resp);
    return result;
  }

  public TemplateInfoResp getTemplate(GetSchemaTemplatePlan getSchemaTemplatePlan) {
    TemplateInfoResp result = new TemplateInfoResp();
    List<Template> list = new ArrayList<>();
    try {
      list.add(templateTable.getTemplate(getSchemaTemplatePlan.getTemplateName()));
      result.setTemplateList(list);
      result.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    } catch (MetadataException e) {
      LOGGER.error(e.getMessage(), e);
      result.setStatus(RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
    }
    return result;
  }

  public synchronized TemplateInfoResp checkTemplateSettable(
      CheckTemplateSettablePlan checkTemplateSettablePlan) {
    TemplateInfoResp resp = new TemplateInfoResp();
    PartialPath path;
    try {
      path = new PartialPath(checkTemplateSettablePlan.getPath());
    } catch (IllegalPathException e) {
      LOGGER.error(e.getMessage());
      resp.setStatus(RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
      return resp;
    }

    try {
      mTree.checkTemplateOnPath(path);
      resp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
      resp.setTemplateList(
          Collections.singletonList(
              templateTable.getTemplate(checkTemplateSettablePlan.getName())));
    } catch (MetadataException e) {
      LOGGER.error(e.getMessage(), e);
      resp.setStatus(RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
    }

    return resp;
  }

  // Before execute this method, checkTemplateSettable method should be invoked first and the whole
  // process must be synchronized
  public synchronized TSStatus setSchemaTemplate(SetSchemaTemplatePlan setSchemaTemplatePlan) {
    PartialPath path;
    try {
      path = new PartialPath(setSchemaTemplatePlan.getPath());
    } catch (IllegalPathException e) {
      LOGGER.error(e.getMessage());
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }

    try {
      int templateId = templateTable.getTemplate(setSchemaTemplatePlan.getName()).getId();
      mTree.getNodeWithAutoCreate(path).setSchemaTemplateId(templateId);
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (MetadataException e) {
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  public PathInfoResp getPathsSetTemplate(GetPathsSetTemplatePlan getPathsSetTemplatePlan) {
    PathInfoResp pathInfoResp = new PathInfoResp();
    TSStatus status;
    try {
      int templateId = templateTable.getTemplate(getPathsSetTemplatePlan.getName()).getId();
      pathInfoResp.setPathList(mTree.getPathsSetOnTemplate(templateId));
      status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (MetadataException e) {
      status = RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
    pathInfoResp.setStatus(status);
    return pathInfoResp;
  }

  public AllTemplateSetInfoResp getAllTemplateSetInfo() {
    List<Template> templateList = templateTable.getAllTemplate();
    Map<Integer, List<String>> templateSetInfo = new HashMap<>();
    int id;
    for (Template template : templateList) {
      id = template.getId();
      try {
        List<String> pathList = mTree.getPathsSetOnTemplate(id);
        if (!pathList.isEmpty()) {
          templateSetInfo.put(id, pathList);
        }
      } catch (MetadataException e) {
        LOGGER.error("Error occurred when get paths set on template {}", id, e);
      }
    }

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      ReadWriteIOUtils.write(templateSetInfo.size(), outputStream);

      for (Template template : templateList) {
        if (templateSetInfo.containsKey(template.getId())) {
          template.serialize(outputStream);

          List<String> pathsSetTemplate = templateSetInfo.get(template.getId());
          ReadWriteIOUtils.write(pathsSetTemplate.size(), outputStream);
          for (String path : pathsSetTemplate) {
            ReadWriteIOUtils.write(path, outputStream);
          }
        }
      }
    } catch (IOException ignored) {

    }

    return new AllTemplateSetInfoResp(outputStream.toByteArray());
  }

  public Map<String, TStorageGroupSchema> getMatchedStorageGroupSchemasByOneName(
      String[] storageGroupPathPattern) {
    Map<String, TStorageGroupSchema> schemaMap = new HashMap<>();
    storageGroupReadWriteLock.readLock().lock();
    try {
      PartialPath patternPath = new PartialPath(storageGroupPathPattern);
      List<PartialPath> matchedPaths = mTree.getBelongedStorageGroups(patternPath);
      for (PartialPath path : matchedPaths) {
        schemaMap.put(
            path.getFullPath(), mTree.getStorageGroupNodeByPath(path).getStorageGroupSchema());
      }
    } catch (MetadataException e) {
      LOGGER.warn("Error StorageGroup name", e);
    } finally {
      storageGroupReadWriteLock.readLock().unlock();
    }
    return schemaMap;
  }

  @TestOnly
  public void clear() {
    mTree.clear();
  }
}
