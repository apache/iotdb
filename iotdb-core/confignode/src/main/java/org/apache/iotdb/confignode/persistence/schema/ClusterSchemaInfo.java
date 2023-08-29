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
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.request.read.database.CountDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.read.database.GetDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.CheckTemplateSettablePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetPathsSetTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetTemplateSetInfoPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.AdjustMaxRegionGroupNumPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetDataReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetSchemaReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTimePartitionIntervalPlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CommitSetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.DropSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.ExtendSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.PreSetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.PreUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.RollbackPreUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.SetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.UnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.response.database.CountDatabaseResp;
import org.apache.iotdb.confignode.consensus.response.database.DatabaseSchemaResp;
import org.apache.iotdb.confignode.consensus.response.partition.PathInfoResp;
import org.apache.iotdb.confignode.consensus.response.template.AllTemplateSetInfoResp;
import org.apache.iotdb.confignode.consensus.response.template.TemplateInfoResp;
import org.apache.iotdb.confignode.consensus.response.template.TemplateSetInfoResp;
import org.apache.iotdb.confignode.exception.DatabaseNotExistsException;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.db.exception.metadata.SchemaQuotaExceededException;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.db.schemaengine.template.TemplateInternalRPCUtil;
import org.apache.iotdb.db.schemaengine.template.alter.TemplateExtendInfo;
import org.apache.iotdb.rpc.RpcUtils;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_MATCH_PATTERN;
import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_TEMPLATE;
import static org.apache.iotdb.commons.schema.SchemaConstant.SYSTEM_DATABASE_PATTERN;

/**
 * The {@link ClusterSchemaInfo} stores cluster schemaEngine. The cluster schemaEngine including: 1.
 * StorageGroupSchema 2. Template (Not implement yet)
 */
public class ClusterSchemaInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterSchemaInfo.class);
  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  // Database read write lock
  private final ReentrantReadWriteLock databaseReadWriteLock;
  private final ConfigMTree mTree;

  private static final String SNAPSHOT_FILENAME = "cluster_schema.bin";

  private final String ERROR_NAME = "Error Database name";

  private final TemplateTable templateTable;

  private final TemplatePreSetTable templatePreSetTable;

  public ClusterSchemaInfo() throws IOException {
    databaseReadWriteLock = new ReentrantReadWriteLock();

    try {
      mTree = new ConfigMTree();
      templateTable = new TemplateTable();
      templatePreSetTable = new TemplatePreSetTable();
    } catch (MetadataException e) {
      LOGGER.error("Can't construct ClusterSchemaInfo", e);
      throw new IOException(e);
    }
  }

  // ======================================================
  // Consensus read/write interfaces
  // ======================================================

  /**
   * Cache DatabaseSchema.
   *
   * @param plan DatabaseSchemaPlan
   * @return SUCCESS_STATUS if the Database is set successfully.
   */
  public TSStatus createDatabase(DatabaseSchemaPlan plan) {
    TSStatus result = new TSStatus();
    databaseReadWriteLock.writeLock().lock();
    try {
      // Set Database
      TDatabaseSchema databaseSchema = plan.getSchema();
      PartialPath partialPathName = new PartialPath(databaseSchema.getName());
      mTree.setStorageGroup(partialPathName);

      // Set DatabaseSchema
      mTree
          .getDatabaseNodeByDatabasePath(partialPathName)
          .getAsMNode()
          .setDatabaseSchema(databaseSchema);

      result.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (MetadataException e) {
      LOGGER.error(ERROR_NAME, e);
      result.setCode(e.getErrorCode()).setMessage(e.getMessage());
    } finally {
      databaseReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  /**
   * Alter DatabaseSchema.
   *
   * @param plan DatabaseSchemaPlan
   * @return SUCCESS_STATUS if the DatabaseSchema is altered successfully.
   */
  public TSStatus alterDatabase(DatabaseSchemaPlan plan) {
    TSStatus result = new TSStatus();
    databaseReadWriteLock.writeLock().lock();
    try {
      TDatabaseSchema alterSchema = plan.getSchema();
      PartialPath partialPathName = new PartialPath(alterSchema.getName());

      TDatabaseSchema currentSchema =
          mTree.getDatabaseNodeByDatabasePath(partialPathName).getAsMNode().getDatabaseSchema();
      // TODO: Support alter other fields
      if (alterSchema.isSetMinSchemaRegionGroupNum()) {
        currentSchema.setMinSchemaRegionGroupNum(alterSchema.getMinSchemaRegionGroupNum());
        currentSchema.setMaxSchemaRegionGroupNum(
            Math.max(
                currentSchema.getMinSchemaRegionGroupNum(),
                currentSchema.getMaxSchemaRegionGroupNum()));
        LOGGER.info(
            "[AdjustRegionGroupNum] The minimum number of SchemaRegionGroups for Database: {} is adjusted to: {}",
            currentSchema.getName(),
            currentSchema.getMinSchemaRegionGroupNum());
        LOGGER.info(
            "[AdjustRegionGroupNum] The maximum number of SchemaRegionGroups for Database: {} is adjusted to: {}",
            currentSchema.getName(),
            currentSchema.getMaxSchemaRegionGroupNum());
      }
      if (alterSchema.isSetMinDataRegionGroupNum()) {
        currentSchema.setMinDataRegionGroupNum(alterSchema.getMinDataRegionGroupNum());
        currentSchema.setMaxDataRegionGroupNum(
            Math.max(
                currentSchema.getMinDataRegionGroupNum(),
                currentSchema.getMaxDataRegionGroupNum()));
        LOGGER.info(
            "[AdjustRegionGroupNum] The minimum number of DataRegionGroups for Database: {} is adjusted to: {}",
            currentSchema.getName(),
            currentSchema.getMinDataRegionGroupNum());
        LOGGER.info(
            "[AdjustRegionGroupNum] The maximum number of DataRegionGroups for Database: {} is adjusted to: {}",
            currentSchema.getName(),
            currentSchema.getMaxDataRegionGroupNum());
      }

      mTree
          .getDatabaseNodeByDatabasePath(partialPathName)
          .getAsMNode()
          .setDatabaseSchema(currentSchema);
      result.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (MetadataException e) {
      LOGGER.error(ERROR_NAME, e);
      result.setCode(e.getErrorCode()).setMessage(e.getMessage());
    } finally {
      databaseReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  /**
   * Delete Database.
   *
   * @param plan DeleteDatabasePlan
   * @return SUCCESS_STATUS
   */
  public TSStatus deleteDatabase(DeleteDatabasePlan plan) {
    TSStatus result = new TSStatus();
    databaseReadWriteLock.writeLock().lock();
    try {
      // Delete Database
      String storageGroup = plan.getName();
      PartialPath partialPathName = new PartialPath(storageGroup);
      mTree.deleteDatabase(partialPathName);

      result.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (MetadataException e) {
      LOGGER.warn("Database not exist", e);
      result
          .setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode())
          .setMessage("Database not exist: " + e.getMessage());
    } finally {
      databaseReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  /**
   * Check database limit if necessary.
   *
   * @throws SchemaQuotaExceededException if the number of databases exceeds the limit
   * @throws MetadataException if other exceptions happen
   */
  public void checkDatabaseLimit() throws MetadataException {
    int limit = COMMON_CONFIG.getDatabaseLimitThreshold();
    if (limit > 0) {
      databaseReadWriteLock.readLock().lock();
      try {
        int count =
            mTree.getDatabaseNum(ALL_MATCH_PATTERN, false)
                - mTree.getDatabaseNum(SYSTEM_DATABASE_PATTERN, false);
        if (count >= limit) {
          throw new SchemaQuotaExceededException(limit);
        }
      } finally {
        databaseReadWriteLock.readLock().unlock();
      }
    }
  }

  /** @return The number of matched Databases by the specified Database pattern */
  public CountDatabaseResp countMatchedDatabases(CountDatabasePlan plan) {
    CountDatabaseResp result = new CountDatabaseResp();
    databaseReadWriteLock.readLock().lock();
    try {
      PartialPath patternPath = new PartialPath(plan.getDatabasePattern());
      result.setCount(mTree.getDatabaseNum(patternPath, false));
      result.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    } catch (MetadataException e) {
      LOGGER.error(ERROR_NAME, e);
      result.setStatus(
          new TSStatus(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode())
              .setMessage(ERROR_NAME + ": " + e.getMessage()));
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
    return result;
  }

  /** @return All DatabaseSchemas that matches to the specified Database pattern */
  public DatabaseSchemaResp getMatchedDatabaseSchemas(GetDatabasePlan plan) {
    DatabaseSchemaResp result = new DatabaseSchemaResp();
    databaseReadWriteLock.readLock().lock();
    try {
      Map<String, TDatabaseSchema> schemaMap = new HashMap<>();
      PartialPath patternPath = new PartialPath(plan.getDatabasePattern());
      List<PartialPath> matchedPaths = mTree.getMatchedDatabases(patternPath, false);
      for (PartialPath path : matchedPaths) {
        schemaMap.put(
            path.getFullPath(),
            mTree.getDatabaseNodeByDatabasePath(path).getAsMNode().getDatabaseSchema());
      }
      result.setSchemaMap(schemaMap);
      result.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    } catch (MetadataException e) {
      LOGGER.error(ERROR_NAME, e);
      result.setStatus(
          new TSStatus(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode())
              .setMessage(ERROR_NAME + ": " + e.getMessage()));
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
    return result;
  }

  public TSStatus setTTL(SetTTLPlan plan) {
    TSStatus result = new TSStatus();
    databaseReadWriteLock.writeLock().lock();
    try {
      PartialPath patternPath = new PartialPath(plan.getDatabasePathPattern());
      List<PartialPath> matchedPaths = mTree.getBelongedDatabases(patternPath);
      if (!matchedPaths.isEmpty()) {
        for (PartialPath path : matchedPaths) {
          mTree.getDatabaseNodeByDatabasePath(path).setDataTTL(plan.getTTL());
        }
        result.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      } else {
        result.setCode(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode());
        result.setMessage("Database does not exist");
      }
    } catch (MetadataException e) {
      LOGGER.error(ERROR_NAME, e);
      result.setCode(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode()).setMessage(ERROR_NAME);
    } finally {
      databaseReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  public TSStatus setSchemaReplicationFactor(SetSchemaReplicationFactorPlan plan) {
    TSStatus result = new TSStatus();
    databaseReadWriteLock.writeLock().lock();
    try {
      PartialPath path = new PartialPath(plan.getDatabase());
      if (mTree.isDatabaseAlreadySet(path)) {
        mTree
            .getDatabaseNodeByDatabasePath(path)
            .getAsMNode()
            .getDatabaseSchema()
            .setSchemaReplicationFactor(plan.getSchemaReplicationFactor());
        result.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      } else {
        result.setCode(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode());
      }
    } catch (MetadataException e) {
      LOGGER.error(ERROR_NAME, e);
      result.setCode(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode()).setMessage(ERROR_NAME);
    } finally {
      databaseReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  public TSStatus setDataReplicationFactor(SetDataReplicationFactorPlan plan) {
    TSStatus result = new TSStatus();
    databaseReadWriteLock.writeLock().lock();
    try {
      PartialPath path = new PartialPath(plan.getDatabase());
      if (mTree.isDatabaseAlreadySet(path)) {
        mTree
            .getDatabaseNodeByDatabasePath(path)
            .getAsMNode()
            .getDatabaseSchema()
            .setDataReplicationFactor(plan.getDataReplicationFactor());
        result.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      } else {
        result.setCode(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode());
      }
    } catch (MetadataException e) {
      LOGGER.error(ERROR_NAME, e);
      result.setCode(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode()).setMessage(ERROR_NAME);
    } finally {
      databaseReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  public TSStatus setTimePartitionInterval(SetTimePartitionIntervalPlan plan) {
    TSStatus result = new TSStatus();
    databaseReadWriteLock.writeLock().lock();
    try {
      PartialPath path = new PartialPath(plan.getDatabase());
      if (mTree.isDatabaseAlreadySet(path)) {
        mTree
            .getDatabaseNodeByDatabasePath(path)
            .getAsMNode()
            .getDatabaseSchema()
            .setTimePartitionInterval(plan.getTimePartitionInterval());
        result.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      } else {
        result.setCode(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode());
      }
    } catch (MetadataException e) {
      LOGGER.error(ERROR_NAME, e);
      result.setCode(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode()).setMessage(ERROR_NAME);
    } finally {
      databaseReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  /**
   * Adjust the maximum RegionGroup count of each Database.
   *
   * @param plan AdjustMaxRegionGroupCountPlan
   * @return SUCCESS_STATUS
   */
  public TSStatus adjustMaxRegionGroupCount(AdjustMaxRegionGroupNumPlan plan) {
    TSStatus result = new TSStatus();
    databaseReadWriteLock.writeLock().lock();
    try {
      for (Map.Entry<String, Pair<Integer, Integer>> entry :
          plan.getMaxRegionGroupNumMap().entrySet()) {
        PartialPath path = new PartialPath(entry.getKey());
        TDatabaseSchema databaseSchema =
            mTree.getDatabaseNodeByDatabasePath(path).getAsMNode().getDatabaseSchema();
        databaseSchema.setMaxSchemaRegionGroupNum(entry.getValue().getLeft());
        databaseSchema.setMaxDataRegionGroupNum(entry.getValue().getRight());
      }
      result.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (MetadataException e) {
      LOGGER.error(ERROR_NAME, e);
      result.setCode(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode());
    } finally {
      databaseReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  // ======================================================
  // Leader scheduling interfaces
  // ======================================================

  /**
   * Only leader use this interface.
   *
   * @return List {@literal <}DatabaseName{@literal >}, all Databases' name.
   */
  public List<String> getDatabaseNames() {
    List<String> databases = new ArrayList<>();
    databaseReadWriteLock.readLock().lock();
    try {
      List<PartialPath> namePaths = mTree.getAllDatabasePaths();
      for (PartialPath path : namePaths) {
        databases.add(path.getFullPath());
      }
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
    return databases;
  }

  /**
   * Check if the specified DatabaseName is valid.
   *
   * @param databaseName The specified DatabaseName
   * @throws MetadataException If the DatabaseName invalid i.e. the specified DatabaseName is
   *     already exist, or it's a prefix of another DatabaseName
   */
  public void isDatabaseNameValid(String databaseName) throws MetadataException {
    databaseReadWriteLock.readLock().lock();
    try {
      mTree.checkDatabaseAlreadySet(new PartialPath(databaseName));
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
  }

  /**
   * Only leader use this interface. Get the specific DatabaseSchema
   *
   * @param database DatabaseName
   * @return The specific DatabaseSchema
   * @throws DatabaseNotExistsException When the specific Database doesn't exist
   */
  public TDatabaseSchema getMatchedDatabaseSchemaByName(String database)
      throws DatabaseNotExistsException {
    databaseReadWriteLock.readLock().lock();
    try {
      return mTree
          .getDatabaseNodeByDatabasePath(new PartialPath(database))
          .getAsMNode()
          .getDatabaseSchema();
    } catch (MetadataException e) {
      throw new DatabaseNotExistsException(database);
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
  }

  /**
   * Only leader use this interface. Get the matched DatabaseSchemas.
   *
   * @param rawPathList Databases' path patterns or full paths
   * @return All DatabaseSchemas that matches to the specific Database patterns
   */
  public Map<String, TDatabaseSchema> getMatchedDatabaseSchemasByName(List<String> rawPathList) {
    Map<String, TDatabaseSchema> schemaMap = new HashMap<>();
    databaseReadWriteLock.readLock().lock();
    try {
      for (String rawPath : rawPathList) {
        PartialPath patternPath = new PartialPath(rawPath);
        List<PartialPath> matchedPaths = mTree.getMatchedDatabases(patternPath, false);
        for (PartialPath path : matchedPaths) {
          schemaMap.put(
              path.getFullPath(),
              mTree.getDatabaseNodeByPath(path).getAsMNode().getDatabaseSchema());
        }
      }
    } catch (MetadataException e) {
      LOGGER.warn(ERROR_NAME, e);
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
    return schemaMap;
  }

  /**
   * Only leader use this interface. Get the maxRegionGroupNum of specified Database.
   *
   * @param database DatabaseName
   * @param consensusGroupType SchemaRegion or DataRegion
   * @return maxSchemaRegionGroupNum or maxDataRegionGroupNum
   */
  public int getMinRegionGroupNum(String database, TConsensusGroupType consensusGroupType) {
    databaseReadWriteLock.readLock().lock();
    try {
      PartialPath path = new PartialPath(database);
      TDatabaseSchema storageGroupSchema =
          mTree.getDatabaseNodeByDatabasePath(path).getAsMNode().getDatabaseSchema();
      switch (consensusGroupType) {
        case SchemaRegion:
          return storageGroupSchema.getMinSchemaRegionGroupNum();
        case DataRegion:
        default:
          return storageGroupSchema.getMinDataRegionGroupNum();
      }
    } catch (MetadataException e) {
      LOGGER.warn(ERROR_NAME, e);
      return -1;
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
  }

  /**
   * Only leader use this interface. Get the maxRegionGroupNum of specified Database.
   *
   * @param database DatabaseName
   * @param consensusGroupType SchemaRegion or DataRegion
   * @return maxSchemaRegionGroupNum or maxDataRegionGroupNum
   */
  public int getMaxRegionGroupNum(String database, TConsensusGroupType consensusGroupType) {
    databaseReadWriteLock.readLock().lock();
    try {
      PartialPath path = new PartialPath(database);
      TDatabaseSchema storageGroupSchema =
          mTree.getDatabaseNodeByDatabasePath(path).getAsMNode().getDatabaseSchema();
      switch (consensusGroupType) {
        case SchemaRegion:
          return storageGroupSchema.getMaxSchemaRegionGroupNum();
        case DataRegion:
        default:
          return storageGroupSchema.getMaxDataRegionGroupNum();
      }
    } catch (MetadataException e) {
      LOGGER.warn(ERROR_NAME, e);
      return -1;
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    return processMTreeTakeSnapshot(snapshotDir)
        && templateTable.processTakeSnapshot(snapshotDir)
        && templatePreSetTable.processTakeSnapshot(snapshotDir);
  }

  public boolean processMTreeTakeSnapshot(File snapshotDir) throws IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take snapshot, because snapshot file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }

    File tmpFile = new File(snapshotFile.getAbsolutePath() + "-" + UUID.randomUUID());

    databaseReadWriteLock.readLock().lock();
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
      databaseReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws IOException {
    processMTreeLoadSnapshot(snapshotDir);
    templateTable.processLoadSnapshot(snapshotDir);
    templatePreSetTable.processLoadSnapshot(snapshotDir);
  }

  public void processMTreeLoadSnapshot(File snapshotDir) throws IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot,snapshot file [{}] is not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }
    databaseReadWriteLock.writeLock().lock();
    try (FileInputStream fileInputStream = new FileInputStream(snapshotFile);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream)) {
      // Load snapshot of MTree
      mTree.clear();
      mTree.deserialize(bufferedInputStream);
    } finally {
      databaseReadWriteLock.writeLock().unlock();
    }
  }

  public Pair<List<PartialPath>, Set<PartialPath>> getNodesListInGivenLevel(
      PartialPath partialPath, int level) {
    Pair<List<PartialPath>, Set<PartialPath>> matchedPathsInNextLevel =
        new Pair(new HashSet<>(), new HashSet<>());
    databaseReadWriteLock.readLock().lock();
    try {
      matchedPathsInNextLevel = mTree.getNodesListInGivenLevel(partialPath, level, true);
    } catch (MetadataException e) {
      LOGGER.error("Error get matched paths in given level.", e);
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
    return matchedPathsInNextLevel;
  }

  public Pair<Set<TSchemaNode>, Set<PartialPath>> getChildNodePathInNextLevel(
      PartialPath partialPath) {
    Pair<Set<TSchemaNode>, Set<PartialPath>> matchedPathsInNextLevel =
        new Pair<>(new HashSet<>(), new HashSet<>());
    databaseReadWriteLock.readLock().lock();
    try {
      matchedPathsInNextLevel = mTree.getChildNodePathInNextLevel(partialPath);
    } catch (MetadataException e) {
      LOGGER.error("Error get matched paths in next level.", e);
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
    return matchedPathsInNextLevel;
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
      String templateName = getSchemaTemplatePlan.getTemplateName();
      if (templateName.equals(ONE_LEVEL_PATH_WILDCARD)) {
        list.addAll(templateTable.getAllTemplate());
      } else {
        list.add(templateTable.getTemplate(templateName));
      }
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
      mTree.setTemplate(templateId, path);
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (MetadataException e) {
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  public synchronized TSStatus preSetSchemaTemplate(
      PreSetSchemaTemplatePlan preSetSchemaTemplatePlan) {
    PartialPath path;
    try {
      path = new PartialPath(preSetSchemaTemplatePlan.getPath());
    } catch (IllegalPathException e) {
      LOGGER.error(e.getMessage());
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }

    try {
      int templateId = templateTable.getTemplate(preSetSchemaTemplatePlan.getName()).getId();
      if (preSetSchemaTemplatePlan.isRollback()) {
        rollbackPreSetSchemaTemplate(templateId, path);
      } else {
        preSetSchemaTemplate(templateId, path);
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (MetadataException e) {
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  private void preSetSchemaTemplate(int templateId, PartialPath templateSetPath)
      throws MetadataException {
    templatePreSetTable.preSetTemplate(templateId, templateSetPath);
    mTree.setTemplate(templateId, templateSetPath);
  }

  private void rollbackPreSetSchemaTemplate(int templateId, PartialPath templateSetPath)
      throws MetadataException {
    try {
      mTree.unsetTemplate(templateId, templateSetPath);
    } catch (MetadataException ignore) {
      // node not exists or not set template
    }
    templatePreSetTable.removeSetTemplate(templateId, templateSetPath);
  }

  public synchronized TSStatus commitSetSchemaTemplate(
      CommitSetSchemaTemplatePlan commitSetSchemaTemplatePlan) {
    PartialPath path;
    try {
      path = new PartialPath(commitSetSchemaTemplatePlan.getPath());
    } catch (IllegalPathException e) {
      LOGGER.error(e.getMessage());
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }

    try {
      int templateId = templateTable.getTemplate(commitSetSchemaTemplatePlan.getName()).getId();
      if (commitSetSchemaTemplatePlan.isRollback()) {
        rollbackCommitSetSchemaTemplate(templateId, path);
      } else {
        commitSetSchemaTemplate(templateId, path);
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (MetadataException e) {
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  private void commitSetSchemaTemplate(int templateId, PartialPath templateSetPath) {
    templatePreSetTable.removeSetTemplate(templateId, templateSetPath);
  }

  private void rollbackCommitSetSchemaTemplate(int templateId, PartialPath templateSetPath)
      throws MetadataException {
    mTree.unsetTemplate(templateId, templateSetPath);
  }

  public PathInfoResp getPathsSetTemplate(GetPathsSetTemplatePlan getPathsSetTemplatePlan) {
    PathInfoResp pathInfoResp = new PathInfoResp();
    TSStatus status;
    try {
      String templateName = getPathsSetTemplatePlan.getName();
      int templateId;
      if (templateName.equals(ONE_LEVEL_PATH_WILDCARD)) {
        templateId = ALL_TEMPLATE;
      } else {
        templateId = templateTable.getTemplate(templateName).getId();
      }
      pathInfoResp.setPathList(mTree.getPathsSetOnTemplate(templateId, false));
      status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (MetadataException e) {
      status = RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
    pathInfoResp.setStatus(status);
    return pathInfoResp;
  }

  public AllTemplateSetInfoResp getAllTemplateSetInfo() {
    List<Template> templateList = templateTable.getAllTemplate();
    Map<Integer, List<Pair<String, Boolean>>> templateSetInfo = new HashMap<>();
    int id;
    for (Template template : templateList) {
      id = template.getId();
      try {
        List<String> pathList = mTree.getPathsSetOnTemplate(id, true);
        if (!pathList.isEmpty()) {
          List<Pair<String, Boolean>> pathSetInfoList = new ArrayList<>();
          for (String path : pathList) {
            pathSetInfoList.add(
                new Pair<>(path, templatePreSetTable.isPreSet(id, new PartialPath(path))));
          }
          templateSetInfo.put(id, pathSetInfoList);
        }
      } catch (MetadataException e) {
        LOGGER.error("Error occurred when get paths set on template {}", id, e);
      }
    }

    Map<Template, List<Pair<String, Boolean>>> templateSetInfoMap = new HashMap<>();
    for (Template template : templateList) {
      if (templateSetInfo.containsKey(template.getId())) {
        templateSetInfoMap.put(template, templateSetInfo.get(template.getId()));
      }
    }

    return new AllTemplateSetInfoResp(
        TemplateInternalRPCUtil.generateAddAllTemplateSetInfoBytes(templateSetInfoMap));
  }

  /**
   * Get the templateId set on paths covered by input path pattern. Resolve the input path patterns
   * into specified path pattern start with template set path. The result set is organized as
   * specified path pattern -> template id
   */
  public TemplateSetInfoResp getTemplateSetInfo(GetTemplateSetInfoPlan plan) {
    TemplateSetInfoResp resp = new TemplateSetInfoResp();
    try {

      Map<PartialPath, Set<Integer>> allTemplateSetInfo = new HashMap<>();
      for (PartialPath pattern : plan.getPatternList()) {
        Map<Integer, Set<PartialPath>> templateSetInfo = mTree.getTemplateSetInfo(pattern);
        if (templateSetInfo.isEmpty()) {
          continue;
        }
        templateSetInfo.forEach(
            (templateId, templateSetPathList) -> {
              for (PartialPath templateSetPath : templateSetPathList) {
                pattern
                    .alterPrefixPath(templateSetPath)
                    .forEach(
                        path ->
                            allTemplateSetInfo
                                .computeIfAbsent(path, k -> new HashSet<>())
                                .add(templateId));
              }
            });
      }
      Map<PartialPath, List<Template>> result = new HashMap<>();
      for (Map.Entry<PartialPath, Set<Integer>> entry : allTemplateSetInfo.entrySet()) {
        List<Template> templateList = new ArrayList<>(entry.getValue().size());
        for (int templateId : entry.getValue()) {
          templateList.add(templateTable.getTemplate(templateId));
        }
        result.put(entry.getKey(), templateList);
      }
      resp.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
      resp.setPatternTemplateMap(result);
      return resp;
    } catch (MetadataException e) {
      LOGGER.error(e.getMessage(), e);
      resp.setStatus(RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
    }
    return resp;
  }

  public TSStatus preUnsetSchemaTemplate(PreUnsetSchemaTemplatePlan plan) {
    try {
      mTree.preUnsetTemplate(plan.getTemplateId(), plan.getPath());
      return StatusUtils.OK;
    } catch (MetadataException e) {
      LOGGER.error(e.getMessage(), e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  public TSStatus rollbackUnsetSchemaTemplate(RollbackPreUnsetSchemaTemplatePlan plan) {
    try {
      mTree.rollbackUnsetTemplate(plan.getTemplateId(), plan.getPath());
      return StatusUtils.OK;
    } catch (MetadataException e) {
      LOGGER.error(e.getMessage(), e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  public TSStatus unsetSchemaTemplate(UnsetSchemaTemplatePlan plan) {
    try {
      mTree.unsetTemplate(plan.getTemplateId(), plan.getPath());
      return StatusUtils.OK;
    } catch (MetadataException e) {
      LOGGER.error(e.getMessage(), e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  public TSStatus dropSchemaTemplate(DropSchemaTemplatePlan dropSchemaTemplatePlan) {
    try {
      templateTable.dropTemplate(dropSchemaTemplatePlan.getTemplateName());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (MetadataException e) {
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  public TSStatus extendSchemaTemplate(ExtendSchemaTemplatePlan extendSchemaTemplatePlan) {
    TemplateExtendInfo templateExtendInfo = extendSchemaTemplatePlan.getTemplateExtendInfo();
    try {
      templateTable.extendTemplate(templateExtendInfo);
      return RpcUtils.SUCCESS_STATUS;
    } catch (MetadataException e) {
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  public Map<String, TDatabaseSchema> getMatchedDatabaseSchemasByOneName(
      String[] databasePathPattern) {
    Map<String, TDatabaseSchema> schemaMap = new HashMap<>();
    databaseReadWriteLock.readLock().lock();
    try {
      PartialPath patternPath = new PartialPath(databasePathPattern);
      List<PartialPath> matchedPaths = mTree.getBelongedDatabases(patternPath);
      for (PartialPath path : matchedPaths) {
        schemaMap.put(
            path.getFullPath(), mTree.getDatabaseNodeByPath(path).getAsMNode().getDatabaseSchema());
      }
    } catch (MetadataException e) {
      LOGGER.warn(ERROR_NAME, e);
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
    return schemaMap;
  }

  @TestOnly
  public void clear() {
    mTree.clear();
  }
}
