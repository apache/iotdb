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
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.request.read.database.CountDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.read.database.GetDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.read.table.DescTablePlan;
import org.apache.iotdb.confignode.consensus.request.read.table.FetchTablePlan;
import org.apache.iotdb.confignode.consensus.request.read.table.ShowTablePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.CheckTemplateSettablePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetPathsSetTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetTemplateSetInfoPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.AdjustMaxRegionGroupNumPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetDataReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetSchemaReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTimePartitionIntervalPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.AddTableColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitCreateTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreCreateTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreDeleteColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreDeleteTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.RenameTableColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.RollbackCreateTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTablePropertiesPlan;
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
import org.apache.iotdb.confignode.consensus.response.table.DescTableResp;
import org.apache.iotdb.confignode.consensus.response.table.FetchTableResp;
import org.apache.iotdb.confignode.consensus.response.table.ShowTableResp;
import org.apache.iotdb.confignode.consensus.response.template.AllTemplateSetInfoResp;
import org.apache.iotdb.confignode.consensus.response.template.TemplateInfoResp;
import org.apache.iotdb.confignode.consensus.response.template.TemplateSetInfoResp;
import org.apache.iotdb.confignode.exception.DatabaseNotExistsException;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TTableInfo;
import org.apache.iotdb.db.exception.metadata.DatabaseModelException;
import org.apache.iotdb.db.exception.metadata.SchemaQuotaExceededException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.db.schemaengine.template.TemplateInternalRPCUtil;
import org.apache.iotdb.db.schemaengine.template.alter.TemplateExtendInfo;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Pair;
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
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.TTL_INFINITE;
import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_MATCH_PATTERN;
import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_MATCH_SCOPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_TEMPLATE;
import static org.apache.iotdb.commons.schema.SchemaConstant.SYSTEM_DATABASE_PATTERN;
import static org.apache.iotdb.commons.schema.table.TsTable.TTL_PROPERTY;

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
   * @param plan {@link DatabaseSchemaPlan}
   * @return {@link TSStatusCode#SUCCESS_STATUS} if the Database is set successfully.
   */
  public TSStatus createDatabase(final DatabaseSchemaPlan plan) {
    final TSStatus result = new TSStatus();
    databaseReadWriteLock.writeLock().lock();
    try {
      // Set Database
      final TDatabaseSchema databaseSchema = plan.getSchema();
      final PartialPath partialPathName = PartialPath.getDatabasePath(databaseSchema.getName());
      mTree.setStorageGroup(partialPathName);

      // Set DatabaseSchema
      mTree
          .getDatabaseNodeByDatabasePath(partialPathName)
          .getAsMNode()
          .setDatabaseSchema(databaseSchema);

      result.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (final MetadataException e) {
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
   * @return {@link TSStatusCode#SUCCESS_STATUS} if the DatabaseSchema is altered successfully.
   */
  public TSStatus alterDatabase(final DatabaseSchemaPlan plan) {
    final TSStatus result = new TSStatus();
    databaseReadWriteLock.writeLock().lock();
    try {
      final TDatabaseSchema alterSchema = plan.getSchema();
      final PartialPath partialPathName = new PartialPath(alterSchema.getName());

      final TDatabaseSchema currentSchema =
          mTree.getDatabaseNodeByDatabasePath(partialPathName).getAsMNode().getDatabaseSchema();

      // Model conflict detection
      if (alterSchema.isIsTableModel() && !currentSchema.isIsTableModel()) {
        final DatabaseModelException exception =
            new DatabaseModelException(currentSchema.getName(), false);
        return RpcUtils.getStatus(exception.getErrorCode(), exception.getMessage());
      } else if (!alterSchema.isIsTableModel() && currentSchema.isIsTableModel()) {
        final DatabaseModelException exception =
            new DatabaseModelException(currentSchema.getName(), true);
        return RpcUtils.getStatus(exception.getErrorCode(), exception.getMessage());
      }

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
    } catch (final MetadataException e) {
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
   * @return {@link TSStatusCode#SUCCESS_STATUS}
   */
  public TSStatus deleteDatabase(final DeleteDatabasePlan plan) {
    final TSStatus result = new TSStatus();
    databaseReadWriteLock.writeLock().lock();
    try {
      // Delete Database
      mTree.deleteDatabase(getQualifiedDatabasePartialPath(plan.getName()));

      result.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (final MetadataException e) {
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
            mTree.getDatabaseNum(ALL_MATCH_PATTERN, ALL_MATCH_SCOPE, false)
                - mTree.getDatabaseNum(SYSTEM_DATABASE_PATTERN, ALL_MATCH_SCOPE, false);
        if (count >= limit) {
          throw new SchemaQuotaExceededException(limit);
        }
      } finally {
        databaseReadWriteLock.readLock().unlock();
      }
    }
  }

  /**
   * @return The number of matched Databases by the specified Database pattern
   */
  public CountDatabaseResp countMatchedDatabases(final CountDatabasePlan plan) {
    final CountDatabaseResp result = new CountDatabaseResp();
    databaseReadWriteLock.readLock().lock();
    try {
      final PartialPath patternPath = new PartialPath(plan.getDatabasePattern());
      result.setCount(mTree.getDatabaseNum(patternPath, plan.getScope(), false));
      result.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    } catch (final MetadataException e) {
      LOGGER.error(ERROR_NAME, e);
      result.setStatus(
          new TSStatus(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode())
              .setMessage(ERROR_NAME + ": " + e.getMessage()));
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
    return result;
  }

  /**
   * @return All DatabaseSchemas that matches to the specified Database pattern
   */
  public DatabaseSchemaResp getMatchedDatabaseSchemas(final GetDatabasePlan plan) {
    final DatabaseSchemaResp result = new DatabaseSchemaResp();
    databaseReadWriteLock.readLock().lock();
    try {
      final Map<String, TDatabaseSchema> schemaMap = new HashMap<>();
      final PartialPath patternPath = new PartialPath(plan.getDatabasePattern());
      final List<PartialPath> matchedPaths =
          mTree.getMatchedDatabases(patternPath, plan.getScope(), false);
      for (final PartialPath path : matchedPaths) {
        schemaMap.put(
            path.getFullPath(),
            mTree.getDatabaseNodeByDatabasePath(path).getAsMNode().getDatabaseSchema());
      }
      result.setSchemaMap(schemaMap);
      result.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    } catch (final MetadataException e) {
      LOGGER.error(ERROR_NAME, e);
      result.setStatus(
          new TSStatus(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode())
              .setMessage(ERROR_NAME + ": " + e.getMessage()));
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
    return result;
  }

  public TSStatus setSchemaReplicationFactor(final SetSchemaReplicationFactorPlan plan) {
    final TSStatus result = new TSStatus();
    databaseReadWriteLock.writeLock().lock();
    try {
      final PartialPath path = getQualifiedDatabasePartialPath(plan.getDatabase());
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
    } catch (final MetadataException e) {
      LOGGER.error(ERROR_NAME, e);
      result.setCode(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode()).setMessage(ERROR_NAME);
    } finally {
      databaseReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  public TSStatus setDataReplicationFactor(final SetDataReplicationFactorPlan plan) {
    final TSStatus result = new TSStatus();
    databaseReadWriteLock.writeLock().lock();
    try {
      final PartialPath path = getQualifiedDatabasePartialPath(plan.getDatabase());
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
    } catch (final MetadataException e) {
      LOGGER.error(ERROR_NAME, e);
      result.setCode(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode()).setMessage(ERROR_NAME);
    } finally {
      databaseReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  public TSStatus setTimePartitionInterval(final SetTimePartitionIntervalPlan plan) {
    final TSStatus result = new TSStatus();
    databaseReadWriteLock.writeLock().lock();
    try {
      final PartialPath path = getQualifiedDatabasePartialPath(plan.getDatabase());
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
    } catch (final MetadataException e) {
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
   * @return {@link TSStatusCode#SUCCESS_STATUS}
   */
  public TSStatus adjustMaxRegionGroupCount(final AdjustMaxRegionGroupNumPlan plan) {
    final TSStatus result = new TSStatus();
    databaseReadWriteLock.writeLock().lock();
    try {
      for (final Map.Entry<String, Pair<Integer, Integer>> entry :
          plan.getMaxRegionGroupNumMap().entrySet()) {
        final TDatabaseSchema databaseSchema =
            mTree
                .getDatabaseNodeByDatabasePath(getQualifiedDatabasePartialPath(entry.getKey()))
                .getAsMNode()
                .getDatabaseSchema();
        databaseSchema.setMaxSchemaRegionGroupNum(entry.getValue().getLeft());
        databaseSchema.setMaxDataRegionGroupNum(entry.getValue().getRight());
      }
      result.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (final MetadataException e) {
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
  public List<String> getDatabaseNames(final Boolean isTableModel) {
    databaseReadWriteLock.readLock().lock();
    try {
      return mTree.getAllDatabasePaths(isTableModel).stream()
          .map(PartialPath::getFullPath)
          .collect(Collectors.toList());
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
  }

  /**
   * Check if the specified DatabaseName is valid.
   *
   * @param databaseName The specified DatabaseName
   * @throws MetadataException If the DatabaseName invalid i.e. the specified DatabaseName is
   *     already exist, or it's a prefix of another DatabaseName
   */
  public void isDatabaseNameValid(final String databaseName) throws MetadataException {
    databaseReadWriteLock.readLock().lock();
    try {
      mTree.checkDatabaseAlreadySet(getQualifiedDatabasePartialPath(databaseName));
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
  }

  /**
   * Only leader use this interface. Get the specific {@link TDatabaseSchema}
   *
   * @param database DatabaseName
   * @return The specific DatabaseSchema
   * @throws DatabaseNotExistsException When the specific Database doesn't exist
   */
  public TDatabaseSchema getMatchedDatabaseSchemaByName(final String database)
      throws DatabaseNotExistsException {
    databaseReadWriteLock.readLock().lock();
    try {
      return mTree
          .getDatabaseNodeByDatabasePath(getQualifiedDatabasePartialPath(database))
          .getAsMNode()
          .getDatabaseSchema();
    } catch (final MetadataException e) {
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
  public Map<String, TDatabaseSchema> getMatchedDatabaseSchemasByName(
      final List<String> rawPathList) {
    final Map<String, TDatabaseSchema> schemaMap = new HashMap<>();
    databaseReadWriteLock.readLock().lock();
    try {
      for (final String rawPath : rawPathList) {
        final PartialPath patternPath = getQualifiedDatabasePartialPath(rawPath);
        final List<PartialPath> matchedPaths =
            mTree.getMatchedDatabases(patternPath, ALL_MATCH_SCOPE, false);
        for (final PartialPath path : matchedPaths) {
          schemaMap.put(
              path.getFullPath(),
              mTree.getDatabaseNodeByPath(path).getAsMNode().getDatabaseSchema());
        }
      }
    } catch (final MetadataException e) {
      LOGGER.warn(ERROR_NAME, e);
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
    return schemaMap;
  }

  /**
   * Only leader use this interface. Get the matched DatabaseSchemas.
   *
   * @param prefix prefix path such as root.a
   * @return All DatabaseSchemas that matches to the prefix path such as root.a.db1, root.a.db2
   */
  public Map<String, TDatabaseSchema> getMatchedDatabaseSchemasByPrefix(PartialPath prefix) {
    Map<String, TDatabaseSchema> schemaMap = new HashMap<>();
    databaseReadWriteLock.readLock().lock();
    try {
      List<PartialPath> matchedPaths = mTree.getMatchedDatabases(prefix, ALL_MATCH_SCOPE, true);
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

  /**
   * Only leader use this interface. Get the maxRegionGroupNum of specified Database.
   *
   * @param database DatabaseName
   * @param consensusGroupType SchemaRegion or DataRegion
   * @return maxSchemaRegionGroupNum or maxDataRegionGroupNum
   */
  public int getMinRegionGroupNum(
      final String database, final TConsensusGroupType consensusGroupType) {
    databaseReadWriteLock.readLock().lock();
    try {
      final TDatabaseSchema storageGroupSchema =
          mTree
              .getDatabaseNodeByDatabasePath(PartialPath.getDatabasePath(database))
              .getAsMNode()
              .getDatabaseSchema();
      switch (consensusGroupType) {
        case SchemaRegion:
          return storageGroupSchema.getMinSchemaRegionGroupNum();
        case DataRegion:
        default:
          return storageGroupSchema.getMinDataRegionGroupNum();
      }
    } catch (final MetadataException e) {
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
  public int getMaxRegionGroupNum(
      final String database, final TConsensusGroupType consensusGroupType) {
    databaseReadWriteLock.readLock().lock();
    try {
      final TDatabaseSchema storageGroupSchema =
          mTree
              .getDatabaseNodeByDatabasePath(PartialPath.getDatabasePath(database))
              .getAsMNode()
              .getDatabaseSchema();
      switch (consensusGroupType) {
        case SchemaRegion:
          return storageGroupSchema.getMaxSchemaRegionGroupNum();
        case DataRegion:
        default:
          return storageGroupSchema.getMaxDataRegionGroupNum();
      }
    } catch (final MetadataException e) {
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
      FileOutputStream fileOutputStream = new FileOutputStream(tmpFile);
      BufferedOutputStream outputStream = new BufferedOutputStream(fileOutputStream);
      try {
        // Take snapshot for MTree
        mTree.serialize(outputStream);
        outputStream.flush();
      } finally {
        outputStream.flush();
        fileOutputStream.getFD().sync();
        outputStream.close();
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
      PartialPath partialPath, int level, PathPatternTree scope) {
    Pair<List<PartialPath>, Set<PartialPath>> matchedPathsInNextLevel =
        new Pair(new HashSet<>(), new HashSet<>());
    databaseReadWriteLock.readLock().lock();
    try {
      matchedPathsInNextLevel = mTree.getNodesListInGivenLevel(partialPath, level, true, scope);
    } catch (MetadataException e) {
      LOGGER.error("Error get matched paths in given level.", e);
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
    return matchedPathsInNextLevel;
  }

  public Pair<Set<TSchemaNode>, Set<PartialPath>> getChildNodePathInNextLevel(
      PartialPath partialPath, PathPatternTree scope) {
    Pair<Set<TSchemaNode>, Set<PartialPath>> matchedPathsInNextLevel =
        new Pair<>(new HashSet<>(), new HashSet<>());
    databaseReadWriteLock.readLock().lock();
    try {
      matchedPathsInNextLevel = mTree.getChildNodePathInNextLevel(partialPath, scope);
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

  public Template getTemplate(int id) throws MetadataException {
    return templateTable.getTemplate(id);
  }

  public synchronized TemplateInfoResp checkTemplateSettable(
      final CheckTemplateSettablePlan checkTemplateSettablePlan) {
    final TemplateInfoResp resp = new TemplateInfoResp();
    final PartialPath path;
    try {
      path = new PartialPath(checkTemplateSettablePlan.getPath());
    } catch (final IllegalPathException e) {
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
    } catch (final MetadataException e) {
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
      PathPatternTree scope = getPathsSetTemplatePlan.getScope();
      int templateId;
      if (templateName.equals(ONE_LEVEL_PATH_WILDCARD)) {
        templateId = ALL_TEMPLATE;
      } else {
        templateId = templateTable.getTemplate(templateName).getId();
      }
      pathInfoResp.setPathList(mTree.getPathsSetOnTemplate(templateId, scope, false));
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
        List<String> pathList = mTree.getPathsSetOnTemplate(id, ALL_MATCH_SCOPE, true);
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
  public TemplateSetInfoResp getTemplateSetInfo(final GetTemplateSetInfoPlan plan) {
    final TemplateSetInfoResp resp = new TemplateSetInfoResp();
    try {
      final Map<PartialPath, Set<Integer>> allTemplateSetInfo = new HashMap<>();
      for (final PartialPath pattern : plan.getPatternList()) {
        final Map<Integer, Set<PartialPath>> templateSetInfo = mTree.getTemplateSetInfo(pattern);
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
      final String[] databasePathPattern) {
    final Map<String, TDatabaseSchema> schemaMap = new HashMap<>();
    databaseReadWriteLock.readLock().lock();
    try {
      final PartialPath patternPath = new PartialPath(databasePathPattern);
      final List<PartialPath> matchedPaths = mTree.getBelongedDatabases(patternPath);
      for (final PartialPath path : matchedPaths) {
        schemaMap.put(
            path.getFullPath(), mTree.getDatabaseNodeByPath(path).getAsMNode().getDatabaseSchema());
      }
    } catch (final MetadataException e) {
      LOGGER.warn(ERROR_NAME, e);
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
    return schemaMap;
  }

  // region table management

  public TSStatus preCreateTable(final PreCreateTablePlan plan) {
    databaseReadWriteLock.writeLock().lock();
    try {
      mTree.preCreateTable(getQualifiedDatabasePartialPath(plan.getDatabase()), plan.getTable());
      return RpcUtils.SUCCESS_STATUS;
    } catch (final MetadataException e) {
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } finally {
      databaseReadWriteLock.writeLock().unlock();
    }
  }

  public TSStatus rollbackCreateTable(final RollbackCreateTablePlan plan) {
    databaseReadWriteLock.writeLock().lock();
    try {
      mTree.rollbackCreateTable(
          getQualifiedDatabasePartialPath(plan.getDatabase()), plan.getTableName());
      return RpcUtils.SUCCESS_STATUS;
    } catch (final MetadataException e) {
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } finally {
      databaseReadWriteLock.writeLock().unlock();
    }
  }

  public TSStatus commitCreateTable(final CommitCreateTablePlan plan) {
    databaseReadWriteLock.writeLock().lock();
    try {
      mTree.commitCreateTable(
          getQualifiedDatabasePartialPath(plan.getDatabase()), plan.getTableName());
      return RpcUtils.SUCCESS_STATUS;
    } catch (final MetadataException e) {
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } finally {
      databaseReadWriteLock.writeLock().unlock();
    }
  }

  public TSStatus preDeleteTable(final PreDeleteTablePlan plan) {
    databaseReadWriteLock.writeLock().lock();
    try {
      mTree.preDeleteTable(
          getQualifiedDatabasePartialPath(plan.getDatabase()), plan.getTableName());
      return RpcUtils.SUCCESS_STATUS;
    } catch (final MetadataException e) {
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } finally {
      databaseReadWriteLock.writeLock().unlock();
    }
  }

  public TSStatus dropTable(final CommitDeleteTablePlan plan) {
    databaseReadWriteLock.writeLock().lock();
    try {
      mTree.dropTable(getQualifiedDatabasePartialPath(plan.getDatabase()), plan.getTableName());
      return RpcUtils.SUCCESS_STATUS;
    } catch (final MetadataException e) {
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } finally {
      databaseReadWriteLock.writeLock().unlock();
    }
  }

  public ShowTableResp showTables(final ShowTablePlan plan) {
    databaseReadWriteLock.readLock().lock();
    try {
      return new ShowTableResp(
          StatusUtils.OK,
          plan.isDetails()
              ? mTree
                  .getAllTablesUnderSpecificDatabase(
                      getQualifiedDatabasePartialPath(plan.getDatabase()))
                  .stream()
                  .map(
                      pair -> {
                        final TTableInfo info =
                            new TTableInfo(
                                pair.getLeft().getTableName(),
                                pair.getLeft().getPropValue(TTL_PROPERTY).orElse(TTL_INFINITE));
                        info.setState(pair.getRight().ordinal());
                        return info;
                      })
                  .collect(Collectors.toList())
              : mTree
                  .getAllUsingTablesUnderSpecificDatabase(
                      getQualifiedDatabasePartialPath(plan.getDatabase()))
                  .stream()
                  .map(
                      tsTable ->
                          new TTableInfo(
                              tsTable.getTableName(),
                              tsTable.getPropValue(TTL_PROPERTY).orElse(TTL_INFINITE)))
                  .collect(Collectors.toList()));
    } catch (final MetadataException e) {
      return new ShowTableResp(
          RpcUtils.getStatus(e.getErrorCode(), e.getMessage()), Collections.emptyList());
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
  }

  public FetchTableResp fetchTables(final FetchTablePlan plan) {
    databaseReadWriteLock.readLock().lock();
    try {
      final Map<String, Map<String, TsTable>> result = new HashMap<>();
      for (final Map.Entry<String, Set<String>> database2Tables :
          plan.getFetchTableMap().entrySet()) {
        result.put(
            database2Tables.getKey(),
            mTree.getSpecificTablesUnderSpecificDatabase(
                getQualifiedDatabasePartialPath(database2Tables.getKey()),
                database2Tables.getValue()));
      }
      return new FetchTableResp(StatusUtils.OK, result);
    } catch (final MetadataException e) {
      return new FetchTableResp(
          RpcUtils.getStatus(e.getErrorCode(), e.getMessage()), Collections.emptyMap());
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
  }

  public DescTableResp descTable(final DescTablePlan plan) {
    databaseReadWriteLock.readLock().lock();
    try {
      final PartialPath databasePath = getQualifiedDatabasePartialPath(plan.getDatabase());
      if (plan.isDetails()) {
        final Pair<TsTable, Set<String>> pair =
            mTree.getTableSchemaDetails(databasePath, plan.getTableName());
        return new DescTableResp(StatusUtils.OK, pair.getLeft(), pair.getRight());
      }
      return new DescTableResp(
          StatusUtils.OK, mTree.getUsingTableSchema(databasePath, plan.getTableName()), null);
    } catch (final MetadataException e) {
      return new DescTableResp(RpcUtils.getStatus(e.getErrorCode(), e.getMessage()), null, null);
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
  }

  public Map<String, List<TsTable>> getAllUsingTables() {
    databaseReadWriteLock.readLock().lock();
    try {
      return mTree.getAllUsingTables();
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
  }

  public Map<String, List<TsTable>> getAllPreCreateTables() {
    databaseReadWriteLock.readLock().lock();
    try {
      return mTree.getAllPreCreateTables();
    } catch (final MetadataException e) {
      LOGGER.warn(e.getMessage(), e);
      throw new RuntimeException(e);
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
  }

  public Optional<TsTable> getTsTableIfExists(final String database, final String tableName)
      throws MetadataException {
    databaseReadWriteLock.readLock().lock();
    try {
      return mTree.getTableIfExists(getQualifiedDatabasePartialPath(database), tableName);
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
  }

  public TSStatus addTableColumn(final AddTableColumnPlan plan) {
    databaseReadWriteLock.writeLock().lock();
    try {
      if (plan.isRollback()) {
        mTree.rollbackAddTableColumn(
            getQualifiedDatabasePartialPath(plan.getDatabase()),
            plan.getTableName(),
            plan.getColumnSchemaList());
      } else {
        mTree.addTableColumn(
            getQualifiedDatabasePartialPath(plan.getDatabase()),
            plan.getTableName(),
            plan.getColumnSchemaList());
      }
      return RpcUtils.SUCCESS_STATUS;
    } catch (final MetadataException e) {
      LOGGER.warn(e.getMessage(), e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } finally {
      databaseReadWriteLock.writeLock().unlock();
    }
  }

  public TSStatus renameTableColumn(final RenameTableColumnPlan plan) {
    final String databaseName = PathUtils.qualifyDatabaseName(plan.getDatabase());
    databaseReadWriteLock.writeLock().lock();
    try {
      mTree.renameTableColumn(
          new PartialPath(databaseName), plan.getTableName(), plan.getOldName(), plan.getNewName());
      return RpcUtils.SUCCESS_STATUS;
    } catch (final MetadataException e) {
      LOGGER.warn(e.getMessage(), e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } finally {
      databaseReadWriteLock.writeLock().unlock();
    }
  }

  public TSStatus setTableProperties(final SetTablePropertiesPlan plan) {
    databaseReadWriteLock.writeLock().lock();
    try {
      mTree.setTableProperties(
          getQualifiedDatabasePartialPath(plan.getDatabase()),
          plan.getTableName(),
          plan.getProperties());
      return RpcUtils.SUCCESS_STATUS;
    } catch (final MetadataException e) {
      LOGGER.warn(e.getMessage(), e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } finally {
      databaseReadWriteLock.writeLock().unlock();
    }
  }

  public TSStatus preDeleteColumn(final PreDeleteColumnPlan plan) {
    databaseReadWriteLock.writeLock().lock();
    try {
      final TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      if (mTree.preDeleteColumn(
          getQualifiedDatabasePartialPath(plan.getDatabase()),
          plan.getTableName(),
          plan.getColumnName())) {
        status.setMessage("");
      }
      return status;
    } catch (final MetadataException e) {
      LOGGER.warn(e.getMessage(), e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } catch (final SemanticException e) {
      return RpcUtils.getStatus(TSStatusCode.SEMANTIC_ERROR.getStatusCode(), e.getMessage());
    } finally {
      databaseReadWriteLock.writeLock().unlock();
    }
  }

  public TSStatus commitDeleteColumn(final CommitDeleteColumnPlan plan) {
    databaseReadWriteLock.writeLock().lock();
    try {
      mTree.commitDeleteColumn(
          getQualifiedDatabasePartialPath(plan.getDatabase()),
          plan.getTableName(),
          plan.getColumnName());
      return RpcUtils.SUCCESS_STATUS;
    } catch (final MetadataException e) {
      LOGGER.warn(e.getMessage(), e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } finally {
      databaseReadWriteLock.writeLock().unlock();
    }
  }

  private PartialPath getQualifiedDatabasePartialPath(final String database)
      throws IllegalPathException {
    return PartialPath.getDatabasePath(PathUtils.qualifyDatabaseName(database));
  }

  // endregion

  @TestOnly
  public void clear() {
    mTree.clear();
  }
}
