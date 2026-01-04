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
import org.apache.iotdb.commons.schema.table.TableNodeStatus;
import org.apache.iotdb.commons.schema.table.TableType;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.TsTableInternalRPCUtil;
import org.apache.iotdb.commons.schema.template.Template;
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
import org.apache.iotdb.confignode.consensus.request.write.table.AlterColumnDataTypePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitCreateTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreCreateTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreDeleteColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreDeleteTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.RenameTableColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.RenameTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.RollbackCreateTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTableColumnCommentPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTableCommentPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTablePropertiesPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.PreCreateTableViewPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.PreDeleteViewColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.PreDeleteViewPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.SetViewCommentPlan;
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
import org.apache.iotdb.confignode.consensus.response.table.DescTable4InformationSchemaResp;
import org.apache.iotdb.confignode.consensus.response.table.DescTableResp;
import org.apache.iotdb.confignode.consensus.response.table.FetchTableResp;
import org.apache.iotdb.confignode.consensus.response.table.ShowTable4InformationSchemaResp;
import org.apache.iotdb.confignode.consensus.response.table.ShowTableResp;
import org.apache.iotdb.confignode.consensus.response.template.AllTemplateSetInfoResp;
import org.apache.iotdb.confignode.consensus.response.template.TemplateInfoResp;
import org.apache.iotdb.confignode.consensus.response.template.TemplateSetInfoResp;
import org.apache.iotdb.confignode.exception.DatabaseNotExistsException;
import org.apache.iotdb.confignode.persistence.schema.ConfigMTree.TableSchemaDetails;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TTableColumnInfo;
import org.apache.iotdb.confignode.rpc.thrift.TTableInfo;
import org.apache.iotdb.db.exception.metadata.DatabaseNotSetException;
import org.apache.iotdb.db.exception.metadata.SchemaQuotaExceededException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.schemaengine.template.TemplateInternalRPCUtil;
import org.apache.iotdb.db.schemaengine.template.alter.TemplateExtendInfo;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.annotations.TableModel;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.TTL_INFINITE;
import static org.apache.iotdb.commons.path.PartialPath.getQualifiedDatabasePartialPath;
import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_MATCH_PATTERN;
import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_MATCH_SCOPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_TEMPLATE;
import static org.apache.iotdb.commons.schema.SchemaConstant.SYSTEM_DATABASE_PATTERN;
import static org.apache.iotdb.commons.schema.table.Audit.TABLE_MODEL_AUDIT_DATABASE;
import static org.apache.iotdb.commons.schema.table.Audit.TREE_MODEL_AUDIT_DATABASE;
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
  private final ConfigMTree treeModelMTree;
  private final ConfigMTree tableModelMTree;

  private static final String TREE_SNAPSHOT_FILENAME = "cluster_schema.bin";
  private static final String TABLE_SNAPSHOT_FILENAME = "table_cluster_schema.bin";
  private static final String ERROR_NAME = "Error Database name";

  private final TemplateTable templateTable;

  private final TemplatePreSetTable templatePreSetTable;

  public ClusterSchemaInfo() throws IOException {
    databaseReadWriteLock = new ReentrantReadWriteLock();

    try {
      treeModelMTree = new ConfigMTree(false);
      tableModelMTree = new ConfigMTree(true);
      templateTable = new TemplateTable();
      templatePreSetTable = new TemplatePreSetTable();
    } catch (final MetadataException e) {
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
      final PartialPath partialPathName = getQualifiedDatabasePartialPath(databaseSchema.getName());

      final ConfigMTree mTree = databaseSchema.isIsTableModel() ? tableModelMTree : treeModelMTree;
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
      final PartialPath partialPathName =
          PartialPath.getQualifiedDatabasePartialPath(alterSchema.getName());

      final ConfigMTree mTree =
          plan.getSchema().isIsTableModel() ? tableModelMTree : treeModelMTree;

      final TDatabaseSchema currentSchema =
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

      if (alterSchema.isSetTTL()) {
        currentSchema.setTTL(alterSchema.getTTL());
        LOGGER.info(
            "[SetTTL] The ttl of Database: {} is adjusted to: {}",
            currentSchema.getName(),
            currentSchema.getTTL());
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
      final boolean isTableModel = PathUtils.isTableModelDatabase(plan.getName());
      // Delete Database
      (isTableModel ? tableModelMTree : treeModelMTree)
          .deleteDatabase(getQualifiedDatabasePartialPath(plan.getName()));

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
    final int limit = COMMON_CONFIG.getDatabaseLimitThreshold();
    if (limit > 0) {
      databaseReadWriteLock.readLock().lock();
      try {
        final int count =
            treeModelMTree.getDatabaseNum(ALL_MATCH_PATTERN, ALL_MATCH_SCOPE, false, false)
                - treeModelMTree.getDatabaseNum(
                    SYSTEM_DATABASE_PATTERN, ALL_MATCH_SCOPE, false, false)
                + tableModelMTree.getDatabaseNum(ALL_MATCH_PATTERN, ALL_MATCH_SCOPE, false, false);
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
      result.setCount(
          (plan.isTableModel() ? tableModelMTree : treeModelMTree)
              .getDatabaseNum(patternPath, plan.getScope(), false, plan.isCanSeeAuditDB()));
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
      final ConfigMTree mTree = plan.isTableModel() ? tableModelMTree : treeModelMTree;
      final List<PartialPath> matchedPaths =
          mTree.getMatchedDatabases(patternPath, plan.getScope(), false);
      for (final PartialPath path : matchedPaths) {
        final TDatabaseSchema schema =
            mTree.getDatabaseNodeByDatabasePath(path).getAsMNode().getDatabaseSchema();
        schemaMap.put(schema.getName(), schema);
      }

      // can not see audit db, remove it
      if (!plan.isCanSeeAuditDB()) {
        if (plan.isTableModel()) {
          schemaMap.remove(TABLE_MODEL_AUDIT_DATABASE);
        } else {
          schemaMap.remove(TREE_MODEL_AUDIT_DATABASE);
        }
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
      final ConfigMTree mTree =
          PathUtils.isTableModelDatabase(plan.getDatabase()) ? tableModelMTree : treeModelMTree;
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
      final ConfigMTree mTree =
          PathUtils.isTableModelDatabase(plan.getDatabase()) ? tableModelMTree : treeModelMTree;
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
      final ConfigMTree mTree =
          PathUtils.isTableModelDatabase(plan.getDatabase()) ? tableModelMTree : treeModelMTree;
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
            (PathUtils.isTableModelDatabase(entry.getKey()) ? tableModelMTree : treeModelMTree)
                .getDatabaseNodeByDatabasePath(getQualifiedDatabasePartialPath(entry.getKey()))
                .getAsMNode()
                .getDatabaseSchema();
        databaseSchema.setMaxSchemaRegionGroupNum(entry.getValue().getLeft());
        databaseSchema.setMaxDataRegionGroupNum(entry.getValue().getRight());
      }
      result.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (final MetadataException e) {
      LOGGER.info(
          "Database inconsistency detected when adjusting max region group count, message: {}, will be corrected by the following adjusting plans",
          e.getMessage());
      result.setCode(e.getErrorCode()).setMessage(e.getMessage());
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
      final List<String> results = new ArrayList<>();
      if (!Boolean.TRUE.equals(isTableModel)) {
        treeModelMTree.getAllDatabasePaths(isTableModel).stream()
            .map(PartialPath::getFullPath)
            .forEach(results::add);
      }
      if (!Boolean.FALSE.equals(isTableModel)) {
        tableModelMTree.getAllDatabasePaths(isTableModel).stream()
            .map(path -> path.getNodes()[1])
            .forEach(results::add);
      }
      return results;
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
  public void isDatabaseNameValid(final String databaseName, final boolean isTableModel)
      throws MetadataException {
    databaseReadWriteLock.readLock().lock();
    try {
      (isTableModel ? tableModelMTree : treeModelMTree)
          .checkDatabaseAlreadySet(getQualifiedDatabasePartialPath(databaseName));
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
      return (PathUtils.isTableModelDatabase(database) ? tableModelMTree : treeModelMTree)
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
      final List<String> rawPathList, final Boolean isTableModel) {
    final Map<String, TDatabaseSchema> schemaMap = new HashMap<>();
    databaseReadWriteLock.readLock().lock();
    try {
      if (!Boolean.FALSE.equals(isTableModel)) {
        enrichSchemaMap(rawPathList, tableModelMTree, schemaMap);
      }
      if (!Boolean.TRUE.equals(isTableModel)) {
        enrichSchemaMap(rawPathList, treeModelMTree, schemaMap);
      }
    } catch (final MetadataException e) {
      LOGGER.warn(ERROR_NAME, e);
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
    return schemaMap;
  }

  private void enrichSchemaMap(
      final List<String> rawPathList,
      final ConfigMTree mTree,
      final Map<String, TDatabaseSchema> schemaMap)
      throws MetadataException {
    for (final String rawPath : rawPathList) {
      final PartialPath patternPath = getQualifiedDatabasePartialPath(rawPath);
      final List<PartialPath> matchedPaths =
          mTree.getMatchedDatabases(patternPath, ALL_MATCH_SCOPE, false);
      for (final PartialPath path : matchedPaths) {
        schemaMap.put(
            path.getFullPath(), mTree.getDatabaseNodeByPath(path).getAsMNode().getDatabaseSchema());
      }
    }
  }

  /**
   * Only leader use this interface. Get the matched DatabaseSchemas.
   *
   * @param prefix prefix path such as root.a
   * @return All DatabaseSchemas that matches to the prefix path such as root.a.db1, root.a.db2
   */
  public Map<String, TDatabaseSchema> getMatchedDatabaseSchemasByPrefix(final PartialPath prefix) {
    final Map<String, TDatabaseSchema> schemaMap = new HashMap<>();
    databaseReadWriteLock.readLock().lock();
    try {
      final List<PartialPath> matchedPaths =
          treeModelMTree.getMatchedDatabases(prefix, ALL_MATCH_SCOPE, true);
      for (final PartialPath path : matchedPaths) {
        schemaMap.put(
            path.getFullPath(),
            treeModelMTree.getDatabaseNodeByPath(path).getAsMNode().getDatabaseSchema());
      }
    } catch (final MetadataException e) {
      LOGGER.warn(ERROR_NAME, e);
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
    return schemaMap;
  }

  @TableModel
  public long getDatabaseMaxTTL(final String database) {
    databaseReadWriteLock.readLock().lock();
    try {
      return tableModelMTree
          .getAllTablesUnderSpecificDatabase(PartialPath.getQualifiedDatabasePartialPath(database))
          .stream()
          .map(pair -> pair.getLeft().getTableTTL())
          .reduce(Long::max)
          .orElse(Long.MAX_VALUE);
    } catch (final MetadataException e) {
      LOGGER.warn(
          ERROR_NAME + " when trying to get max ttl under one database, use Long.MAX_VALUE.", e);
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
    return Long.MAX_VALUE;
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
          (PathUtils.isTableModelDatabase(database) ? tableModelMTree : treeModelMTree)
              .getDatabaseNodeByDatabasePath(getQualifiedDatabasePartialPath(database))
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
          (PathUtils.isTableModelDatabase(database) ? tableModelMTree : treeModelMTree)
              .getDatabaseNodeByDatabasePath(getQualifiedDatabasePartialPath(database))
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
  public boolean processTakeSnapshot(final File snapshotDir) throws IOException {
    return processDatabaseSchemaSnapshot(
            snapshotDir, TREE_SNAPSHOT_FILENAME, treeModelMTree::serialize)
        && processDatabaseSchemaSnapshot(
            snapshotDir, TABLE_SNAPSHOT_FILENAME, tableModelMTree::serialize)
        && templateTable.processTakeSnapshot(snapshotDir)
        && templatePreSetTable.processTakeSnapshot(snapshotDir);
  }

  public boolean processDatabaseSchemaSnapshot(
      final File snapshotDir,
      final String snapshotFileName,
      final SerDeFunction<OutputStream> function)
      throws IOException {
    final File snapshotFile = new File(snapshotDir, snapshotFileName);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take snapshot, because snapshot file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }

    final File tmpFile = new File(snapshotFile.getAbsolutePath() + "-" + UUID.randomUUID());

    databaseReadWriteLock.readLock().lock();
    try {
      final FileOutputStream fileOutputStream = new FileOutputStream(tmpFile);
      final BufferedOutputStream outputStream = new BufferedOutputStream(fileOutputStream);
      try {
        function.apply(outputStream);
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
  public void processLoadSnapshot(final File snapshotDir) throws IOException {
    processMTreeLoadSnapshot(
        snapshotDir,
        TREE_SNAPSHOT_FILENAME,
        stream -> {
          treeModelMTree.clear();
          treeModelMTree.deserialize(stream);
        });
    processMTreeLoadSnapshot(
        snapshotDir,
        TABLE_SNAPSHOT_FILENAME,
        stream -> {
          tableModelMTree.clear();
          tableModelMTree.deserialize(stream);
        });
    templateTable.processLoadSnapshot(snapshotDir);
    templatePreSetTable.processLoadSnapshot(snapshotDir);
  }

  public void processMTreeLoadSnapshot(
      final File snapshotDir,
      final String snapshotFileName,
      final SerDeFunction<InputStream> function)
      throws IOException {
    final File snapshotFile = new File(snapshotDir, snapshotFileName);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot,snapshot file [{}] is not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }
    databaseReadWriteLock.writeLock().lock();
    try (final FileInputStream fileInputStream = new FileInputStream(snapshotFile);
        final BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream)) {
      // Load snapshot of MTree
      function.apply(bufferedInputStream);
    } finally {
      databaseReadWriteLock.writeLock().unlock();
    }
  }

  @FunctionalInterface
  public interface SerDeFunction<T> {
    void apply(final T stream) throws IOException;
  }

  public Pair<List<PartialPath>, Set<PartialPath>> getNodesListInGivenLevel(
      PartialPath partialPath, int level, PathPatternTree scope) {
    Pair<List<PartialPath>, Set<PartialPath>> matchedPathsInNextLevel =
        new Pair(new HashSet<>(), new HashSet<>());
    databaseReadWriteLock.readLock().lock();
    try {
      matchedPathsInNextLevel =
          treeModelMTree.getNodesListInGivenLevel(partialPath, level, true, scope);
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
      matchedPathsInNextLevel = treeModelMTree.getChildNodePathInNextLevel(partialPath, scope);
    } catch (MetadataException e) {
      LOGGER.error("Error get matched paths in next level.", e);
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
    return matchedPathsInNextLevel;
  }

  public TSStatus createSchemaTemplate(final CreateSchemaTemplatePlan createSchemaTemplatePlan) {
    try {
      final Template template = createSchemaTemplatePlan.getTemplate();
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
      treeModelMTree.checkTemplateOnPath(path);
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
  public synchronized TSStatus setSchemaTemplate(
      final SetSchemaTemplatePlan setSchemaTemplatePlan) {
    final PartialPath path;
    try {
      path = new PartialPath(setSchemaTemplatePlan.getPath());
    } catch (final IllegalPathException e) {
      LOGGER.error(e.getMessage());
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }

    try {
      final int templateId = templateTable.getTemplate(setSchemaTemplatePlan.getName()).getId();
      treeModelMTree.setTemplate(templateId, path);
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (final MetadataException e) {
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  public synchronized TSStatus preSetSchemaTemplate(
      final PreSetSchemaTemplatePlan preSetSchemaTemplatePlan) {
    final PartialPath path;
    try {
      path = new PartialPath(preSetSchemaTemplatePlan.getPath());
    } catch (final IllegalPathException e) {
      LOGGER.error(e.getMessage());
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }

    try {
      final int templateId = templateTable.getTemplate(preSetSchemaTemplatePlan.getName()).getId();
      if (preSetSchemaTemplatePlan.isRollback()) {
        rollbackPreSetSchemaTemplate(templateId, path);
      } else {
        preSetSchemaTemplate(templateId, path);
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (final MetadataException e) {
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  private void preSetSchemaTemplate(final int templateId, final PartialPath templateSetPath)
      throws MetadataException {
    templatePreSetTable.preSetTemplate(templateId, templateSetPath);
    treeModelMTree.setTemplate(templateId, templateSetPath);
  }

  private void rollbackPreSetSchemaTemplate(final int templateId, final PartialPath templateSetPath)
      throws MetadataException {
    try {
      treeModelMTree.unsetTemplate(templateId, templateSetPath);
    } catch (final MetadataException ignore) {
      // node not exists or not set template
    }
    templatePreSetTable.removeSetTemplate(templateId, templateSetPath);
  }

  public synchronized TSStatus commitSetSchemaTemplate(
      final CommitSetSchemaTemplatePlan commitSetSchemaTemplatePlan) {
    final PartialPath path;
    try {
      path = new PartialPath(commitSetSchemaTemplatePlan.getPath());
    } catch (final IllegalPathException e) {
      LOGGER.error(e.getMessage());
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }

    try {
      final int templateId =
          templateTable.getTemplate(commitSetSchemaTemplatePlan.getName()).getId();
      if (commitSetSchemaTemplatePlan.isRollback()) {
        rollbackCommitSetSchemaTemplate(templateId, path);
      } else {
        commitSetSchemaTemplate(templateId, path);
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (final MetadataException e) {
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  private void commitSetSchemaTemplate(final int templateId, final PartialPath templateSetPath) {
    templatePreSetTable.removeSetTemplate(templateId, templateSetPath);
  }

  private void rollbackCommitSetSchemaTemplate(
      final int templateId, final PartialPath templateSetPath) throws MetadataException {
    treeModelMTree.unsetTemplate(templateId, templateSetPath);
  }

  public PathInfoResp getPathsSetTemplate(final GetPathsSetTemplatePlan getPathsSetTemplatePlan) {
    final PathInfoResp pathInfoResp = new PathInfoResp();
    TSStatus status;
    try {
      final String templateName = getPathsSetTemplatePlan.getName();
      final PathPatternTree scope = getPathsSetTemplatePlan.getScope();
      final int templateId;
      if (templateName.equals(ONE_LEVEL_PATH_WILDCARD)) {
        templateId = ALL_TEMPLATE;
      } else {
        templateId = templateTable.getTemplate(templateName).getId();
      }
      pathInfoResp.setPathList(treeModelMTree.getPathsSetOnTemplate(templateId, scope, false));
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
        List<String> pathList = treeModelMTree.getPathsSetOnTemplate(id, ALL_MATCH_SCOPE, true);
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
        final Map<Integer, Set<PartialPath>> templateSetInfo =
            treeModelMTree.getTemplateSetInfo(pattern);
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
      treeModelMTree.preUnsetTemplate(plan.getTemplateId(), plan.getPath());
      return StatusUtils.OK;
    } catch (MetadataException e) {
      LOGGER.error(e.getMessage(), e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  public TSStatus rollbackUnsetSchemaTemplate(RollbackPreUnsetSchemaTemplatePlan plan) {
    try {
      treeModelMTree.rollbackUnsetTemplate(plan.getTemplateId(), plan.getPath());
      return StatusUtils.OK;
    } catch (MetadataException e) {
      LOGGER.error(e.getMessage(), e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  public TSStatus unsetSchemaTemplate(UnsetSchemaTemplatePlan plan) {
    try {
      treeModelMTree.unsetTemplate(plan.getTemplateId(), plan.getPath());
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

  // region table management

  public TSStatus preCreateTable(final PreCreateTablePlan plan) {
    return executeWithLock(
        () ->
            tableModelMTree.preCreateTable(
                getQualifiedDatabasePartialPath(plan.getDatabase()), plan.getTable()));
  }

  public TSStatus preCreateTableView(final PreCreateTableViewPlan plan) {
    return executeWithLock(
        () ->
            tableModelMTree.preCreateTableView(
                getQualifiedDatabasePartialPath(plan.getDatabase()),
                plan.getTable(),
                plan.getStatus()));
  }

  public TSStatus rollbackCreateTable(final RollbackCreateTablePlan plan) {
    return executeWithLock(
        () ->
            tableModelMTree.rollbackCreateTable(
                getQualifiedDatabasePartialPath(plan.getDatabase()), plan.getTableName()));
  }

  public TSStatus commitCreateTable(final CommitCreateTablePlan plan) {
    return executeWithLock(
        () ->
            tableModelMTree.commitCreateTable(
                getQualifiedDatabasePartialPath(plan.getDatabase()), plan.getTableName()));
  }

  public TSStatus preDeleteTable(final PreDeleteTablePlan plan) {
    return executeWithLock(
        () ->
            tableModelMTree.preDeleteTable(
                getQualifiedDatabasePartialPath(plan.getDatabase()),
                plan.getTableName(),
                plan instanceof PreDeleteViewPlan));
  }

  public TSStatus dropTable(final CommitDeleteTablePlan plan) {
    return executeWithLock(
        () ->
            tableModelMTree.dropTable(
                getQualifiedDatabasePartialPath(plan.getDatabase()), plan.getTableName()));
  }

  public TSStatus renameTable(final RenameTablePlan plan) {
    return executeWithLock(
        () ->
            tableModelMTree.renameTable(
                getQualifiedDatabasePartialPath(plan.getDatabase()),
                plan.getTableName(),
                plan.getNewName()));
  }

  public TSStatus setTableComment(final SetTableCommentPlan plan) {
    return executeWithLock(
        () ->
            tableModelMTree.setTableComment(
                getQualifiedDatabasePartialPath(plan.getDatabase()),
                plan.getTableName(),
                plan.getComment(),
                plan instanceof SetViewCommentPlan));
  }

  public TSStatus setTableColumnComment(final SetTableColumnCommentPlan plan) {
    return executeWithLock(
        () ->
            tableModelMTree.setTableColumnComment(
                getQualifiedDatabasePartialPath(plan.getDatabase()),
                plan.getTableName(),
                plan.getColumnName(),
                plan.getComment()));
  }

  private TSStatus executeWithLock(final ThrowingRunnable runnable) {
    databaseReadWriteLock.writeLock().lock();
    try {
      runnable.run();
      return RpcUtils.SUCCESS_STATUS;
    } catch (final MetadataException e) {
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } catch (final SemanticException e) {
      return RpcUtils.getStatus(TSStatusCode.SEMANTIC_ERROR.getStatusCode(), e.getMessage());
    } catch (final Throwable e) {
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
    } finally {
      databaseReadWriteLock.writeLock().unlock();
    }
  }

  public interface ThrowingRunnable {
    void run() throws Throwable;
  }

  public ShowTableResp showTables(final ShowTablePlan plan) {
    databaseReadWriteLock.readLock().lock();
    try {
      return new ShowTableResp(
          StatusUtils.OK,
          plan.isDetails()
              ? tableModelMTree
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
                        pair.getLeft()
                            .getPropValue(TsTable.COMMENT_KEY)
                            .ifPresent(info::setComment);
                        info.setType(
                            TreeViewSchema.isTreeViewTable(pair.getLeft())
                                ? TableType.VIEW_FROM_TREE.ordinal()
                                : TableType.BASE_TABLE.ordinal());
                        return info;
                      })
                  .collect(Collectors.toList())
              : tableModelMTree
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

  public ShowTable4InformationSchemaResp showTables4InformationSchema() {
    databaseReadWriteLock.readLock().lock();
    try {
      return new ShowTable4InformationSchemaResp(
          StatusUtils.OK,
          tableModelMTree.getAllTables().entrySet().stream()
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey,
                      entry ->
                          entry.getValue().stream()
                              .map(
                                  pair -> {
                                    final TTableInfo info =
                                        new TTableInfo(
                                            pair.getLeft().getTableName(),
                                            pair.getLeft()
                                                .getPropValue(TTL_PROPERTY)
                                                .orElse(TTL_INFINITE));
                                    info.setState(pair.getRight().ordinal());
                                    pair.getLeft()
                                        .getPropValue(TsTable.COMMENT_KEY)
                                        .ifPresent(info::setComment);
                                    info.setType(
                                        TreeViewSchema.isTreeViewTable(pair.getLeft())
                                            ? TableType.VIEW_FROM_TREE.ordinal()
                                            : TableType.BASE_TABLE.ordinal());
                                    return info;
                                  })
                              .collect(Collectors.toList()))));
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
        try {
          result.put(
              database2Tables.getKey(),
              tableModelMTree.getSpecificTablesUnderSpecificDatabase(
                  getQualifiedDatabasePartialPath(database2Tables.getKey()),
                  database2Tables.getValue()));
        } catch (final DatabaseNotSetException ignore) {
          // continue
        }
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
        final TableSchemaDetails details =
            tableModelMTree.getTableSchemaDetails(databasePath, plan.getTableName());
        return new DescTableResp(
            StatusUtils.OK, details.table, details.preDeletedColumns, details.preAlteredColumns);
      }
      return new DescTableResp(
          StatusUtils.OK,
          tableModelMTree.getUsingTableSchema(databasePath, plan.getTableName()),
          null,
          null);
    } catch (final MetadataException e) {
      return new DescTableResp(
          RpcUtils.getStatus(e.getErrorCode(), e.getMessage()), null, null, null);
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
  }

  public DescTable4InformationSchemaResp descTable4InformationSchema() {
    databaseReadWriteLock.readLock().lock();
    try {
      return new DescTable4InformationSchemaResp(
          StatusUtils.OK,
          tableModelMTree.getAllDatabasePaths(true).stream()
              .collect(
                  Collectors.toMap(
                      databasePath -> PathUtils.unQualifyDatabaseName(databasePath.getFullPath()),
                      databasePath -> {
                        try {
                          return tableModelMTree
                              .getAllTablesUnderSpecificDatabase(databasePath)
                              .stream()
                              .map(
                                  pair -> {
                                    try {
                                      return tableModelMTree.getTableSchemaDetails(
                                          databasePath, pair.getLeft().getTableName());
                                    } catch (final MetadataException ignore) {
                                      // Table path must exist because the "getTableSchemaDetails()"
                                      // is called in databaseReadWriteLock.readLock().
                                    }
                                    return new TableSchemaDetails();
                                  })
                              .collect(
                                  Collectors.toMap(
                                      tableSchemaDetails -> tableSchemaDetails.table.getTableName(),
                                      tableSchemaDetails ->
                                          new TTableColumnInfo()
                                              .setTableInfo(
                                                  TsTableInternalRPCUtil.serializeSingleTsTable(
                                                      tableSchemaDetails.table))
                                              .setPreDeletedColumns(
                                                  tableSchemaDetails.preDeletedColumns)
                                              .setPreAlteredColumns(
                                                  tableSchemaDetails
                                                      .preAlteredColumns
                                                      .entrySet()
                                                      .stream()
                                                      .collect(
                                                          Collectors.toMap(
                                                              Entry::getKey,
                                                              e -> e.getValue().serialize())))));
                        } catch (final MetadataException ignore) {
                          // Database path must exist because the "getAllDatabasePaths()" is called
                          // in databaseReadWriteLock.readLock().
                        }
                        return Collections.emptyMap();
                      })));
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
  }

  public Map<String, List<TsTable>> getAllUsingTables() {
    databaseReadWriteLock.readLock().lock();
    try {
      return tableModelMTree.getAllUsingTables();
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
  }

  public Map<String, List<TsTable>> getAllPreCreateTables() {
    databaseReadWriteLock.readLock().lock();
    try {
      return tableModelMTree.getAllPreCreateTables();
    } catch (final MetadataException e) {
      LOGGER.warn(e.getMessage(), e);
      throw new RuntimeException(e);
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
  }

  public Optional<Pair<TsTable, TableNodeStatus>> getTsTableIfExists(
      final String database, final String tableName) throws MetadataException {
    databaseReadWriteLock.readLock().lock();
    try {
      return tableModelMTree.getTableAndStatusIfExists(
          getQualifiedDatabasePartialPath(database), tableName);
    } finally {
      databaseReadWriteLock.readLock().unlock();
    }
  }

  public TSStatus addTableColumn(final AddTableColumnPlan plan) {
    return executeWithLock(
        () -> {
          if (plan.isRollback()) {
            tableModelMTree.rollbackAddTableColumn(
                getQualifiedDatabasePartialPath(plan.getDatabase()),
                plan.getTableName(),
                plan.getColumnSchemaList());
          } else {
            tableModelMTree.addTableColumn(
                getQualifiedDatabasePartialPath(plan.getDatabase()),
                plan.getTableName(),
                plan.getColumnSchemaList());
          }
        });
  }

  public TSStatus renameTableColumn(final RenameTableColumnPlan plan) {
    return executeWithLock(
        () ->
            tableModelMTree.renameTableColumn(
                getQualifiedDatabasePartialPath(plan.getDatabase()),
                plan.getTableName(),
                plan.getOldName(),
                plan.getNewName()));
  }

  public TSStatus setTableProperties(final SetTablePropertiesPlan plan) {
    return executeWithLock(
        () ->
            tableModelMTree.setTableProperties(
                getQualifiedDatabasePartialPath(plan.getDatabase()),
                plan.getTableName(),
                plan.getProperties()));
  }

  public TSStatus preDeleteColumn(final PreDeleteColumnPlan plan) {
    databaseReadWriteLock.writeLock().lock();
    try {
      final TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      if (tableModelMTree.preDeleteColumn(
          getQualifiedDatabasePartialPath(plan.getDatabase()),
          plan.getTableName(),
          plan.getColumnName(),
          plan instanceof PreDeleteViewColumnPlan)) {
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
    return executeWithLock(
        () ->
            tableModelMTree.commitDeleteColumn(
                getQualifiedDatabasePartialPath(plan.getDatabase()),
                plan.getTableName(),
                plan.getColumnName()));
  }

  public TSStatus preAlterColumnDataType(
      String databaseName, String tableName, String columnName, TSDataType dataType) {
    databaseReadWriteLock.writeLock().lock();
    try {
      final TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      tableModelMTree.preAlterColumnDataType(
          getQualifiedDatabasePartialPath(databaseName), tableName, columnName, dataType);
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

  public TSStatus commitAlterColumnDataType(AlterColumnDataTypePlan plan) {
    databaseReadWriteLock.writeLock().lock();
    try {
      tableModelMTree.commitAlterColumnDataType(
          getQualifiedDatabasePartialPath(plan.getDatabase()),
          plan.getTableName(),
          plan.getColumnName(),
          plan.getNewType());
      return RpcUtils.SUCCESS_STATUS;
    } catch (final MetadataException e) {
      LOGGER.warn(e.getMessage(), e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } finally {
      databaseReadWriteLock.writeLock().unlock();
    }
  }

  // endregion

  @TestOnly
  public void clear() {
    treeModelMTree.clear();
    tableModelMTree.clear();
  }
}
