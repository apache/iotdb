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
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.exception.table.ColumnNotExistsException;
import org.apache.iotdb.commons.exception.table.DropInvalidCategoryColumnException;
import org.apache.iotdb.commons.exception.table.TableNotExistsException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.table.TableNodeStatus;
import org.apache.iotdb.commons.schema.table.TableType;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.TsTableInternalRPCUtil;
import org.apache.iotdb.commons.schema.table.ViewColumnSchemaUtils;
import org.apache.iotdb.commons.schema.table.WritableView;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
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
import org.apache.iotdb.confignode.consensus.request.write.table.AbstractTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.AddTableColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.AlterColumnDataTypePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreAlterColumnDataTypePlan;
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
import org.apache.iotdb.confignode.consensus.response.table.PreDeleteColumnStatus;
import org.apache.iotdb.confignode.consensus.response.table.ShowTable4InformationSchemaResp;
import org.apache.iotdb.confignode.consensus.response.table.ShowTableResp;
import org.apache.iotdb.confignode.consensus.response.template.AllTemplateSetInfoResp;
import org.apache.iotdb.confignode.consensus.response.template.TemplateInfoResp;
import org.apache.iotdb.confignode.consensus.response.template.TemplateSetInfoResp;
import org.apache.iotdb.confignode.exception.DatabaseNotExistsException;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.persistence.schema.ConfigMTree.TableSchemaDetails;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TTableColumnInfo;
import org.apache.iotdb.confignode.rpc.thrift.TTableInfo;
import org.apache.iotdb.db.exception.metadata.DatabaseNotSetException;
import org.apache.iotdb.db.exception.metadata.SchemaQuotaExceededException;
import org.apache.iotdb.db.schemaengine.template.TemplateInternalRPCUtil;
import org.apache.iotdb.db.schemaengine.template.alter.TemplateExtendInfo;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.AddWritableViewColumnPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.CommitCreateWritableViewPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.CommitDeleteWritableViewColumnPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.PreDeleteWritableViewColumnPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.PreDeleteWritableViewPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.RollbackCreateWritableViewPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.SetWritableViewCommentPlan;
import org.apache.tsfile.annotations.TableModel;
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
import java.util.Objects;
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
      LOGGER.error(ConfigNodeMessages.CAN_T_CONSTRUCT_CLUSTERSCHEMAINFO, e);
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
            ConfigNodeMessages.ADJUSTREGIONGROUPNUM_THE_MINIMUM_NUMBER_OF_SCHEMAREGIONGROUPS_FOR,
            currentSchema.getName(),
            currentSchema.getMinSchemaRegionGroupNum());
        LOGGER.info(
            ConfigNodeMessages.ADJUSTREGIONGROUPNUM_THE_MAXIMUM_NUMBER_OF_SCHEMAREGIONGROUPS_FOR,
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
            ConfigNodeMessages.ADJUSTREGIONGROUPNUM_THE_MINIMUM_NUMBER_OF_DATAREGIONGROUPS_FOR,
            currentSchema.getName(),
            currentSchema.getMinDataRegionGroupNum());
        LOGGER.info(
            ConfigNodeMessages.ADJUSTREGIONGROUPNUM_THE_MAXIMUM_NUMBER_OF_DATAREGIONGROUPS_FOR,
            currentSchema.getName(),
            currentSchema.getMaxDataRegionGroupNum());
      }

      if (alterSchema.isSetTTL()) {
        currentSchema.setTTL(alterSchema.getTTL());
        LOGGER.info(
            ConfigNodeMessages.SETTTL_THE_TTL_OF_DATABASE_IS_ADJUSTED_TO,
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
      LOGGER.warn(ConfigNodeMessages.DATABASE_NOT_EXIST, e);
      result
          .setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode())
          .setMessage(ConfigNodeMessages.DATABASE_NOT_EXIST + e.getMessage());
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
          ConfigNodeMessages
              .DATABASE_INCONSISTENCY_DETECTED_WHEN_ADJUSTING_MAX_REGION_GROUP_COUNT_MESSAGE,
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
          ConfigNodeMessages.FAILED_TO_TAKE_SNAPSHOT_BECAUSE_SNAPSHOT_FILE_IS_ALREADY_EXIST,
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
              ConfigNodeMessages.CAN_T_DELETE_TEMPORARY_SNAPSHOT_FILE_RETRYING,
              tmpFile.getAbsolutePath());
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
          ConfigNodeMessages.FAILED_TO_LOAD_SNAPSHOT_SNAPSHOT_FILE_IS_NOT_EXIST_2,
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
      LOGGER.error(ConfigNodeMessages.ERROR_GET_MATCHED_PATHS_IN_GIVEN_LEVEL, e);
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
      LOGGER.error(ConfigNodeMessages.ERROR_GET_MATCHED_PATHS_IN_NEXT_LEVEL, e);
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
        LOGGER.error(ConfigNodeMessages.ERROR_OCCURRED_WHEN_GET_PATHS_SET_ON_TEMPLATE, id, e);
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

  public TSStatus rollbackCreateWritableView(final RollbackCreateWritableViewPlan plan) {
    return executeWithLock(
        () -> {
          if (plan.isRestoreView()) {
            tableModelMTree.preCreateTableView(
                getQualifiedDatabasePartialPath(plan.getDatabase()),
                plan.getTable(),
                plan.getStatus());
          } else {
            tableModelMTree.rollbackCreateTable(
                getQualifiedDatabasePartialPath(plan.getDatabase()),
                plan.getTable().getTableName());
          }
          if (Objects.nonNull(plan.getOriginalDatabase())
              && Objects.nonNull(plan.getOriginalTable())) {
            try {
              tableModelMTree.replaceOriginalTable(
                  getQualifiedDatabasePartialPath(plan.getOriginalDatabase()),
                  plan.getOriginalTable());
            } catch (final MetadataException e) {
              if (!isMissingSourceTable(e)) {
                throw e;
              }
              LOGGER.warn(
                  ProcedureMessages
                      .SKIP_ROLLBACK_SCHEMA_CASCADE_FOR_WRITABLE_VIEW_MISSING_SOURCE_DETAIL,
                  plan.getDatabase(),
                  plan.getTable().getTableName(),
                  plan.getOriginalDatabase(),
                  plan.getOriginalTable().getTableName(),
                  e.getMessage());
            }
          }
        });
  }

  public TSStatus commitCreateTable(final AbstractTablePlan plan) {
    return executeWithLock(
        () -> {
          tableModelMTree.commitCreateTable(
              getQualifiedDatabasePartialPath(plan.getDatabase()), plan.getTableName());
          executeOriginalIfPresent(
              plan,
              () ->
                  tableModelMTree.replaceOriginalTable(
                      getQualifiedDatabasePartialPath(plan.getOriginalDatabase()),
                      ((CommitCreateWritableViewPlan) plan).getOriginalTable()));
        });
  }

  public TSStatus preDeleteTable(final PreDeleteTablePlan plan) {
    return executeWithLock(
        () -> {
          final TableType tableType = getTableType(plan);
          tableModelMTree.preDeleteTable(
              getQualifiedDatabasePartialPath(plan.getDatabase()), plan.getTableName(), tableType);
          executeOriginalIfPresent(
              plan,
              () ->
                  tableModelMTree.preDeleteTable(
                      getQualifiedDatabasePartialPath(plan.getOriginalDatabase()),
                      plan.getOriginalTableName(),
                      TableType.BASE_TABLE));
        });
  }

  public TSStatus dropTable(final CommitDeleteTablePlan plan) {
    return executeWithLock(
        () -> {
          tableModelMTree.dropTable(
              getQualifiedDatabasePartialPath(plan.getDatabase()), plan.getTableName());
          executeOriginalIfPresent(
              plan,
              () ->
                  tableModelMTree.dropTable(
                      getQualifiedDatabasePartialPath(plan.getOriginalDatabase()),
                      plan.getOriginalTableName()));
        });
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
        () -> {
          final TableType tableType = getTableType(plan);
          tableModelMTree.setTableComment(
              getQualifiedDatabasePartialPath(plan.getDatabase()),
              plan.getTableName(),
              plan.getComment(),
              tableType);
          executeOriginalIfPresent(
              plan,
              () ->
                  tableModelMTree.setTableComment(
                      getQualifiedDatabasePartialPath(plan.getOriginalDatabase()),
                      plan.getOriginalTableName(),
                      plan.getComment(),
                      TableType.BASE_TABLE));
        });
  }

  public TSStatus setTableColumnComment(final SetTableColumnCommentPlan plan) {
    return executeWithLock(
        () -> {
          tableModelMTree.setTableColumnComment(
              getQualifiedDatabasePartialPath(plan.getDatabase()),
              plan.getTableName(),
              plan.getColumnName(),
              plan.getComment());
          executeOriginalIfPresent(
              plan,
              () -> {
                if (Objects.nonNull(plan.getOriginalColumnName())) {
                  tableModelMTree.setTableColumnComment(
                      getQualifiedDatabasePartialPath(plan.getOriginalDatabase()),
                      plan.getOriginalTableName(),
                      plan.getOriginalColumnName(),
                      plan.getComment());
                }
              });
        });
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
                        final TTableInfo info = buildTTableInfo(pair.getLeft(), true);
                        info.setState(pair.getRight().ordinal());
                        return info;
                      })
                  .collect(Collectors.toList())
              : tableModelMTree
                  .getAllUsingTablesUnderSpecificDatabase(
                      getQualifiedDatabasePartialPath(plan.getDatabase()))
                  .stream()
                  .map(tsTable -> buildTTableInfo(tsTable, false))
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
                                    final TTableInfo info = buildTTableInfo(pair.getLeft(), true);
                                    info.setState(pair.getRight().ordinal());
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
            rollbackWritableViewColumnMappings(plan);
          } else {
            tableModelMTree.addTableColumn(
                getQualifiedDatabasePartialPath(plan.getDatabase()),
                plan.getTableName(),
                plan.getColumnSchemaList());
            addWritableViewColumnMappings(plan);
          }
          executeOriginalIfPresent(
              plan,
              () -> {
                if (plan.isRollback()) {
                  tableModelMTree.rollbackAddTableColumn(
                      getQualifiedDatabasePartialPath(plan.getOriginalDatabase()),
                      plan.getOriginalTableName(),
                      ((AddWritableViewColumnPlan) plan).getOriginalColumnSchemaList());
                } else {
                  tableModelMTree.addTableColumn(
                      getQualifiedDatabasePartialPath(plan.getOriginalDatabase()),
                      plan.getOriginalTableName(),
                      ((AddWritableViewColumnPlan) plan).getOriginalColumnSchemaList());
                }
              });
        });
  }

  public TSStatus renameTableColumn(final RenameTableColumnPlan plan) {
    return executeWithLock(
        () -> {
          tableModelMTree.renameTableColumn(
              getQualifiedDatabasePartialPath(plan.getDatabase()),
              plan.getTableName(),
              plan.getOldName(),
              plan.getNewName());
        });
  }

  public TSStatus setTableProperties(final SetTablePropertiesPlan plan) {
    return executeWithLock(
        () -> {
          tableModelMTree.setTableProperties(
              getQualifiedDatabasePartialPath(plan.getDatabase()),
              plan.getTableName(),
              plan.getProperties());
          executeOriginalIfPresent(
              plan,
              () -> {
                final Map<String, String> copiedProperties = new HashMap<>(plan.getProperties());
                copiedProperties.remove(WritableView.SCHEMA_CASCADE);
                tableModelMTree.setTableProperties(
                    getQualifiedDatabasePartialPath(plan.getOriginalDatabase()),
                    plan.getOriginalTableName(),
                    copiedProperties);
              });
        });
  }

  private void executeOriginalIfPresent(
      final AbstractTablePlan plan, final ThrowingRunnable runnable) throws Throwable {
    if (Objects.isNull(plan.getOriginalDatabase()) || Objects.isNull(plan.getOriginalTableName())) {
      return;
    }
    try {
      runnable.run();
    } catch (final Throwable e) {
      if (!isIdempotentSourceCascadeFailure(e)) {
        throw e;
      }
      final int errorCode = getSourceCascadeMetadataErrorCode(e);
      if (isMissingSourceTable(errorCode)) {
        LOGGER.warn(
            ProcedureMessages.SKIP_SCHEMA_CASCADE_FOR_WRITABLE_VIEW_MISSING_SOURCE_DETAIL,
            plan.getDatabase(),
            plan.getTableName(),
            plan.getOriginalDatabase(),
            plan.getOriginalTableName(),
            e.getMessage());
      }
      // Writable view source-side cascade is best-effort. The view-side DDL is already applied,
      // while missing source metadata is treated as an idempotent no-op.
    }
  }

  private static boolean isIdempotentSourceCascadeFailure(final Throwable throwable) {
    final int errorCode = getSourceCascadeMetadataErrorCode(throwable);
    return errorCode == TSStatusCode.TABLE_NOT_EXISTS.getStatusCode()
        || errorCode == TSStatusCode.COLUMN_NOT_EXISTS.getStatusCode()
        || errorCode == TSStatusCode.DATABASE_NOT_EXIST.getStatusCode();
  }

  private static int getSourceCascadeMetadataErrorCode(final Throwable throwable) {
    for (Throwable current = throwable; Objects.nonNull(current); current = current.getCause()) {
      if (current instanceof MetadataException) {
        return ((MetadataException) current).getErrorCode();
      }
    }
    return -1;
  }

  private static boolean isMissingSourceTable(final MetadataException e) {
    return isMissingSourceTable(e.getErrorCode());
  }

  private static boolean isMissingSourceTable(final int errorCode) {
    return errorCode == TSStatusCode.TABLE_NOT_EXISTS.getStatusCode()
        || errorCode == TSStatusCode.DATABASE_NOT_EXIST.getStatusCode();
  }

  private static TTableInfo buildTTableInfo(
      final TsTable table, final boolean includeOriginalTableName) {
    final TTableInfo info =
        new TTableInfo(table.getTableName(), table.getPropValue(TTL_PROPERTY).orElse(TTL_INFINITE));
    info.setType(table.getType());
    table.getPropValue(TsTable.COMMENT_KEY).ifPresent(info::setComment);
    if (includeOriginalTableName && table instanceof WritableView) {
      info.setOriginalTableName(((WritableView) table).getSourceTableName());
    }
    return info;
  }

  private static TableType getTableType(final Object plan) {
    if (plan instanceof PreDeleteWritableViewPlan
        || plan instanceof SetWritableViewCommentPlan
        || plan instanceof PreDeleteWritableViewColumnPlan) {
      return TableType.WRITABLE_VIEW;
    }
    if (plan instanceof PreDeleteViewPlan
        || plan instanceof SetViewCommentPlan
        || plan instanceof PreDeleteViewColumnPlan) {
      return TableType.VIEW_FROM_TREE;
    }
    return TableType.BASE_TABLE;
  }

  private void cascadePreDeleteColumn(
      final PreDeleteColumnPlan plan,
      final PartialPath database,
      final TsTableColumnCategory viewColumnCategory,
      final TSStatus status)
      throws Throwable {
    if (!(plan instanceof PreDeleteWritableViewColumnPlan)) {
      logDefensiveAssertion(
          ProcedureMessages.UNEXPECTED_PLAN_CARRIES_ORIGINAL_TABLE_METADATA_FOR_DROP_COLUMN,
          plan.getClass().getName(),
          plan.getDatabase(),
          plan.getTableName(),
          plan.getColumnName());
      return;
    }

    final PartialPath sourceDatabase = getQualifiedDatabasePartialPath(plan.getOriginalDatabase());
    final String originalTableName = plan.getOriginalTableName();
    final String originalColumnName =
        ((PreDeleteWritableViewColumnPlan) plan).getOriginalColumnName();

    boolean sourceColumnPreDeleted = false;
    try {
      final TsTableColumnCategory sourceColumnCategory =
          getColumnCategory(sourceDatabase, originalTableName, originalColumnName);
      if (sourceColumnCategory != viewColumnCategory) {
        throw new MetadataException(
            String.format(
                ProcedureMessages.FAILED_TO_DROP_WRITABLE_VIEW_SOURCE_COLUMN_CATEGORY_MISMATCH,
                sourceDatabase,
                originalTableName,
                originalColumnName,
                sourceColumnCategory,
                database,
                plan.getTableName(),
                plan.getColumnName(),
                viewColumnCategory));
      }
      tableModelMTree.preDeleteColumn(
          sourceDatabase, originalTableName, originalColumnName, TableType.BASE_TABLE);
      sourceColumnPreDeleted = true;
    } catch (final Exception e) {
      if (isIdempotentSourceCascadeFailure(e)) {
        return;
      }
      if (e instanceof DropInvalidCategoryColumnException
          && ((DropInvalidCategoryColumnException) e).getInvalidCategory()
              == TsTableColumnCategory.TAG
          && viewColumnCategory == TsTableColumnCategory.TAG) {
        status.setMessage(PreDeleteColumnStatus.ORIGINAL_TAG_COLUMN_CASCADE_SKIPPED.name());
        return;
      }
      rollbackPreDeletedColumns(
          database,
          plan.getTableName(),
          plan.getColumnName(),
          sourceDatabase,
          originalTableName,
          originalColumnName,
          sourceColumnPreDeleted);
      throw e;
    }
  }

  private TsTableColumnCategory getColumnCategory(
      final PartialPath database, final String tableName, final String columnName)
      throws MetadataException {
    final Optional<Pair<TsTable, TableNodeStatus>> tableOptional =
        tableModelMTree.getTableAndStatusIfExists(database, tableName);
    if (!tableOptional.isPresent()) {
      throw new TableNotExistsException(
          PathUtils.unQualifyDatabaseName(database.getFullPath()), tableName);
    }
    final TsTableColumnSchema columnSchema = tableOptional.get().left.getColumnSchema(columnName);
    if (Objects.isNull(columnSchema)) {
      throw new ColumnNotExistsException(
          PathUtils.unQualifyDatabaseName(database.getFullPath()), tableName, columnName);
    }
    return columnSchema.getColumnCategory();
  }

  private void rollbackPreDeletedColumns(
      final PartialPath database,
      final String tableName,
      final String columnName,
      final PartialPath sourceDatabase,
      final String originalTableName,
      final String originalColumnName,
      final boolean sourceColumnPreDeleted)
      throws MetadataException {
    tableModelMTree.rollbackPreDeleteColumn(database, tableName, columnName);
    if (sourceColumnPreDeleted) {
      tableModelMTree.rollbackPreDeleteColumn(
          sourceDatabase, originalTableName, originalColumnName);
    }
  }

  private void logDefensiveAssertion(final String message, final Object... args) {
    LOGGER.warn(
        String.format(message, args),
        new Exception(ProcedureMessages.DEFENSIVE_ASSERTION_STACK_TRACE));
  }

  public TSStatus preDeleteColumn(final PreDeleteColumnPlan plan) {
    databaseReadWriteLock.writeLock().lock();
    try {
      final TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      final PartialPath database = getQualifiedDatabasePartialPath(plan.getDatabase());
      final TsTableColumnCategory columnCategory =
          tableModelMTree.preDeleteColumn(
              database, plan.getTableName(), plan.getColumnName(), getTableType(plan));
      if (columnCategory == TsTableColumnCategory.ATTRIBUTE) {
        status.setMessage(PreDeleteColumnStatus.ATTRIBUTE.name());
      }
      executeOriginalIfPresent(
          plan, () -> cascadePreDeleteColumn(plan, database, columnCategory, status));
      return status;
    } catch (final MetadataException e) {
      LOGGER.warn(e.getMessage(), e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } catch (final SemanticException e) {
      return RpcUtils.getStatus(TSStatusCode.SEMANTIC_ERROR.getStatusCode(), e.getMessage());
    } catch (final Throwable e) {
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
    } finally {
      databaseReadWriteLock.writeLock().unlock();
    }
  }

  public TSStatus commitDeleteColumn(final CommitDeleteColumnPlan plan) {
    return executeWithLock(
        () -> {
          tableModelMTree.commitDeleteColumn(
              getQualifiedDatabasePartialPath(plan.getDatabase()),
              plan.getTableName(),
              plan.getColumnName());
          rollbackWritableViewColumnMappings(plan);
          executeOriginalIfPresent(
              plan,
              () ->
                  tableModelMTree.commitDeleteColumn(
                      getQualifiedDatabasePartialPath(plan.getOriginalDatabase()),
                      plan.getOriginalTableName(),
                      plan.getOriginalColumnName()));
        });
  }

  private void addWritableViewColumnMappings(final AddTableColumnPlan plan)
      throws MetadataException {
    if (!(plan instanceof AddWritableViewColumnPlan)) {
      return;
    }

    final WritableView writableView = getWritableView(plan.getDatabase(), plan.getTableName());
    if (Objects.isNull(writableView)) {
      return;
    }

    for (final TsTableColumnSchema viewColumnSchema : plan.getColumnSchemaList()) {
      // View-only adds still carry the source column through the shared column-mapping metadata.
      final String sourceColumnName =
          Optional.ofNullable(ViewColumnSchemaUtils.getSourceName(viewColumnSchema))
              .orElse(viewColumnSchema.getColumnName());
      writableView.putViewColumnSourceColumnMapping(
          viewColumnSchema.getColumnName(), sourceColumnName);
    }
  }

  private void rollbackWritableViewColumnMappings(final AbstractTablePlan plan)
      throws MetadataException {
    final boolean expectsWritableViewMappings =
        plan instanceof AddWritableViewColumnPlan
            || plan instanceof CommitDeleteWritableViewColumnPlan;
    final WritableView writableView = getWritableView(plan.getDatabase(), plan.getTableName());
    if (Objects.isNull(writableView)) {
      if (expectsWritableViewMappings) {
        logDefensiveAssertion(
            ProcedureMessages.EXPECTED_WRITABLE_VIEW_WHEN_ROLLING_BACK_COLUMN_MAPPINGS,
            plan.getClass().getName(),
            plan.getDatabase(),
            plan.getTableName());
      }
      return;
    }

    if (plan instanceof AddWritableViewColumnPlan) {
      ((AddWritableViewColumnPlan) plan)
          .getColumnSchemaList()
          .forEach(
              columnSchema ->
                  writableView.removeViewColumnSourceColumnMapping(columnSchema.getColumnName()));
    } else if (plan instanceof CommitDeleteWritableViewColumnPlan) {
      writableView.removeViewColumnSourceColumnMapping(
          ((CommitDeleteWritableViewColumnPlan) plan).getColumnName());
    } else {
      logDefensiveAssertion(
          ProcedureMessages.UNEXPECTED_PLAN_ROLL_BACK_WRITABLE_VIEW_COLUMN_MAPPINGS,
          plan.getClass().getName(),
          plan.getDatabase(),
          plan.getTableName());
    }
  }

  private WritableView getWritableView(final String database, final String tableName)
      throws MetadataException {
    final Optional<Pair<TsTable, TableNodeStatus>> tableOptional =
        tableModelMTree.getTableAndStatusIfExists(
            getQualifiedDatabasePartialPath(database), tableName);
    if (!tableOptional.isPresent() || !(tableOptional.get().left instanceof WritableView)) {
      return null;
    }
    return (WritableView) tableOptional.get().left;
  }

  public TSStatus preAlterColumnDataType(final PreAlterColumnDataTypePlan plan) {
    return executeWithLock(
        () -> {
          tableModelMTree.preAlterColumnDataType(
              getQualifiedDatabasePartialPath(plan.getDatabase()),
              plan.getTableName(),
              plan.getColumnName(),
              plan.getNewType());
          executeOriginalIfPresent(
              plan,
              () ->
                  tableModelMTree.preAlterColumnDataType(
                      getQualifiedDatabasePartialPath(plan.getOriginalDatabase()),
                      plan.getOriginalTableName(),
                      plan.getOriginalColumnName(),
                      plan.getNewType()));
        });
  }

  public TSStatus commitAlterColumnDataType(final AlterColumnDataTypePlan plan) {
    return executeWithLock(
        () -> {
          tableModelMTree.commitAlterColumnDataType(
              getQualifiedDatabasePartialPath(plan.getDatabase()),
              plan.getTableName(),
              plan.getColumnName(),
              plan.getNewType());
          executeOriginalIfPresent(
              plan,
              () ->
                  tableModelMTree.commitAlterColumnDataType(
                      getQualifiedDatabasePartialPath(plan.getOriginalDatabase()),
                      plan.getOriginalTableName(),
                      plan.getOriginalColumnName(),
                      plan.getNewType()));
        });
  }

  // endregion

  @TestOnly
  public void clear() {
    treeModelMTree.clear();
    tableModelMTree.clear();
  }
}
