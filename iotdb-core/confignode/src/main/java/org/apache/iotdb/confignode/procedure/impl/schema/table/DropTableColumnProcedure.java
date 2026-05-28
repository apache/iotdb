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

package org.apache.iotdb.confignode.procedure.impl.schema.table;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreDeleteColumnPlan;
import org.apache.iotdb.confignode.consensus.response.table.PreDeleteColumnStatus;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.schema.SchemaUtils;
import org.apache.iotdb.confignode.procedure.state.schema.DropTableColumnState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteColumnDataReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateColumnCacheReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;

public class DropTableColumnProcedure
    extends AbstractAlterOrDropTableColumnProcedure<DropTableColumnState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DropTableColumnProcedure.class);
  private boolean isAttributeColumn;
  private boolean skipOriginalTagCascade;

  public DropTableColumnProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public DropTableColumnProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final String columnName,
      final boolean isGeneratedByPipe) {
    super(database, tableName, queryId, columnName, isGeneratedByPipe);
  }

  @Override
  protected String getActionMessage() {
    return "drop table column";
  }

  @Override
  protected Flow executeFromState(
      final ConfigNodeProcedureEnv env, final DropTableColumnState state)
      throws InterruptedException {
    mayInitOriginal();
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case CHECK_AND_INVALIDATE_COLUMN:
          LOGGER.info(
              ProcedureMessages.CHECK_AND_INVALIDATE_COLUMN_IN_WHEN_DROPPING_COLUMN,
              columnName,
              database,
              tableName);
          checkAndPreDeleteColumn(env);
          break;
        case INVALIDATE_CACHE:
          LOGGER.info(
              ProcedureMessages.INVALIDATING_CACHE_FOR_COLUMN_IN_WHEN_DROPPING_COLUMN,
              columnName,
              database,
              tableName);
          invalidateCache(env);
          break;
        case EXECUTE_ON_REGIONS:
          LOGGER.info(
              ProcedureMessages.EXECUTING_ON_REGION_FOR_COLUMN_IN_WHEN_DROPPING_COLUMN,
              columnName,
              database,
              tableName);
          executeOnRegions(env);
          break;
        case DROP_COLUMN:
          LOGGER.info(
              ProcedureMessages.DROPPING_COLUMN_IN_ON_CONFIGNODE, columnName, database, tableName);
          dropColumn(env);
          if (skipOriginalTagCascade) {
            setFailure(buildSkipOriginalTagCascadeWarning());
          }
          return Flow.NO_MORE_STATE;
        default:
          setFailure(
              new ProcedureException(ProcedureMessages.UNRECOGNIZED_DROPTABLECOLUMNSTATE + state));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          ProcedureMessages.DROPTABLECOLUMN_COSTS_MS,
          database,
          tableName,
          state,
          (System.currentTimeMillis() - startTime));
    }
  }

  private void checkAndPreDeleteColumn(final ConfigNodeProcedureEnv env) {
    final TSStatus status =
        SchemaUtils.executeInConsensusLayer(createPreDeleteColumnPlan(), env, LOGGER);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status)));
      return;
    }
    handlePreDeleteColumnStatus(status);
    setNextState(DropTableColumnState.INVALIDATE_CACHE);
  }

  private void invalidateCache(final ConfigNodeProcedureEnv env) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();
    final TInvalidateColumnCacheReq req =
        new TInvalidateColumnCacheReq(database, tableName, columnName, isAttributeColumn);

    appendOriginalColumnForCacheInvalidation(req);
    final DataNodeAsyncRequestContext<TInvalidateColumnCacheReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.INVALIDATE_COLUMN_CACHE, req, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    final Map<Integer, TSStatus> statusMap = clientHandler.getResponseMap();
    for (final TSStatus status : statusMap.values()) {
      // All dataNodes must clear the related schemaEngine cache
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error(
            ProcedureMessages.FAILED_TO_INVALIDATE_COLUMN_S_CACHE_OF_TABLE,
            isAttributeColumn ? "attribute" : "measurement",
            columnName,
            database,
            tableName);
        setFailure(
            new ProcedureException(
                new MetadataException(
                    String.format(
                        ProcedureMessages.INVALIDATE_COLUMN_CACHE_FAILED_FOR_TABLE,
                        columnName,
                        database,
                        tableName))));
        return;
      }
    }

    // View does not need to be executed on regions
    setNextState(getStateAfterInvalidateCache());
  }

  private void executeOnRegions(final ConfigNodeProcedureEnv env) {
    if (!shouldExecuteOnRegions()) {
      setNextState(DropTableColumnState.DROP_COLUMN);
      return;
    }
    final String database = getRegionOperationDatabase();
    final String tableName = getRegionOperationTableName();
    final String columnName = getRegionOperationColumnName();

    final Map<TConsensusGroupId, TRegionReplicaSet> relatedRegionGroup =
        isAttributeColumn
            ? env.getConfigManager().getRelatedSchemaRegionGroup4TableModel(database)
            : env.getConfigManager().getRelatedDataRegionGroup4TableModel(database);

    if (!relatedRegionGroup.isEmpty()) {
      new TableRegionTaskExecutor<>(
              "delete data for drop table",
              env,
              relatedRegionGroup,
              CnToDnAsyncRequestType.DELETE_COLUMN_DATA,
              ((dataNodeLocation, consensusGroupIdList) ->
                  new TDeleteColumnDataReq(
                      new ArrayList<>(consensusGroupIdList),
                      tableName,
                      columnName,
                      isAttributeColumn)))
          .execute();
    }

    setNextState(DropTableColumnState.DROP_COLUMN);
  }

  private void dropColumn(final ConfigNodeProcedureEnv env) {
    final TSStatus status =
        env.getConfigManager()
            .getClusterSchemaManager()
            .executePlan(createCommitDeleteColumnPlan(), isGeneratedByPipe);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status)));
    }
  }

  protected ConfigPhysicalPlan createPreDeleteColumnPlan() {
    return new PreDeleteColumnPlan(database, tableName, columnName);
  }

  protected DropTableColumnState getStateAfterInvalidateCache() {
    return DropTableColumnState.EXECUTE_ON_REGIONS;
  }

  protected void appendOriginalColumnForCacheInvalidation(
      final TInvalidateColumnCacheReq invalidateColumnCacheReq) {
    if (!hasOriginalColumn() || !shouldIncludeOriginalColumnInCacheInvalidation()) {
      return;
    }
    invalidateColumnCacheReq.setOriginalDatabase(getOriginalDatabaseForColumn());
    invalidateColumnCacheReq.setOriginalTableName(getOriginalTableNameForColumn());
    invalidateColumnCacheReq.setOriginalColumnName(getOriginalColumnName());
  }

  protected boolean shouldIncludeOriginalColumnInCacheInvalidation() {
    return true;
  }

  protected String getRegionOperationDatabase() {
    return database;
  }

  protected String getRegionOperationTableName() {
    return tableName;
  }

  protected String getRegionOperationColumnName() {
    return columnName;
  }

  protected boolean shouldExecuteOnRegions() {
    return Objects.nonNull(getRegionOperationColumnName());
  }

  protected ConfigPhysicalPlan createCommitDeleteColumnPlan() {
    return new CommitDeleteColumnPlan(database, tableName, columnName);
  }

  protected boolean isOriginalTagCascadeSkipped() {
    return skipOriginalTagCascade;
  }

  private void handlePreDeleteColumnStatus(final TSStatus status) {
    if (!status.isSetMessage()) {
      return;
    }
    final PreDeleteColumnStatus preDeleteColumnStatus =
        PreDeleteColumnStatus.parse(status.getMessage());
    if (Objects.isNull(preDeleteColumnStatus)) {
      return;
    }
    if (preDeleteColumnStatus == PreDeleteColumnStatus.ORIGINAL_TAG_COLUMN_CASCADE_SKIPPED) {
      skipOriginalTagCascade = true;
      return;
    }
    if (preDeleteColumnStatus == PreDeleteColumnStatus.ATTRIBUTE) {
      isAttributeColumn = true;
    }
  }

  @Override
  protected boolean isRollbackSupported(final DropTableColumnState state) {
    return false;
  }

  @Override
  protected void rollbackState(
      final ConfigNodeProcedureEnv configNodeProcedureEnv,
      final DropTableColumnState dropTableState)
      throws IOException, InterruptedException, ProcedureException {
    // Do nothing
  }

  @Override
  protected DropTableColumnState getState(final int stateId) {
    return DropTableColumnState.values()[stateId];
  }

  @Override
  protected int getStateId(final DropTableColumnState dropTableColumnState) {
    return dropTableColumnState.ordinal();
  }

  @Override
  protected DropTableColumnState getInitialState() {
    return DropTableColumnState.CHECK_AND_INVALIDATE_COLUMN;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_DROP_TABLE_COLUMN_PROCEDURE.getTypeCode()
            : ProcedureType.DROP_TABLE_COLUMN_PROCEDURE.getTypeCode());
    innerSerialize(stream);
  }

  protected void innerSerialize(final DataOutputStream stream) throws IOException {
    super.innerSerialize(stream);

    ReadWriteIOUtils.write(isAttributeColumn, stream);
    ReadWriteIOUtils.write(skipOriginalTagCascade, stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);

    this.isAttributeColumn = ReadWriteIOUtils.readBool(byteBuffer);
    this.skipOriginalTagCascade = ReadWriteIOUtils.readBool(byteBuffer);
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o)
        && Objects.equals(isAttributeColumn, ((DropTableColumnProcedure) o).isAttributeColumn)
        && Objects.equals(
            skipOriginalTagCascade, ((DropTableColumnProcedure) o).skipOriginalTagCascade);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), isAttributeColumn, skipOriginalTagCascade);
  }

  private ProcedureException buildSkipOriginalTagCascadeWarning() {
    return new ProcedureException(
        new IoTDBException(
            new TSStatus(TSStatusCode.COLUMN_CATEGORY_MISMATCH.getStatusCode())
                .setMessage(
                    "WARNING: The original column to be deleted in a cascade is a tag, which is not allowed to be deleted. The original column remains unchanged.")));
  }
}
