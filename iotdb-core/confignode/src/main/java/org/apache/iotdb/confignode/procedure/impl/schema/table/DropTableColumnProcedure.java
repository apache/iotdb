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
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreDeleteColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.CommitDeleteViewColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.PreDeleteViewColumnPlan;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.manager.lease.ClusterCachePropagator;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.schema.SchemaUtils;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.DropViewColumnProcedure;
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
    extends AbstractAlterOrDropTableProcedure<DropTableColumnState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DropTableColumnProcedure.class);

  private String columnName;
  private boolean isAttributeColumn;

  public DropTableColumnProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public DropTableColumnProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final String columnName,
      final boolean isGeneratedByPipe) {
    super(database, tableName, queryId, isGeneratedByPipe);
    this.columnName = columnName;
  }

  @Override
  protected String getActionMessage() {
    return "drop table column";
  }

  @Override
  protected Flow executeFromState(
      final ConfigNodeProcedureEnv env, final DropTableColumnState state)
      throws InterruptedException {
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
        SchemaUtils.executeInConsensusLayer(
            this instanceof DropViewColumnProcedure
                ? new PreDeleteViewColumnPlan(database, tableName, columnName)
                : new PreDeleteColumnPlan(database, tableName, columnName),
            env,
            LOGGER);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      isAttributeColumn = status.isSetMessage();
      setNextState(DropTableColumnState.INVALIDATE_CACHE);
    } else {
      setFailure(new ProcedureException(new IoTDBException(status)));
    }
  }

  private void invalidateCache(final ConfigNodeProcedureEnv env) {
    TInvalidateColumnCacheReq req =
        new TInvalidateColumnCacheReq(database, tableName, columnName, isAttributeColumn);
    final boolean proceeded =
        new ClusterCachePropagator(SchemaUtils.filterFencedDataNode(env.getConfigManager()))
            .propagate(targets -> broadCastInvalidateCache(req, targets));

    if (!proceeded) {
      LOGGER.warn(
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
    // View does not need to be executed on regions
    setNextState(
        this instanceof DropViewColumnProcedure
            ? DropTableColumnState.DROP_COLUMN
            : DropTableColumnState.EXECUTE_ON_REGIONS);
  }

  private Map<Integer, TSStatus> broadCastInvalidateCache(
      TInvalidateColumnCacheReq req, Map<Integer, TDataNodeLocation> targets) {

    final DataNodeAsyncRequestContext<TInvalidateColumnCacheReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.INVALIDATE_COLUMN_CACHE, req, targets);
    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequest(
            clientHandler,
            ClusterCachePropagator.BROADCAST_RPC_RETRY,
            ClusterCachePropagator.BROADCAST_RPC_TIMEOUT_MS);
    return clientHandler.getResponseMap();
  }

  private void executeOnRegions(final ConfigNodeProcedureEnv env) {
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
            .executePlan(
                this instanceof DropViewColumnProcedure
                    ? new CommitDeleteViewColumnPlan(database, tableName, columnName)
                    : new CommitDeleteColumnPlan(database, tableName, columnName),
                isGeneratedByPipe);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status)));
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
    super.serialize(stream);

    ReadWriteIOUtils.write(columnName, stream);
    ReadWriteIOUtils.write(isAttributeColumn, stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);

    this.columnName = ReadWriteIOUtils.readString(byteBuffer);
    this.isAttributeColumn = ReadWriteIOUtils.readBool(byteBuffer);
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o)
        && Objects.equals(columnName, ((DropTableColumnProcedure) o).columnName)
        && Objects.equals(isAttributeColumn, ((DropTableColumnProcedure) o).isAttributeColumn);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), columnName, isAttributeColumn);
  }
}
