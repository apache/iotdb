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
import org.apache.iotdb.commons.schema.table.PreDeleteTsTable;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreDeleteTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.RollbackPreDeleteTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.CommitDeleteViewPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.PreDeleteViewPlan;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.manager.lease.ClusterCachePropagator;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.schema.SchemaUtils;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.DropViewProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.DropTableState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteDataOrDevicesForDropTableReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateTableCacheReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class DropTableProcedure extends AbstractAlterOrDropTableProcedure<DropTableState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DropTableProcedure.class);

  public DropTableProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public DropTableProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final boolean isGeneratedByPipe) {
    super(database, tableName, queryId, isGeneratedByPipe);
  }

  @Override
  protected String getActionMessage() {
    return "drop table";
  }

  @Override
  protected Flow executeFromState(final ConfigNodeProcedureEnv env, final DropTableState state)
      throws InterruptedException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case CHECK_AND_INVALIDATE_TABLE:
          LOGGER.info(
              ProcedureMessages.CHECK_AND_INVALIDATE_TABLE_WHEN_DROPPING_TABLE,
              database,
              tableName);
          checkAndPreDeleteTable(env);
          break;
        case PRE_DELETE:
          LOGGER.info(
              ProcedureMessages.PRE_RELEASE_DELETE_TABLE_WHEN_DROPPING_TABLE, database, tableName);
          preDelete(env);
          break;
        case DELETE_DATA:
          LOGGER.info(ProcedureMessages.DELETING_DATA_FOR_TABLE, database, tableName);
          deleteData(env);
          break;
        case DELETE_DEVICES:
          LOGGER.info(
              ProcedureMessages.DELETING_DEVICES_FOR_TABLE_WHEN_DROPPING_TABLE,
              database,
              tableName);
          deleteSchema(env);
          break;
        case DROP_TABLE:
          LOGGER.info(ProcedureMessages.DROPPING_TABLE_ON_CONFIGNODE, database, tableName);
          dropTable(env);
          break;
        case COMMIT_DELETE:
          LOGGER.info(
              ProcedureMessages.COMMIT_RELEASE_DELETE_TABLE_WHEN_DROPPING_TABLE,
              database,
              tableName);
          commitRelease(env);
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException(ProcedureMessages.UNRECOGNIZED_DROPTABLESTATE + state));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          ProcedureMessages.DROPTABLE_COSTS_MS,
          database,
          tableName,
          state,
          (System.currentTimeMillis() - startTime));
    }
  }

  private void checkAndPreDeleteTable(final ConfigNodeProcedureEnv env) {
    final TSStatus status =
        SchemaUtils.executeInConsensusLayer(
            this instanceof DropViewProcedure
                ? new PreDeleteViewPlan(database, tableName)
                : new PreDeleteTablePlan(database, tableName),
            env,
            LOGGER);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setNextState(DropTableState.PRE_DELETE);
      table = new PreDeleteTsTable(tableName);
    } else {
      setFailure(new ProcedureException(new IoTDBException(status)));
    }
  }

  private void preDelete(final ConfigNodeProcedureEnv env) {
    TInvalidateTableCacheReq req = new TInvalidateTableCacheReq(database, tableName);
    final boolean proceeded =
        new ClusterCachePropagator(SchemaUtils.filterFencedDataNode(env.getConfigManager()))
            .propagate(targets -> broadCastInvalidateCache(req, targets));

    if (!proceeded) {
      LOGGER.warn(
          ProcedureMessages.FAILED_TO_INVALIDATE_SCHEMAENGINE_CACHE_OF_TABLE, database, tableName);
      setFailure(
          new ProcedureException(
              new MetadataException(ProcedureMessages.INVALIDATE_SCHEMAENGINE_CACHE_FAILED)));
      return;
    }
    setNextState(
        this instanceof DropViewProcedure ? DropTableState.DROP_TABLE : DropTableState.DELETE_DATA);
  }

  private Map<Integer, TSStatus> broadCastInvalidateCache(
      final TInvalidateTableCacheReq req, final Map<Integer, TDataNodeLocation> targets) {
    final DataNodeAsyncRequestContext<TInvalidateTableCacheReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(CnToDnAsyncRequestType.PRE_DELETE_TABLE, req, targets);
    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequest(
            clientHandler,
            ClusterCachePropagator.BROADCAST_RPC_RETRY,
            ClusterCachePropagator.BROADCAST_RPC_TIMEOUT_MS);
    return clientHandler.getResponseMap();
  }

  private void deleteData(final ConfigNodeProcedureEnv env) {
    final Map<TConsensusGroupId, TRegionReplicaSet> relatedDataRegionGroup =
        env.getConfigManager().getRelatedDataRegionGroup4TableModel(database);

    if (!relatedDataRegionGroup.isEmpty()) {
      new TableRegionTaskExecutor<>(
              "delete data for drop table",
              env,
              relatedDataRegionGroup,
              CnToDnAsyncRequestType.DELETE_DATA_FOR_DROP_TABLE,
              ((dataNodeLocation, consensusGroupIdList) ->
                  new TDeleteDataOrDevicesForDropTableReq(
                      new ArrayList<>(consensusGroupIdList), tableName)))
          .execute();
    }

    setNextState(DropTableState.DELETE_DEVICES);
  }

  private void deleteSchema(final ConfigNodeProcedureEnv env) {
    final Map<TConsensusGroupId, TRegionReplicaSet> relatedSchemaRegionGroup =
        env.getConfigManager().getRelatedSchemaRegionGroup4TableModel(database);

    if (!relatedSchemaRegionGroup.isEmpty()) {
      new TableRegionTaskExecutor<>(
              "delete devices for drop table",
              env,
              relatedSchemaRegionGroup,
              CnToDnAsyncRequestType.DELETE_DEVICES_FOR_DROP_TABLE,
              ((dataNodeLocation, consensusGroupIdList) ->
                  new TDeleteDataOrDevicesForDropTableReq(
                      new ArrayList<>(consensusGroupIdList), tableName)))
          .execute();
    }

    setNextState(DropTableState.DROP_TABLE);
  }

  private void dropTable(final ConfigNodeProcedureEnv env) {
    final TSStatus status =
        env.getConfigManager()
            .getClusterSchemaManager()
            .executePlan(
                this instanceof DropViewProcedure
                    ? new CommitDeleteViewPlan(database, tableName)
                    : new CommitDeleteTablePlan(database, tableName),
                isGeneratedByPipe);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status)));
    } else {
      setNextState(DropTableState.COMMIT_DELETE);
    }
  }

  @Override
  protected boolean isRollbackSupported(final DropTableState state) {
    return state == DropTableState.CHECK_AND_INVALIDATE_TABLE || state == DropTableState.PRE_DELETE;
  }

  @Override
  protected void rollbackState(final ConfigNodeProcedureEnv env, final DropTableState state)
      throws IOException, InterruptedException, ProcedureException {
    if (state == DropTableState.PRE_DELETE) {
      final TSStatus status =
          SchemaUtils.executeInConsensusLayer(
              new RollbackPreDeleteTablePlan(database, tableName), env, LOGGER);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new ProcedureException(
            String.format(ProcedureMessages.ROLLBACK_PRE_DELETE_TABLE_FAILED, database, tableName));
      }
    }
    // CHECK_AND_INVALIDATE_TABLE: consensus plan failed so no state changed, nothing to revert
  }

  @Override
  protected DropTableState getState(final int stateId) {
    return DropTableState.values()[stateId];
  }

  @Override
  protected int getStateId(final DropTableState dropTableState) {
    return dropTableState.ordinal();
  }

  @Override
  protected DropTableState getInitialState() {
    return DropTableState.CHECK_AND_INVALIDATE_TABLE;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_DROP_TABLE_PROCEDURE.getTypeCode()
            : ProcedureType.DROP_TABLE_PROCEDURE.getTypeCode());
    super.serialize(stream);
  }

  protected void innerSerialize(final DataOutputStream stream) throws IOException {
    super.serialize(stream);
  }
}
