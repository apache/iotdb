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
import org.apache.iotdb.mpp.rpc.thrift.TDropTableModProgress;
import org.apache.iotdb.mpp.rpc.thrift.TGetDropTableModProgressReq;
import org.apache.iotdb.mpp.rpc.thrift.TGetDropTableModProgressResp;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateTableCacheReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class DropTableProcedure extends AbstractAlterOrDropTableProcedure<DropTableState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DropTableProcedure.class);

  // Track the replica actually executing each region, including changes caused by retries.
  // Count each region once; followers may have different local TsFiles and apply asynchronously.
  private final Map<TConsensusGroupId, TDataNodeLocation> modProgressTargets =
      new ConcurrentHashMap<>();
  private final Object modProgressRefreshMonitor = new Object();
  private CompletableFuture<Void> modProgressRefreshFuture;
  private volatile int modProgressRegionCount;
  private volatile TDropTableModProgress modFileProgress;
  private volatile boolean modProgressFinished;

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

  public String getDropProgress() {
    if (isFinished()) {
      return getState().name();
    }
    final DropTableState state = getCurrentState();
    return state == null ? getState().name() : state.name();
  }

  public TDropTableModProgress getModFileProgress() {
    final TDropTableModProgress progress = modFileProgress;
    return progress == null ? null : progress.deepCopy();
  }

  /** Shares an in-flight counter refresh among concurrent observability requests. */
  public void updateModFileProgress() {
    runModProgressRefresh(this::refreshModFileProgress, false);
  }

  private void runModProgressRefresh(final Runnable action, final boolean retryAfterWait) {
    while (true) {
      final CompletableFuture<Void> refreshFuture;
      final boolean refreshOwner;
      synchronized (modProgressRefreshMonitor) {
        if (modProgressRefreshFuture == null) {
          refreshFuture = new CompletableFuture<>();
          modProgressRefreshFuture = refreshFuture;
          refreshOwner = true;
        } else {
          refreshFuture = modProgressRefreshFuture;
          refreshOwner = false;
        }
      }
      if (!refreshOwner) {
        refreshFuture.join();
        if (retryAfterWait) {
          continue;
        }
        return;
      }
      try {
        action.run();
        return;
      } catch (final RuntimeException | Error e) {
        refreshFuture.completeExceptionally(e);
        throw e;
      } finally {
        synchronized (modProgressRefreshMonitor) {
          if (modProgressRefreshFuture == refreshFuture) {
            modProgressRefreshFuture = null;
          }
          refreshFuture.complete(null);
        }
      }
    }
  }

  private void refreshModFileProgress() {
    if (modProgressFinished
        || isFinished()
        || getCurrentState() != DropTableState.DELETE_DATA
        || modProgressTargets.isEmpty()
        || modProgressTargets.size() != modProgressRegionCount) {
      return;
    }
    final DataNodeAsyncRequestContext<TGetDropTableModProgressReq, TGetDropTableModProgressResp>
        requestContext =
            new DataNodeAsyncRequestContext<>(CnToDnAsyncRequestType.GET_DROP_TABLE_MOD_PROGRESS);
    final Map<Integer, List<TConsensusGroupId>> regionsByNode = new HashMap<>();
    final Map<TConsensusGroupId, TDataNodeLocation> targets = new HashMap<>(modProgressTargets);
    targets.forEach(
        (region, location) -> {
          requestContext.putNodeLocation(location.getDataNodeId(), location);
          regionsByNode
              .computeIfAbsent(location.getDataNodeId(), ignored -> new ArrayList<>())
              .add(region);
        });
    for (final Map.Entry<Integer, List<TConsensusGroupId>> entry : regionsByNode.entrySet()) {
      requestContext.putRequest(
          entry.getKey(), new TGetDropTableModProgressReq(getProcId(), entry.getValue()));
    }
    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequest(requestContext, 1, 2_000L, true);

    // A deletion retry may have selected a different replica while these RPCs were in flight.
    if (targets.equals(modProgressTargets)) {
      // A failed refresh is unknown, including the final refresh. Do not freeze stale counters
      // with remaining files after deletion has finished.
      modFileProgress = aggregateModFileProgress(targets, requestContext.getResponseMap());
    }
  }

  static TDropTableModProgress aggregateModFileProgress(
      final Map<TConsensusGroupId, TDataNodeLocation> targets,
      final Map<Integer, TGetDropTableModProgressResp> responses) {
    long written = 0;
    long remaining = 0;
    boolean hasUnknown = false;
    for (final Map.Entry<TConsensusGroupId, TDataNodeLocation> target : targets.entrySet()) {
      final TGetDropTableModProgressResp response =
          responses.get(target.getValue().getDataNodeId());
      if (response == null
          || response.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          || !response.isSetRegionProgress()
          || !response.getRegionProgress().containsKey(target.getKey().getId())) {
        // Unknown is different from zero. In particular, an old DataNode does not report counts.
        return null;
      }
      final TDropTableModProgress progress =
          response.getRegionProgress().get(target.getKey().getId());
      written += progress.getWrittenModFileCount();
      if (progress.isSetRemainingModFileCount()) {
        remaining += progress.getRemainingModFileCount();
      } else {
        hasUnknown = true;
      }
    }
    final TDropTableModProgress progress = new TDropTableModProgress(written);
    if (!hasUnknown) {
      progress.setRemainingModFileCount(remaining);
    }
    return progress;
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
          // A DataNode that misses this commit keeps the table in its pre-delete state, which only
          // a later read of that table repairs. Retry until every reachable DataNode acks.
          commitReleaseWithHA(env);
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
      modProgressRegionCount = relatedDataRegionGroup.size();
      new TableRegionTaskExecutor<>(
              "delete data for drop table",
              env,
              relatedDataRegionGroup,
              CnToDnAsyncRequestType.DELETE_DATA_FOR_DROP_TABLE,
              ((dataNodeLocation, consensusGroupIdList) -> {
                consensusGroupIdList.forEach(
                    region -> modProgressTargets.put(region, dataNodeLocation));
                return new TDeleteDataOrDevicesForDropTableReq(
                        new ArrayList<>(consensusGroupIdList), tableName)
                    .setProcedureId(getProcId());
              }))
          .execute();
      // Wait for an in-flight query, then publish the final snapshot atomically with the state
      // transition so a query cannot overwrite it after deletion finishes.
      runModProgressRefresh(
          () -> {
            refreshModFileProgress();
            modProgressFinished = true;
            setNextState(DropTableState.DELETE_DEVICES);
          },
          true);
    } else {
      modFileProgress = new TDropTableModProgress(0).setRemainingModFileCount(0);
      setNextState(DropTableState.DELETE_DEVICES);
    }
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

  /**
   * The data is deleted logically once DELETE_DATA has completed: the deletion is applied to every
   * data region, and only the physical removal of the mod files continues in the background. The
   * procedure is therefore not rolled back once it has moved on to DELETE_DEVICES. A view holds no
   * data and goes straight to DROP_TABLE, see {@link DropViewProcedure}.
   */
  @Override
  protected boolean isRollbackSupported(final DropTableState state) {
    switch (state) {
      case CHECK_AND_INVALIDATE_TABLE:
      case PRE_DELETE:
      case DELETE_DATA:
        return true;
      default:
        return false;
    }
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
    final TDropTableModProgress progress = modFileProgress;
    stream.writeBoolean(progress != null);
    if (progress != null) {
      stream.writeLong(progress.getWrittenModFileCount());
      stream.writeLong(
          progress.isSetRemainingModFileCount() ? progress.getRemainingModFileCount() : -1);
    }
  }

  @Override
  public void deserialize(final ByteBuffer buffer) {
    super.deserialize(buffer);
    if (!(this instanceof DropViewProcedure) && buffer.hasRemaining() && buffer.get() != 0) {
      final TDropTableModProgress progress = new TDropTableModProgress(buffer.getLong());
      final long remaining = buffer.getLong();
      if (remaining >= 0) {
        progress.setRemainingModFileCount(remaining);
      }
      modFileProgress = progress;
    }
  }

  protected void innerSerialize(final DataOutputStream stream) throws IOException {
    super.serialize(stream);
  }
}
