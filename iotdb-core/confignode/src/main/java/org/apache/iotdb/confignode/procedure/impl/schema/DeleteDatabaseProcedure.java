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

package org.apache.iotdb.confignode.procedure.impl.schema;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.utils.ThriftConfigNodeSerDeUtils;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.consensus.request.write.database.PreDeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.region.OfferRegionMaintainTasksPlan;
import org.apache.iotdb.confignode.manager.partition.PartitionMetrics;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionDeleteTask;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.DeleteStorageGroupState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DeleteDatabaseProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, DeleteStorageGroupState> {
  private static final Logger LOG = LoggerFactory.getLogger(DeleteDatabaseProcedure.class);
  private static final int RETRY_THRESHOLD = 5;

  private TDatabaseSchema deleteDatabaseSchema;

  public DeleteDatabaseProcedure(boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public DeleteDatabaseProcedure(TDatabaseSchema deleteDatabaseSchema, boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
    this.deleteDatabaseSchema = deleteDatabaseSchema;
  }

  public TDatabaseSchema getDeleteDatabaseSchema() {
    return deleteDatabaseSchema;
  }

  public void setDeleteDatabaseSchema(TDatabaseSchema deleteDatabaseSchema) {
    this.deleteDatabaseSchema = deleteDatabaseSchema;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, DeleteStorageGroupState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    if (deleteDatabaseSchema == null) {
      return Flow.NO_MORE_STATE;
    }
    try {
      switch (state) {
        case PRE_DELETE_DATABASE:
          LOG.info(
              "[DeleteDatabaseProcedure] Pre delete database: {}", deleteDatabaseSchema.getName());
          env.preDeleteDatabase(
              PreDeleteDatabasePlan.PreDeleteType.EXECUTE, deleteDatabaseSchema.getName());
          setNextState(DeleteStorageGroupState.INVALIDATE_CACHE);
          break;
        case INVALIDATE_CACHE:
          LOG.info(
              "[DeleteDatabaseProcedure] Invalidate cache of database: {}",
              deleteDatabaseSchema.getName());
          if (env.invalidateCache(deleteDatabaseSchema.getName())) {
            setNextState(DeleteStorageGroupState.DELETE_DATABASE_SCHEMA);
          } else {
            setFailure(new ProcedureException("[DeleteDatabaseProcedure] Invalidate cache failed"));
          }
          break;
        case DELETE_DATABASE_SCHEMA:
          LOG.info(
              "[DeleteDatabaseProcedure] Delete DatabaseSchema: {}",
              deleteDatabaseSchema.getName());

          // Submit RegionDeleteTasks
          OfferRegionMaintainTasksPlan dataRegionDeleteTaskOfferPlan =
              new OfferRegionMaintainTasksPlan();
          List<TRegionReplicaSet> regionReplicaSets =
              env.getAllReplicaSets(deleteDatabaseSchema.getName());
          List<TRegionReplicaSet> schemaRegionReplicaSets = new ArrayList<>();
          regionReplicaSets.forEach(
              regionReplicaSet -> {
                // Clear heartbeat cache along the way
                env.getConfigManager()
                    .getLoadManager()
                    .removeRegionGroupRelatedCache(regionReplicaSet.getRegionId());

                if (regionReplicaSet
                    .getRegionId()
                    .getType()
                    .equals(TConsensusGroupType.SchemaRegion)) {
                  schemaRegionReplicaSets.add(regionReplicaSet);
                } else {
                  regionReplicaSet
                      .getDataNodeLocations()
                      .forEach(
                          targetDataNode ->
                              dataRegionDeleteTaskOfferPlan.appendRegionMaintainTask(
                                  new RegionDeleteTask(
                                      targetDataNode, regionReplicaSet.getRegionId())));
                }
              });

          if (!dataRegionDeleteTaskOfferPlan.getRegionMaintainTaskList().isEmpty()) {
            // submit async data region delete task
            env.getConfigManager().getConsensusManager().write(dataRegionDeleteTaskOfferPlan);
          }

          // try sync delete schemaengine region
          DataNodeAsyncRequestContext<TConsensusGroupId, TSStatus> asyncClientHandler =
              new DataNodeAsyncRequestContext<>(CnToDnAsyncRequestType.DELETE_REGION);
          Map<Integer, RegionDeleteTask> schemaRegionDeleteTaskMap = new HashMap<>();
          int requestIndex = 0;
          for (TRegionReplicaSet schemaRegionReplicaSet : schemaRegionReplicaSets) {
            for (TDataNodeLocation dataNodeLocation :
                schemaRegionReplicaSet.getDataNodeLocations()) {
              asyncClientHandler.putRequest(requestIndex, schemaRegionReplicaSet.getRegionId());
              asyncClientHandler.putNodeLocation(requestIndex, dataNodeLocation);
              schemaRegionDeleteTaskMap.put(
                  requestIndex,
                  new RegionDeleteTask(dataNodeLocation, schemaRegionReplicaSet.getRegionId()));
              requestIndex++;
            }
          }
          if (!schemaRegionDeleteTaskMap.isEmpty()) {
            CnToDnInternalServiceAsyncRequestManager.getInstance()
                .sendAsyncRequestWithRetry(asyncClientHandler);
            for (Map.Entry<Integer, TSStatus> entry :
                asyncClientHandler.getResponseMap().entrySet()) {
              if (entry.getValue().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                LOG.info(
                    "[DeleteDatabaseProcedure] Successfully delete SchemaRegion[{}] on {}",
                    asyncClientHandler.getRequest(entry.getKey()),
                    schemaRegionDeleteTaskMap.get(entry.getKey()).getTargetDataNode());
                schemaRegionDeleteTaskMap.remove(entry.getKey());
              } else {
                LOG.warn(
                    "[DeleteDatabaseProcedure] Failed to delete SchemaRegion[{}] on {}. Submit to async deletion.",
                    asyncClientHandler.getRequest(entry.getKey()),
                    schemaRegionDeleteTaskMap.get(entry.getKey()).getTargetDataNode());
              }
            }

            if (!schemaRegionDeleteTaskMap.isEmpty()) {
              // submit async schemaengine region delete task for failed sync execution
              OfferRegionMaintainTasksPlan schemaRegionDeleteTaskOfferPlan =
                  new OfferRegionMaintainTasksPlan();
              schemaRegionDeleteTaskMap
                  .values()
                  .forEach(schemaRegionDeleteTaskOfferPlan::appendRegionMaintainTask);
              env.getConfigManager().getConsensusManager().write(schemaRegionDeleteTaskOfferPlan);
            }
          }

          env.getConfigManager()
              .getLoadManager()
              .clearDataPartitionPolicyTable(deleteDatabaseSchema.getName());
          LOG.info("data partition policy table cleared.");

          // Delete Database metrics
          PartitionMetrics.unbindDatabaseRelatedMetricsWhenUpdate(
              MetricService.getInstance(), deleteDatabaseSchema.getName());

          // Delete DatabasePartitionTable
          final TSStatus deleteConfigResult =
              env.deleteDatabaseConfig(deleteDatabaseSchema.getName(), isGeneratedByPipe);

          if (deleteConfigResult.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            LOG.info(
                "[DeleteDatabaseProcedure] Database: {} is deleted successfully",
                deleteDatabaseSchema.getName());
            return Flow.NO_MORE_STATE;
          } else if (getCycles() > RETRY_THRESHOLD) {
            setFailure(
                new ProcedureException("[DeleteDatabaseProcedure] Delete DatabaseSchema failed"));
          }
      }
    } catch (ConsensusException | TException | IOException e) {
      if (isRollbackSupported(state)) {
        setFailure(
            new ProcedureException(
                "[DeleteDatabaseProcedure] Delete database "
                    + deleteDatabaseSchema.getName()
                    + " failed "
                    + state));
      } else {
        LOG.error(
            "[DeleteDatabaseProcedure] Retriable error trying to delete database {}, state {}",
            deleteDatabaseSchema.getName(),
            state,
            e);
        if (getCycles() > RETRY_THRESHOLD) {
          setFailure(new ProcedureException("[DeleteDatabaseProcedure] State stuck at " + state));
        }
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, DeleteStorageGroupState state)
      throws IOException, InterruptedException {
    switch (state) {
      case PRE_DELETE_DATABASE:
      case INVALIDATE_CACHE:
        LOG.info(
            "[DeleteDatabaseProcedure] Rollback to preDeleted: {}", deleteDatabaseSchema.getName());
        env.preDeleteDatabase(
            PreDeleteDatabasePlan.PreDeleteType.ROLLBACK, deleteDatabaseSchema.getName());
        break;
      default:
        break;
    }
  }

  @Override
  protected boolean isRollbackSupported(DeleteStorageGroupState state) {
    switch (state) {
      case PRE_DELETE_DATABASE:
      case INVALIDATE_CACHE:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected DeleteStorageGroupState getState(int stateId) {
    return DeleteStorageGroupState.values()[stateId];
  }

  @Override
  protected int getStateId(DeleteStorageGroupState deleteStorageGroupState) {
    return deleteStorageGroupState.ordinal();
  }

  @Override
  protected DeleteStorageGroupState getInitialState() {
    return DeleteStorageGroupState.PRE_DELETE_DATABASE;
  }

  public String getDatabase() {
    return deleteDatabaseSchema.getName();
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_DELETE_DATABASE_PROCEDURE.getTypeCode()
            : ProcedureType.DELETE_DATABASE_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ThriftConfigNodeSerDeUtils.serializeTDatabaseSchema(deleteDatabaseSchema, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      deleteDatabaseSchema = ThriftConfigNodeSerDeUtils.deserializeTDatabaseSchema(byteBuffer);
    } catch (ThriftSerDeException e) {
      LOG.error("Error in deserialize DeleteStorageGroupProcedure", e);
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof DeleteDatabaseProcedure) {
      DeleteDatabaseProcedure thatProc = (DeleteDatabaseProcedure) that;
      return thatProc.getProcId() == this.getProcId()
          && thatProc.getCurrentState().equals(this.getCurrentState())
          && thatProc.getCycles() == this.getCycles()
          && thatProc.isGeneratedByPipe == this.isGeneratedByPipe
          && thatProc.deleteDatabaseSchema.equals(this.getDeleteDatabaseSchema());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getProcId(), getCurrentState(), getCycles(), isGeneratedByPipe, deleteDatabaseSchema);
  }
}
