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

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.utils.ThriftConfigNodeSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.write.database.PreDeleteDatabasePlan;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.manager.partition.PartitionMetrics;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.impl.region.RemoveRegionGroupProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.DeleteDatabaseState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

public class DeleteDatabaseProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, DeleteDatabaseState> {
  private static final Logger LOG = LoggerFactory.getLogger(DeleteDatabaseProcedure.class);
  private static final int RETRY_THRESHOLD = 5;

  private TDatabaseSchema deleteDatabaseSchema;

  public DeleteDatabaseProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public DeleteDatabaseProcedure(
      final TDatabaseSchema deleteDatabaseSchema, final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
    this.deleteDatabaseSchema = deleteDatabaseSchema;
  }

  public TDatabaseSchema getDeleteDatabaseSchema() {
    return deleteDatabaseSchema;
  }

  public void setDeleteDatabaseSchema(final TDatabaseSchema deleteDatabaseSchema) {
    this.deleteDatabaseSchema = deleteDatabaseSchema;
  }

  @Override
  protected Flow executeFromState(final ConfigNodeProcedureEnv env, final DeleteDatabaseState state)
      throws InterruptedException {
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
          setNextState(DeleteDatabaseState.INVALIDATE_CACHE);
          break;
        case INVALIDATE_CACHE:
          LOG.info(
              "[DeleteDatabaseProcedure] Invalidate cache of database: {}",
              deleteDatabaseSchema.getName());
          if (env.invalidateCache(deleteDatabaseSchema.getName())) {
            setNextState(DeleteDatabaseState.DELETE_DATABASE_SCHEMA);
          } else {
            setFailure(
                new ProcedureException(
                    ProcedureMessages.DELETEDATABASEPROCEDURE_INVALIDATE_CACHE_FAILED));
          }
          break;
        case DELETE_DATABASE_SCHEMA:
          LOG.info(
              "[DeleteDatabaseProcedure] Delete DatabaseSchema: {}",
              deleteDatabaseSchema.getName());

          // Enqueue deletion of every region group (both schema and data regions) of this database.
          // Each is submitted as an INDEPENDENT root RemoveRegionGroupProcedure rather than a
          // child:
          // this procedure only submits the deletions and then returns, so it can neither wait for
          // nor be failed/rolled-back by a slow or failing region deletion. Each carries its own
          // copy of the replica set, so the deletion still completes (and survives leader change /
          // restart) even after the next state drops the partition table.
          //
          // Submission is intentionally NOT guarded by isStateDeserialized(): the executor persists
          // a procedure at a state BEFORE that state's body has run (it advances the state on the
          // previous cycle, then may stop at the inter-state boundary on a leader switch — see
          // ProcedureExecutor#executeProcedure). So a recovery that lands on this state means the
          // submission has NOT happened yet; skipping it would drop every region group's cleanup
          // while the next state still drops the partition table, orphaning the region peers/data
          // on
          // disk with no record of where they live. Re-submitting on recovery is safe instead:
          // every RemoveRegionGroupProcedure gets a fresh procId and performs an idempotent delete,
          // so a duplicate is harmless whereas a skip leaks data.
          final List<TRegionReplicaSet> regionReplicaSets =
              env.getAllReplicaSets(deleteDatabaseSchema.getName());
          regionReplicaSets.forEach(
              regionReplicaSet -> {
                // Clear heartbeat cache along the way
                env.getConfigManager()
                    .getLoadManager()
                    .removeRegionGroupRelatedCache(regionReplicaSet.getRegionId());
                env.getConfigManager()
                    .getProcedureManager()
                    .getExecutor()
                    .submitProcedure(new RemoveRegionGroupProcedure(regionReplicaSet));
              });
          setNextState(DeleteDatabaseState.DELETE_DATABASE_CONFIG);
          break;
        case DELETE_DATABASE_CONFIG:
          env.getConfigManager()
              .getLoadManager()
              .clearDataPartitionPolicyTable(deleteDatabaseSchema.getName());
          LOG.info(
              "[DeleteDatabaseProcedure] The data partition policy table of database: {} is cleared.",
              deleteDatabaseSchema.getName());

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
                new ProcedureException(
                    ProcedureMessages.DELETEDATABASEPROCEDURE_DELETE_DATABASESCHEMA_FAILED));
          }
      }
    } catch (final TException | IOException e) {
      if (isRollbackSupported(state)) {
        setFailure(
            new ProcedureException(
                ProcedureMessages.DELETEDATABASEPROCEDURE_DELETE_DATABASE
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
          setFailure(
              new ProcedureException(
                  ProcedureMessages.DELETEDATABASEPROCEDURE_STATE_STUCK_AT + state));
        }
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final ConfigNodeProcedureEnv env, final DeleteDatabaseState state)
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
  protected boolean isRollbackSupported(final DeleteDatabaseState state) {
    switch (state) {
      case PRE_DELETE_DATABASE:
      case INVALIDATE_CACHE:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected DeleteDatabaseState getState(final int stateId) {
    return DeleteDatabaseState.values()[stateId];
  }

  @Override
  protected int getStateId(final DeleteDatabaseState deleteDatabaseState) {
    return deleteDatabaseState.ordinal();
  }

  @Override
  protected DeleteDatabaseState getInitialState() {
    return DeleteDatabaseState.PRE_DELETE_DATABASE;
  }

  public String getDatabase() {
    return deleteDatabaseSchema.getName();
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_DELETE_DATABASE_PROCEDURE.getTypeCode()
            : ProcedureType.DELETE_DATABASE_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ThriftConfigNodeSerDeUtils.serializeTDatabaseSchema(deleteDatabaseSchema, stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      deleteDatabaseSchema = ThriftConfigNodeSerDeUtils.deserializeTDatabaseSchema(byteBuffer);
    } catch (final ThriftSerDeException e) {
      LOG.error(ProcedureMessages.ERROR_IN_DESERIALIZE_DELETEDATABASEPROCEDURE, e);
    }
  }

  @Override
  public boolean equals(final Object that) {
    if (that instanceof DeleteDatabaseProcedure) {
      final DeleteDatabaseProcedure thatProc = (DeleteDatabaseProcedure) that;
      return thatProc.getProcId() == this.getProcId()
          && Objects.equals(thatProc.getCurrentState(), this.getCurrentState())
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
