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

package org.apache.iotdb.confignode.procedure.impl;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.utils.ThriftConfigNodeSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.write.PreDeleteStorageGroupPlan;
import org.apache.iotdb.confignode.procedure.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.state.DeleteStorageGroupState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DeleteStorageGroupProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, DeleteStorageGroupState> {
  private static final Logger LOG = LoggerFactory.getLogger(DeleteStorageGroupProcedure.class);
  private static final int retryThreshold = 5;

  private TStorageGroupSchema deleteSgSchema;

  public DeleteStorageGroupProcedure() {
    super();
  }

  public DeleteStorageGroupProcedure(TStorageGroupSchema deleteSgSchema) {
    super();
    this.deleteSgSchema = deleteSgSchema;
  }

  public TStorageGroupSchema getDeleteSgSchema() {
    return deleteSgSchema;
  }

  public void setDeleteSgSchema(TStorageGroupSchema deleteSgSchema) {
    this.deleteSgSchema = deleteSgSchema;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, DeleteStorageGroupState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    if (deleteSgSchema == null) {
      return Flow.NO_MORE_STATE;
    }
    try {
      switch (state) {
        case DELETE_STORAGE_GROUP_PREPARE:
          // TODO: lock related ClusterSchemaInfo, PartitionInfo and Regions
          setNextState(DeleteStorageGroupState.DELETE_PRE);
          break;
        case DELETE_PRE:
          LOG.info("Pre delete for Storage group {}", deleteSgSchema.getName());
          env.preDelete(PreDeleteStorageGroupPlan.PreDeleteType.EXECUTE, deleteSgSchema.getName());
          setNextState(DeleteStorageGroupState.INVALIDATE_CACHE);
          break;
        case INVALIDATE_CACHE:
          LOG.info("Invalidate cache of {}", deleteSgSchema.getName());
          if (env.invalidateCache(deleteSgSchema.getName())) {
            setNextState(DeleteStorageGroupState.DELETE_CONFIG);
          } else {
            setFailure(new ProcedureException("Invalidate cache failed"));
          }
          break;
        case DELETE_CONFIG:
          LOG.info("Delete config info of {}", deleteSgSchema.getName());
          TSStatus status = env.deleteConfig(deleteSgSchema.getName());
          if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            return Flow.NO_MORE_STATE;
          } else if (getCycles() > retryThreshold) {
            setFailure(new ProcedureException("Delete config info id failed"));
          }
      }
    } catch (TException | IOException e) {
      if (isRollbackSupported(state)) {
        setFailure(new ProcedureException("Delete storage group failed " + state));
      } else {
        LOG.error(
            "Retriable error trying to delete storage group {}, state {}",
            deleteSgSchema.getName(),
            state,
            e);
        if (getCycles() > retryThreshold) {
          setFailure(new ProcedureException("State stuck at " + state));
        }
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, DeleteStorageGroupState state)
      throws IOException, InterruptedException {
    switch (state) {
      case DELETE_PRE:
      case INVALIDATE_CACHE:
        LOG.info("Rollback preDeleted:{}", deleteSgSchema.getName());
        env.preDelete(PreDeleteStorageGroupPlan.PreDeleteType.ROLLBACK, deleteSgSchema.getName());
        break;
    }
  }

  @Override
  protected boolean isRollbackSupported(DeleteStorageGroupState state) {
    switch (state) {
      case DELETE_PRE:
      case INVALIDATE_CACHE:
        return true;
    }
    return false;
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
    return DeleteStorageGroupState.DELETE_STORAGE_GROUP_PREPARE;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeInt(ProcedureFactory.ProcedureType.DELETE_STORAGE_GROUP_PROCEDURE.ordinal());
    super.serialize(stream);
    ThriftConfigNodeSerDeUtils.serializeTStorageGroupSchema(deleteSgSchema, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      deleteSgSchema = ThriftConfigNodeSerDeUtils.deserializeTStorageGroupSchema(byteBuffer);
    } catch (ThriftSerDeException e) {
      LOG.error("Error in deserialize DeleteStorageGroupProcedure", e);
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof DeleteStorageGroupProcedure) {
      DeleteStorageGroupProcedure thatProc = (DeleteStorageGroupProcedure) that;
      return thatProc.getProcId() == this.getProcId()
          && thatProc.getState() == this.getState()
          && thatProc.deleteSgSchema.equals(this.getDeleteSgSchema());
    }
    return false;
  }
}
