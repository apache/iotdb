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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.commons.utils.ThriftConfigNodeSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.write.DeleteStorageGroupReq;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.state.DeleteStorageGroupState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.mpp.rpc.thrift.InternalService;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateCacheReq;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DeleteStorageGroupProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, DeleteStorageGroupState> {
  private static final Logger LOG = LoggerFactory.getLogger(Procedure.class);
  private static final int retryThreshold = 5;

  private static boolean byPassForTest = false;

  private ConfigManager configManager;

  @TestOnly
  public static void setByPassForTest(boolean byPass) {
    byPassForTest = byPass;
  }

  private TStorageGroupSchema deleteSgSchema;

  public DeleteStorageGroupProcedure() {
    super();
  }

  public DeleteStorageGroupProcedure(
      TStorageGroupSchema deleteSgSchema, ConfigManager configManager) {
    super();
    this.deleteSgSchema = deleteSgSchema;
    this.configManager = configManager;
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
    String storageGroupName = deleteSgSchema.getName();
    List<TConsensusGroupId> dataRegionGroupIds = deleteSgSchema.getDataRegionGroupIds();
    List<TConsensusGroupId> schemaRegionGroupIds = deleteSgSchema.getSchemaRegionGroupIds();
    List<TRegionReplicaSet> dataRegionReplicaSets =
        new ArrayList<>(
            configManager.getPartitionManager().getRegionReplicaSets(dataRegionGroupIds));
    List<TRegionReplicaSet> schemaRegionReplicaSets =
        new ArrayList<>(
            configManager.getPartitionManager().getRegionReplicaSets(schemaRegionGroupIds));
    try {
      switch (state) {
        case DELETE_STORAGE_GROUP_PREPARE:
          // TODO: lock related ClusterSchemaInfo, PartitionInfo and Regions
          setNextState(DeleteStorageGroupState.DELETE_DATA_REGION);
          break;
        case DELETE_DATA_REGION:
          LOG.info("Delete dataRegions of {}", storageGroupName);
          if (byPassForTest || deleteRegion(env, dataRegionReplicaSets)) {
            setNextState(DeleteStorageGroupState.DELETE_SCHEMA_REGION);
          }
          break;
        case DELETE_SCHEMA_REGION:
          LOG.info("Delete schemaRegions of {}", storageGroupName);
          if (byPassForTest || deleteRegion(env, schemaRegionReplicaSets)) {
            setNextState(DeleteStorageGroupState.DELETE_CONFIG);
          }
          break;
        case DELETE_CONFIG:
          LOG.info("Delete config info of {}", storageGroupName);
          TSStatus status = deleteConfig(env, deleteSgSchema);
          if (verifySucceed(status)) {
            if (byPassForTest) {
              return Flow.NO_MORE_STATE;
            }
            setNextState(DeleteStorageGroupState.INVALIDATE_CACHE);
          } else if (getCycles() > retryThreshold) {
            setFailure(
                new org.apache.iotdb.confignode.procedure.exception.ProcedureException(
                    "Delete config info id failed, status is " + status));
          }
          break;
        case INVALIDATE_CACHE:
          LOG.info("Invalidate cache of {}", storageGroupName);
          invalidateCache(env, storageGroupName);
          return Flow.NO_MORE_STATE;
      }
    } catch (TException | IOException e) {
      LOG.error(
          "Retriable error trying to delete storage group {}, state {}",
          storageGroupName,
          state,
          e);
      if (getCycles() > retryThreshold) {
        setFailure(
            new org.apache.iotdb.confignode.procedure.exception.ProcedureException(
                "State stack at " + state));
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  private TSStatus deleteConfig(ConfigNodeProcedureEnv env, TStorageGroupSchema deleteSgSchema) {
    DeleteStorageGroupReq deleteStorageGroupReq = new DeleteStorageGroupReq(deleteSgSchema);
    return env.getConfigManager()
        .getClusterSchemaManager()
        .deleteStorageGroup(deleteStorageGroupReq);
  }

  private boolean deleteRegion(
      ConfigNodeProcedureEnv env, List<TRegionReplicaSet> regionReplicaSets) throws TException {
    for (TRegionReplicaSet dataRegionReplicaSet : regionReplicaSets) {
      TConsensusGroupId regionId = dataRegionReplicaSet.getRegionId();
      InternalService.Client dataNodeClient = null;
      try {
        dataNodeClient = env.getDataNodeClient(dataRegionReplicaSet);
        if (dataNodeClient != null) {
          TSStatus status = dataNodeClient.deleteRegion(regionId);
          if (status.getCode() != StatusUtils.OK.getCode()) {
            if (getCycles() > retryThreshold) {
              setFailure(
                  new org.apache.iotdb.confignode.procedure.exception.ProcedureException(
                      "Delete data region id=" + regionId + " failed, status is " + status));
            }
            return false;
          }
          LOG.info("Delete region {} success", regionId);
        }
      } catch (IOException e) {
        LOG.error("Connect dataRegion-{} failed", dataRegionReplicaSet.getRegionId(), e);
        if (getCycles() > retryThreshold) {
          setFailure(
              new ProcedureException(
                  "Delete data region id=" + regionId + " failed", e.getCause()));
        }
        return false;
      }
    }
    return true;
  }

  private void invalidateCache(ConfigNodeProcedureEnv env, String storageGroupName)
      throws IOException, TException {
    List<TDataNodeInfo> allDataNodes =
        env.getConfigManager().getNodeManager().getOnlineDataNodes(-1);
    TInvalidateCacheReq invalidateCacheReq = new TInvalidateCacheReq();
    invalidateCacheReq.setStorageGroup(true);
    invalidateCacheReq.setFullPath(storageGroupName);
    for (TDataNodeInfo dataNodeInfo : allDataNodes) {
      env.getDataNodeClient(dataNodeInfo.getLocation()).invalidateSchemaCache(invalidateCacheReq);
      env.getDataNodeClient(dataNodeInfo.getLocation())
          .invalidatePartitionCache(invalidateCacheReq);
    }
  }

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv clusterProcedureEnvironment,
      DeleteStorageGroupState deleteStorageGroupState)
      throws IOException, InterruptedException {}

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
  public void serialize(ByteBuffer byteBuffer) {
    byteBuffer.putInt(ProcedureFactory.ProcedureType.DELETE_STORAGE_GROUP_PROCEDURE.ordinal());
    super.serialize(byteBuffer);
    ThriftConfigNodeSerDeUtils.serializeTStorageGroupSchema(deleteSgSchema, byteBuffer);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      deleteSgSchema = ThriftConfigNodeSerDeUtils.deserializeTStorageGroupSchema(byteBuffer);
    } catch (ThriftSerDeException e) {
      LOG.error("error in deser", e);
    }
  }

  public boolean verifySucceed(TSStatus status) {
    return status.getCode() == StatusUtils.OK.getCode();
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
