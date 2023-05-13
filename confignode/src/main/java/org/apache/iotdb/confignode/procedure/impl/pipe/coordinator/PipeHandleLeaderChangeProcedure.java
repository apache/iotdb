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

package org.apache.iotdb.confignode.procedure.impl.pipe.coordinator;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.confignode.consensus.request.write.pipe.coordinator.PipeHandleLeaderChangePlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.node.AbstractNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.node.AddConfigNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.node.RemoveConfigNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.node.RemoveDataNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.pipe.plugin.CreatePipePluginProcedure;
import org.apache.iotdb.confignode.procedure.state.pipe.coordinator.HandleLeaderChangeState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.pipe.api.exception.PipeManagementException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

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

/**
 * This class extends {@link AbstractNodeProcedure} to make sure that when a {@link
 * PipeHandleLeaderChangeProcedure} is executed, the {@link AddConfigNodeProcedure}, {@link
 * RemoveConfigNodeProcedure} or {@link RemoveDataNodeProcedure} will not be executed at the same
 * time.
 */
public class PipeHandleLeaderChangeProcedure
    extends AbstractNodeProcedure<HandleLeaderChangeState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreatePipePluginProcedure.class);

  private static final int RETRY_THRESHOLD = 3;

  protected boolean isRollbackFromHandleOnDataNodesSuccessful = false;

  private Map<TConsensusGroupId, Pair<Integer, Integer>> leaderMap = new HashMap<>();

  public PipeHandleLeaderChangeProcedure() {
    super();
  }

  public PipeHandleLeaderChangeProcedure(Map<TConsensusGroupId, Pair<Integer, Integer>> leaderMap) {
    super();
    this.leaderMap = leaderMap;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, HandleLeaderChangeState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    try {
      switch (state) {
        case HANDLE_ON_CONFIG_NODES:
          return executeFromHandleOnConfigNodes(env);
        case HANDLE_ON_DATA_NODES:
          return executeFromHandleOnDataNodes(env);
      }
    } catch (Exception e) {
      if (isRollbackSupported(state)) {
        LOGGER.error("HandleLeaderChangeProcedure failed in state {}, will rollback", state, e);
        setFailure(new ProcedureException(e.getMessage()));
      } else {
        LOGGER.error("Retrievable error trying to handle leader change, state: {}", state, e);
        if (getCycles() > RETRY_THRESHOLD) {
          LOGGER.error("Fail to handle leader change after {} retries", getCycles());
          setFailure(new ProcedureException(e.getMessage()));
        }
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  private Flow executeFromHandleOnConfigNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("HandleLeaderChangeProcedure: executeFromHandleOnConfigNodes");

    final ConfigManager configNodeManager = env.getConfigManager();

    Map<TConsensusGroupId, Integer> newLeaderMap = new HashMap<>();
    leaderMap.forEach((regionId, leaderPair) -> newLeaderMap.put(regionId, leaderPair.getRight()));
    final PipeHandleLeaderChangePlan pipeHandleLeaderChangePlan =
        new PipeHandleLeaderChangePlan(newLeaderMap);

    final ConsensusWriteResponse response =
        configNodeManager.getConsensusManager().write(pipeHandleLeaderChangePlan);
    if (!response.isSuccessful()) {
      throw new PipeManagementException(response.getErrorMessage());
    }

    setNextState(HandleLeaderChangeState.HANDLE_ON_DATA_NODES);
    return Flow.HAS_MORE_STATE;
  }

  private Flow executeFromHandleOnDataNodes(ConfigNodeProcedureEnv env) throws IOException {
    LOGGER.info("HandleLeaderChangeProcedure: executeFromHandleOnDataNodes");

    pushPipeMetaToDataNodes(env);

    return Flow.NO_MORE_STATE;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, HandleLeaderChangeState state)
      throws IOException, InterruptedException, ProcedureException {
    switch (state) {
      case HANDLE_ON_CONFIG_NODES:
        if (!isRollbackFromHandleOnDataNodesSuccessful) {
          rollbackFromHandleOnConfigNodes(env);
        }
        break;
      case HANDLE_ON_DATA_NODES:
        rollbackFromHandleOnConfigNodes(env);
        rollbackFromHandleOnDataNodes(env);
        isRollbackFromHandleOnDataNodesSuccessful = true;
        break;
    }
  }

  private void rollbackFromHandleOnConfigNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("HandleLeaderChangeProcedure: rollbackFromHandleOnConfigNodes");

    final ConfigManager configNodeManager = env.getConfigManager();

    Map<TConsensusGroupId, Integer> oldLeaderMap = new HashMap<>();
    leaderMap.forEach((regionId, leaderPair) -> oldLeaderMap.put(regionId, leaderPair.getLeft()));
    final PipeHandleLeaderChangePlan pipeHandleLeaderChangePlan =
        new PipeHandleLeaderChangePlan(oldLeaderMap);

    final ConsensusWriteResponse response =
        configNodeManager.getConsensusManager().write(pipeHandleLeaderChangePlan);
    if (!response.isSuccessful()) {
      throw new PipeManagementException(response.getErrorMessage());
    }
  }

  private void rollbackFromHandleOnDataNodes(ConfigNodeProcedureEnv env) throws IOException {
    LOGGER.info("CreatePipePluginProcedure: rollbackFromCreateOnDataNodes");

    pushPipeMetaToDataNodes(env);
  }

  protected void pushPipeMetaToDataNodes(ConfigNodeProcedureEnv env) throws IOException {
    final List<ByteBuffer> pipeMetaBinaryList = new ArrayList<>();
    for (PipeMeta pipeMeta :
        env.getConfigManager()
            .getPipeManager()
            .getPipeTaskCoordinator()
            .getPipeTaskInfo()
            .getPipeMetaList()) {
      pipeMetaBinaryList.add(pipeMeta.serialize());
    }

    if (RpcUtils.squashResponseStatusList(env.pushPipeMetaToDataNodes(pipeMetaBinaryList)).getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeManagementException("Failed to push pipe meta list to data nodes");
    }
  }

  @Override
  protected HandleLeaderChangeState getState(int stateId) {
    return HandleLeaderChangeState.values()[stateId];
  }

  @Override
  protected int getStateId(HandleLeaderChangeState handleLeaderChangeState) {
    return handleLeaderChangeState.ordinal();
  }

  @Override
  protected HandleLeaderChangeState getInitialState() {
    return HandleLeaderChangeState.HANDLE_ON_CONFIG_NODES;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.HANDLE_LEADER_CHANGE_PROCEDURE.getTypeCode());
    super.serialize(stream);
    stream.writeBoolean(isRollbackFromHandleOnDataNodesSuccessful);
    stream.writeInt(leaderMap.size());
    for (Map.Entry<TConsensusGroupId, Pair<Integer, Integer>> entry : leaderMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey().getId(), stream);
      ReadWriteIOUtils.write(entry.getValue().getLeft(), stream);
      ReadWriteIOUtils.write(entry.getValue().getRight(), stream);
    }
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    isRollbackFromHandleOnDataNodesSuccessful = ReadWriteIOUtils.readBool(byteBuffer);
    int size = byteBuffer.getInt();
    for (int i = 0; i < size; ++i) {
      leaderMap.put(
          new TConsensusGroupId(
              TConsensusGroupType.DataRegion, ReadWriteIOUtils.readInt(byteBuffer)),
          new Pair<>(ReadWriteIOUtils.readInt(byteBuffer), ReadWriteIOUtils.readInt(byteBuffer)));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PipeHandleLeaderChangeProcedure that = (PipeHandleLeaderChangeProcedure) o;
    return this.leaderMap.equals(that.leaderMap)
        && this.isRollbackFromHandleOnDataNodesSuccessful
            == that.isRollbackFromHandleOnDataNodesSuccessful;
  }

  @Override
  public int hashCode() {
    return Objects.hash(leaderMap, isRollbackFromHandleOnDataNodesSuccessful);
  }
}
