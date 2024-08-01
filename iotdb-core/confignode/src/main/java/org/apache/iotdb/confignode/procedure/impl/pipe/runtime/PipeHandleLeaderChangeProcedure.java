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

package org.apache.iotdb.confignode.procedure.impl.pipe.runtime;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleLeaderChangePlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.AbstractOperatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PipeHandleLeaderChangeProcedure extends AbstractOperatePipeProcedureV2 {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeHandleLeaderChangeProcedure.class);

  private Map<TConsensusGroupId, Pair<Integer, Integer>> regionGroupToOldAndNewLeaderPairMap =
      new HashMap<>();

  public PipeHandleLeaderChangeProcedure() {
    super();
  }

  public PipeHandleLeaderChangeProcedure(
      Map<TConsensusGroupId, Pair<Integer, Integer>> regionGroupToOldAndNewLeaderPairMap) {
    super();
    this.regionGroupToOldAndNewLeaderPairMap = regionGroupToOldAndNewLeaderPairMap;
  }

  @Override
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.HANDLE_LEADER_CHANGE;
  }

  @Override
  public boolean executeFromValidateTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeHandleLeaderChangeProcedure: executeFromValidateTask");

    // Nothing needs to be checked
    return true;
  }

  @Override
  public void executeFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeHandleLeaderChangeProcedure: executeFromCalculateInfoForTask");

    // Nothing needs to be calculated
  }

  @Override
  public void executeFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeHandleLeaderChangeProcedure: executeFromHandleOnConfigNodes");

    final Map<TConsensusGroupId, Integer> newConsensusGroupIdToLeaderConsensusIdMap =
        new HashMap<>();
    regionGroupToOldAndNewLeaderPairMap.forEach(
        (regionGroupId, oldNewLeaderPair) ->
            newConsensusGroupIdToLeaderConsensusIdMap.put(
                regionGroupId, oldNewLeaderPair.getRight()));

    final PipeHandleLeaderChangePlan pipeHandleLeaderChangePlan =
        new PipeHandleLeaderChangePlan(newConsensusGroupIdToLeaderConsensusIdMap);
    TSStatus response;
    try {
      response = env.getConfigManager().getConsensusManager().write(pipeHandleLeaderChangePlan);
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      response = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      response.setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(response.getMessage());
    }
  }

  @Override
  public void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeHandleLeaderChangeProcedure: executeFromHandleOnDataNodes");

    pushPipeMetaToDataNodesIgnoreException(env);
  }

  @Override
  public void rollbackFromValidateTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeHandleLeaderChangeProcedure: rollbackFromValidateTask");

    // Nothing to do
  }

  @Override
  public void rollbackFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeHandleLeaderChangeProcedure: rollbackFromCalculateInfoForTask");

    // Nothing to do
  }

  @Override
  public void rollbackFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeHandleLeaderChangeProcedure: rollbackFromHandleOnConfigNodes");

    // Nothing to do
  }

  @Override
  public void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeHandleLeaderChangeProcedure: rollbackFromCreateOnDataNodes");

    // Nothing to do
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.PIPE_HANDLE_LEADER_CHANGE_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(regionGroupToOldAndNewLeaderPairMap.size(), stream);
    for (Map.Entry<TConsensusGroupId, Pair<Integer, Integer>> entry :
        regionGroupToOldAndNewLeaderPairMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey().getId(), stream);
      ReadWriteIOUtils.write(entry.getValue().getLeft(), stream);
      ReadWriteIOUtils.write(entry.getValue().getRight(), stream);
    }
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    final int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      final int dataRegionGroupId = ReadWriteIOUtils.readInt(byteBuffer);
      final int oldDataRegionLeaderId = ReadWriteIOUtils.readInt(byteBuffer);
      final int newDataRegionLeaderId = ReadWriteIOUtils.readInt(byteBuffer);
      regionGroupToOldAndNewLeaderPairMap.put(
          new TConsensusGroupId(TConsensusGroupType.DataRegion, dataRegionGroupId),
          new Pair<>(oldDataRegionLeaderId, newDataRegionLeaderId));
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
    return getProcId() == that.getProcId()
        && getCurrentState().equals(that.getCurrentState())
        && getCycles() == that.getCycles()
        && this.regionGroupToOldAndNewLeaderPairMap.equals(
            that.regionGroupToOldAndNewLeaderPairMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getProcId(), getCurrentState(), getCycles(), regionGroupToOldAndNewLeaderPairMap);
  }
}
