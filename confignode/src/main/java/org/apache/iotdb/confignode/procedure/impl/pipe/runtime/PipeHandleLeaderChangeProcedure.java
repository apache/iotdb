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
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleLeaderChangePlan;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.AbstractOperatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

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

  private Map<TConsensusGroupId, Pair<Integer, Integer>> dataRegionGroupToOldAndNewLeaderPairMap =
      new HashMap<>();

  public PipeHandleLeaderChangeProcedure() {
    super();
  }

  public PipeHandleLeaderChangeProcedure(
      Map<TConsensusGroupId, Pair<Integer, Integer>> dataRegionGroupToOldAndNewLeaderPairMap) {
    super();
    this.dataRegionGroupToOldAndNewLeaderPairMap = dataRegionGroupToOldAndNewLeaderPairMap;
  }

  @Override
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.HANDLE_LEADER_CHANGE;
  }

  @Override
  protected void executeFromValidateTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeHandleLeaderChangeProcedure: executeFromValidateTask");

    // nothing needs to be checked
  }

  @Override
  protected void executeFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeHandleLeaderChangeProcedure: executeFromCalculateInfoForTask");

    // nothing needs to be calculated
  }

  @Override
  protected void executeFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeHandleLeaderChangeProcedure: executeFromHandleOnConfigNodes");

    final Map<TConsensusGroupId, Integer> newDataRegionGroupIdToLeaderDataRegionIdMap =
        new HashMap<>();
    dataRegionGroupToOldAndNewLeaderPairMap.forEach(
        (regionGroupId, oldNewLeaderPair) ->
            newDataRegionGroupIdToLeaderDataRegionIdMap.put(
                regionGroupId, oldNewLeaderPair.getRight()));

    final PipeHandleLeaderChangePlan pipeHandleLeaderChangePlan =
        new PipeHandleLeaderChangePlan(newDataRegionGroupIdToLeaderDataRegionIdMap);

    final ConsensusWriteResponse response =
        env.getConfigManager().getConsensusManager().write(pipeHandleLeaderChangePlan);
    if (!response.isSuccessful()) {
      throw new PipeException(response.getErrorMessage());
    }
  }

  @Override
  protected void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeHandleLeaderChangeProcedure: executeFromHandleOnDataNodes");

    pushPipeMetaToDataNodesIgnoreException(env);
  }

  @Override
  protected void rollbackFromValidateTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeHandleLeaderChangeProcedure: rollbackFromValidateTask");

    // nothing to do
  }

  @Override
  protected void rollbackFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeHandleLeaderChangeProcedure: rollbackFromCalculateInfoForTask");

    // nothing to do
  }

  @Override
  protected void rollbackFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeHandleLeaderChangeProcedure: rollbackFromHandleOnConfigNodes");

    final Map<TConsensusGroupId, Integer> oldDataRegionGroupIdToLeaderDataRegionIdMap =
        new HashMap<>();
    dataRegionGroupToOldAndNewLeaderPairMap.forEach(
        (regionGroupId, oldNewLeaderPair) ->
            oldDataRegionGroupIdToLeaderDataRegionIdMap.put(
                regionGroupId, oldNewLeaderPair.getLeft()));

    final PipeHandleLeaderChangePlan pipeHandleLeaderChangePlan =
        new PipeHandleLeaderChangePlan(oldDataRegionGroupIdToLeaderDataRegionIdMap);

    final ConsensusWriteResponse response =
        env.getConfigManager().getConsensusManager().write(pipeHandleLeaderChangePlan);
    if (!response.isSuccessful()) {
      throw new PipeException(response.getErrorMessage());
    }
  }

  @Override
  protected void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeHandleLeaderChangeProcedure: rollbackFromCreateOnDataNodes");

    pushPipeMetaToDataNodesIgnoreException(env);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.PIPE_HANDLE_LEADER_CHANGE_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(dataRegionGroupToOldAndNewLeaderPairMap.size(), stream);
    for (Map.Entry<TConsensusGroupId, Pair<Integer, Integer>> entry :
        dataRegionGroupToOldAndNewLeaderPairMap.entrySet()) {
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
      dataRegionGroupToOldAndNewLeaderPairMap.put(
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
    return this.dataRegionGroupToOldAndNewLeaderPairMap.equals(
        that.dataRegionGroupToOldAndNewLeaderPairMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataRegionGroupToOldAndNewLeaderPairMap);
  }
}
