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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleMetaChangePlan;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.AbstractOperatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.procedure.impl.pipe.util.PipeExternalSourceLoadBalancer;
import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaResp;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTERNAL_EXTRACTOR_PARALLELISM_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTERNAL_EXTRACTOR_PARALLELISM_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTERNAL_SOURCE_PARALLELISM_KEY;

public class PipeMetaSyncProcedure extends AbstractOperatePipeProcedureV2 {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeMetaSyncProcedure.class);

  private static final long MIN_EXECUTION_INTERVAL_MS =
      PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 / 2;
  // No need to serialize this field
  private static final AtomicLong LAST_EXECUTION_TIME = new AtomicLong(0);

  public PipeMetaSyncProcedure() {
    super();
  }

  @Override
  protected AtomicReference<PipeTaskInfo> acquireLockInternal(
      ConfigNodeProcedureEnv configNodeProcedureEnv) {
    return configNodeProcedureEnv
        .getConfigManager()
        .getPipeManager()
        .getPipeTaskCoordinator()
        .tryLock();
  }

  @Override
  protected ProcedureLockState acquireLock(ConfigNodeProcedureEnv configNodeProcedureEnv) {
    // Skip the procedure if the last execution time is within the minimum execution interval.
    // Often used to prevent the procedure from being executed too frequently when system reboot.
    if (System.currentTimeMillis() - LAST_EXECUTION_TIME.get() < MIN_EXECUTION_INTERVAL_MS) {
      // Skip by setting the pipeTaskInfo to null
      pipeTaskInfo = null;
      LOGGER.debug(
          "PipeMetaSyncProcedure: acquireLock, skip the procedure due to the last execution time {}",
          LAST_EXECUTION_TIME.get());
      return ProcedureLockState.LOCK_ACQUIRED;
    }

    return super.acquireLock(configNodeProcedureEnv);
  }

  @Override
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.SYNC_PIPE_META;
  }

  @Override
  public boolean executeFromValidateTask(ConfigNodeProcedureEnv env) {
    LOGGER.debug("PipeMetaSyncProcedure: executeFromValidateTask");

    LAST_EXECUTION_TIME.set(System.currentTimeMillis());
    return true;
  }

  @Override
  public void executeFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    LOGGER.debug("PipeMetaSyncProcedure: executeFromCalculateInfoForTask");

    // Re-balance the external source tasks here in case of any changes in the dataRegion
    pipeTaskInfo
        .get()
        .getPipeMetaList()
        .forEach(
            pipeMeta -> {
              if (!pipeMeta.getStaticMeta().isSourceExternal()) {
                return;
              }

              final PipeExternalSourceLoadBalancer loadBalancer =
                  new PipeExternalSourceLoadBalancer(
                      pipeMeta
                          .getStaticMeta()
                          .getSourceParameters()
                          .getStringOrDefault(
                              Arrays.asList(
                                  PipeSourceConstant.EXTERNAL_EXTRACTOR_BALANCE_STRATEGY_KEY,
                                  PipeSourceConstant.EXTERNAL_SOURCE_BALANCE_STRATEGY_KEY),
                              PipeSourceConstant.EXTERNAL_EXTRACTOR_BALANCE_PROPORTION_STRATEGY));
              final int parallelism =
                  pipeMeta
                      .getStaticMeta()
                      .getSourceParameters()
                      .getIntOrDefault(
                          Arrays.asList(
                              EXTERNAL_EXTRACTOR_PARALLELISM_KEY, EXTERNAL_SOURCE_PARALLELISM_KEY),
                          EXTERNAL_EXTRACTOR_PARALLELISM_DEFAULT_VALUE);
              final Map<Integer, PipeTaskMeta> consensusGroupIdToTaskMetaMap =
                  pipeMeta.getRuntimeMeta().getConsensusGroupId2TaskMetaMap();

              // do balance here
              final Map<Integer, Integer> taskId2LeaderDataNodeId =
                  loadBalancer.balance(
                      parallelism,
                      pipeMeta.getStaticMeta(),
                      ConfigNode.getInstance().getConfigManager());

              taskId2LeaderDataNodeId.forEach(
                  (taskIndex, newLeader) -> {
                    if (consensusGroupIdToTaskMetaMap.containsKey(taskIndex)) {
                      consensusGroupIdToTaskMetaMap.get(taskIndex).setLeaderNodeId(newLeader);
                    } else {
                      consensusGroupIdToTaskMetaMap.put(
                          taskIndex, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, newLeader));
                    }
                  });
              final Set<Integer> taskIdToRemove =
                  consensusGroupIdToTaskMetaMap.keySet().stream()
                      .filter(taskId -> !taskId2LeaderDataNodeId.containsKey(taskId))
                      .collect(Collectors.toSet());
              taskIdToRemove.forEach(consensusGroupIdToTaskMetaMap::remove);
            });
  }

  @Override
  public void executeFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    LOGGER.debug("PipeMetaSyncProcedure: executeFromWriteConfigNodeConsensus");

    final List<PipeMeta> pipeMetaList = new ArrayList<>();
    for (final PipeMeta pipeMeta : pipeTaskInfo.get().getPipeMetaList()) {
      pipeMetaList.add(pipeMeta);
    }

    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new PipeHandleMetaChangePlan(pipeMetaList));
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
  public void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws PipeException, IOException {
    LOGGER.debug("PipeMetaSyncProcedure: executeFromOperateOnDataNodes");

    Map<Integer, TPushPipeMetaResp> respMap = pushPipeMetaToDataNodes(env);
    if (pipeTaskInfo.get().recordDataNodePushPipeMetaExceptions(respMap)) {
      throw new PipeException(
          String.format(
              "Failed to push pipe meta to dataNodes, details: %s",
              parsePushPipeMetaExceptionForPipe(null, respMap)));
    }
  }

  @Override
  public void rollbackFromValidateTask(ConfigNodeProcedureEnv env) {
    LOGGER.debug("PipeMetaSyncProcedure: rollbackFromValidateTask");

    // Do nothing
  }

  @Override
  public void rollbackFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    LOGGER.debug("PipeMetaSyncProcedure: rollbackFromCalculateInfoForTask");

    // Do nothing
  }

  @Override
  public void rollbackFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    LOGGER.debug("PipeMetaSyncProcedure: rollbackFromWriteConfigNodeConsensus");

    // Do nothing
  }

  @Override
  public void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.debug("PipeMetaSyncProcedure: rollbackFromOperateOnDataNodes");

    // Do nothing
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.PIPE_META_SYNC_PROCEDURE.getTypeCode());
    super.serialize(stream);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    return o instanceof PipeMetaSyncProcedure;
  }

  @Override
  public int hashCode() {
    return 0;
  }
}
