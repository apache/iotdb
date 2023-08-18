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

package org.apache.iotdb.confignode.procedure.impl.pipe;

import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.node.AbstractNodeProcedure;
import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;
import org.apache.iotdb.confignode.procedure.state.pipe.task.OperatePipeTaskState;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaResp;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This procedure manages 4 kinds of PIPE operations: {@link PipeTaskOperation#CREATE_PIPE}, {@link
 * PipeTaskOperation#START_PIPE}, {@link PipeTaskOperation#STOP_PIPE} and {@link
 * PipeTaskOperation#DROP_PIPE}.
 *
 * <p>This class extends {@link AbstractNodeProcedure} to make sure that pipe task procedures can be
 * executed in sequence and node procedures can be locked when a pipe task procedure is running.
 */
public abstract class AbstractOperatePipeProcedureV2
    extends AbstractNodeProcedure<OperatePipeTaskState> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractOperatePipeProcedureV2.class);

  private static final int RETRY_THRESHOLD = 1;

  // Only used in rollback to reduce the number of network calls
  protected boolean isRollbackFromOperateOnDataNodesSuccessful = false;

  // This variable should not be serialized into procedure store,
  // putting it here is just for convenience
  protected AtomicReference<PipeTaskInfo> pipeTaskInfo;

  @Override
  protected ProcedureLockState acquireLock(ConfigNodeProcedureEnv configNodeProcedureEnv) {
    configNodeProcedureEnv.getSchedulerLock().lock();
    try {
      if (configNodeProcedureEnv.getNodeLock().tryLock(this)) {
        pipeTaskInfo =
            configNodeProcedureEnv
                .getConfigManager()
                .getPipeManager()
                .getPipeTaskCoordinator()
                .lock();
        LOGGER.info("ProcedureId {} acquire lock.", getProcId());
        return ProcedureLockState.LOCK_ACQUIRED;
      }
      configNodeProcedureEnv.getNodeLock().waitProcedure(this);
      LOGGER.info("ProcedureId {} wait for lock.", getProcId());
      return ProcedureLockState.LOCK_EVENT_WAIT;
    } finally {
      configNodeProcedureEnv.getSchedulerLock().unlock();
    }
  }

  @Override
  protected void releaseLock(ConfigNodeProcedureEnv configNodeProcedureEnv) {
    configNodeProcedureEnv.getSchedulerLock().lock();
    try {
      LOGGER.info("ProcedureId {} release lock.", getProcId());
      if (pipeTaskInfo != null) {
        configNodeProcedureEnv
            .getConfigManager()
            .getPipeManager()
            .getPipeTaskCoordinator()
            .unlock();
      }
      if (configNodeProcedureEnv.getNodeLock().releaseLock(this)) {
        configNodeProcedureEnv
            .getNodeLock()
            .wakeWaitingProcedures(configNodeProcedureEnv.getScheduler());
      }
    } finally {
      configNodeProcedureEnv.getSchedulerLock().unlock();
    }
  }

  protected abstract PipeTaskOperation getOperation();

  /**
   * Execute at state {@link OperatePipeTaskState#VALIDATE_TASK}.
   *
   * @throws PipeException if validation for pipe parameters failed
   */
  protected abstract void executeFromValidateTask(ConfigNodeProcedureEnv env) throws PipeException;

  /** Execute at state {@link OperatePipeTaskState#CALCULATE_INFO_FOR_TASK}. */
  protected abstract void executeFromCalculateInfoForTask(ConfigNodeProcedureEnv env);

  /**
   * Execute at state {@link OperatePipeTaskState#WRITE_CONFIG_NODE_CONSENSUS}.
   *
   * @throws PipeException if configNode consensus write failed
   */
  protected abstract void executeFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env)
      throws PipeException;

  /**
   * Execute at state {@link OperatePipeTaskState#OPERATE_ON_DATA_NODES}.
   *
   * @throws PipeException if push pipe metas to dataNodes failed
   * @throws IOException Exception when Serializing to byte buffer
   */
  protected abstract void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws PipeException, IOException;

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, OperatePipeTaskState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    try {
      switch (state) {
        case VALIDATE_TASK:
          executeFromValidateTask(env);
          setNextState(OperatePipeTaskState.CALCULATE_INFO_FOR_TASK);
          break;
        case CALCULATE_INFO_FOR_TASK:
          executeFromCalculateInfoForTask(env);
          setNextState(OperatePipeTaskState.WRITE_CONFIG_NODE_CONSENSUS);
          break;
        case WRITE_CONFIG_NODE_CONSENSUS:
          executeFromWriteConfigNodeConsensus(env);
          setNextState(OperatePipeTaskState.OPERATE_ON_DATA_NODES);
          break;
        case OPERATE_ON_DATA_NODES:
          executeFromOperateOnDataNodes(env);
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException(
              String.format("Unknown state during executing operatePipeProcedure, %s", state));
      }
    } catch (Exception e) {
      // Retry before rollback
      if (getCycles() < RETRY_THRESHOLD) {
        LOGGER.warn(
            "Encountered error when trying to {} at state [{}], retry [{}/{}]",
            getOperation(),
            state,
            getCycles() + 1,
            RETRY_THRESHOLD,
            e);
        // Wait 3s for next retry
        TimeUnit.MILLISECONDS.sleep(3000L);
      } else {
        LOGGER.warn(
            "All {} retries failed when trying to {} at state [{}], will rollback...",
            RETRY_THRESHOLD,
            getOperation(),
            state,
            e);
        setFailure(
            new ProcedureException(
                String.format("Fail to %s because %s", getOperation().name(), e.getMessage())));
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected boolean isRollbackSupported(OperatePipeTaskState state) {
    return true;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, OperatePipeTaskState state)
      throws IOException, InterruptedException, ProcedureException {
    switch (state) {
      case VALIDATE_TASK:
        rollbackFromValidateTask(env);
        break;
      case CALCULATE_INFO_FOR_TASK:
        rollbackFromCalculateInfoForTask(env);
        break;
      case WRITE_CONFIG_NODE_CONSENSUS:
        // rollbackFromWriteConfigNodeConsensus can be called before rollbackFromOperateOnDataNodes
        // so we need to check if rollbackFromOperateOnDataNodes is successful executed
        // if yes, we don't need to call rollbackFromWriteConfigNodeConsensus again
        if (!isRollbackFromOperateOnDataNodesSuccessful) {
          rollbackFromWriteConfigNodeConsensus(env);
        }
        break;
      case OPERATE_ON_DATA_NODES:
        // We have to make sure that rollbackFromOperateOnDataNodes is executed before
        // rollbackFromWriteConfigNodeConsensus, because rollbackFromOperateOnDataNodes is
        // executed based on the consensus of config nodes that is written by
        // rollbackFromWriteConfigNodeConsensus
        rollbackFromWriteConfigNodeConsensus(env);
        rollbackFromOperateOnDataNodes(env);
        isRollbackFromOperateOnDataNodesSuccessful = true;
        break;
      default:
        LOGGER.error("Unsupported roll back STATE [{}]", state);
    }
  }

  protected abstract void rollbackFromValidateTask(ConfigNodeProcedureEnv env);

  protected abstract void rollbackFromCalculateInfoForTask(ConfigNodeProcedureEnv env);

  protected abstract void rollbackFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env);

  protected abstract void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws IOException;

  @Override
  protected OperatePipeTaskState getState(int stateId) {
    return OperatePipeTaskState.values()[stateId];
  }

  @Override
  protected int getStateId(OperatePipeTaskState state) {
    return state.ordinal();
  }

  @Override
  protected OperatePipeTaskState getInitialState() {
    return OperatePipeTaskState.VALIDATE_TASK;
  }

  /**
   * Pushing all the pipeMeta's to all the dataNodes, forcing an update to the pipe's runtime state.
   *
   * @param env ConfigNodeProcedureEnv
   * @return The responseMap after pushing pipe meta
   * @throws IOException Exception when Serializing to byte buffer
   */
  protected Map<Integer, TPushPipeMetaResp> pushPipeMetaToDataNodes(ConfigNodeProcedureEnv env)
      throws IOException {
    final List<ByteBuffer> pipeMetaBinaryList = new ArrayList<>();
    for (PipeMeta pipeMeta : pipeTaskInfo.get().getPipeMetaList()) {
      pipeMetaBinaryList.add(pipeMeta.serialize());
    }

    return env.pushAllPipeMetaToDataNodes(pipeMetaBinaryList);
  }

  /**
   * Parsing the given pipe's or all pipes' pushPipeMeta exceptions to string.
   *
   * @param pipeName The given pipe's pipe name, null if report all pipes' exceptions.
   * @param respMap The responseMap after pushing pipe meta
   * @return Error messages for the given pipe after pushing pipe meta
   */
  protected String parsePushPipeMetaExceptionForPipe(
      String pipeName, Map<Integer, TPushPipeMetaResp> respMap) {
    final StringBuilder exceptionMessageBuilder = new StringBuilder();

    for (Map.Entry<Integer, TPushPipeMetaResp> respEntry : respMap.entrySet()) {
      int dataNodeId = respEntry.getKey();
      TPushPipeMetaResp resp = respEntry.getValue();

      if (resp.getStatus().getCode() == TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode()) {
        if (!resp.isSetExceptionMessages()) {
          exceptionMessageBuilder.append(
              String.format(
                  "DataNodeId: %s, Message: Internal error while processing pushPipeMeta on dataNodes.",
                  dataNodeId));
          continue;
        }

        AtomicBoolean hasException = new AtomicBoolean(false);

        resp.getExceptionMessages()
            .forEach(
                message -> {
                  // Ignore the timeStamp for simplicity
                  if (pipeName == null) {
                    hasException.set(true);
                    exceptionMessageBuilder.append(
                        String.format(
                            "PipeName: %s, Message: %s",
                            message.getPipeName(), message.getMessage()));
                  } else if (pipeName.equals(message.getPipeName())) {
                    hasException.set(true);
                    exceptionMessageBuilder.append(
                        String.format("Message: %s", message.getMessage()));
                  }
                });

        if (hasException.get()) {
          // Only print dataNodeId if the given pipe meets exception on that node
          exceptionMessageBuilder.insert(0, String.format("DataNodeId: %s, ", dataNodeId));
          exceptionMessageBuilder.append(". ");
        }
      }
    }
    return exceptionMessageBuilder.toString();
  }

  protected void pushPipeMetaToDataNodesIgnoreException(ConfigNodeProcedureEnv env) {
    try {
      // Ignore the exceptions reported
      pushPipeMetaToDataNodes(env);
    } catch (Exception e) {
      LOGGER.info("Failed to push pipe meta list to data nodes, will retry later.", e);
    }
  }

  /**
   * Pushing one pipeMeta to all the dataNodes, forcing an update to the pipe's runtime state.
   *
   * @param pipeName pipe name of the pipe to push
   * @param env ConfigNodeProcedureEnv
   * @return The responseMap after pushing pipe meta
   * @throws IOException Exception when Serializing to byte buffer
   */
  protected Map<Integer, TPushPipeMetaResp> pushSinglePipeMetaToDataNodes(
      String pipeName, ConfigNodeProcedureEnv env) throws IOException {
    return env.pushSinglePipeMetaToDataNodes(
        pipeTaskInfo.get().getPipeMetaByPipeName(pipeName).serialize());
  }

  /**
   * Drop a pipe on all the dataNodes.
   *
   * @param pipeName pipe name of the pipe to drop
   * @param env ConfigNodeProcedureEnv
   * @return The responseMap after pushing pipe meta
   */
  protected Map<Integer, TPushPipeMetaResp> dropSinglePipeOnDataNodes(
      String pipeName, ConfigNodeProcedureEnv env) {
    return env.dropSinglePipeOnDataNodes(pipeName);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    super.serialize(stream);
    ReadWriteIOUtils.write(isRollbackFromOperateOnDataNodesSuccessful, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    isRollbackFromOperateOnDataNodesSuccessful = ReadWriteIOUtils.readBool(byteBuffer);
  }
}
