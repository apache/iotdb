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
import org.apache.iotdb.confignode.manager.pipe.metric.PipeProcedureMetrics;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.node.AbstractNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.pipe.runtime.PipeMetaSyncProcedure;
import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;
import org.apache.iotdb.confignode.procedure.state.pipe.task.OperatePipeTaskState;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaResp;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This procedure manages 2 kinds of PIPE operations:
 *
 * <ul>
 *   <li>PIPE user task operations:
 *       <ul>
 *         <li>{@link PipeTaskOperation#CREATE_PIPE}
 *         <li>{@link PipeTaskOperation#START_PIPE}
 *         <li>{@link PipeTaskOperation#STOP_PIPE}
 *         <li>{@link PipeTaskOperation#DROP_PIPE}
 *         <li>{@link PipeTaskOperation#ALTER_PIPE}
 *       </ul>
 *   <li>PIPE runtime task operations:
 *       <ul>
 *         <li>{@link PipeTaskOperation#HANDLE_LEADER_CHANGE}
 *         <li>{@link PipeTaskOperation#SYNC_PIPE_META}
 *         <li>{@link PipeTaskOperation#HANDLE_PIPE_META_CHANGE}
 *       </ul>
 * </ul>
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

  private static final String SKIP_PIPE_PROCEDURE_MESSAGE =
      "Try to start a RUNNING pipe or stop a STOPPED pipe, do nothing.";

  protected AtomicReference<PipeTaskInfo> acquireLockInternal(
      ConfigNodeProcedureEnv configNodeProcedureEnv) {
    return configNodeProcedureEnv
        .getConfigManager()
        .getPipeManager()
        .getPipeTaskCoordinator()
        .lock();
  }

  @Override
  protected ProcedureLockState acquireLock(ConfigNodeProcedureEnv configNodeProcedureEnv) {
    LOGGER.info("ProcedureId {} try to acquire pipe lock.", getProcId());
    pipeTaskInfo = acquireLockInternal(configNodeProcedureEnv);
    if (pipeTaskInfo == null) {
      LOGGER.warn("ProcedureId {} failed to acquire pipe lock.", getProcId());
    } else {
      LOGGER.info("ProcedureId {} acquired pipe lock.", getProcId());
    }

    final ProcedureLockState procedureLockState = super.acquireLock(configNodeProcedureEnv);
    switch (procedureLockState) {
      case LOCK_ACQUIRED:
        if (pipeTaskInfo == null) {
          LOGGER.warn(
              "ProcedureId {}: LOCK_ACQUIRED. The following procedure should not be executed without pipe lock.",
              getProcId());
        } else {
          LOGGER.info(
              "ProcedureId {}: LOCK_ACQUIRED. The following procedure should be executed with pipe lock.",
              getProcId());
        }
        break;
      case LOCK_EVENT_WAIT:
        if (pipeTaskInfo == null) {
          LOGGER.warn("ProcedureId {}: LOCK_EVENT_WAIT. Without acquiring pipe lock.", getProcId());
        } else {
          LOGGER.info("ProcedureId {}: LOCK_EVENT_WAIT. Pipe lock will be released.", getProcId());
          configNodeProcedureEnv
              .getConfigManager()
              .getPipeManager()
              .getPipeTaskCoordinator()
              .unlock();
          pipeTaskInfo = null;
        }
        break;
      default:
        if (pipeTaskInfo == null) {
          LOGGER.error(
              "ProcedureId {}: {}. Invalid lock state. Without acquiring pipe lock.",
              getProcId(),
              procedureLockState);
        } else {
          LOGGER.error(
              "ProcedureId {}: {}. Invalid lock state. Pipe lock will be released.",
              getProcId(),
              procedureLockState);
          configNodeProcedureEnv
              .getConfigManager()
              .getPipeManager()
              .getPipeTaskCoordinator()
              .unlock();
          pipeTaskInfo = null;
        }
        break;
    }
    return procedureLockState;
  }

  @Override
  protected void releaseLock(ConfigNodeProcedureEnv configNodeProcedureEnv) {
    super.releaseLock(configNodeProcedureEnv);

    if (pipeTaskInfo == null) {
      LOGGER.warn("ProcedureId {} release lock. No need to release pipe lock.", getProcId());
    } else {
      LOGGER.info("ProcedureId {} release lock. Pipe lock will be released.", getProcId());
      if (this instanceof PipeMetaSyncProcedure) {
        configNodeProcedureEnv
            .getConfigManager()
            .getPipeManager()
            .getPipeTaskCoordinator()
            .updateLastSyncedVersion();
      }
      PipeProcedureMetrics.getInstance()
          .updateTimer(this.getOperation().getName(), this.elapsedTime());
      configNodeProcedureEnv.getConfigManager().getPipeManager().getPipeTaskCoordinator().unlock();
      pipeTaskInfo = null;
    }
  }

  protected abstract PipeTaskOperation getOperation();

  /**
   * Execute at state {@link OperatePipeTaskState#VALIDATE_TASK}.
   *
   * @return true if this procedure can skip subsequent stages (start RUNNING pipe or stop STOPPED
   *     pipe without runtime exception)
   * @throws PipeException if validation for pipe parameters failed
   */
  public abstract boolean executeFromValidateTask(ConfigNodeProcedureEnv env) throws PipeException;

  /** Execute at state {@link OperatePipeTaskState#CALCULATE_INFO_FOR_TASK}. */
  public abstract void executeFromCalculateInfoForTask(ConfigNodeProcedureEnv env);

  /**
   * Execute at state {@link OperatePipeTaskState#WRITE_CONFIG_NODE_CONSENSUS}.‘
   *
   * @throws PipeException if configNode consensus write failed
   */
  public abstract void executeFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env)
      throws PipeException;

  /**
   * Execute at state {@link OperatePipeTaskState#OPERATE_ON_DATA_NODES}.
   *
   * @throws PipeException if push pipe metas to dataNodes failed
   * @throws IOException Exception when Serializing to byte buffer
   */
  public abstract void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws PipeException, IOException;

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, OperatePipeTaskState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    if (pipeTaskInfo == null) {
      LOGGER.warn(
          "ProcedureId {}: Pipe lock is not acquired, executeFromState's execution will be skipped.",
          getProcId());
      return Flow.NO_MORE_STATE;
    }

    try {
      switch (state) {
        case VALIDATE_TASK:
          if (executeFromValidateTask(env)) {
            LOGGER.warn("ProcedureId {}: {}", getProcId(), SKIP_PIPE_PROCEDURE_MESSAGE);
            // On client side, the message returned after the successful execution of the pipe
            // command corresponding to this procedure is "Msg: The statement is executed
            // successfully."
            this.setResult(SKIP_PIPE_PROCEDURE_MESSAGE.getBytes(StandardCharsets.UTF_8));
            return Flow.NO_MORE_STATE;
          }
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
            "ProcedureId {}: Encountered error when trying to {} at state [{}], retry [{}/{}]",
            getProcId(),
            getOperation(),
            state,
            getCycles() + 1,
            RETRY_THRESHOLD,
            e);
        // Wait 3s for next retry
        TimeUnit.MILLISECONDS.sleep(3000L);
      } else {
        LOGGER.warn(
            "ProcedureId {}: All {} retries failed when trying to {} at state [{}], will rollback...",
            getProcId(),
            RETRY_THRESHOLD,
            getOperation(),
            state,
            e);
        setFailure(
            new ProcedureException(
                String.format(
                    "ProcedureId %s: Fail to %s because %s",
                    getProcId(), getOperation().name(), e.getMessage())));
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
    if (pipeTaskInfo == null) {
      LOGGER.warn(
          "ProcedureId {}: Pipe lock is not acquired, rollbackState({})'s execution will be skipped.",
          getProcId(),
          state);
      return;
    }

    switch (state) {
      case VALIDATE_TASK:
        try {
          rollbackFromValidateTask(env);
        } catch (Exception e) {
          LOGGER.warn("ProcedureId {}: Failed to rollback from validate task.", getProcId(), e);
        }
        break;
      case CALCULATE_INFO_FOR_TASK:
        try {
          rollbackFromCalculateInfoForTask(env);
        } catch (Exception e) {
          LOGGER.warn(
              "ProcedureId {}: Failed to rollback from calculate info for task.", getProcId(), e);
        }
        break;
      case WRITE_CONFIG_NODE_CONSENSUS:
        try {
          // rollbackFromWriteConfigNodeConsensus can be called before
          // rollbackFromOperateOnDataNodes.
          // So we need to check if rollbackFromOperateOnDataNodes is successfully executed.
          // If yes, we don't need to call rollbackFromWriteConfigNodeConsensus again.
          if (!isRollbackFromOperateOnDataNodesSuccessful) {
            rollbackFromWriteConfigNodeConsensus(env);
          }
        } catch (Exception e) {
          LOGGER.warn(
              "ProcedureId {}: Failed to rollback from write config node consensus.",
              getProcId(),
              e);
        }
        break;
      case OPERATE_ON_DATA_NODES:
        try {
          // We have to make sure that rollbackFromOperateOnDataNodes is executed before
          // rollbackFromWriteConfigNodeConsensus, because rollbackFromOperateOnDataNodes is
          // executed based on the consensus of config nodes that is written by
          // rollbackFromWriteConfigNodeConsensus
          rollbackFromWriteConfigNodeConsensus(env);
          rollbackFromOperateOnDataNodes(env);
          isRollbackFromOperateOnDataNodesSuccessful = true;
        } catch (Exception e) {
          LOGGER.warn(
              "ProcedureId {}: Failed to rollback from operate on data nodes.", getProcId(), e);
        }
        break;
      default:
        LOGGER.error("Unsupported roll back STATE [{}]", state);
    }
  }

  public abstract void rollbackFromValidateTask(ConfigNodeProcedureEnv env);

  public abstract void rollbackFromCalculateInfoForTask(ConfigNodeProcedureEnv env);

  public abstract void rollbackFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env);

  public abstract void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
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
   * Pushing all the pipeMeta's to all the dataNodes, forcing an update to the pipe's runtime state.
   *
   * @param env ConfigNodeProcedureEnv
   * @param pipeTaskInfo PipeTaskInfo managed outside this procedure
   * @return The responseMap after pushing pipe meta
   * @throws IOException Exception when Serializing to byte buffer
   */
  public static Map<Integer, TPushPipeMetaResp> pushPipeMetaToDataNodes(
      ConfigNodeProcedureEnv env, AtomicReference<PipeTaskInfo> pipeTaskInfo) throws IOException {
    final List<ByteBuffer> pipeMetaBinaryList = new ArrayList<>();
    for (PipeMeta pipeMeta : pipeTaskInfo.get().getPipeMetaList()) {
      pipeMetaBinaryList.add(pipeMeta.serialize());
    }

    return env.pushAllPipeMetaToDataNodes(pipeMetaBinaryList);
  }

  /**
   * Parsing the given pipe's or all pipes' pushPipeMeta exceptions to string.
   *
   * @param pipeName The given pipe's pipe name, {@code null} if report all pipes' exceptions.
   * @param respMap The responseMap after pushing pipe meta
   * @return Error messages for the given pipe after pushing pipe meta
   */
  public static String parsePushPipeMetaExceptionForPipe(
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
