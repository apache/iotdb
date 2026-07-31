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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.manager.pipe.metric.overview.PipeProcedureMetrics;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.node.AbstractNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.pipe.runtime.PipeMetaSyncProcedure;
import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;
import org.apache.iotdb.confignode.procedure.state.pipe.task.OperatePipeTaskState;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.db.pipe.source.dataregion.DataRegionListeningFilter;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaResp;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
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

  // Only used in rollback to avoid executing rollbackFromValidateTask multiple times
  // Pure in-memory object, not involved in snapshot serialization and deserialization.
  // TODO: consider serializing this variable later
  protected boolean isRollbackFromValidateTaskSuccessful = false;

  // This variable should not be serialized into procedure store,
  // putting it here is just for convenience
  protected AtomicReference<PipeTaskInfo> pipeTaskInfo;

  // Only used to release global locks before retrying the same state. Do not serialize it because a
  // recovered procedure is already re-scheduled by the procedure framework.
  private transient boolean shouldYieldAfterExecution;

  // These fields are only used to report where a running Pipe procedure is blocked when the caller
  // times out. They do not affect procedure execution and do not need to be persisted.
  private volatile PipeProcedureExecutionStage executionStage =
      PipeProcedureExecutionStage.WAITING_FOR_PROCEDURE_WORKER;
  private volatile String lastExecutionExceptionMessage;
  private final AtomicReference<Procedure<?>> nodeLockOwnerProcedure = new AtomicReference<>();
  private final AtomicReference<Set<Integer>> pendingDataNodeIds =
      new AtomicReference<>(Collections.emptySet());

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
    LOGGER.debug(ProcedureMessages.PROCEDUREID_TRY_TO_ACQUIRE_PIPE_LOCK, getProcId());
    executionStage = PipeProcedureExecutionStage.WAITING_FOR_PIPE_TASK_COORDINATOR_LOCK;
    pipeTaskInfo = acquireLockInternal(configNodeProcedureEnv);
    if (pipeTaskInfo == null) {
      LOGGER.warn(ProcedureMessages.PROCEDUREID_FAILED_TO_ACQUIRE_PIPE_LOCK, getProcId());
    } else {
      LOGGER.debug(ProcedureMessages.PROCEDUREID_ACQUIRED_PIPE_LOCK, getProcId());
    }

    executionStage = PipeProcedureExecutionStage.WAITING_FOR_NODE_LOCK;
    final ProcedureLockState procedureLockState = super.acquireLock(configNodeProcedureEnv);
    switch (procedureLockState) {
      case LOCK_ACQUIRED:
        nodeLockOwnerProcedure.set(null);
        updateExecutionStage(getCurrentState(), false);
        if (pipeTaskInfo == null) {
          LOGGER.warn(
              ProcedureMessages
                  .PROCEDUREID_LOCK_ACQUIRED_THE_FOLLOWING_PROCEDURE_SHOULD_NOT_BE_EXECUTED,
              getProcId());
        } else {
          LOGGER.debug(
              ProcedureMessages
                  .PROCEDUREID_LOCK_ACQUIRED_THE_FOLLOWING_PROCEDURE_SHOULD_BE_EXECUTED_WITH,
              getProcId());
        }
        break;
      case LOCK_EVENT_WAIT:
        nodeLockOwnerProcedure.set(configNodeProcedureEnv.getNodeLock().getLockOwnerProcedure());
        if (pipeTaskInfo == null) {
          LOGGER.warn(
              ProcedureMessages.PROCEDUREID_LOCK_EVENT_WAIT_WITHOUT_ACQUIRING_PIPE_LOCK,
              getProcId());
        } else {
          LOGGER.debug(
              ProcedureMessages.PROCEDUREID_LOCK_EVENT_WAIT_PIPE_LOCK_WILL_BE_RELEASED,
              getProcId());
          releasePipeTaskCoordinatorLock(configNodeProcedureEnv);
        }
        break;
      default:
        if (pipeTaskInfo == null) {
          LOGGER.error(
              ProcedureMessages.PROCEDUREID_INVALID_LOCK_STATE_WITHOUT_ACQUIRING_PIPE_LOCK,
              getProcId(),
              procedureLockState);
        } else {
          LOGGER.error(
              ProcedureMessages.PROCEDUREID_INVALID_LOCK_STATE_PIPE_LOCK_WILL_BE_RELEASED,
              getProcId(),
              procedureLockState);
          releasePipeTaskCoordinatorLock(configNodeProcedureEnv);
        }
        break;
    }
    return procedureLockState;
  }

  @Override
  protected void releaseLock(ConfigNodeProcedureEnv configNodeProcedureEnv) {
    super.releaseLock(configNodeProcedureEnv);

    if (pipeTaskInfo == null) {
      LOGGER.warn(
          ProcedureMessages.PROCEDUREID_RELEASE_LOCK_NO_NEED_TO_RELEASE_PIPE_LOCK, getProcId());
    } else {
      LOGGER.debug(
          ProcedureMessages.PROCEDUREID_RELEASE_LOCK_PIPE_LOCK_WILL_BE_RELEASED, getProcId());
      if (isSuccess() && this instanceof PipeMetaSyncProcedure) {
        configNodeProcedureEnv
            .getConfigManager()
            .getPipeManager()
            .getPipeTaskCoordinator()
            .updateLastSyncedVersion();
      }
      if (isFinished()) {
        PipeProcedureMetrics.getInstance()
            .updateTimer(this.getOperation().getName(), this.elapsedTime());
      }
      releasePipeTaskCoordinatorLock(configNodeProcedureEnv);
      if (!isFinished()) {
        executionStage = PipeProcedureExecutionStage.WAITING_FOR_PROCEDURE_WORKER;
      }
    }
  }

  private void releasePipeTaskCoordinatorLock(ConfigNodeProcedureEnv configNodeProcedureEnv) {
    // Clear before releasing the semaphore to avoid clobbering a re-scheduled execution's marker.
    pipeTaskInfo = null;
    configNodeProcedureEnv.getConfigManager().getPipeManager().getPipeTaskCoordinator().unlock();
  }

  protected abstract PipeTaskOperation getOperation();

  /**
   * Execute at state {@link OperatePipeTaskState#VALIDATE_TASK}.
   *
   * @return false if this procedure can skip subsequent stages (start RUNNING pipe or stop STOPPED
   *     pipe without runtime exception)
   * @throws PipeException if validation for pipe parameters failed
   */
  public abstract boolean executeFromValidateTask(ConfigNodeProcedureEnv env) throws PipeException;

  /** Execute at state {@link OperatePipeTaskState#CALCULATE_INFO_FOR_TASK}. */
  public abstract void executeFromCalculateInfoForTask(ConfigNodeProcedureEnv env);

  /**
   * Execute at state {@link OperatePipeTaskState#WRITE_CONFIG_NODE_CONSENSUS}.
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
      throws InterruptedException {
    shouldYieldAfterExecution = false;
    updateExecutionStage(state, false);
    if (pipeTaskInfo == null) {
      LOGGER.warn(
          ProcedureMessages.PROCEDUREID_PIPE_LOCK_IS_NOT_ACQUIRED_EXECUTEFROMSTATE_S_EXECUTION_WILL,
          getProcId());
      return Flow.NO_MORE_STATE;
    }

    try {
      Objects.requireNonNull(state);
      switch (state) {
        case VALIDATE_TASK:
          if (!executeFromValidateTask(env)) {
            lastExecutionExceptionMessage = null;
            LOGGER.info(ProcedureMessages.PROCEDUREID, getProcId(), SKIP_PIPE_PROCEDURE_MESSAGE);
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
          lastExecutionExceptionMessage = null;
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException(
              String.format(
                  ProcedureMessages.UNKNOWN_STATE_DURING_EXECUTING_OPERATEPIPEPROCEDURE, state));
      }
      lastExecutionExceptionMessage = null;
    } catch (Exception e) {
      lastExecutionExceptionMessage = getExceptionMessage(e);
      // Retry before rollback
      if (getCycles() < RETRY_THRESHOLD) {
        LOGGER.warn(
            ProcedureMessages.PROCEDUREID_ENCOUNTERED_ERROR_WHEN_TRYING_TO_AT_STATE_RETRY,
            getProcId(),
            getOperation(),
            state,
            getCycles() + 1,
            RETRY_THRESHOLD,
            e);
        setNextState(getCurrentState());
        shouldYieldAfterExecution = true;
      } else {
        LOGGER.warn(
            ProcedureMessages.PROCEDUREID_ALL_RETRIES_FAILED_WHEN_TRYING_TO_AT_STATE_WILL,
            getProcId(),
            RETRY_THRESHOLD,
            getOperation(),
            state,
            e);
        setFailure(
            new ProcedureException(
                String.format(
                    ProcedureMessages.PROCEDUREID_FAIL_TO_BECAUSE,
                    getProcId(),
                    getOperation().name(),
                    e.getMessage())));
        return Flow.NO_MORE_STATE;
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected boolean isYieldAfterExecution(final ConfigNodeProcedureEnv env) {
    return shouldYieldAfterExecution;
  }

  @Override
  protected boolean isRollbackSupported(OperatePipeTaskState state) {
    return true;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, OperatePipeTaskState state)
      throws IOException, InterruptedException, ProcedureException {
    updateExecutionStage(state, true);
    if (pipeTaskInfo == null) {
      LOGGER.warn(
          ProcedureMessages.PROCEDUREID_PIPE_LOCK_IS_NOT_ACQUIRED_ROLLBACKSTATE_S_EXECUTION_WILL,
          getProcId(),
          state);
      return;
    }

    Objects.requireNonNull(state);
    switch (state) {
      case VALIDATE_TASK:
        if (!isRollbackFromValidateTaskSuccessful) {
          try {
            rollbackFromValidateTask(env);
            isRollbackFromValidateTaskSuccessful = true;
          } catch (Exception e) {
            LOGGER.warn(
                ProcedureMessages.PROCEDUREID_FAILED_TO_ROLLBACK_FROM_VALIDATE_TASK,
                getProcId(),
                e);
          }
        }
        break;
      case CALCULATE_INFO_FOR_TASK:
        try {
          rollbackFromCalculateInfoForTask(env);
        } catch (Exception e) {
          LOGGER.warn(
              ProcedureMessages.PROCEDUREID_FAILED_TO_ROLLBACK_FROM_CALCULATE_INFO_FOR_TASK,
              getProcId(),
              e);
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
              ProcedureMessages.PROCEDUREID_FAILED_TO_ROLLBACK_FROM_WRITE_CONFIG_NODE_CONSENSUS,
              getProcId(),
              e);
        }
        break;
      case OPERATE_ON_DATA_NODES:
        try {
          // We have to make sure that rollbackFromOperateOnDataNodes is executed after
          // rollbackFromWriteConfigNodeConsensus, because rollbackFromOperateOnDataNodes is
          // executed based on the consensus of config nodes that is written by
          // rollbackFromWriteConfigNodeConsensus
          rollbackFromWriteConfigNodeConsensus(env);
          rollbackFromOperateOnDataNodes(env);
          isRollbackFromOperateOnDataNodesSuccessful = true;
        } catch (Exception e) {
          LOGGER.warn(
              ProcedureMessages.PROCEDUREID_FAILED_TO_ROLLBACK_FROM_OPERATE_ON_DATA_NODES,
              getProcId(),
              e);
        }
        break;
      default:
        LOGGER.error(ProcedureMessages.UNSUPPORTED_ROLL_BACK_STATE, state);
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

  public final String getTimeoutDiagnosticMessage() {
    final PipeProcedureExecutionStage currentExecutionStage = executionStage;
    return String.format(
        ProcedureMessages
            .MESSAGE_PIPE_OPERATION_ARG_TIMED_OUT_PROCEDUREID_ARG_STUCK_AT_ARG_REASON_ARG_THE_PROCEDURE_IS_STILL_RUNNING_7EEAC50E,
        getOperation().name(),
        getProcId(),
        currentExecutionStage.name(),
        getTimeoutReason(currentExecutionStage));
  }

  private String getTimeoutReason(final PipeProcedureExecutionStage currentExecutionStage) {
    final String failureMessage = getFailureMessage();
    if (currentExecutionStage.isRollback()) {
      return getRollbackTimeoutReason(failureMessage);
    }
    if (isFailed() && failureMessage != null) {
      return String.format(
          ProcedureMessages.MESSAGE_THE_STATE_FAILED_WITH_ARG_AND_ROLLBACK_IS_PENDING_E7B43829,
          failureMessage);
    }
    if (lastExecutionExceptionMessage != null) {
      return String.format(
          ProcedureMessages
              .MESSAGE_THE_PREVIOUS_ATTEMPT_FAILED_WITH_ARG_AND_THIS_STATE_IS_BEING_RETRIED_7A541F27,
          lastExecutionExceptionMessage);
    }

    return switch (currentExecutionStage) {
      case WAITING_FOR_PROCEDURE_WORKER ->
          ProcedureMessages
              .MESSAGE_NO_PROCEDURE_WORKER_IS_CURRENTLY_AVAILABLE_WORKERS_MAY_BE_BUSY_OR_BLOCKED_BY_OTHER_PROCEDURES_AB0B1595;
      case WAITING_FOR_PIPE_TASK_COORDINATOR_LOCK ->
          ProcedureMessages
              .MESSAGE_WAITING_TO_ACQUIRE_THE_PIPETASKCOORDINATOR_LOCK_BECAUSE_ANOTHER_PIPE_OPERATION_IS_HOLDING_IT_25A3B6B8;
      case WAITING_FOR_NODE_LOCK -> getNodeLockTimeoutReason();
      case VALIDATE_TASK ->
          ProcedureMessages
              .MESSAGE_PIPE_REQUEST_OR_PLUGIN_VALIDATION_HAS_NOT_COMPLETED_A_PLUGIN_CHECK_OR_METADATA_ACCESS_MAY_BE_SLOW_57C36CEF;
      case CALCULATE_INFO_FOR_TASK ->
          ProcedureMessages
              .MESSAGE_PIPE_METADATA_CALCULATION_HAS_NOT_COMPLETED_METADATA_ACCESS_OR_LOCAL_CALCULATION_MAY_BE_SLOW_DEBF2504;
      case WRITE_CONFIG_NODE_CONSENSUS ->
          ProcedureMessages
              .MESSAGE_THE_CONFIGNODE_CONSENSUS_WRITE_HAS_NOT_RETURNED_RUN_SHOW_CLUSTER_TO_CHECK_NODE_STATUS_B0A6E1A7;
      case OPERATE_ON_DATA_NODES -> getDataNodeTimeoutReason();
      case ROLLBACK_VALIDATE_TASK,
          ROLLBACK_CALCULATE_INFO_FOR_TASK,
          ROLLBACK_WRITE_CONFIG_NODE_CONSENSUS,
          ROLLBACK_OPERATE_ON_DATA_NODES ->
          getRollbackTimeoutReason(failureMessage);
    };
  }

  private static String getRollbackTimeoutReason(final String failureMessage) {
    return failureMessage == null
        ? ProcedureMessages.MESSAGE_ROLLING_BACK_AFTER_AN_EARLIER_FAILURE_850D0AF5
        : String.format(
            ProcedureMessages.MESSAGE_ROLLING_BACK_AFTER_FAILURE_ARG_474DF456, failureMessage);
  }

  private String getNodeLockTimeoutReason() {
    final Procedure<?> lockOwnerProcedure = nodeLockOwnerProcedure.get();
    if (lockOwnerProcedure == null) {
      return ProcedureMessages
          .MESSAGE_WAITING_TO_ACQUIRE_THE_CONFIGNODE_NODE_LOCK_BECAUSE_ANOTHER_NODE_PROCEDURE_IS_HOLDING_IT_56494E86;
    }
    final String operation =
        lockOwnerProcedure instanceof AbstractOperatePipeProcedureV2
            ? ((AbstractOperatePipeProcedureV2) lockOwnerProcedure).getOperation().name()
            : lockOwnerProcedure.getClass().getSimpleName();
    return String.format(
        ProcedureMessages
            .MESSAGE_WAITING_TO_ACQUIRE_THE_CONFIGNODE_NODE_LOCK_HELD_BY_ARG_PROCEDUREID_ARG_3F432041,
        operation,
        lockOwnerProcedure.getProcId());
  }

  private String getDataNodeTimeoutReason() {
    final Set<Integer> currentPendingDataNodeIds = new TreeSet<>(pendingDataNodeIds.get());
    return currentPendingDataNodeIds.isEmpty()
        ? ProcedureMessages
            .MESSAGE_THE_PIPE_METADATA_PUSH_HAS_NOT_COMPLETED_RUN_SHOW_CLUSTER_TO_CHECK_DATANODE_STATUS_A8F3F0A0
        : String.format(
            ProcedureMessages
                .MESSAGE_DATANODES_ARG_HAVE_NOT_RESPONDED_TO_THE_PIPE_METADATA_PUSH_RUN_SHOW_CLUSTER_TO_CHECK_THEIR_STATUS_9C2F806F,
            currentPendingDataNodeIds);
  }

  private String getFailureMessage() {
    if (getException() != null) {
      return getException().getMessage();
    }
    return lastExecutionExceptionMessage;
  }

  private static String getExceptionMessage(final Exception exception) {
    return exception.getMessage() == null || exception.getMessage().isEmpty()
        ? exception.getClass().getSimpleName()
        : exception.getMessage();
  }

  protected final void updateExecutionStage(
      final OperatePipeTaskState state, final boolean isRollback) {
    if (state == null) {
      executionStage = PipeProcedureExecutionStage.WAITING_FOR_PROCEDURE_WORKER;
      return;
    }
    executionStage =
        switch (state) {
          case VALIDATE_TASK ->
              isRollback
                  ? PipeProcedureExecutionStage.ROLLBACK_VALIDATE_TASK
                  : PipeProcedureExecutionStage.VALIDATE_TASK;
          case CALCULATE_INFO_FOR_TASK ->
              isRollback
                  ? PipeProcedureExecutionStage.ROLLBACK_CALCULATE_INFO_FOR_TASK
                  : PipeProcedureExecutionStage.CALCULATE_INFO_FOR_TASK;
          case WRITE_CONFIG_NODE_CONSENSUS ->
              isRollback
                  ? PipeProcedureExecutionStage.ROLLBACK_WRITE_CONFIG_NODE_CONSENSUS
                  : PipeProcedureExecutionStage.WRITE_CONFIG_NODE_CONSENSUS;
          case OPERATE_ON_DATA_NODES ->
              isRollback
                  ? PipeProcedureExecutionStage.ROLLBACK_OPERATE_ON_DATA_NODES
                  : PipeProcedureExecutionStage.OPERATE_ON_DATA_NODES;
        };
  }

  protected final void setPendingDataNodeIds(final Set<Integer> pendingDataNodeIds) {
    this.pendingDataNodeIds.set(pendingDataNodeIds);
  }

  private enum PipeProcedureExecutionStage {
    WAITING_FOR_PROCEDURE_WORKER,
    WAITING_FOR_PIPE_TASK_COORDINATOR_LOCK,
    WAITING_FOR_NODE_LOCK,
    VALIDATE_TASK,
    CALCULATE_INFO_FOR_TASK,
    WRITE_CONFIG_NODE_CONSENSUS,
    OPERATE_ON_DATA_NODES,
    ROLLBACK_VALIDATE_TASK(true),
    ROLLBACK_CALCULATE_INFO_FOR_TASK(true),
    ROLLBACK_WRITE_CONFIG_NODE_CONSENSUS(true),
    ROLLBACK_OPERATE_ON_DATA_NODES(true);

    private final boolean rollback;

    PipeProcedureExecutionStage() {
      this(false);
    }

    PipeProcedureExecutionStage(final boolean rollback) {
      this.rollback = rollback;
    }

    private boolean isRollback() {
      return rollback;
    }
  }

  /**
   * Pushing all the pipeMeta's to all the dataNodes, forcing an update to the pipe's runtime state.
   *
   * @param env ConfigNodeProcedureEnv
   * @return The responseMap after pushing pipe meta
   * @throws IOException Exception when Serializing to byte buffer
   */
  protected Map<Integer, TPushPipeMetaResp> pushPipeMetaToDataNodes(
      final ConfigNodeProcedureEnv env) throws IOException {
    final List<ByteBuffer> pipeMetaBinaryList = new ArrayList<>();
    for (final PipeMeta pipeMeta : pipeTaskInfo.get().getPipeMetaList()) {
      pipeMetaBinaryList.add(copyAndFilterOutNonWorkingDataRegionPipeTasks(pipeMeta).serialize());
    }
    return env.pushAllPipeMetaToDataNodes(pipeMetaBinaryList, this::setPendingDataNodeIds);
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
      final ConfigNodeProcedureEnv env, final AtomicReference<PipeTaskInfo> pipeTaskInfo)
      throws IOException {
    final List<ByteBuffer> pipeMetaBinaryList = new ArrayList<>();
    for (final PipeMeta pipeMeta : pipeTaskInfo.get().getPipeMetaList()) {
      pipeMetaBinaryList.add(copyAndFilterOutNonWorkingDataRegionPipeTasks(pipeMeta).serialize());
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
      final String pipeName, final Map<Integer, TPushPipeMetaResp> respMap) {
    final StringBuilder exceptionMessageBuilder = new StringBuilder();
    final StringBuilder enoughMemoryMessageBuilder = new StringBuilder();

    for (final Map.Entry<Integer, TPushPipeMetaResp> respEntry : respMap.entrySet()) {
      final int dataNodeId = respEntry.getKey();
      final TPushPipeMetaResp resp = respEntry.getValue();

      if (resp.getStatus().getCode()
          == TSStatusCode.PIPE_PUSH_META_NOT_ENOUGH_MEMORY.getStatusCode()) {
        exceptionMessageBuilder.append(String.format("DataNodeId: %s,", dataNodeId));
        resp.getExceptionMessages()
            .forEach(
                message -> {
                  // Ignore the timeStamp for simplicity
                  if (pipeName == null) {
                    enoughMemoryMessageBuilder.append(
                        String.format(
                            "PipeName: %s, Message: %s",
                            message.getPipeName(), message.getMessage()));
                  } else if (pipeName.equals(message.getPipeName())) {
                    enoughMemoryMessageBuilder.append(
                        String.format("Message: %s", message.getMessage()));
                  }
                });
        enoughMemoryMessageBuilder.append(".");
        continue;
      }

      if (resp.getStatus().getCode() == TSStatusCode.PIPE_PUSH_META_TIMEOUT.getStatusCode()) {
        exceptionMessageBuilder.append(
            String.format(
                "DataNodeId: %s, Message: Timeout to wait for lock while processing pushPipeMeta on dataNodes.",
                dataNodeId));
        continue;
      }

      if (resp.getStatus().getCode() == TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode()) {
        if (!resp.isSetExceptionMessages()) {
          final String statusMessage = resp.getStatus().getMessage();
          exceptionMessageBuilder.append(
              String.format(
                  "DataNodeId: %s, Message: Internal error while processing pushPipeMeta on dataNodes.%s",
                  dataNodeId, statusMessage == null ? "" : " " + statusMessage));
          continue;
        }

        final AtomicBoolean hasException = new AtomicBoolean(false);

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

    final String enoughMemoryMessage = enoughMemoryMessageBuilder.toString();
    if (!enoughMemoryMessage.isEmpty()) {
      throw new PipeException(enoughMemoryMessage);
    }

    return exceptionMessageBuilder.toString();
  }

  protected void pushPipeMetaToDataNodesIgnoreException(ConfigNodeProcedureEnv env) {
    try {
      // Ignore the exceptions reported
      pushPipeMetaToDataNodes(env);
    } catch (Exception e) {
      LOGGER.info(ProcedureMessages.FAILED_TO_PUSH_PIPE_META_LIST_TO_DATA_NODES_WILL, e);
    }
  }

  protected Map<Integer, TPushPipeMetaResp> pushPipeMetaToDataNodesBestEffortAndGetResponse(
      ConfigNodeProcedureEnv env) throws IOException {
    final List<ByteBuffer> pipeMetaBinaryList = new ArrayList<>();
    for (final PipeMeta pipeMeta : pipeTaskInfo.get().getPipeMetaList()) {
      pipeMetaBinaryList.add(copyAndFilterOutNonWorkingDataRegionPipeTasks(pipeMeta).serialize());
    }
    return env.pushAllPipeMetaToDataNodesBestEffort(
        pipeMetaBinaryList, this::setPendingDataNodeIds);
  }

  protected void pushPipeMetaToDataNodesBestEffort(ConfigNodeProcedureEnv env) {
    try {
      pushPipeMetaToDataNodesBestEffortAndGetResponse(env);
    } catch (Exception e) {
      LOGGER.info(ProcedureMessages.FAILED_TO_PUSH_PIPE_META_LIST_TO_DATA_NODES_WILL, e);
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
        copyAndFilterOutNonWorkingDataRegionPipeTasks(
                pipeTaskInfo.get().getPipeMetaByPipeName(pipeName))
            .serialize(),
        this::setPendingDataNodeIds);
  }

  protected Map<Integer, TPushPipeMetaResp> pushSinglePipeMetaToDataNodes(
      String pipeName, boolean isTableModel, ConfigNodeProcedureEnv env) throws IOException {
    return env.pushSinglePipeMetaToDataNodes(
        copyAndFilterOutNonWorkingDataRegionPipeTasks(
                pipeTaskInfo.get().getPipeMetaByPipeName(pipeName, isTableModel))
            .serialize(),
        this::setPendingDataNodeIds);
  }

  protected Map<Integer, TPushPipeMetaResp> pushSinglePipeMetaToDataNodes(
      PipeStaticMeta pipeStaticMeta, ConfigNodeProcedureEnv env) throws IOException {
    return env.pushSinglePipeMetaToDataNodes(
        copyAndFilterOutNonWorkingDataRegionPipeTasks(
                pipeTaskInfo.get().getPipeMetaByPipeStaticMeta(pipeStaticMeta))
            .serialize(),
        this::setPendingDataNodeIds);
  }

  protected Map<Integer, TPushPipeMetaResp> pushSinglePipeMetaToDataNodes4Realtime(
      String pipeName, ConfigNodeProcedureEnv env) throws IOException {
    final PipeMeta pipeMeta = pipeTaskInfo.get().getPipeMetaByPipeName(pipeName);
    // Note that although the altered pipe has progress in it,
    // if we alter it to realtime we should ignore the previous data
    if (!pipeMeta.getStaticMeta().isSourceExternal()) {
      pipeMeta
          .getStaticMeta()
          .getSourceParameters()
          .addOrReplaceEquivalentAttributes(
              new PipeParameters(
                  Collections.singletonMap(
                      SystemConstant.RESTART_OR_NEWLY_ADDED_KEY, Boolean.FALSE.toString())));
    }
    return env.pushSinglePipeMetaToDataNodes(
        copyAndFilterOutNonWorkingDataRegionPipeTasks(pipeMeta).serialize(),
        this::setPendingDataNodeIds);
  }

  protected Map<Integer, TPushPipeMetaResp> pushSinglePipeMetaToDataNodes4Realtime(
      String pipeName, boolean isTableModel, ConfigNodeProcedureEnv env) throws IOException {
    final PipeMeta pipeMeta = pipeTaskInfo.get().getPipeMetaByPipeName(pipeName, isTableModel);
    // Note that although the altered pipe has progress in it,
    // if we alter it to realtime we should ignore the previous data
    if (!pipeMeta.getStaticMeta().isSourceExternal()) {
      pipeMeta
          .getStaticMeta()
          .getSourceParameters()
          .addOrReplaceEquivalentAttributes(
              new PipeParameters(
                  Collections.singletonMap(
                      SystemConstant.RESTART_OR_NEWLY_ADDED_KEY, Boolean.FALSE.toString())));
    }
    return env.pushSinglePipeMetaToDataNodes(
        copyAndFilterOutNonWorkingDataRegionPipeTasks(pipeMeta).serialize(),
        this::setPendingDataNodeIds);
  }

  protected Map<Integer, TPushPipeMetaResp> pushSinglePipeMetaToDataNodes4Realtime(
      PipeStaticMeta pipeStaticMeta, ConfigNodeProcedureEnv env) throws IOException {
    final PipeMeta pipeMeta = pipeTaskInfo.get().getPipeMetaByPipeStaticMeta(pipeStaticMeta);
    // Note that although the altered pipe has progress in it,
    // if we alter it to realtime we should ignore the previous data
    if (!pipeMeta.getStaticMeta().isSourceExternal()) {
      pipeMeta
          .getStaticMeta()
          .getSourceParameters()
          .addOrReplaceEquivalentAttributes(
              new PipeParameters(
                  Collections.singletonMap(
                      SystemConstant.RESTART_OR_NEWLY_ADDED_KEY, Boolean.FALSE.toString())));
    }
    return env.pushSinglePipeMetaToDataNodes(
        copyAndFilterOutNonWorkingDataRegionPipeTasks(pipeMeta).serialize(),
        this::setPendingDataNodeIds);
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
    return env.dropSinglePipeOnDataNodes(pipeName, this::setPendingDataNodeIds);
  }

  public static PipeMeta copyAndFilterOutNonWorkingDataRegionPipeTasks(PipeMeta originalPipeMeta)
      throws IOException {
    final PipeMeta copiedPipeMeta = originalPipeMeta.deepCopy4TaskAgent();

    copiedPipeMeta
        .getRuntimeMeta()
        .getConsensusGroupId2TaskMetaMap()
        .entrySet()
        .removeIf(
            consensusGroupId2TaskMeta -> {
              if (originalPipeMeta.getStaticMeta().isSourceExternal()) {
                // should keep the external source tasks
                return false;
              }
              final String database;
              try {
                database =
                    ConfigNode.getInstance()
                        .getConfigManager()
                        .getPartitionManager()
                        .getRegionDatabase(
                            new TConsensusGroupId(
                                // We assume that the consensus group id is a data region id.
                                TConsensusGroupType.DataRegion,
                                consensusGroupId2TaskMeta.getKey()));
                if (database == null) {
                  // If the consensus group id is not a data region id, we keep it.
                  // If the consensus group id is a data region id, but the database is not found,
                  // we keep it.
                  return false;
                }
              } catch (final Exception ignore) {
                // In case of any exception, we keep the consensus group id.
                return false;
              }

              final boolean isTableModel;
              try {
                final TDatabaseSchema schema =
                    ConfigNode.getInstance()
                        .getConfigManager()
                        .getClusterSchemaManager()
                        .getDatabaseSchemaByName(database);
                if (schema == null) {
                  // If the database is not found, we keep it.
                  return false;
                }
                isTableModel = schema.isIsTableModel();
              } catch (final Exception ignore) {
                // If the database is not found, we keep it.
                return false;
              }

              try {
                return !DataRegionListeningFilter.shouldDatabaseBeListened(
                    copiedPipeMeta.getStaticMeta().getSourceParameters(), isTableModel, database);
              } catch (final Exception e) {
                return false;
              }
            });

    return copiedPipeMeta;
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
