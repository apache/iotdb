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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.node.AbstractNodeProcedure;
import org.apache.iotdb.confignode.procedure.state.pipe.task.OperatePipeTaskState;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * This procedure manage 4 kinds of PIPE operations: CREATE, START, STOP and DROP.
 *
 * <p>This class extends AbstractNodeProcedure to make sure that pipe task procedures can be
 * executed in sequence and node procedures can be locked when a pipe task procedure is running.
 */
public abstract class AbstractOperatePipeProcedureV2
    extends AbstractNodeProcedure<OperatePipeTaskState> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractOperatePipeProcedureV2.class);

  private static final int RETRY_THRESHOLD = 1;

  // only used in rollback to reduce the number of network calls
  protected boolean isRollbackFromOperateOnDataNodesSuccessful = false;

  protected abstract PipeTaskOperation getOperation();

  /**
   * Execute at state VALIDATE_TASK
   *
   * @return true if procedure can finish directly
   */
  protected abstract void executeFromValidateTask(ConfigNodeProcedureEnv env) throws PipeException;

  /** Execute at state CALCULATE_INFO_FOR_TASK */
  protected abstract void executeFromCalculateInfoForTask(ConfigNodeProcedureEnv env)
      throws PipeException;

  /** Execute at state WRITE_CONFIG_NODE_CONSENSUS */
  protected abstract void executeFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env)
      throws PipeException;

  /** Execute at state OPERATE_ON_DATA_NODES */
  protected abstract void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws PipeException, IOException;

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, OperatePipeTaskState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    try {
      switch (state) {
        case VALIDATE_TASK:
          env.getConfigManager().getPipeManager().getPipeTaskCoordinator().lock();
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
          env.getConfigManager().getPipeManager().getPipeTaskCoordinator().unlock();
          return Flow.NO_MORE_STATE;
      }
    } catch (Exception e) {
      if (isRollbackSupported(state)) {
        LOGGER.error("Fail in OperatePipeProcedure", e);
        setFailure(new ProcedureException(e.getMessage()));
      } else {
        LOGGER.error("Retrievable error trying to {} at state [{}]", getOperation(), state, e);
        if (getCycles() > RETRY_THRESHOLD) {
          setFailure(
              new ProcedureException(
                  String.format("Fail to %s because %s", getOperation().name(), e.getMessage())));
        }
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
        try {
          rollbackFromValidateTask(env);
        } finally {
          env.getConfigManager().getPipeManager().getPipeTaskCoordinator().unlock();
        }
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
        // we have to make sure that rollbackFromOperateOnDataNodes is executed before
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

  protected TSStatus pushPipeMetaToDataNodes(ConfigNodeProcedureEnv env) throws IOException {
    final List<ByteBuffer> pipeMetaBinaryList = new ArrayList<>();
    for (PipeMeta pipeMeta :
        env.getConfigManager()
            .getPipeManager()
            .getPipeTaskCoordinator()
            .getPipeTaskInfo()
            .getPipeMetaList()) {
      pipeMetaBinaryList.add(pipeMeta.serialize());
    }

    return RpcUtils.squashResponseStatusList(env.pushPipeMetaToDataNodes(pipeMetaBinaryList));
  }

  protected void pushPipeMetaToDataNodesIgnoreException(ConfigNodeProcedureEnv env) {
    try {
      pushPipeMetaToDataNodes(env);
    } catch (Throwable throwable) {
      LOGGER.info("Failed to push pipe meta list to data nodes, will retry later.", throwable);
    }
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
