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

package org.apache.iotdb.confignode.procedure.impl.subscription.subscription;

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.DropPipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.subscription.AbstractOperateSubscriptionProcedure;
import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaResp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.confignode.procedure.impl.pipe.AbstractOperatePipeProcedureV2.copyAndFilterOutNonWorkingDataRegionPipeTasks;

public abstract class AbstractOperateSubscriptionAndPipeProcedure
    extends AbstractOperateSubscriptionProcedure {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractOperateSubscriptionAndPipeProcedure.class);

  protected AtomicReference<PipeTaskInfo> pipeTaskInfo;

  @Override
  protected ProcedureLockState acquireLock(ConfigNodeProcedureEnv configNodeProcedureEnv) {
    LOGGER.info(
        ProcedureMessages.PROCEDUREID_TRY_TO_ACQUIRE_SUBSCRIPTION_AND_PIPE_LOCK, getProcId());

    pipeTaskInfo =
        configNodeProcedureEnv.getConfigManager().getPipeManager().getPipeTaskCoordinator().lock();
    if (pipeTaskInfo == null) {
      LOGGER.warn(ProcedureMessages.PROCEDUREID_FAILED_TO_ACQUIRE_PIPE_LOCK, getProcId());
    } else {
      LOGGER.info(ProcedureMessages.PROCEDUREID_ACQUIRED_PIPE_LOCK, getProcId());
    }

    final ProcedureLockState procedureLockState = super.acquireLock(configNodeProcedureEnv);

    switch (procedureLockState) {
      case LOCK_ACQUIRED:
        if (pipeTaskInfo == null) {
          LOGGER.warn(
              ProcedureMessages
                  .PROCEDUREID_LOCK_ACQUIRED_THE_FOLLOWING_PROCEDURE_SHOULD_NOT_BE_EXECUTED,
              getProcId());
        } else {
          LOGGER.info(
              ProcedureMessages
                  .PROCEDUREID_LOCK_ACQUIRED_THE_FOLLOWING_PROCEDURE_SHOULD_BE_EXECUTED_WITH_2,
              getProcId());
        }
        break;
      case LOCK_EVENT_WAIT:
        if (pipeTaskInfo == null) {
          LOGGER.warn(
              ProcedureMessages.PROCEDUREID_LOCK_EVENT_WAIT_WITHOUT_ACQUIRING_PIPE_LOCK,
              getProcId());
        } else {
          LOGGER.info(
              ProcedureMessages.PROCEDUREID_LOCK_EVENT_WAIT_PIPE_LOCK_WILL_BE_RELEASED,
              getProcId());
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
              ProcedureMessages.PROCEDUREID_INVALID_LOCK_STATE_WITHOUT_ACQUIRING_PIPE_LOCK,
              getProcId(),
              procedureLockState);
        } else {
          LOGGER.error(
              ProcedureMessages.PROCEDUREID_INVALID_LOCK_STATE_PIPE_LOCK_WILL_BE_RELEASED,
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
      LOGGER.warn(
          ProcedureMessages.PROCEDUREID_RELEASE_LOCK_NO_NEED_TO_RELEASE_PIPE_LOCK, getProcId());
    } else {
      LOGGER.info(
          ProcedureMessages.PROCEDUREID_RELEASE_LOCK_PIPE_LOCK_WILL_BE_RELEASED, getProcId());
      configNodeProcedureEnv.getConfigManager().getPipeManager().getPipeTaskCoordinator().unlock();
      pipeTaskInfo = null;
    }
  }

  /**
   * Pushing multiple pipeMetas to all the dataNodes, forcing an update to the pipes' runtime state.
   *
   * @param pipeStaticMetas pipe static metas of the pipes to push
   * @param env ConfigNodeProcedureEnv
   * @return The responseMap after pushing pipe meta
   * @throws IOException Exception when Serializing to byte buffer
   */
  protected Map<Integer, TPushPipeMetaResp> pushMultiPipeMetaToDataNodes(
      List<PipeStaticMeta> pipeStaticMetas, ConfigNodeProcedureEnv env) throws IOException {
    final List<ByteBuffer> pipeMetaBinaryList = new ArrayList<>();
    for (PipeStaticMeta pipeStaticMeta : pipeStaticMetas) {
      PipeMeta pipeMeta = pipeTaskInfo.get().getPipeMetaByPipeStaticMeta(pipeStaticMeta);
      if (pipeMeta == null) {
        LOGGER.warn(
            ProcedureMessages.PIPE_NOT_FOUND_IN_PIPETASKINFO_CAN_NOT_PUSH_ITS_META,
            pipeStaticMeta.getPipeName());
        continue;
      }
      pipeMetaBinaryList.add(copyAndFilterOutNonWorkingDataRegionPipeTasks(pipeMeta).serialize());
    }

    return env.pushMultiPipeMetaToDataNodes(pipeMetaBinaryList);
  }

  /**
   * Drop multiple pipes on all the dataNodes.
   *
   * @param dropPipeProcedures drop pipe procedures that captured the pipe metas to drop
   * @param env ConfigNodeProcedureEnv
   * @return The responseMap after pushing pipe meta
   */
  protected Map<Integer, TPushPipeMetaResp> dropMultiPipeOnDataNodes(
      List<DropPipeProcedureV2> dropPipeProcedures, ConfigNodeProcedureEnv env) throws IOException {
    boolean hasMissingPipeMeta = false;
    final List<String> pipeNamesToDrop = new ArrayList<>();
    final List<ByteBuffer> pipeMetaBinaryList = new ArrayList<>();
    for (final DropPipeProcedureV2 dropPipeProcedure : dropPipeProcedures) {
      pipeNamesToDrop.add(dropPipeProcedure.getPipeName());
      final PipeMeta pipeMetaToDrop = dropPipeProcedure.getPipeMetaToDrop();
      if (pipeMetaToDrop == null) {
        hasMissingPipeMeta = true;
        continue;
      }

      final PipeMeta droppedPipeMeta =
          copyAndFilterOutNonWorkingDataRegionPipeTasks(pipeMetaToDrop);
      droppedPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.DROPPED);
      pipeMetaBinaryList.add(droppedPipeMeta.serialize());
    }

    return hasMissingPipeMeta
        ? env.dropMultiPipeOnDataNodes(pipeNamesToDrop)
        : env.pushMultiPipeMetaToDataNodes(pipeMetaBinaryList);
  }
}
