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
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
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
    LOGGER.info("ProcedureId {} try to acquire subscription and pipe lock.", getProcId());

    pipeTaskInfo =
        configNodeProcedureEnv.getConfigManager().getPipeManager().getPipeTaskCoordinator().lock();
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
              "ProcedureId {}: LOCK_ACQUIRED. The following procedure should be executed with subscription and pipe lock.",
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
      configNodeProcedureEnv.getConfigManager().getPipeManager().getPipeTaskCoordinator().unlock();
      pipeTaskInfo = null;
    }
  }

  /**
   * Pushing multiple pipeMetas to all the dataNodes, forcing an update to the pipes' runtime state.
   *
   * @param pipeNames pipe names of the pipes to push
   * @param env ConfigNodeProcedureEnv
   * @return The responseMap after pushing pipe meta
   * @throws IOException Exception when Serializing to byte buffer
   */
  protected Map<Integer, TPushPipeMetaResp> pushMultiPipeMetaToDataNodes(
      List<String> pipeNames, ConfigNodeProcedureEnv env) throws IOException {
    final List<ByteBuffer> pipeMetaBinaryList = new ArrayList<>();
    for (String pipeName : pipeNames) {
      PipeMeta pipeMeta = pipeTaskInfo.get().getPipeMetaByPipeName(pipeName);
      if (pipeMeta == null) {
        LOGGER.warn("Pipe {} not found in PipeTaskInfo, can not push its meta.", pipeName);
        continue;
      }
      pipeMetaBinaryList.add(copyAndFilterOutNonWorkingDataRegionPipeTasks(pipeMeta).serialize());
    }

    return env.pushMultiPipeMetaToDataNodes(pipeMetaBinaryList);
  }

  /**
   * Drop multiple pipes on all the dataNodes.
   *
   * @param pipeNames pipe names of the pipes to drop
   * @param env ConfigNodeProcedureEnv
   * @return The responseMap after pushing pipe meta
   */
  protected Map<Integer, TPushPipeMetaResp> dropMultiPipeOnDataNodes(
      List<String> pipeNames, ConfigNodeProcedureEnv env) {
    return env.dropMultiPipeOnDataNodes(pipeNames);
  }
}
