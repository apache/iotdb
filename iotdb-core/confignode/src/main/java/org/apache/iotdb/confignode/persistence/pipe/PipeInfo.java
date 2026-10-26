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

package org.apache.iotdb.confignode.persistence.pipe;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleLeaderChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleMetaChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.AlterPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.OperateMultiplePipesPlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusPlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusWithStoppedByRuntimeExceptionPlanV2;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
import org.apache.iotdb.confignode.manager.pipe.agent.PipeConfigNodeAgent;
import org.apache.iotdb.confignode.manager.pipe.agent.runtime.PipeConfigRegionListener;
import org.apache.iotdb.confignode.manager.pipe.agent.task.PipeConfigNodeSubtask;
import org.apache.iotdb.confignode.manager.pipe.agent.task.PipeConfigNodeTaskAgent;
import org.apache.iotdb.confignode.manager.pipe.metric.overview.PipeTemporaryMetaInCoordinatorMetrics;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaRespExceptionMessage;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

public class PipeInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeInfo.class);

  private final PipePluginInfo pipePluginInfo;
  private final PipeTaskInfo pipeTaskInfo;

  public PipeInfo() throws IOException {
    this(null);
  }

  public PipeInfo(final Function<String, String> pipeUserCurrentPasswordProvider)
      throws IOException {
    pipePluginInfo = new PipePluginInfo();
    pipeTaskInfo = new PipeTaskInfo(pipeUserCurrentPasswordProvider);
  }

  public PipePluginInfo getPipePluginInfo() {
    return pipePluginInfo;
  }

  public PipeTaskInfo getPipeTaskInfo() {
    return pipeTaskInfo;
  }

  /////////////////////////////////  Non-query  /////////////////////////////////

  @SuppressWarnings("java:S2201")
  public TSStatus createPipe(final CreatePipePlanV2 plan) {
    try {
      final Optional<PipeMeta> pipeMetaBeforeCreation =
          Optional.ofNullable(pipeTaskInfo.getPipeMetaByPipeStaticMeta(plan.getPipeStaticMeta()));

      pipeTaskInfo.createPipe(plan);

      final TPushPipeMetaRespExceptionMessage message =
          PipeConfigNodeAgent.task()
              .handleSinglePipeMetaChanges(
                  pipeTaskInfo.getPipeMetaByPipeStaticMeta(plan.getPipeStaticMeta()));
      if (message == null) {
        pipeMetaBeforeCreation.orElseGet(
            () -> {
              try {
                PipeConfigNodeAgent.runtime()
                    .increaseListenerReference(plan.getPipeStaticMeta().getSourceParameters());
                return null;
              } catch (final Exception e) {
                throw new PipeException(
                    ConfigNodeMessages.FAILED_TO_INCREASE_LISTENER_REFERENCE, e);
              }
            });
        PipeTemporaryMetaInCoordinatorMetrics.getInstance()
            .handleTemporaryMetaChanges(pipeTaskInfo.getPipeMetaList());
        return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      } else {
        return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
            .setMessage(message.getMessage());
      }
    } catch (final Exception e) {
      LOGGER.error(ConfigNodeMessages.FAILED_TO_CREATE_PIPE, e);
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
          .setMessage(ConfigNodeMessages.FAILED_TO_CREATE_PIPE_BECAUSE + e.getMessage());
    }
  }

  public TSStatus setPipeStatus(final SetPipeStatusPlanV2 plan) {
    try {
      pipeTaskInfo.setPipeStatus(plan);

      PipeConfigNodeAgent.task()
          .handleSinglePipeMetaChanges(
              pipeTaskInfo.getPipeMetaByPipeName(plan.getPipeName(), plan.isTableModel()));
      PipeTemporaryMetaInCoordinatorMetrics.getInstance()
          .handleTemporaryMetaChanges(pipeTaskInfo.getPipeMetaList());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (final Exception e) {
      LOGGER.error(ConfigNodeMessages.FAILED_TO_SET_PIPE_STATUS, e);
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
          .setMessage(ConfigNodeMessages.FAILED_TO_SET_PIPE_STATUS_BECAUSE + e.getMessage());
    }
  }

  public TSStatus setPipeStatusWithStoppedByRuntimeException(
      final SetPipeStatusWithStoppedByRuntimeExceptionPlanV2 plan) {
    try {
      pipeTaskInfo.setPipeStatusWithStoppedByRuntimeException(plan);

      PipeConfigNodeAgent.task()
          .handleSinglePipeMetaChanges(
              pipeTaskInfo.getPipeMetaByPipeName(plan.getPipeName(), plan.isTableModel()));
      PipeTemporaryMetaInCoordinatorMetrics.getInstance()
          .handleTemporaryMetaChanges(pipeTaskInfo.getPipeMetaList());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (final Exception e) {
      LOGGER.error(
          ConfigNodeMessages.FAILED_TO_SET_PIPE_STATUS_WITH_STOPPED_BY_RUNTIME_EXCEPTION, e);
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
          .setMessage(
              ConfigNodeMessages
                      .MESSAGE_FAILED_SET_PIPE_STATUS_STOPPED_RUNTIME_EXCEPTION_FLAG_BECAUSE_BFEA15AA
                  + e.getMessage());
    }
  }

  public TSStatus dropPipe(final DropPipePlanV2 plan) {
    try {
      final Optional<PipeMeta> pipeMetaBeforeDrop =
          Optional.ofNullable(
              pipeTaskInfo.getPipeMetaByPipeName(plan.getPipeName(), plan.isTableModel()));

      pipeTaskInfo.dropPipe(plan);

      final TPushPipeMetaRespExceptionMessage message =
          pipeMetaBeforeDrop
              .map(
                  meta -> {
                    meta.getRuntimeMeta().getStatus().set(PipeStatus.DROPPED);
                    return PipeConfigNodeAgent.task().handleSinglePipeMetaChanges(meta);
                  })
              .orElse(null);
      if (message == null) {
        pipeMetaBeforeDrop.ifPresent(
            meta -> {
              try {
                PipeConfigNodeAgent.runtime()
                    .decreaseListenerReference(meta.getStaticMeta().getSourceParameters());
              } catch (final Exception e) {
                throw new PipeException(
                    ConfigNodeMessages.FAILED_TO_DECREASE_LISTENER_REFERENCE, e);
              }
            });
        PipeTemporaryMetaInCoordinatorMetrics.getInstance()
            .handleTemporaryMetaChanges(pipeTaskInfo.getPipeMetaList());
        return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      } else {
        return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
            .setMessage(message.getMessage());
      }
    } catch (final Exception e) {
      LOGGER.error(ConfigNodeMessages.FAILED_TO_DROP_PIPE, e);
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
          .setMessage(ConfigNodeMessages.FAILED_TO_DROP_PIPE_BECAUSE + e.getMessage());
    }
  }

  public TSStatus alterPipe(final AlterPipePlanV2 plan) {
    try {
      final Optional<PipeMeta> pipeMetaBeforeAlter =
          Optional.ofNullable(
              pipeTaskInfo.getPipeMetaByPipeStaticMeta(plan.getCurrentPipeStaticMeta()));

      pipeTaskInfo.alterPipe(plan);

      final TPushPipeMetaRespExceptionMessage message =
          PipeConfigNodeAgent.task()
              .handleSinglePipeMetaChanges(
                  pipeTaskInfo.getPipeMetaByPipeStaticMeta(plan.getPipeStaticMeta()));
      if (message == null) {
        PipeConfigNodeAgent.runtime()
            .increaseListenerReference(plan.getPipeStaticMeta().getSourceParameters());
        pipeMetaBeforeAlter.ifPresent(
            meta -> {
              try {
                PipeConfigNodeAgent.runtime()
                    .decreaseListenerReference(meta.getStaticMeta().getSourceParameters());
              } catch (final Exception e) {
                throw new PipeException(
                    ConfigNodeMessages.FAILED_TO_DECREASE_LISTENER_REFERENCE, e);
              }
            });
        PipeTemporaryMetaInCoordinatorMetrics.getInstance()
            .handleTemporaryMetaChanges(pipeTaskInfo.getPipeMetaList());
        return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      } else {
        return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
            .setMessage(message.getMessage());
      }
    } catch (final Exception e) {
      LOGGER.error(ConfigNodeMessages.FAILED_TO_ALTER_PIPE, e);
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
          .setMessage(ConfigNodeMessages.FAILED_TO_ALTER_PIPE_BECAUSE + e.getMessage());
    }
  }

  /**
   * Note: This interface is only used for subscription and thus irrelevant to the {@link
   * PipeConfigNodeSubtask}. Hence, we can skip the operation of {@link PipeConfigNodeTaskAgent} and
   * {@link PipeConfigRegionListener} here.
   *
   * @param plans An {@link OperateMultiplePipesPlanV2} consisting of many subPlans
   * @return result {@link TSStatus}
   */
  public TSStatus operateMultiplePipes(final OperateMultiplePipesPlanV2 plans) {
    try {
      final TSStatus status = pipeTaskInfo.operateMultiplePipes(plans);
      PipeTemporaryMetaInCoordinatorMetrics.getInstance()
          .handleTemporaryMetaChanges(pipeTaskInfo.getPipeMetaList());
      return status;
    } catch (final Exception e) {
      LOGGER.error(ConfigNodeMessages.FAILED_TO_CREATE_MULTIPLE_PIPES, e);
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
          .setMessage(ConfigNodeMessages.FAILED_TO_CREATE_MULTIPLE_PIPES_BECAUSE + e.getMessage());
    }
  }

  public TSStatus handleLeaderChange(final PipeHandleLeaderChangePlan plan) {
    try {
      pipeTaskInfo.handleLeaderChange(plan);

      final List<PipeMeta> pipeMetaListFromCoordinator = new ArrayList<>();
      for (final PipeMeta pipeMeta : pipeTaskInfo.getPipeMetaList()) {
        pipeMetaListFromCoordinator.add(pipeMeta);
      }
      PipeConfigNodeAgent.task().handlePipeMetaChanges(pipeMetaListFromCoordinator);
      PipeTemporaryMetaInCoordinatorMetrics.getInstance()
          .handleTemporaryMetaChanges(pipeTaskInfo.getPipeMetaList());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (final Exception e) {
      LOGGER.error(ConfigNodeMessages.FAILED_TO_HANDLE_LEADER_CHANGE, e);
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
          .setMessage(ConfigNodeMessages.FAILED_TO_HANDLE_LEADER_CHANGE_BECAUSE + e.getMessage());
    }
  }

  public TSStatus handleMetaChanges(final PipeHandleMetaChangePlan plan) {
    try {
      pipeTaskInfo.handleMetaChanges(plan);

      final List<PipeMeta> pipeMetaListFromCoordinator = new ArrayList<>();
      for (final PipeMeta pipeMeta : plan.getPipeMetaList()) {
        pipeMetaListFromCoordinator.add(
            pipeTaskInfo.getPipeMetaByPipeStaticMeta(pipeMeta.getStaticMeta()));
      }
      PipeConfigNodeAgent.task().handlePipeMetaChanges(pipeMetaListFromCoordinator);
      PipeTemporaryMetaInCoordinatorMetrics.getInstance()
          .handleTemporaryMetaChanges(pipeTaskInfo.getPipeMetaList());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (final Exception e) {
      LOGGER.error(ConfigNodeMessages.FAILED_TO_HANDLE_META_CHANGES, e);
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
          .setMessage(ConfigNodeMessages.FAILED_TO_HANDLE_META_CHANGES_BECAUSE + e.getMessage());
    }
  }

  /////////////////////////////////  SnapshotProcessor  /////////////////////////////////

  @Override
  public boolean processTakeSnapshot(final File snapshotDir) throws IOException {
    return pipeTaskInfo.processTakeSnapshot(snapshotDir)
        && pipePluginInfo.processTakeSnapshot(snapshotDir);
  }

  @Override
  public void processLoadSnapshot(final File snapshotDir) throws IOException {
    Exception loadPipeTaskInfoException = null;
    Exception loadPipePluginInfoException = null;

    try {
      pipeTaskInfo.processLoadSnapshot(snapshotDir);
    } catch (final Exception ex) {
      LOGGER.error(ConfigNodeMessages.FAILED_TO_LOAD_PIPE_TASK_INFO_FROM_SNAPSHOT, ex);
      loadPipeTaskInfoException = ex;
    }

    try {
      pipePluginInfo.processLoadSnapshot(snapshotDir);
    } catch (final Exception ex) {
      LOGGER.error(ConfigNodeMessages.FAILED_TO_LOAD_PIPE_PLUGIN_INFO_FROM_SNAPSHOT, ex);
      loadPipePluginInfoException = ex;
    }

    if (loadPipeTaskInfoException != null || loadPipePluginInfoException != null) {
      throw new IOException(
          ConfigNodeMessages.FAILED_TO_LOAD_PIPE_INFO_FROM_SNAPSHOT
              + ConfigNodeMessages.EXCEPTION_LOADPIPETASKINFOEXCEPTION_2270468E
              + loadPipeTaskInfoException
              + ConfigNodeMessages.EXCEPTION_LOADPIPEPLUGININFOEXCEPTION_40362E11
              + loadPipePluginInfoException);
    }
  }

  /////////////////////////////////  equals & hashCode  /////////////////////////////////

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PipeInfo that = (PipeInfo) o;
    return Objects.equals(pipePluginInfo, that.pipePluginInfo)
        && Objects.equals(pipeTaskInfo, that.pipeTaskInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pipePluginInfo, pipeTaskInfo);
  }

  @Override
  public String toString() {
    return "PipeInfo{"
        + "pipePluginInfo="
        + pipePluginInfo
        + ", pipeTaskInfo="
        + pipeTaskInfo
        + '}';
  }
}
