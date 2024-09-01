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
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleLeaderChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleMetaChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.AlterPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.OperateMultiplePipesPlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusPlanV2;
import org.apache.iotdb.confignode.manager.pipe.agent.PipeConfigNodeAgent;
import org.apache.iotdb.confignode.manager.pipe.agent.runtime.PipeConfigRegionListener;
import org.apache.iotdb.confignode.manager.pipe.agent.task.PipeConfigNodeTaskAgent;
import org.apache.iotdb.confignode.manager.pipe.execution.PipeConfigNodeSubtask;
import org.apache.iotdb.confignode.manager.pipe.metric.PipeTemporaryMetaMetrics;
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

public class PipeInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeInfo.class);

  private final PipePluginInfo pipePluginInfo;
  private final PipeTaskInfo pipeTaskInfo;

  public PipeInfo() throws IOException {
    pipePluginInfo = new PipePluginInfo();
    pipeTaskInfo = new PipeTaskInfo();
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
          Optional.ofNullable(
              pipeTaskInfo.getPipeMetaByPipeName(plan.getPipeStaticMeta().getPipeName()));

      pipeTaskInfo.createPipe(plan);

      final TPushPipeMetaRespExceptionMessage message =
          PipeConfigNodeAgent.task()
              .handleSinglePipeMetaChanges(
                  pipeTaskInfo.getPipeMetaByPipeName(plan.getPipeStaticMeta().getPipeName()));
      if (message == null) {
        pipeMetaBeforeCreation.orElseGet(
            () -> {
              try {
                PipeConfigNodeAgent.runtime()
                    .increaseListenerReference(plan.getPipeStaticMeta().getExtractorParameters());
                return null;
              } catch (Exception e) {
                throw new PipeException("Failed to increase listener reference", e);
              }
            });
        PipeTemporaryMetaMetrics.getInstance()
            .handleTemporaryMetaChanges(pipeTaskInfo.getPipeMetaList());
        return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      } else {
        return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
            .setMessage(message.getMessage());
      }
    } catch (final Exception e) {
      LOGGER.error("Failed to create pipe", e);
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
          .setMessage("Failed to create pipe, because " + e.getMessage());
    }
  }

  public TSStatus setPipeStatus(final SetPipeStatusPlanV2 plan) {
    try {
      pipeTaskInfo.setPipeStatus(plan);

      PipeConfigNodeAgent.task()
          .handleSinglePipeMetaChanges(pipeTaskInfo.getPipeMetaByPipeName(plan.getPipeName()));
      PipeTemporaryMetaMetrics.getInstance()
          .handleTemporaryMetaChanges(pipeTaskInfo.getPipeMetaList());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (final Exception e) {
      LOGGER.error("Failed to set pipe status", e);
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
          .setMessage("Failed to set pipe status, because " + e.getMessage());
    }
  }

  public TSStatus dropPipe(final DropPipePlanV2 plan) {
    try {
      final Optional<PipeMeta> pipeMetaBeforeDrop =
          Optional.ofNullable(pipeTaskInfo.getPipeMetaByPipeName(plan.getPipeName()));

      pipeTaskInfo.dropPipe(plan);

      final TPushPipeMetaRespExceptionMessage message =
          PipeConfigNodeAgent.task().handleDropPipe(plan.getPipeName());
      if (message == null) {
        pipeMetaBeforeDrop.ifPresent(
            meta -> {
              try {
                PipeConfigNodeAgent.runtime()
                    .decreaseListenerReference(meta.getStaticMeta().getExtractorParameters());
              } catch (final Exception e) {
                throw new PipeException("Failed to decrease listener reference", e);
              }
            });
        PipeTemporaryMetaMetrics.getInstance()
            .handleTemporaryMetaChanges(pipeTaskInfo.getPipeMetaList());
        return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      } else {
        return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
            .setMessage(message.getMessage());
      }
    } catch (final Exception e) {
      LOGGER.error("Failed to drop pipe", e);
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
          .setMessage("Failed to drop pipe, because " + e.getMessage());
    }
  }

  public TSStatus alterPipe(final AlterPipePlanV2 plan) {
    try {
      pipeTaskInfo.alterPipe(plan);

      PipeConfigNodeAgent.task()
          .handleSinglePipeMetaChanges(
              pipeTaskInfo.getPipeMetaByPipeName(plan.getPipeStaticMeta().getPipeName()));
      PipeTemporaryMetaMetrics.getInstance()
          .handleTemporaryMetaChanges(pipeTaskInfo.getPipeMetaList());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (final Exception e) {
      LOGGER.error("Failed to alter pipe", e);
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
          .setMessage("Failed to alter pipe, because " + e.getMessage());
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
      PipeTemporaryMetaMetrics.getInstance()
          .handleTemporaryMetaChanges(pipeTaskInfo.getPipeMetaList());
      return status;
    } catch (final Exception e) {
      LOGGER.error("Failed to create multiple pipes", e);
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
          .setMessage("Failed to create multiple pipes, because " + e.getMessage());
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
      PipeTemporaryMetaMetrics.getInstance()
          .handleTemporaryMetaChanges(pipeTaskInfo.getPipeMetaList());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (final Exception e) {
      LOGGER.error("Failed to handle leader change", e);
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
          .setMessage("Failed to handle leader change, because " + e.getMessage());
    }
  }

  public TSStatus handleMetaChanges(final PipeHandleMetaChangePlan plan) {
    try {
      pipeTaskInfo.handleMetaChanges(plan);

      final List<PipeMeta> pipeMetaListFromCoordinator = new ArrayList<>();
      for (final PipeMeta pipeMeta : plan.getPipeMetaList()) {
        pipeMetaListFromCoordinator.add(
            pipeTaskInfo.getPipeMetaByPipeName(pipeMeta.getStaticMeta().getPipeName()));
      }
      PipeConfigNodeAgent.task().handlePipeMetaChanges(pipeMetaListFromCoordinator);
      PipeTemporaryMetaMetrics.getInstance()
          .handleTemporaryMetaChanges(pipeTaskInfo.getPipeMetaList());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (final Exception e) {
      LOGGER.error("Failed to handle meta changes", e);
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
          .setMessage("Failed to handle meta changes, because " + e.getMessage());
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

      for (final PipeMeta pipeMeta : pipeTaskInfo.getPipeMetaList()) {
        PipeConfigNodeAgent.runtime()
            .increaseListenerReference(pipeMeta.getStaticMeta().getExtractorParameters());
      }
    } catch (final Exception ex) {
      LOGGER.error("Failed to load pipe task info from snapshot", ex);
      loadPipeTaskInfoException = ex;
    }

    try {
      pipePluginInfo.processLoadSnapshot(snapshotDir);
    } catch (final Exception ex) {
      LOGGER.error("Failed to load pipe plugin info from snapshot", ex);
      loadPipePluginInfoException = ex;
    }

    if (loadPipeTaskInfoException != null || loadPipePluginInfoException != null) {
      throw new IOException(
          "Failed to load pipe info from snapshot, "
              + "loadPipeTaskInfoException="
              + loadPipeTaskInfoException
              + ", loadPipePluginInfoException="
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
