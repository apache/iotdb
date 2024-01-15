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

package org.apache.iotdb.db.pipe.agent.task;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MetaProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.agent.task.PipeTaskAgent;
import org.apache.iotdb.commons.pipe.task.PipeTask;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.extractor.dataregion.PipeDataRegionFilter;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.pipe.extractor.schemaregion.PipeSchemaNodeFilter;
import org.apache.iotdb.db.pipe.extractor.schemaregion.SchemaNodeListeningQueue;
import org.apache.iotdb.db.pipe.task.PipeDataNodeTask;
import org.apache.iotdb.db.pipe.task.builder.PipeDataNodeBuilder;
import org.apache.iotdb.db.pipe.task.builder.PipeDataNodeTaskBuilder;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.OperateSchemaQueueNode;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatResp;
import org.apache.iotdb.mpp.rpc.thrift.TPipeHeartbeatReq;
import org.apache.iotdb.mpp.rpc.thrift.TPipeHeartbeatResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaRespExceptionMessage;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PipeTaskDataNodeAgent extends PipeTaskAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTaskDataNodeAgent.class);

  protected static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  ////////////////////////// Pipe Task Management Entry //////////////////////////

  @Override
  protected boolean isShutdown() {
    return PipeAgent.runtime().isShutdown();
  }

  @Override
  protected Map<Integer, PipeTask> buildPipeTasks(PipeMeta pipeMetaFromConfigNode) {
    return new PipeDataNodeBuilder(pipeMetaFromConfigNode).build();
  }

  ///////////////////////// Manage by regionGroupId /////////////////////////

  @Override
  protected void createPipeTask(
      int consensusGroupId, PipeStaticMeta pipeStaticMeta, PipeTaskMeta pipeTaskMeta)
      throws IllegalPathException {
    if (pipeTaskMeta.getLeaderNodeId() == CONFIG.getDataNodeId()) {
      final PipeDataNodeTask pipeTask;
      final PipeParameters extractorParameters = pipeStaticMeta.getExtractorParameters();

      // Advance the extractor parameters parsing logic to avoid creating un-relevant pipeTasks
      if (Boolean.TRUE.equals(
              StorageEngine.getInstance()
                      .getAllDataRegionIds()
                      .contains(new DataRegionId(consensusGroupId))
                  && PipeDataRegionFilter.getDataRegionListenPair(extractorParameters).getLeft())
          || SchemaEngine.getInstance()
                  .getAllSchemaRegionIds()
                  .contains(new SchemaRegionId(consensusGroupId))
              && !PipeSchemaNodeFilter.getPipeListenSet(extractorParameters).isEmpty()) {
        pipeTask =
            new PipeDataNodeTaskBuilder(pipeStaticMeta, consensusGroupId, pipeTaskMeta).build();
      } else {
        throw new UnsupportedOperationException(
            "Unsupported consensus group id: " + consensusGroupId);
      }
      pipeTask.create();
      pipeTaskManager.addPipeTask(pipeStaticMeta, consensusGroupId, pipeTask);
    }

    pipeMetaKeeper
        .getPipeMeta(pipeStaticMeta.getPipeName())
        .getRuntimeMeta()
        .getConsensusGroupId2TaskMetaMap()
        .put(consensusGroupId, pipeTaskMeta);
  }

  @Override
  public List<TPushPipeMetaRespExceptionMessage> handlePipeMetaChangesInternal(
      List<PipeMeta> pipeMetaListFromCoordinator) {
    List<TPushPipeMetaRespExceptionMessage> exceptionMessages =
        super.handlePipeMetaChangesInternal(pipeMetaListFromCoordinator);
    // Clear useless events for listening queues
    try {
      // Remove used messages
      final Map<Integer, Long> earliestIndexMap = new HashMap<>();

      for (PipeMeta pipeMeta : pipeMetaListFromCoordinator) {
        Map<Integer, PipeTaskMeta> metaMap =
            pipeMeta.getRuntimeMeta().getConsensusGroupId2TaskMetaMap();

        if (!PipeSchemaNodeFilter.getPipeListenSet(
                pipeMeta.getStaticMeta().getExtractorParameters())
            .isEmpty()) {
          for (SchemaRegionId regionId : SchemaEngine.getInstance().getAllSchemaRegionIds()) {
            int id = regionId.getId();
            PipeTaskMeta schemaMeta = metaMap.get(id);

            if (schemaMeta != null) {
              ProgressIndex schemaIndex = schemaMeta.getProgressIndex();
              if (schemaIndex instanceof MetaProgressIndex
                  && ((MetaProgressIndex) schemaIndex).getIndex()
                      < earliestIndexMap.getOrDefault(id, Long.MAX_VALUE)) {
                earliestIndexMap.put(id, ((MetaProgressIndex) schemaIndex).getIndex());
              } else if (!earliestIndexMap.containsKey(id)) {
                // Do not clear "minimumProgressIndex"s related queue to avoid clearing
                // the queue when there are schema tasks just started and transferring
                earliestIndexMap.put(id, Long.MAX_VALUE);
              }
            }
          }
        }
      }

      earliestIndexMap.forEach(
          (schemaId, index) -> SchemaNodeListeningQueue.getInstance(schemaId).removeBefore(index));

      // Close queues of no sending pipe because there may be no pipeTasks originally for
      // listening queues
      SchemaEngine.getInstance()
          .getAllSchemaRegionIds()
          .forEach(
              schemaRegionId -> {
                int id = schemaRegionId.getId();
                if (!earliestIndexMap.containsKey(id)
                    && SchemaNodeListeningQueue.getInstance(id).isLeaderReady()
                    && SchemaNodeListeningQueue.getInstance(id).isOpened()) {
                  try {
                    SchemaRegionConsensusImpl.getInstance()
                        .write(
                            schemaRegionId, new OperateSchemaQueueNode(new PlanNodeId(""), false));
                  } catch (ConsensusException e) {
                    LOGGER.warn(
                        "Failed to close listening queue for schemaRegion {}, because {}",
                        schemaRegionId,
                        e.getMessage());
                  }
                }
              });
    } catch (Exception e) {
      final String errorMessage =
          String.format("Failed to handle pipe meta changes because %s", e.getMessage());
      LOGGER.warn("Failed to handle pipe meta changes because ", e);
      exceptionMessages.add(
          new TPushPipeMetaRespExceptionMessage(null, errorMessage, System.currentTimeMillis()));
    }
    return exceptionMessages;
  }

  public synchronized void stopAllPipesWithCriticalException() {
    super.stopAllPipesWithCriticalException(
        IoTDBDescriptor.getInstance().getConfig().getDataNodeId());
  }

  ///////////////////////// Heartbeat /////////////////////////

  public synchronized void collectPipeMetaList(TDataNodeHeartbeatResp resp) throws TException {
    // Try the lock instead of directly acquire it to prevent the block of the cluster heartbeat
    // 10s is the half of the HEARTBEAT_TIMEOUT_TIME defined in class BaseNodeCache in ConfigNode
    if (!tryReadLockWithTimeOut(10)) {
      return;
    }
    try {
      collectPipeMetaListInternal(resp);
    } finally {
      releaseReadLock();
    }
  }

  private void collectPipeMetaListInternal(TDataNodeHeartbeatResp resp) throws TException {
    // Do nothing if data node is removing or removed, or request does not need pipe meta list
    if (PipeAgent.runtime().isShutdown()) {
      return;
    }

    final List<ByteBuffer> pipeMetaBinaryList = new ArrayList<>();
    try {
      for (final PipeMeta pipeMeta : pipeMetaKeeper.getPipeMetaList()) {
        pipeMetaBinaryList.add(pipeMeta.serialize());
        LOGGER.info("Reporting pipe meta: {}", pipeMeta.coreReportMessage());
      }
    } catch (IOException e) {
      throw new TException(e);
    }
    resp.setPipeMetaList(pipeMetaBinaryList);
  }

  public synchronized void collectPipeMetaList(TPipeHeartbeatReq req, TPipeHeartbeatResp resp)
      throws TException {
    acquireReadLock();
    try {
      collectPipeMetaListInternal(req, resp);
    } finally {
      releaseReadLock();
    }
  }

  private void collectPipeMetaListInternal(TPipeHeartbeatReq req, TPipeHeartbeatResp resp)
      throws TException {
    // Do nothing if data node is removing or removed, or request does not need pipe meta list
    if (PipeAgent.runtime().isShutdown()) {
      return;
    }
    LOGGER.info("Received pipe heartbeat request {} from config node.", req.heartbeatId);

    final List<ByteBuffer> pipeMetaBinaryList = new ArrayList<>();
    try {
      for (final PipeMeta pipeMeta : pipeMetaKeeper.getPipeMetaList()) {
        pipeMetaBinaryList.add(pipeMeta.serialize());
        LOGGER.info("Reporting pipe meta: {}", pipeMeta.coreReportMessage());
      }
    } catch (IOException e) {
      throw new TException(e);
    }
    resp.setPipeMetaList(pipeMetaBinaryList);

    PipeInsertionDataNodeListener.getInstance().listenToHeartbeat(true);
  }
}
